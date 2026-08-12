use super::input_queue::TurnInput;
use super::session::Session;
use super::turn_context::TurnContext;
use crate::codex_thread::TryStartTurnIfIdleError;
use crate::codex_thread::TryStartTurnIfIdleRejectionReason;
use crate::context::ContextualUserFragment;
use crate::context::ExecCommandCompletionNotification;
use crate::context::MonitorNotification;
use crate::context::SubagentNotification;
use crate::tasks::MailboxParentProvenance;
use crate::tasks::RegularTask;
use codex_protocol::config_types::ModeKind;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::InterAgentCommunication;
use futures::future::BoxFuture;
use std::sync::Arc;

enum BackgroundTurnContext<'a> {
    Existing(&'a TurnContext),
    Default,
}

impl Session {
    pub(crate) async fn persist_background_input_batch(
        self: &Arc<Self>,
        turn_context: Arc<TurnContext>,
        items: Vec<TurnInput>,
        parent_turn: crate::session::input_queue::PendingParentTurn,
        delivery_guard: tokio::sync::OwnedMutexGuard<()>,
    ) {
        if items.is_empty() {
            return;
        }
        let session = Arc::clone(self);
        let persistence = tokio::spawn(async move {
            let _delivery_guard = delivery_guard;
            let publishes_agent_completion_activity = items.iter().any(|item| {
                matches!(item, TurnInput::AgentCompletion(_))
                    || matches!(item, TurnInput::ResponseItem(item) if SubagentNotification::is_response_item(item))
            });
            let requests_background_wake = items.iter().any(|item| {
                matches!(item, TurnInput::AgentCompletion(_))
                    || matches!(item, TurnInput::InterAgentCommunication(communication) if communication.trigger_turn)
                    || matches!(item, TurnInput::ResponseItem(item) if crate::context::is_background_notification(item))
            });
            for input in items {
                match input {
                    TurnInput::ResponseItem(item) => {
                        session
                            .record_conversation_items(
                                turn_context.as_ref(),
                                std::slice::from_ref(&item),
                            )
                            .await;
                    }
                    TurnInput::InterAgentCommunication(communication)
                    | TurnInput::AgentCompletion(communication) => {
                        session
                            .record_inter_agent_communication(turn_context.as_ref(), communication)
                            .await;
                    }
                    TurnInput::UserInput { .. } => {
                        unreachable!("foreground input cannot enter background persistence")
                    }
                }
            }
            if requests_background_wake {
                session
                    .input_queue
                    .request_background_wake(parent_turn, publishes_agent_completion_activity);
            }
            if let Err(err) = session.flush_rollout().await {
                tracing::warn!("failed to flush background notification before wake: {err}");
            }
            if requests_background_wake && publishes_agent_completion_activity {
                session.input_queue.request_agent_completion_activity();
            }
            if requests_background_wake {
                session.input_queue.notify_background_wake();
            }
        });
        if let Err(error) = persistence.await {
            tracing::error!("background input persistence task failed: {error}");
        }
    }

    /// Returns the input if there is no active turn to inject into.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "active turn checks and turn state updates must remain atomic"
    )]
    pub async fn inject_if_running(
        &self,
        input: Vec<ResponseItem>,
    ) -> Result<(), Vec<ResponseItem>> {
        let _delivery_guard = self
            .input_queue
            .lock_background_notification_delivery()
            .await;
        self.inject_if_running_under_delivery(input).await
    }

    async fn inject_if_running_under_delivery(
        &self,
        input: Vec<ResponseItem>,
    ) -> Result<(), Vec<ResponseItem>> {
        let mut active = self.active_turn.lock().await;
        match active.as_mut() {
            Some(active_turn) => {
                self.input_queue
                    .drain_mailbox_into_turn_state_before_input(
                        active_turn.turn_state.as_ref(),
                        input.into_iter().map(TurnInput::ResponseItem).collect(),
                    )
                    .await;
                Ok(())
            }
            None => Err(input),
        }
    }

    /// Starts a regular turn with the provided input only if automatic idle work
    /// is allowed for the current session state.
    ///
    /// This is the shared gate for extension-initiated idle work. It refuses to
    /// start a turn when user/client-triggered work is queued or any task is
    /// still active. Work without user input is also rejected in Plan mode.
    /// Active Review tasks are covered by the active-task check because Review
    /// turns are not steerable.
    pub(crate) fn try_start_turn_if_idle(
        self: &Arc<Self>,
        input: Vec<TurnInput>,
    ) -> BoxFuture<'static, Result<(), TryStartTurnIfIdleError>> {
        let session = Arc::clone(self);
        Box::pin(async move { session.try_start_turn_if_idle_inner(input).await })
    }

    async fn try_start_turn_if_idle_inner(
        self: Arc<Self>,
        input: Vec<TurnInput>,
    ) -> Result<(), TryStartTurnIfIdleError> {
        if input.is_empty() {
            return Ok(());
        }
        let _turn_start_guard = self.input_queue.lock_turn_start().await;
        if self
            .shutdown_started
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(TryStartTurnIfIdleError::new(
                TryStartTurnIfIdleRejectionReason::Busy,
                input,
            ));
        }
        let has_user_input = input.iter().any(
            |item| matches!(item, TurnInput::UserInput { content, .. } if !content.is_empty()),
        );
        if self.input_queue.has_trigger_turn_mailbox_items().await {
            return Err(TryStartTurnIfIdleError::new(
                TryStartTurnIfIdleRejectionReason::PendingTriggerTurn,
                input,
            ));
        }
        if !has_user_input && self.collaboration_mode().await.mode == ModeKind::Plan {
            return Err(TryStartTurnIfIdleError::new(
                TryStartTurnIfIdleRejectionReason::PlanMode,
                input,
            ));
        }

        if self.active_turn.lock().await.is_some() {
            return Err(TryStartTurnIfIdleError::new(
                TryStartTurnIfIdleRejectionReason::Busy,
                input,
            ));
        }

        let turn_context = self
            .new_default_turn_with_sub_id(uuid::Uuid::new_v4().to_string())
            .await;
        if !has_user_input && turn_context.mode == ModeKind::Plan {
            return Err(TryStartTurnIfIdleError::new(
                TryStartTurnIfIdleRejectionReason::PlanMode,
                input,
            ));
        }
        self.maybe_emit_model_warnings_for_turn(turn_context.as_ref())
            .await;
        if self.input_queue.has_trigger_turn_mailbox_items().await {
            return Err(TryStartTurnIfIdleError::new(
                TryStartTurnIfIdleRejectionReason::PendingTriggerTurn,
                input,
            ));
        }
        if self.active_turn.lock().await.is_some() {
            return Err(TryStartTurnIfIdleError::new(
                TryStartTurnIfIdleRejectionReason::Busy,
                input,
            ));
        }

        let task_input = if has_user_input {
            self.clear_connector_selection().await;
            for item in &input {
                if let TurnInput::UserInput { content, .. } = item {
                    turn_context.session_telemetry.user_prompt(content);
                }
            }
            input
        } else {
            input
        };
        self.start_task(
            turn_context,
            task_input,
            RegularTask::new(),
            MailboxParentProvenance::Ignore,
        )
        .await;
        Ok(())
    }

    pub(crate) async fn deliver_monitor_notification(
        self: &Arc<Self>,
        notification: MonitorNotification,
        fallback_turn_context: &TurnContext,
    ) {
        let item = ContextualUserFragment::into(notification);
        self.deliver_background_input(
            TurnInput::ResponseItem(item),
            BackgroundTurnContext::Existing(fallback_turn_context),
        )
        .await;
    }

    pub(crate) async fn deliver_exec_command_completion_notification(
        self: &Arc<Self>,
        notification: ExecCommandCompletionNotification,
        fallback_turn_context: &TurnContext,
    ) {
        let item = ContextualUserFragment::into(notification);
        self.deliver_background_input(
            TurnInput::ResponseItem(item),
            BackgroundTurnContext::Existing(fallback_turn_context),
        )
        .await;
    }

    pub(crate) fn deliver_inter_agent_completion(
        self: &Arc<Self>,
        communication: InterAgentCommunication,
    ) -> BoxFuture<'static, ()> {
        let session = Arc::clone(self);
        Box::pin(async move {
            session
                .deliver_background_input(
                    TurnInput::AgentCompletion(communication),
                    BackgroundTurnContext::Default,
                )
                .await;
        })
    }

    pub(crate) fn deliver_subagent_completion_item(
        self: &Arc<Self>,
        item: ResponseItem,
    ) -> BoxFuture<'static, ()> {
        let session = Arc::clone(self);
        Box::pin(async move {
            session
                .deliver_background_input(
                    TurnInput::ResponseItem(item),
                    BackgroundTurnContext::Default,
                )
                .await;
        })
    }

    /// Persist one background notification and request one coalesced idle wake.
    ///
    /// Active regular turns receive the item directly. At every other turn
    /// boundary the item is recorded first, so interruption, shutdown, and a
    /// competing user submission cannot erase it before the next model request.
    async fn deliver_background_input(
        self: &Arc<Self>,
        input: TurnInput,
        fallback_turn_context: BackgroundTurnContext<'_>,
    ) {
        let _delivery_guard = self
            .input_queue
            .lock_background_notification_delivery()
            .await;
        let publishes_agent_completion_activity = matches!(&input, TurnInput::AgentCompletion(_))
            || matches!(&input, TurnInput::ResponseItem(item) if SubagentNotification::is_response_item(item));
        let (ordered_inputs, parent_turn) = self.input_queue.ordered_background_inputs(input).await;
        let (ordered_inputs, parent_turn) = match self
            .input_queue
            .inject_background_inputs_if_running(
                &self.active_turn,
                ordered_inputs,
                parent_turn,
                publishes_agent_completion_activity
                    .then_some(crate::session::input_queue::InputQueueActivity::Mailbox),
            )
            .await
        {
            Ok(()) => return,
            Err((ordered_inputs, parent_turn)) => (ordered_inputs, parent_turn),
        };
        let owned_turn_context;
        let fallback_turn_context = match fallback_turn_context {
            BackgroundTurnContext::Existing(turn_context) => turn_context,
            BackgroundTurnContext::Default => {
                owned_turn_context = self
                    .new_default_turn_with_sub_id(uuid::Uuid::new_v4().to_string())
                    .await;
                owned_turn_context.as_ref()
            }
        };

        for input in ordered_inputs {
            match input {
                TurnInput::ResponseItem(item) => {
                    self.record_conversation_items(
                        fallback_turn_context,
                        std::slice::from_ref(&item),
                    )
                    .await;
                }
                TurnInput::InterAgentCommunication(communication)
                | TurnInput::AgentCompletion(communication) => {
                    self.record_inter_agent_communication(fallback_turn_context, communication)
                        .await;
                }
                TurnInput::UserInput { .. } => {
                    unreachable!("only mailbox and background input reach durable delivery")
                }
            }
        }
        self.input_queue
            .request_background_wake(parent_turn, publishes_agent_completion_activity);
        if let Err(err) = self.flush_rollout().await {
            tracing::warn!("failed to flush background notification before wake: {err}");
        }
        if publishes_agent_completion_activity {
            self.input_queue.request_agent_completion_activity();
        }
        self.input_queue.notify_background_wake();
    }

    /// Start at most one regular turn for background notifications waiting for
    /// a model request. The same lock is used by user/task starts, so no
    /// taskless `ActiveTurn` can steal a submission race.
    pub(crate) fn maybe_start_background_notification_turn_if_idle(
        self: &Arc<Self>,
    ) -> BoxFuture<'static, ()> {
        let session = Arc::clone(self);
        Box::pin(async move {
            let _turn_start_guard = session.input_queue.lock_turn_start().await;
            if session
                .shutdown_started
                .load(std::sync::atomic::Ordering::Acquire)
                || !session.input_queue.background_wake_requested()
                || session.input_queue.has_trigger_turn_mailbox_items().await
                || session.active_turn.lock().await.is_some()
            {
                return;
            }
            if session.collaboration_mode().await.mode == ModeKind::Plan {
                return;
            }

            let turn_context = session
                .new_default_turn_with_sub_id(uuid::Uuid::new_v4().to_string())
                .await;
            if turn_context.mode == ModeKind::Plan {
                return;
            }
            session
                .maybe_emit_model_warnings_for_turn(turn_context.as_ref())
                .await;
            if session.input_queue.has_trigger_turn_mailbox_items().await
                || session.active_turn.lock().await.is_some()
            {
                return;
            }
            session
                .start_task(
                    turn_context,
                    Vec::new(),
                    RegularTask::new(),
                    MailboxParentProvenance::Attribute,
                )
                .await;
        })
    }

    /// Injects items into active work, or records them without starting a turn.
    pub(crate) async fn inject_no_new_turn(
        &self,
        items: Vec<ResponseItem>,
        current_turn_context: Option<&TurnContext>,
    ) {
        let _delivery_guard = self
            .input_queue
            .lock_background_notification_delivery()
            .await;
        let Err(items) = self.inject_if_running_under_delivery(items).await else {
            return;
        };
        let default_turn_context;
        let turn_context = match current_turn_context {
            Some(turn_context) => turn_context,
            None => {
                default_turn_context = self.new_default_turn().await;
                default_turn_context.as_ref()
            }
        };
        let mailbox = self.input_queue.drain_mailbox_inputs().await;
        let requests_background_wake = mailbox.items.iter().any(|input| {
            matches!(input, TurnInput::InterAgentCommunication(communication) if communication.trigger_turn)
        });
        for input in mailbox.items {
            let TurnInput::InterAgentCommunication(communication) = input else {
                unreachable!("the mailbox contains only inter-agent communications")
            };
            self.record_inter_agent_communication(turn_context, communication)
                .await;
        }
        self.record_conversation_items(turn_context, &items).await;
        if requests_background_wake {
            self.input_queue
                .request_background_wake(mailbox.parent_turn, /*agent_completion*/ false);
            if let Err(err) = self.flush_rollout().await {
                tracing::warn!("failed to flush mailbox input before wake: {err}");
            }
            self.input_queue.notify_background_wake();
        }
    }
}
