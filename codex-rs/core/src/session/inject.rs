use super::input_queue::TurnInput;
use super::session::Session;
use super::turn_context::TurnContext;
use crate::codex_thread::TryStartTurnIfIdleError;
use crate::codex_thread::TryStartTurnIfIdleRejectionReason;
use crate::context::ContextualUserFragment;
use crate::context::MonitorNotification;
use crate::tasks::MailboxParentProvenance;
use crate::tasks::RegularTask;
use codex_protocol::config_types::ModeKind;
use codex_protocol::models::ResponseItem;
use futures::future::BoxFuture;
use std::sync::Arc;

impl Session {
    /// Returns the input if there is no active turn to inject into.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "active turn checks and turn state updates must remain atomic"
    )]
    pub async fn inject_if_running(
        &self,
        input: Vec<ResponseItem>,
    ) -> Result<(), Vec<ResponseItem>> {
        let mut active = self.active_turn.lock().await;
        match active.as_mut() {
            Some(active_turn) => {
                self.input_queue
                    .extend_pending_input_and_accept_mailbox_delivery_for_turn_state(
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
        Box::pin(async move {
            session.try_start_turn_if_idle_inner(input).await
        })
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

    /// Persist one monitor notification and request one coalesced idle wake.
    ///
    /// Active regular turns receive the item directly. At every other turn
    /// boundary the item is recorded first, so interruption, shutdown, and a
    /// competing user submission cannot erase it before the next model request.
    pub(crate) async fn deliver_monitor_notification(
        self: &Arc<Self>,
        notification: MonitorNotification,
        fallback_turn_context: &TurnContext,
    ) {
        let _delivery_guard = self.input_queue.lock_monitor_delivery().await;
        let item = ContextualUserFragment::into(notification);
        if self
            .input_queue
            .inject_monitor_if_running(&self.active_turn, item.clone())
            .await
        {
            return;
        }

        self.record_conversation_items(fallback_turn_context, std::slice::from_ref(&item))
            .await;
        self.input_queue.request_monitor_wake();
        if let Err(err) = self.flush_rollout().await {
            tracing::warn!("failed to flush monitor notification before wake: {err}");
        }
        self.input_queue.notify_monitor_wake();
    }

    /// Start at most one regular turn for monitor history waiting for a model
    /// request. The same lock is used by user/task starts, so no taskless
    /// `ActiveTurn` can steal a submission race.
    pub(crate) fn maybe_start_monitor_turn_if_idle(self: &Arc<Self>) -> BoxFuture<'static, ()> {
        let session = Arc::clone(self);
        Box::pin(async move {
            let _turn_start_guard = session.input_queue.lock_turn_start().await;
            if session
                .shutdown_started
                .load(std::sync::atomic::Ordering::Acquire)
                || !session.input_queue.monitor_wake_requested()
                || session
                    .input_queue
                    .has_trigger_turn_mailbox_items()
                    .await
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
            if session
                .input_queue
                .has_trigger_turn_mailbox_items()
                .await
                || session.active_turn.lock().await.is_some()
            {
                return;
            }
            session
                .start_task(
                    turn_context,
                    Vec::new(),
                    RegularTask::new(),
                    /*input_persisted*/ None,
                    MailboxParentProvenance::Ignore,
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
        let Err(items) = self.inject_if_running(items).await else {
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
        self.record_conversation_items(turn_context, &items).await;
    }
}
