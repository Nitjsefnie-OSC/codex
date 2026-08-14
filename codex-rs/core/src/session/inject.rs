use super::TurnInput;
use super::input_queue::InputQueue;
use super::input_queue::PendingInput;
use super::session::Session;
use super::turn_context::TurnContext;
use crate::context::ContextualUserFragment;
use crate::context::ExecCommandCompletionNotification;
use crate::context::MonitorNotification;
use crate::context::SubagentNotification;
use codex_features::Feature;
use codex_history::CodexHarnessMetadata;
use codex_history::ResponseItemEnvelope;
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
        provenance: crate::session::input_queue::PendingTurnProvenance,
        delivery_guard: tokio::sync::OwnedMutexGuard<()>,
    ) {
        let has_items = !items.is_empty();
        let persisted = self
            .persist_background_input_batch_without_wake(
                turn_context,
                items,
                provenance,
                &delivery_guard,
            )
            .await;
        if has_items && let Err(err) = self.flush_rollout().await {
            tracing::warn!("failed to flush background notification before wake: {err}");
        }
        persisted.publish(&self.input_queue);
    }

    pub(crate) async fn persist_background_input_batch_without_wake(
        self: &Arc<Self>,
        turn_context: Arc<TurnContext>,
        items: Vec<TurnInput>,
        provenance: crate::session::input_queue::PendingTurnProvenance,
        _delivery_guard: &tokio::sync::OwnedMutexGuard<()>,
    ) -> PersistedBackgroundInputBatch {
        let publishes_agent_completion_activity = items.iter().any(|item| {
            matches!(item, TurnInput::AgentCompletion(_))
                || matches!(item, TurnInput::ResponseItem(item) if SubagentNotification::is_response_item(&item.item))
        });
        let requests_background_wake = items.iter().any(|item| {
            matches!(item, TurnInput::AgentCompletion(_))
                || matches!(item, TurnInput::InterAgentCommunication(communication) if communication.trigger_turn)
                || matches!(item, TurnInput::ResponseItem(item) if crate::context::is_background_notification(&item.item))
        });
        if !items.is_empty() {
            for input in items {
                match input {
                    TurnInput::ResponseItem(item) => {
                        self.record_annotated_conversation_items(turn_context.as_ref(), vec![item])
                            .await;
                    }
                    TurnInput::InterAgentCommunication(communication)
                    | TurnInput::AgentCompletion(communication) => {
                        self.record_inter_agent_communication(turn_context.as_ref(), communication)
                            .await;
                    }
                    TurnInput::UserInput { .. } => {
                        unreachable!("foreground input cannot enter background persistence")
                    }
                }
            }
        }
        PersistedBackgroundInputBatch {
            provenance,
            requests_background_wake,
            publishes_agent_completion_activity,
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
                        active_turn
                            .task
                            .as_ref()
                            .map(|task| task.turn_context.turn_metadata_state.as_ref()),
                        input
                            .into_iter()
                            .map(ResponseItemEnvelope::new)
                            .map(TurnInput::ResponseItem)
                            .collect(),
                    )
                    .await;
                Ok(())
            }
            None => Err(input),
        }
    }

    /// Preserves trusted client provenance while items wait for an active turn.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "active turn checks and turn state updates must remain atomic"
    )]
    pub(crate) async fn inject_client_response_items(
        &self,
        items: Vec<ResponseItem>,
        turn_context: &TurnContext,
    ) {
        let items = items
            .into_iter()
            .map(|item| self.annotate_client_response_item(item))
            .collect::<Vec<_>>();
        let mut active = self.active_turn.lock().await;
        if let Some(active_turn) = active.as_mut() {
            self.input_queue
                .extend_pending_input_and_accept_mailbox_delivery_for_turn_state(
                    active_turn.turn_state.as_ref(),
                    items.into_iter().map(TurnInput::ResponseItem).collect(),
                )
                .await;
            return;
        }
        drop(active);
        self.record_annotated_conversation_items(turn_context, items)
            .await;
    }

    pub(crate) fn annotate_client_response_item(&self, item: ResponseItem) -> ResponseItemEnvelope {
        let metadata = (self.enabled(Feature::RetainClientDeveloperMessages)
            && matches!(&item, ResponseItem::Message { role, .. } if role == "developer"))
        .then_some(CodexHarnessMetadata {
            client_authored: true,
        });

        ResponseItemEnvelope { item, metadata }
    }

    pub(crate) async fn record_annotated_conversation_items(
        &self,
        turn_context: &TurnContext,
        items: Vec<ResponseItemEnvelope>,
    ) {
        if !self.enabled(Feature::RetainClientDeveloperMessages)
            || items.iter().all(|item| item.metadata.is_none())
        {
            let items = items
                .into_iter()
                .map(ResponseItemEnvelope::into_item)
                .collect::<Vec<_>>();
            self.record_conversation_items(turn_context, &items).await;
            return;
        }

        let mut annotated_items = Vec::with_capacity(items.len());
        let mut image_preparations = Vec::new();
        for envelope in items {
            let (prepared_items, prepared_images) = self.prepare_conversation_items_for_history(
                turn_context,
                std::slice::from_ref(&envelope.item),
            );
            image_preparations.extend(prepared_images);

            let mut metadata = envelope.metadata;
            annotated_items.extend(prepared_items.into_owned().into_iter().map(|item| {
                ResponseItemEnvelope {
                    item,
                    metadata: metadata.take(),
                }
            }));
        }
        self.record_prepared_conversation_items(turn_context, annotated_items, image_preparations)
            .await;
    }

    /// Persist one monitor notification and request one coalesced idle wake.
    ///
    /// Active regular turns receive the item directly, while compaction queues
    /// it for the regular successor. At every other turn boundary the item is
    /// recorded first, so interruption, shutdown, and a competing user
    /// submission cannot erase it before the next model request.
    pub(crate) async fn deliver_monitor_notification(
        self: &Arc<Self>,
        notification: MonitorNotification,
        fallback_turn_context: &TurnContext,
    ) {
        let item = ContextualUserFragment::into(notification);
        self.deliver_background_input(
            PendingInput::Turn(TurnInput::ResponseItem(item.into())),
            BackgroundTurnContext::Existing(fallback_turn_context),
        )
        .await;
    }

    pub(crate) async fn deliver_monitor_notification_draft(
        self: &Arc<Self>,
        draft: crate::unified_exec::MonitorNotificationDraft,
        fallback_turn_context: &TurnContext,
    ) {
        self.deliver_background_input(
            PendingInput::MonitorNotification(draft),
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
            PendingInput::Turn(TurnInput::ResponseItem(item.into())),
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
                    PendingInput::Turn(TurnInput::AgentCompletion(communication)),
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
                    PendingInput::Turn(TurnInput::ResponseItem(item.into())),
                    BackgroundTurnContext::Default,
                )
                .await;
        })
    }

    /// Persist one background notification and request one coalesced idle wake.
    ///
    /// Active regular turns receive the item directly, while compaction queues
    /// it for the regular successor. At every other turn boundary the item is
    /// recorded first, so interruption, shutdown, and a competing user
    /// submission cannot erase it before the next model request.
    async fn deliver_background_input(
        self: &Arc<Self>,
        input: PendingInput,
        fallback_turn_context: BackgroundTurnContext<'_>,
    ) {
        let _delivery_guard = self
            .input_queue
            .lock_background_notification_delivery()
            .await;
        let publishes_agent_completion_activity = matches!(
            &input,
            PendingInput::Turn(TurnInput::AgentCompletion(_))
        ) || matches!(&input, PendingInput::Turn(TurnInput::ResponseItem(item)) if SubagentNotification::is_response_item(&item.item));
        let (ordered_inputs, provenance) = self.input_queue.ordered_background_inputs(input).await;
        let (ordered_inputs, provenance) = match self
            .input_queue
            .inject_background_inputs_if_running(
                &self.active_turn,
                ordered_inputs,
                provenance,
                publishes_agent_completion_activity
                    .then_some(crate::session::input_queue::InputQueueActivity::Mailbox),
            )
            .await
        {
            Ok(()) => return,
            Err((ordered_inputs, provenance)) => (ordered_inputs, provenance),
        };
        let ordered_inputs = ordered_inputs
            .into_iter()
            .filter_map(PendingInput::materialize)
            .collect::<Vec<_>>();
        let publishes_agent_completion_activity = ordered_inputs.iter().any(|input| {
            matches!(input, TurnInput::AgentCompletion(_))
                || matches!(input, TurnInput::ResponseItem(item) if SubagentNotification::is_response_item(&item.item))
        });
        if ordered_inputs.is_empty() {
            return;
        }
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
                    self.record_annotated_conversation_items(fallback_turn_context, vec![item])
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
            .request_background_wake(provenance, publishes_agent_completion_activity);
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
            if session
                .shutdown_started
                .load(std::sync::atomic::Ordering::Acquire)
                || !session.input_queue.background_wake_requested()
            {
                return;
            }
            if let Err(error) = super::turn_input::handle_background_wake(
                &session,
                uuid::Uuid::now_v7().to_string(),
            )
            .await
            {
                tracing::warn!("failed to start background notification wake turn: {error}");
            }
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
                .request_background_wake(mailbox.provenance, /*agent_completion*/ false);
            if let Err(err) = self.flush_rollout().await {
                tracing::warn!("failed to flush mailbox input before wake: {err}");
            }
            self.input_queue.notify_background_wake();
        }
    }
}

pub(crate) struct PersistedBackgroundInputBatch {
    provenance: crate::session::input_queue::PendingTurnProvenance,
    requests_background_wake: bool,
    publishes_agent_completion_activity: bool,
}

impl PersistedBackgroundInputBatch {
    pub(crate) fn publish(self, input_queue: &InputQueue) {
        let Self {
            provenance,
            requests_background_wake,
            publishes_agent_completion_activity,
        } = self;
        if !requests_background_wake {
            return;
        }
        input_queue.request_background_wake(provenance, publishes_agent_completion_activity);
        if publishes_agent_completion_activity {
            input_queue.request_agent_completion_activity();
        }
        input_queue.notify_background_wake();
    }
}
