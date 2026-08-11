use super::TurnInput as PendingTurnInput;
use super::session::Session;
use super::turn_context::TurnContext;
use crate::context::ContextualUserFragment;
use crate::context::ExecCommandCompletionNotification;
use crate::context::MonitorNotification;
use codex_features::Feature;
use codex_history::CodexHarnessMetadata;
use codex_history::ResponseItemEnvelope;
use codex_protocol::models::ResponseItem;
use codex_protocol::turn_input::TurnInputMode;
use codex_protocol::turn_input::TurnInputRequest;
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
                        input
                            .into_iter()
                            .map(ResponseItemEnvelope::new)
                            .map(PendingTurnInput::ResponseItem)
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
                    items
                        .into_iter()
                        .map(PendingTurnInput::ResponseItem)
                        .collect(),
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
    /// Active regular turns receive the item directly. At every other turn
    /// boundary the item is recorded first, so interruption, shutdown, and a
    /// competing user submission cannot erase it before the next model request.
    pub(crate) async fn deliver_monitor_notification(
        self: &Arc<Self>,
        notification: MonitorNotification,
        fallback_turn_context: &TurnContext,
    ) {
        let item = ContextualUserFragment::into(notification);
        self.deliver_background_notification(item, fallback_turn_context)
            .await;
    }

    pub(crate) async fn deliver_exec_command_completion_notification(
        self: &Arc<Self>,
        notification: ExecCommandCompletionNotification,
        fallback_turn_context: &TurnContext,
    ) {
        let item = ContextualUserFragment::into(notification);
        self.deliver_background_notification(item, fallback_turn_context)
            .await;
    }

    /// Persist one background notification and request one coalesced idle wake.
    ///
    /// Active regular turns receive the item directly. At every other turn
    /// boundary the item is recorded first, so interruption, shutdown, and a
    /// competing user submission cannot erase it before the next model request.
    async fn deliver_background_notification(
        self: &Arc<Self>,
        item: ResponseItem,
        fallback_turn_context: &TurnContext,
    ) {
        let _delivery_guard = self
            .input_queue
            .lock_background_notification_delivery()
            .await;
        if self
            .input_queue
            .inject_background_notification_if_running(&self.active_turn, item.clone())
            .await
        {
            return;
        }

        self.record_conversation_items(fallback_turn_context, std::slice::from_ref(&item))
            .await;
        self.input_queue.request_background_wake();
        if let Err(err) = self.flush_rollout().await {
            tracing::warn!("failed to flush background notification before wake: {err}");
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
            if let Err(error) = super::turn_input::handle(
                &session,
                TurnInputRequest::user_input(Vec::new()),
                TurnInputMode::StartIfIdle,
                uuid::Uuid::now_v7().to_string(),
            )
            .await
            {
                tracing::warn!("failed to start monitor wake turn: {error}");
            }
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
