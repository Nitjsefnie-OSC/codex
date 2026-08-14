mod compact;
mod lifecycle;
mod regular;
mod review;
mod user_shell;

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use codex_diagnostics::Gauge;
use codex_extension_api::ThreadIdleCause;
use futures::future::BoxFuture;
use tokio::select;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::Instrument;
use tracing::Span;
use tracing::field;
use tracing::info_span;
use tracing::trace;
use tracing::trace_span;
use tracing::warn;

use crate::codex_thread::BackgroundTerminalInfo;
use crate::config::Config;
use crate::context::ContextualUserFragment;
use crate::session::PendingInputBatch;
use crate::session::PendingTurnProvenance;
use crate::session::TurnInput;
use crate::session::session::Session;
use crate::session::turn::run_hooks_and_record_inputs;
use crate::session::turn_context::TurnContext;
use crate::state::ActiveTurn;
use crate::state::FinishingTurn;
use crate::state::RunningTask;
use crate::state::TaskKind;
use crate::state::TurnState;
use crate::unified_exec::MonitorAcknowledgement;
use crate::unified_exec::MonitorInfo;
use crate::unified_exec::MonitorOutput;
use crate::unified_exec::MonitorWaitOutcome;
use codex_analytics::TurnProfileFact;
use codex_analytics::TurnTokenUsageFact;
use codex_otel::SessionTelemetry;
use codex_otel::TURN_E2E_DURATION_METRIC;
use codex_otel::TURN_MEMORY_METRIC;
use codex_otel::TURN_NETWORK_PROXY_METRIC;
use codex_otel::TURN_TOKEN_USAGE_METRIC;
use codex_otel::TURN_TOOL_CALL_METRIC;
use codex_otel::TURN_UNIFIED_EXEC_RUNNING_PROCESSES_METRIC;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::TokenUsage;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::protocol::TurnAbortedEvent;
use codex_protocol::protocol::TurnCompleteEvent;
use codex_protocol::protocol::WarningEvent;
use codex_thread_store::PersistContext;

use codex_features::Feature;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::models::ContentItem;
pub(crate) use compact::CompactTask;
pub(crate) use regular::RegularTask;
pub(crate) use review::ReviewTask;
pub(crate) use user_shell::UserShellCommandMode;
pub(crate) use user_shell::UserShellCommandTask;
pub(crate) use user_shell::execute_user_shell_command;

const GRACEFULL_INTERRUPTION_TIMEOUT_MS: u64 = 100;
const TASK_COMPACT_METRIC: &str = "codex.task.compact";
static ACTIVE_TURNS: Gauge = Gauge::new("core.turns.active");

pub(crate) type SessionTaskResult = CodexResult<Option<String>>;

pub(crate) enum MailboxParentProvenance {
    Ignore,
    Attribute,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InterruptedTurnHistoryMarker {
    Disabled,
    ContextualUser,
    Developer,
}

impl InterruptedTurnHistoryMarker {
    pub(crate) fn from_config_and_version(
        config: &Config,
        multi_agent_version: MultiAgentVersion,
    ) -> Self {
        if !config.agent_interrupt_message_enabled {
            return Self::Disabled;
        }
        if multi_agent_version == MultiAgentVersion::V2 {
            Self::Developer
        } else {
            Self::ContextualUser
        }
    }
}

/// Shared model-visible marker used by both the real interrupt path and
/// interrupted fork snapshots.
pub(crate) fn interrupted_turn_history_marker(
    marker: InterruptedTurnHistoryMarker,
) -> Option<ResponseItem> {
    match marker {
        InterruptedTurnHistoryMarker::Disabled => None,
        InterruptedTurnHistoryMarker::ContextualUser => Some(ContextualUserFragment::into(
            crate::context::TurnAborted::new(crate::context::TurnAborted::INTERRUPTED_GUIDANCE),
        )),
        InterruptedTurnHistoryMarker::Developer => {
            let marker = crate::context::TurnAborted::new(
                crate::context::TurnAborted::INTERRUPTED_DEVELOPER_GUIDANCE,
            );
            Some(ResponseItem::Message {
                id: None,
                role: "developer".to_string(),
                content: vec![ContentItem::InputText {
                    text: marker.render(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            })
        }
    }
}

fn emit_turn_network_proxy_metric(
    session_telemetry: &SessionTelemetry,
    network_proxy_active: bool,
    tmp_mem: (&str, &str),
) {
    let active = if network_proxy_active {
        "true"
    } else {
        "false"
    };
    session_telemetry.counter(
        TURN_NETWORK_PROXY_METRIC,
        /*inc*/ 1,
        &[("active", active), tmp_mem],
    );
}

fn emit_turn_memory_metric(
    session_telemetry: &SessionTelemetry,
    feature_enabled: bool,
    config_enabled: bool,
    has_citations: bool,
) {
    let read_allowed = feature_enabled && config_enabled;
    session_telemetry.counter(
        TURN_MEMORY_METRIC,
        /*inc*/ 1,
        &[
            ("read_allowed", bool_tag(read_allowed)),
            ("feature_enabled", bool_tag(feature_enabled)),
            ("config_use_memories", bool_tag(config_enabled)),
            ("has_citations", bool_tag(has_citations)),
        ],
    );
}

pub(crate) fn emit_compact_metric(
    session_telemetry: &SessionTelemetry,
    compact_type: &'static str,
    manual: bool,
) {
    session_telemetry.counter(
        TASK_COMPACT_METRIC,
        /*inc*/ 1,
        &[("type", compact_type), ("manual", bool_tag(manual))],
    );
}

fn bool_tag(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

/// Async task that drives a [`Session`] turn.
///
/// Implementations encapsulate a specific Codex workflow (regular chat,
/// reviews, ghost snapshots, etc.). Each task instance is owned by a
/// [`Session`] and executed on a background Tokio task. The trait is
/// intentionally small: implementers identify themselves via
/// [`SessionTask::kind`], perform their work in [`SessionTask::run`], and may
/// release resources in [`SessionTask::abort`].
pub(crate) trait SessionTask: Send + Sync + 'static {
    /// Describes the type of work the task performs so the session can
    /// surface it in telemetry and UI.
    fn kind(&self) -> TaskKind;

    /// Returns the tracing name for a spawned task span.
    fn span_name(&self) -> &'static str;

    /// Whether background notifications can be consumed as model-turn input by
    /// this task. Standalone command tasks share the regular task kind but do
    /// not sample a model, so they must leave durable wakes intact.
    fn accepts_background_notifications(&self) -> bool {
        true
    }

    /// Executes the task until completion or cancellation.
    ///
    /// Implementations typically stream protocol events using `session` and
    /// `ctx`, returning an optional final agent message when finished. The
    /// provided `cancellation_token` is cancelled when the session requests an
    /// abort; implementers should watch for it and terminate quickly once it
    /// fires. Returning [`Some`] yields a final message that
    /// [`Session::on_task_finished`] will emit to the client. Returning
    /// [`CodexErr::TurnAborted`] completes the task through the aborted-turn
    /// lifecycle instead.
    fn run(
        self: Arc<Self>,
        session: Arc<Session>,
        ctx: Arc<TurnContext>,
        input: Vec<TurnInput>,
        cancellation_token: CancellationToken,
    ) -> impl std::future::Future<Output = SessionTaskResult> + Send;

    /// Gives the task a chance to perform cleanup after an abort.
    ///
    /// The default implementation is a no-op; override this if additional
    /// teardown or notifications are required once
    /// [`Session::abort_all_tasks`] cancels the task.
    fn abort(
        &self,
        session: Arc<Session>,
        ctx: Arc<TurnContext>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            let _ = (session, ctx);
        }
    }
}

pub(crate) trait AnySessionTask: Send + Sync + 'static {
    fn kind(&self) -> TaskKind;

    fn span_name(&self) -> &'static str;

    fn accepts_background_notifications(&self) -> bool;

    fn run(
        self: Arc<Self>,
        session: Arc<Session>,
        ctx: Arc<TurnContext>,
        input: Vec<TurnInput>,
        cancellation_token: CancellationToken,
    ) -> BoxFuture<'static, SessionTaskResult>;

    fn abort<'a>(&'a self, session: Arc<Session>, ctx: Arc<TurnContext>) -> BoxFuture<'a, ()>;
}

impl<T> AnySessionTask for T
where
    T: SessionTask,
{
    fn kind(&self) -> TaskKind {
        SessionTask::kind(self)
    }

    fn span_name(&self) -> &'static str {
        SessionTask::span_name(self)
    }

    fn accepts_background_notifications(&self) -> bool {
        SessionTask::accepts_background_notifications(self)
    }

    fn run(
        self: Arc<Self>,
        session: Arc<Session>,
        ctx: Arc<TurnContext>,
        input: Vec<TurnInput>,
        cancellation_token: CancellationToken,
    ) -> BoxFuture<'static, SessionTaskResult> {
        Box::pin(SessionTask::run(
            self,
            session,
            ctx,
            input,
            cancellation_token,
        ))
    }

    fn abort<'a>(&'a self, session: Arc<Session>, ctx: Arc<TurnContext>) -> BoxFuture<'a, ()> {
        Box::pin(SessionTask::abort(self, session, ctx))
    }
}

impl Session {
    pub async fn spawn_task<T: SessionTask>(
        self: &Arc<Self>,
        turn_context: Arc<TurnContext>,
        input: Vec<TurnInput>,
        task: T,
    ) {
        let _turn_start_guard = self.input_queue.lock_turn_start().await;
        self.abort_all_tasks(TurnAbortReason::Replaced).await;
        self.clear_connector_selection().await;
        self.start_task(turn_context, input, task, MailboxParentProvenance::Ignore)
            .await;
    }

    pub(crate) fn start_task<T: SessionTask>(
        self: &Arc<Self>,
        turn_context: Arc<TurnContext>,
        input: Vec<TurnInput>,
        task: T,
        mailbox_parent_provenance: MailboxParentProvenance,
    ) -> BoxFuture<'static, ()> {
        let session = Arc::clone(self);
        Box::pin(async move {
            session
                .start_task_inner(
                    turn_context,
                    input,
                    task,
                    mailbox_parent_provenance,
                    /*preserve_pending_input*/ false,
                )
                .await;
        })
    }

    async fn start_task_inner<T: SessionTask>(
        self: Arc<Self>,
        turn_context: Arc<TurnContext>,
        input: Vec<TurnInput>,
        task: T,
        mailbox_parent_provenance: MailboxParentProvenance,
        preserve_pending_input: bool,
    ) {
        let task: Arc<dyn AnySessionTask> = Arc::new(task);
        let task_kind = task.kind();
        let span_name = task.span_name();
        let background_delivery_guard = loop {
            let guard = self
                .input_queue
                .lock_background_notification_delivery()
                .await;
            let abort_complete = self
                .active_turn
                .lock()
                .await
                .as_ref()
                .filter(|active_turn| active_turn.aborting)
                .map(|active_turn| Arc::clone(&active_turn.abort_complete));
            let Some(abort_complete) = abort_complete else {
                break guard;
            };
            let notified = abort_complete.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            drop(guard);
            notified.await;
        };
        let started_at = Instant::now();
        let turn_started_at_unix_ms = turn_context
            .turn_timing_state
            .mark_turn_started(started_at)
            .await;
        turn_context
            .turn_metadata_state
            .set_turn_started_at_unix_ms(turn_started_at_unix_ms);
        turn_context
            .turn_metadata_state
            .set_attribute_background_parent_turn(matches!(
                mailbox_parent_provenance,
                MailboxParentProvenance::Attribute
            ));
        let token_usage_at_turn_start = self.total_token_usage().await.unwrap_or_default();

        let cancellation_token = CancellationToken::new();
        let done = Arc::new(Notify::new());
        let (startup_tx, startup_rx) = oneshot::channel();

        self.services
            .guardian_rejection_circuit_breaker
            .lock()
            .await
            .clear_turn(&turn_context.sub_id);

        let (mut pending_items, provenance) = if preserve_pending_input {
            (Vec::new(), PendingTurnProvenance::default())
        } else {
            match self
                .input_queue
                .take_next_pending_input_batch(&self.active_turn)
                .await
            {
                PendingInputBatch::Foreground(items) => (items, PendingTurnProvenance::default()),
                PendingInputBatch::Background { items, provenance } => (items, provenance),
            }
        };
        let mut input = input;
        if task_kind == TaskKind::Regular && !input.is_empty() && !pending_items.is_empty() {
            pending_items.append(&mut input);
            input = std::mem::take(&mut pending_items);
        } else if task_kind != TaskKind::Regular && !pending_items.is_empty() {
            let requests_background_wake = pending_items.iter().any(|input| {
                matches!(input, TurnInput::AgentCompletion(_))
                    || matches!(input, TurnInput::InterAgentCommunication(communication) if communication.trigger_turn)
                    || matches!(input, TurnInput::ResponseItem(item) if crate::context::is_background_notification(&item.item))
            });
            let publishes_agent_completion_activity = pending_items.iter().any(|input| {
                matches!(input, TurnInput::AgentCompletion(_))
                    || matches!(input, TurnInput::ResponseItem(item) if crate::context::SubagentNotification::is_response_item(&item.item))
            });
            run_hooks_and_record_inputs(
                &self,
                &turn_context,
                &pending_items,
                PersistContext::Standard,
            )
            .await;
            if let Err(err) = self.flush_rollout().await {
                warn!("failed to flush queued input before starting non-regular task: {err}");
            }
            if requests_background_wake {
                self.input_queue.request_background_wake(
                    provenance.clone(),
                    publishes_agent_completion_activity,
                );
                if publishes_agent_completion_activity {
                    self.input_queue.request_agent_completion_activity();
                }
                self.input_queue.notify_background_wake();
            }
            pending_items.clear();
        }
        if matches!(
            mailbox_parent_provenance,
            MailboxParentProvenance::Attribute
        ) {
            provenance.apply_to_attributed_turn(turn_context.turn_metadata_state.as_ref());
        } else {
            provenance.mark_root_ambiguity_for_existing(turn_context.turn_metadata_state.as_ref());
        }
        let turn_state = {
            let mut active = self.active_turn.lock().await;
            let turn = active.get_or_insert_with(ActiveTurn::default);
            debug_assert!(turn.task.is_none());
            Arc::clone(&turn.turn_state)
        };
        turn_state.lock().await.token_usage_at_turn_start = token_usage_at_turn_start.clone();
        self.input_queue
            .extend_pending_input_batch_for_turn_state(
                turn_state.as_ref(),
                pending_items,
                provenance,
            )
            .await;
        let mut active = self.active_turn.lock().await;
        let turn = active.get_or_insert_with(ActiveTurn::default);
        debug_assert!(turn.task.is_none());
        let agent_execution_guard = self.services.agent_control.execution_guard(
            turn_context.multi_agent_version,
            &turn_context.session_source,
        );
        let done_clone = Arc::clone(&done);
        let session = Arc::clone(&self);
        let ctx = Arc::clone(&turn_context);
        let task_for_run = Arc::clone(&task);
        let task_input = input;
        let task_cancellation_token = cancellation_token.child_token();
        let token_usage_for_lifecycle = token_usage_at_turn_start.clone();
        // Task-owned turn spans keep a core-owned span open for the
        // full task lifecycle after the submission dispatch span ends.
        let reasoning_effort = turn_context.effective_reasoning_effort_for_tracing();
        let task_span = info_span!(
            "turn",
            otel.name = span_name,
            thread.id = %self.thread_id,
            turn.id = %turn_context.sub_id,
            model = %turn_context.model_info.slug,
            codex.turn.reasoning_effort = %reasoning_effort,
            codex.turn.token_usage.input_tokens = field::Empty,
            codex.turn.token_usage.cached_input_tokens = field::Empty,
            codex.turn.token_usage.cache_write_input_tokens = field::Empty,
            codex.turn.token_usage.non_cached_input_tokens = field::Empty,
            codex.turn.token_usage.output_tokens = field::Empty,
            codex.turn.token_usage.reasoning_output_tokens = field::Empty,
            codex.turn.token_usage.total_tokens = field::Empty,
        );
        let handle = tokio::spawn(
            async move {
                // The active task must be published before the model can
                // clone history. Monitor delivery uses that publication as
                // its startup barrier, avoiding a history/wake race.
                if startup_rx.await.is_err() {
                    return;
                }
                session
                    .emit_turn_start_lifecycle(ctx.as_ref(), &token_usage_for_lifecycle)
                    .await;
                let ctx_for_finish = Arc::clone(&ctx);
                let task_result = task_for_run
                    .run(
                        Arc::clone(&session),
                        ctx,
                        task_input,
                        task_cancellation_token.child_token(),
                    )
                    .instrument(trace_span!("session_task.run"))
                    .await;
                let sess = Arc::clone(&session);
                if let Err(err) = sess.flush_rollout().await {
                    warn!("failed to flush rollout before completing turn: {err}");
                    sess.send_event(
                        ctx_for_finish.as_ref(),
                        EventMsg::Warning(WarningEvent {
                            message: format!(
                                "Failed to save the conversation transcript; Codex will continue retrying. Error: {err}"
                            ),
                        }),
                    )
                    .await;
                }
                if !task_cancellation_token.is_cancelled() {
                    // Finish uniformly from the spawn site so all tasks share the same lifecycle.
                    sess.on_task_finished(Arc::clone(&ctx_for_finish), task_result)
                        .await;
                }
                done_clone.notify_waiters();
            }
            .instrument(task_span),
        );
        let timer = turn_context
            .session_telemetry
            .start_timer(TURN_E2E_DURATION_METRIC, &[])
            .ok();
        let running_task = RunningTask {
            done,
            handle: AbortOnDropHandle::new(handle),
            kind: task_kind,
            task,
            cancellation_token,
            turn_context: Arc::clone(&turn_context),
            _agent_execution_guard: agent_execution_guard,
            _diagnostics_guard: ACTIVE_TURNS.track(),
            _timer: timer,
        };
        turn.task = Some(running_task);
        let _ = startup_tx.send(());
        drop(active);
        drop(background_delivery_guard);
    }

    fn start_regular_task_preserving_pending(
        self: &Arc<Self>,
        turn_context: Arc<TurnContext>,
    ) -> BoxFuture<'static, ()> {
        let session = Arc::clone(self);
        Box::pin(async move {
            session
                .start_task_inner(
                    turn_context,
                    Vec::new(),
                    RegularTask::new(),
                    MailboxParentProvenance::Attribute,
                    /*preserve_pending_input*/ true,
                )
                .await;
        })
    }

    /// Returns whether an extension has marked this thread as durably asleep.
    pub(crate) fn has_outstanding_durable_sleep(&self) -> bool {
        self.services
            .thread_extension_data
            .get::<codex_extension_items::sleep::SleepItem>()
            .is_some()
    }

    /// Starts a regular turn when the session is idle and pending work is waiting.
    ///
    /// Pending work includes mailbox mail marked with `trigger_turn`, or any mailbox mail while
    /// an outstanding durable sleep is attached to the thread.
    ///
    /// This helper generates a fresh sub-id for the synthetic turn before delegating to the
    /// explicit-sub-id variant.
    pub(crate) fn maybe_start_turn_for_pending_work(self: &Arc<Self>) -> BoxFuture<'static, ()> {
        let session = Arc::clone(self);
        Box::pin(async move {
            session
                .maybe_start_turn_for_pending_work_with_sub_id(uuid::Uuid::new_v4().to_string())
                .await;
        })
    }

    /// Starts a regular turn with the provided sub-id when pending work should wake an idle
    /// session.
    ///
    /// The turn is created only when the session is idle and mailbox mail either requests a turn
    /// or can wake an outstanding durable sleep.
    pub(crate) async fn maybe_start_turn_for_pending_work_with_sub_id(
        self: &Arc<Self>,
        sub_id: String,
    ) {
        if self
            .shutdown_started
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }
        if !self.input_queue.has_pending_mailbox_items().await
            || (!self.input_queue.has_trigger_turn_mailbox_items().await
                && !self.has_outstanding_durable_sleep())
        {
            return;
        }

        let _turn_start_guard = self.input_queue.lock_turn_start().await;
        if self
            .shutdown_started
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }
        if self.active_turn.lock().await.is_some() {
            return;
        }

        let turn_context = self.new_default_turn_with_sub_id(sub_id).await;
        self.maybe_emit_model_warnings_for_turn(turn_context.as_ref())
            .await;
        if self.active_turn.lock().await.is_some() {
            return;
        }
        self.start_task(
            turn_context,
            Vec::new(),
            RegularTask::new(),
            MailboxParentProvenance::Attribute,
        )
        .await;
    }

    async fn abort_active_task_in_transaction(
        self: &Arc<Self>,
        reason: TurnAbortReason,
        turn_id: Option<&str>,
    ) -> Option<Arc<TurnContext>> {
        let delivery_guard = self
            .input_queue
            .lock_background_notification_delivery()
            .await;
        let detached = {
            let mut active = self.active_turn.lock().await;
            let active_turn = active.as_mut()?;
            let task = active_turn.task.as_ref()?;
            if turn_id.is_some_and(|turn_id| task.turn_context.sub_id != turn_id) {
                return None;
            }
            if matches!(
                reason,
                TurnAbortReason::Interrupted | TurnAbortReason::BudgetLimited
            ) {
                self.mark_interrupted();
            }
            let task = active_turn.task.take().expect("active task was checked");
            active_turn.aborting = true;
            (
                task,
                Arc::clone(&active_turn.turn_state),
                Arc::clone(&active_turn.abort_complete),
            )
        };
        drop(delivery_guard);

        let (task, turn_state, abort_complete) = detached;
        let turn_context = self.cancel_task_for_abort(task, reason.clone()).await;

        let delivery_guard = self
            .input_queue
            .lock_background_notification_delivery()
            .await;
        let (pending_input, pending_parent_turn) = self
            .input_queue
            .take_pending_background_inputs_for_turn_state(turn_state.as_ref())
            .await;
        let persisted = self
            .persist_background_input_batch_without_wake(
                Arc::clone(&turn_context),
                pending_input,
                pending_parent_turn,
                &delivery_guard,
            )
            .await;
        if let Err(err) = self.flush_rollout().await {
            warn!("failed to flush abort cleanup before terminal event: {err}");
        }
        self.emit_aborted_turn(&turn_context, reason).await;

        let active_turn = {
            let mut active = self.active_turn.lock().await;
            if active.as_ref().is_some_and(|active_turn| {
                active_turn.aborting && Arc::ptr_eq(&active_turn.turn_state, &turn_state)
            }) {
                active.take()
            } else {
                None
            }
        };
        if let Some(active_turn) = active_turn {
            self.input_queue.clear_pending(&active_turn).await;
        }
        drop(delivery_guard);
        persisted.publish(&self.input_queue);
        abort_complete.notify_waiters();
        Some(turn_context)
    }

    pub async fn abort_all_tasks(self: &Arc<Self>, reason: TurnAbortReason) {
        if let Some(turn_context) = self
            .abort_active_task_in_transaction(reason.clone(), None)
            .await
        {
            self.services
                .unified_exec_manager
                .discard_unrecorded_initial_exec_command_outputs()
                .await;
            self.emit_turn_abort_lifecycle(reason.clone(), turn_context.extension_data.as_ref())
                .await;
            if reason == TurnAbortReason::Interrupted {
                self.maybe_start_turn_for_pending_work().await;
                self.input_queue.notify_background_wake();
            }
            return;
        }

        let delivery_guard = self
            .input_queue
            .lock_background_notification_delivery()
            .await;
        let mut publish_background_wake = false;
        if let Some(active_turn) = self.take_active_turn(&reason).await {
            if let Some(finishing) = active_turn.finishing.as_ref() {
                let mut terminal_persisted = finishing.terminal_persisted.clone();
                let terminal_is_persisted = *terminal_persisted.borrow();
                if !terminal_is_persisted && terminal_persisted.changed().await.is_err() {
                    warn!("finishing turn ended before terminal persistence completed");
                }
                let (pending_input, pending_parent_turn) = self
                    .input_queue
                    .take_pending_input_and_mailbox_for_turn_state(active_turn.turn_state.as_ref())
                    .await;
                let has_background = pending_input.iter().any(|input| {
                    matches!(input, TurnInput::AgentCompletion(_))
                        || matches!(input, TurnInput::InterAgentCommunication(communication) if communication.trigger_turn)
                        || matches!(input, TurnInput::ResponseItem(item) if crate::context::is_background_notification(&item.item))
                });
                run_hooks_and_record_inputs(
                    self,
                    &finishing.turn_context,
                    &pending_input,
                    PersistContext::Standard,
                )
                .await;
                let publish_agent_completion_activity = pending_input.iter().any(|input| {
                    matches!(input, TurnInput::AgentCompletion(_))
                        || matches!(input, TurnInput::ResponseItem(item) if crate::context::SubagentNotification::is_response_item(&item.item))
                });
                if has_background {
                    self.input_queue.request_background_wake(
                        pending_parent_turn,
                        publish_agent_completion_activity,
                    );
                }
                if let Err(err) = self.flush_rollout().await {
                    warn!("failed to flush taskless turn input during abort: {err}");
                }
                publish_background_wake = has_background;
                if publish_agent_completion_activity {
                    self.input_queue.request_agent_completion_activity();
                }
                drop(delivery_guard);
            } else {
                self.input_queue
                    .materialize_monitor_drafts_for_turn_state(active_turn.turn_state.as_ref())
                    .await;
                drop(delivery_guard);
            }
        } else {
            drop(delivery_guard);
        }

        if reason == TurnAbortReason::Interrupted && publish_background_wake {
            self.input_queue.notify_background_wake();
        }
    }

    pub(crate) async fn abort_turn_if_active(
        self: &Arc<Self>,
        turn_id: &str,
        reason: TurnAbortReason,
    ) -> bool {
        let Some(turn_context) = self
            .abort_active_task_in_transaction(reason.clone(), Some(turn_id))
            .await
        else {
            return false;
        };

        self.services
            .unified_exec_manager
            .discard_unrecorded_initial_exec_command_outputs()
            .await;
        self.emit_turn_abort_lifecycle(reason.clone(), turn_context.extension_data.as_ref())
            .await;

        if reason == TurnAbortReason::Interrupted {
            self.maybe_start_turn_for_pending_work().await;
            self.input_queue.notify_background_wake();
        }

        true
    }

    pub async fn on_task_finished(
        self: &Arc<Self>,
        turn_context: Arc<TurnContext>,
        task_result: SessionTaskResult,
    ) {
        let (last_agent_message, abort_reason) = match task_result {
            Ok(last_agent_message) => (last_agent_message, None),
            Err(err) if matches!(err.details(), CodexErrorDetails::TurnAborted) => {
                (None, Some(TurnAbortReason::Interrupted))
            }
            Err(err) => {
                warn!(%err, "session task returned an unexpected error");
                self.emit_turn_error_lifecycle(
                    turn_context.as_ref(),
                    err.to_codex_protocol_error(),
                )
                .await;
                self.track_turn_codex_error(turn_context.as_ref(), &err);
                self.send_event(
                    turn_context.as_ref(),
                    EventMsg::Error(err.to_error_event(/*message_prefix*/ None)),
                )
                .await;
                (None, None)
            }
        };
        turn_context
            .turn_metadata_state
            .cancel_git_enrichment_task();

        // Reserve background delivery before detaching the active task. A
        // notification already queued on this turn must be recorded before a
        // later watcher observes the taskless turn and falls back to durable
        // delivery.
        let background_delivery_guard = self
            .input_queue
            .lock_background_notification_delivery()
            .await;
        let (terminal_persisted_tx, terminal_persisted_rx) = tokio::sync::watch::channel(false);
        let turn_state = {
            let mut active = self.active_turn.lock().await;
            active.as_mut().and_then(|active_turn| {
                let task = active_turn.task.take()?;
                task.handle.detach();
                active_turn.finishing = Some(FinishingTurn {
                    turn_context: Arc::clone(&turn_context),
                    terminal_persisted: terminal_persisted_rx.clone(),
                });
                Some(Arc::clone(&active_turn.turn_state))
            })
        };
        let Some(turn_state) = turn_state else {
            return;
        };
        self.input_queue
            .materialize_monitor_drafts_for_turn_state(turn_state.as_ref())
            .await;
        drop(background_delivery_guard);
        self.services
            .unified_exec_manager
            .discard_unrecorded_initial_exec_command_outputs()
            .await;
        let (turn_had_memory_citation, turn_tool_calls, token_usage_at_turn_start) = {
            let ts = turn_state.lock().await;
            (
                ts.has_memory_citation,
                ts.tool_calls,
                ts.token_usage_at_turn_start.clone(),
            )
        };
        // Emit token usage metrics.
        {
            // TODO(jif): drop this
            let tmp_mem = (
                "tmp_mem_enabled",
                if self.enabled(Feature::MemoryTool) {
                    "true"
                } else {
                    "false"
                },
            );
            let network_proxy = self.services.network_proxy.load_full();
            let network_proxy_active = match network_proxy.as_ref() {
                Some(started_network_proxy) => {
                    match started_network_proxy.proxy().current_cfg().await {
                        Ok(config) => config.enabled,
                        Err(err) => {
                            warn!(
                                "failed to read managed network proxy state for turn metrics: {err:#}"
                            );
                            false
                        }
                    }
                }
                None => false,
            };
            emit_turn_network_proxy_metric(
                &self.services.session_telemetry,
                network_proxy_active,
                tmp_mem,
            );
            self.services.session_telemetry.histogram(
                TURN_TOOL_CALL_METRIC,
                i64::try_from(turn_tool_calls).unwrap_or(i64::MAX),
                &[tmp_mem],
            );
            let total_token_usage = self.total_token_usage().await.unwrap_or_default();
            let turn_token_usage = TokenUsage {
                input_tokens: (total_token_usage.input_tokens
                    - token_usage_at_turn_start.input_tokens)
                    .max(0),
                cached_input_tokens: (total_token_usage.cached_input_tokens
                    - token_usage_at_turn_start.cached_input_tokens)
                    .max(0),
                cache_write_input_tokens: (total_token_usage.cache_write_input_tokens
                    - token_usage_at_turn_start.cache_write_input_tokens)
                    .max(0),
                output_tokens: (total_token_usage.output_tokens
                    - token_usage_at_turn_start.output_tokens)
                    .max(0),
                reasoning_output_tokens: (total_token_usage.reasoning_output_tokens
                    - token_usage_at_turn_start.reasoning_output_tokens)
                    .max(0),
                total_tokens: (total_token_usage.total_tokens
                    - token_usage_at_turn_start.total_tokens)
                    .max(0),
                codex_rollout_budget_units: None,
            };
            let current_span = Span::current();
            current_span.record(
                "codex.turn.token_usage.input_tokens",
                turn_token_usage.input_tokens,
            );
            current_span.record(
                "codex.turn.token_usage.cached_input_tokens",
                turn_token_usage.cached_input(),
            );
            current_span.record(
                "codex.turn.token_usage.cache_write_input_tokens",
                turn_token_usage.cache_write_input_tokens,
            );
            current_span.record(
                "codex.turn.token_usage.non_cached_input_tokens",
                turn_token_usage.non_cached_input(),
            );
            current_span.record(
                "codex.turn.token_usage.output_tokens",
                turn_token_usage.output_tokens,
            );
            current_span.record(
                "codex.turn.token_usage.reasoning_output_tokens",
                turn_token_usage.reasoning_output_tokens,
            );
            current_span.record(
                "codex.turn.token_usage.total_tokens",
                turn_token_usage.total_tokens,
            );
            self.services
                .analytics_events_client
                .track_turn_token_usage(TurnTokenUsageFact {
                    turn_id: turn_context.sub_id.clone(),
                    thread_id: self.thread_id.to_string(),
                    token_usage: turn_token_usage.clone(),
                });
            self.services.session_telemetry.histogram(
                TURN_TOKEN_USAGE_METRIC,
                turn_token_usage.total_tokens,
                &[("token_type", "total"), tmp_mem],
            );
            self.services.session_telemetry.histogram(
                TURN_TOKEN_USAGE_METRIC,
                turn_token_usage.input_tokens,
                &[("token_type", "input"), tmp_mem],
            );
            self.services.session_telemetry.histogram(
                TURN_TOKEN_USAGE_METRIC,
                turn_token_usage.cached_input(),
                &[("token_type", "cached_input"), tmp_mem],
            );
            self.services.session_telemetry.histogram(
                TURN_TOKEN_USAGE_METRIC,
                turn_token_usage.cache_write_input_tokens,
                &[("token_type", "cache_write_input"), tmp_mem],
            );
            self.services.session_telemetry.histogram(
                TURN_TOKEN_USAGE_METRIC,
                turn_token_usage.output_tokens,
                &[("token_type", "output"), tmp_mem],
            );
            self.services.session_telemetry.histogram(
                TURN_TOKEN_USAGE_METRIC,
                turn_token_usage.reasoning_output_tokens,
                &[("token_type", "reasoning_output"), tmp_mem],
            );
        }
        emit_turn_memory_metric(
            &self.services.session_telemetry,
            turn_context.config.features.enabled(Feature::MemoryTool),
            turn_context.config.memories.use_memories,
            turn_had_memory_citation,
        );
        self.services.session_telemetry.counter(
            TURN_UNIFIED_EXEC_RUNNING_PROCESSES_METRIC,
            i64::try_from(self.list_background_terminals().await.len()).unwrap_or(i64::MAX),
            &[],
        );
        let started_at = turn_context.turn_timing_state.started_at_unix_secs().await;
        let (completed_at, duration_ms, profile) = turn_context
            .turn_timing_state
            .complete_profile_and_duration_ms()
            .await;
        self.services
            .analytics_events_client
            .track_turn_profile(TurnProfileFact {
                turn_id: turn_context.sub_id.clone(),
                profile,
            });
        let idle_cause = if matches!(
            abort_reason.as_ref(),
            Some(TurnAbortReason::Interrupted | TurnAbortReason::BudgetLimited)
        ) {
            ThreadIdleCause::Interrupted
        } else if abort_reason.is_none() && turn_context.terminal_error.lock().await.is_some() {
            ThreadIdleCause::Failed
        } else {
            ThreadIdleCause::Completed
        };
        let event = if let Some(reason) = abort_reason {
            self.emit_turn_abort_lifecycle(reason.clone(), turn_context.extension_data.as_ref())
                .await;
            EventMsg::TurnAborted(TurnAbortedEvent {
                turn_id: Some(turn_context.sub_id.clone()),
                reason,
                started_at,
                completed_at,
                duration_ms,
            })
        } else {
            let time_to_first_token_ms = turn_context
                .turn_timing_state
                .time_to_first_token_ms()
                .await;
            let error = turn_context.terminal_error.lock().await.clone();
            self.emit_turn_stop_lifecycle(turn_context.extension_data.as_ref())
                .await;
            EventMsg::TurnComplete(TurnCompleteEvent {
                turn_id: turn_context.sub_id.clone(),
                last_agent_message,
                error,
                started_at,
                completed_at,
                duration_ms,
                time_to_first_token_ms,
            })
        };
        self.send_event(turn_context.as_ref(), event).await;
        if let Err(err) = self.flush_rollout().await {
            warn!("failed to flush rollout after emitting terminal turn event: {err}");
        }
        terminal_persisted_tx.send_replace(true);
        self.services
            .guardian_rejection_circuit_breaker
            .lock()
            .await
            .clear_turn(&turn_context.sub_id);

        let _turn_start_guard = self.input_queue.lock_turn_start().await;
        let background_delivery_guard = self
            .input_queue
            .lock_background_notification_delivery()
            .await;
        let (has_pending_input, has_user_input, has_foreground_input) = self
            .input_queue
            .pending_input_summary_for_turn_state(turn_state.as_ref())
            .await;
        let shutdown_started = self
            .shutdown_started
            .load(std::sync::atomic::Ordering::Acquire);
        let background_successor_allowed =
            self.collaboration_mode().await.mode != codex_protocol::config_types::ModeKind::Plan;
        let cleared_active_turn = if has_pending_input
            && !shutdown_started
            && (has_user_input || background_successor_allowed)
        {
            let next_turn_context = self
                .new_default_turn_with_sub_id(uuid::Uuid::new_v4().to_string())
                .await;
            if !has_user_input
                && next_turn_context.mode == codex_protocol::config_types::ModeKind::Plan
            {
                let (pending_input, pending_parent_turn) = self
                    .input_queue
                    .take_pending_input_batch_for_turn_state(turn_state.as_ref())
                    .await;
                let cleared = self.clear_taskless_active_turn(&turn_state).await;
                self.persist_background_input_batch(
                    Arc::clone(&turn_context),
                    pending_input,
                    pending_parent_turn,
                    background_delivery_guard,
                )
                .await;
                cleared
            } else {
                drop(background_delivery_guard);
                self.start_regular_task_preserving_pending(next_turn_context)
                    .await;
                false
            }
        } else if has_pending_input && has_foreground_input {
            let (pending_input, _) = self
                .input_queue
                .take_pending_input_batch_for_turn_state(turn_state.as_ref())
                .await;
            let cleared = self.clear_taskless_active_turn(&turn_state).await;
            run_hooks_and_record_inputs(
                self,
                &turn_context,
                &pending_input,
                PersistContext::Standard,
            )
            .await;
            if let Err(err) = self.flush_rollout().await {
                warn!("failed to flush pending input during shutdown: {err}");
            }
            drop(background_delivery_guard);
            cleared
        } else if has_pending_input {
            let (pending_input, pending_parent_turn) = self
                .input_queue
                .take_pending_input_batch_for_turn_state(turn_state.as_ref())
                .await;
            let cleared = self.clear_taskless_active_turn(&turn_state).await;
            self.persist_background_input_batch(
                Arc::clone(&turn_context),
                pending_input,
                pending_parent_turn,
                background_delivery_guard,
            )
            .await;
            cleared
        } else {
            let cleared = self.clear_taskless_active_turn(&turn_state).await;
            drop(background_delivery_guard);
            cleared
        };
        if cleared_active_turn {
            self.emit_thread_idle_lifecycle_if_idle(idle_cause).await;
        }
        drop(_turn_start_guard);
        if cleared_active_turn {
            self.maybe_start_turn_for_pending_work().await;
            self.input_queue.notify_background_wake();
        }
    }

    async fn take_active_turn(&self, reason: &TurnAbortReason) -> Option<ActiveTurn> {
        let mut active = self.active_turn.lock().await;
        if active
            .as_ref()
            .is_some_and(|active_turn| active_turn.aborting)
        {
            return None;
        }
        if matches!(
            reason,
            TurnAbortReason::Interrupted | TurnAbortReason::BudgetLimited
        ) && active
            .as_ref()
            .is_some_and(|active_turn| active_turn.task.is_some())
        {
            self.mark_interrupted();
        }
        active.take()
    }

    async fn clear_taskless_active_turn(&self, turn_state: &Arc<Mutex<TurnState>>) -> bool {
        let mut active = self.active_turn.lock().await;
        if let Some(active_turn) = active.as_ref()
            && active_turn.task.is_none()
            && Arc::ptr_eq(&active_turn.turn_state, turn_state)
        {
            *active = None;
            true
        } else {
            false
        }
    }

    pub(crate) async fn close_unified_exec_processes(&self) {
        self.services
            .unified_exec_manager
            .terminate_all_processes()
            .await;
    }

    pub(crate) async fn list_background_terminals(&self) -> Vec<BackgroundTerminalInfo> {
        self.services.unified_exec_manager.list_processes().await
    }

    pub(crate) async fn terminate_background_terminal(&self, process_id: i32) -> bool {
        self.services
            .unified_exec_manager
            .terminate_process(process_id)
            .await
    }

    pub(crate) async fn list_monitors(&self) -> Vec<MonitorInfo> {
        self.services.unified_exec_manager.list_monitors().await
    }

    pub(crate) async fn read_monitor_output(
        &self,
        process_id: i32,
        acknowledgement: MonitorAcknowledgement,
    ) -> Option<MonitorOutput> {
        self.services
            .unified_exec_manager
            .read_monitor_output(process_id, acknowledgement)
            .await
    }

    pub(crate) async fn stop_monitor(&self, process_id: i32) -> Option<bool> {
        self.services
            .unified_exec_manager
            .stop_monitor(process_id)
            .await
    }

    pub(crate) async fn wait_for_monitor(
        &self,
        process_id: i32,
        timeout: std::time::Duration,
    ) -> Option<MonitorWaitOutcome> {
        self.services
            .unified_exec_manager
            .wait_for_monitor(process_id, timeout)
            .await
    }

    async fn emit_aborted_turn(
        self: &Arc<Self>,
        turn_context: &Arc<TurnContext>,
        reason: TurnAbortReason,
    ) {
        if reason == TurnAbortReason::Interrupted
            && let Some(marker) = interrupted_turn_history_marker(
                InterruptedTurnHistoryMarker::from_config_and_version(
                    turn_context.config.as_ref(),
                    turn_context.multi_agent_version,
                ),
            )
        {
            self.record_conversation_items(turn_context.as_ref(), std::slice::from_ref(&marker))
                .await;
            // Ensure the marker is durably visible before emitting TurnAborted: some clients
            // synchronously re-read the rollout on receipt of the abort event.
            if let Err(err) = self.flush_rollout().await {
                warn!("failed to flush interrupted-turn marker before emitting TurnAborted: {err}");
            }
        }

        let started_at = turn_context.turn_timing_state.started_at_unix_secs().await;
        let (completed_at, duration_ms, profile) = turn_context
            .turn_timing_state
            .complete_profile_and_duration_ms()
            .await;
        self.services
            .analytics_events_client
            .track_turn_profile(TurnProfileFact {
                turn_id: turn_context.sub_id.clone(),
                profile,
            });
        let event = EventMsg::TurnAborted(TurnAbortedEvent {
            turn_id: Some(turn_context.sub_id.clone()),
            reason,
            started_at,
            completed_at,
            duration_ms,
        });
        self.send_event(turn_context.as_ref(), event).await;
        self.services
            .guardian_rejection_circuit_breaker
            .lock()
            .await
            .clear_turn(&turn_context.sub_id);
        // Regular items were flushed before this terminal event was appended; buffering
        // thread writers may not flush it without another explicit barrier.
        if let Err(err) = self.flush_rollout().await {
            warn!("failed to flush rollout after emitting terminal turn event: {err}");
        }
    }

    async fn cancel_task_for_abort(
        self: &Arc<Self>,
        task: RunningTask,
        reason: TurnAbortReason,
    ) -> Arc<TurnContext> {
        let sub_id = task.turn_context.sub_id.clone();
        let turn_context = Arc::clone(&task.turn_context);

        trace!(task_kind = ?task.kind, sub_id, "aborting running task");
        task.cancellation_token.cancel();
        if reason == TurnAbortReason::Interrupted
            && task
                .turn_context
                .config
                .features
                .enabled(Feature::CodeModeInterrupt)
        {
            self.services
                .code_mode_service
                .interrupt_active_cells()
                .await;
        }
        task.turn_context
            .turn_metadata_state
            .cancel_git_enrichment_task();
        let session_task = task.task;

        select! {
            _ = task.done.notified() => {
            },
            _ = tokio::time::sleep(Duration::from_millis(GRACEFULL_INTERRUPTION_TIMEOUT_MS)) => {
                warn!("task {sub_id} didn't complete gracefully after {}ms", GRACEFULL_INTERRUPTION_TIMEOUT_MS);
            }
        }

        task.handle.abort();
        if let Err(err) = task.handle.await
            && !err.is_cancelled()
        {
            warn!(%err, sub_id, "session task failed while aborting");
        }

        session_task
            .abort(Arc::clone(self), Arc::clone(&task.turn_context))
            .await;
        turn_context
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
