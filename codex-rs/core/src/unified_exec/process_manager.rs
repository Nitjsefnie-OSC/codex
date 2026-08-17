use rand::Rng;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::watch;
use tokio::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::codex_thread::BackgroundTerminalInfo;
use crate::exec_env::CODEX_PERMISSION_PROFILE_ENV_VAR;
use crate::exec_env::CODEX_THREAD_ID_ENV_VAR;
use crate::exec_env::create_env;
use crate::exec_env::inject_apply_patch_env;
use crate::exec_env::inject_permission_profile_env;
use crate::exec_env::inject_session_id_env;
use crate::exec_policy::ExecApprovalRequest;
use crate::plugins::metrics::finish_and_track_measurements;
use crate::sandboxing::ExecOptions;
use crate::sandboxing::ExecRequest;
use crate::sandboxing::ExecServerEnvConfig;
use crate::tools::context::ExecCommandToolOutput;
use crate::tools::events::ToolEmitter;
use crate::tools::events::ToolEventCtx;
use crate::tools::events::ToolEventStage;
use crate::tools::network_approval::DeferredNetworkApproval;
use crate::tools::network_approval::finish_deferred_network_approval;
use crate::tools::orchestrator::ToolOrchestrator;
use crate::tools::runtimes::is_managed_proxy_env_var;
use crate::tools::runtimes::unified_exec::UnifiedExecAttempt;
use crate::tools::runtimes::unified_exec::UnifiedExecRequest as UnifiedExecToolRequest;
use crate::tools::runtimes::unified_exec::UnifiedExecRuntime;
use crate::tools::sandboxing::SandboxAttempt;
use crate::tools::sandboxing::ToolCtx;
use crate::tools::sandboxing::ToolError;
use crate::unified_exec::ExecCommandRequest;
use crate::unified_exec::InitialExecCommandOutputDestination;
use crate::unified_exec::InitialExecCommandState;
use crate::unified_exec::InitialExecOutputPersistenceDecision;
use crate::unified_exec::MAX_UNIFIED_EXEC_PROCESSES;
use crate::unified_exec::MAX_YIELD_TIME_MS;
use crate::unified_exec::MIN_EMPTY_YIELD_TIME_MS;
use crate::unified_exec::MIN_YIELD_TIME_MS;
use crate::unified_exec::MonitorAcknowledgement;
use crate::unified_exec::MonitorAttachment;
use crate::unified_exec::MonitorInfo;
use crate::unified_exec::MonitorOutput;
use crate::unified_exec::MonitorWaitOutcome;
use crate::unified_exec::PendingInitialExecCommandOutput;
use crate::unified_exec::ProcessEntry;
use crate::unified_exec::ProcessStore;
use crate::unified_exec::UnifiedExecContext;
use crate::unified_exec::UnifiedExecError;
use crate::unified_exec::UnifiedExecProcessManager;
use crate::unified_exec::WriteStdinInteractionEvent;
use crate::unified_exec::WriteStdinRequest;
use crate::unified_exec::async_watcher::YieldedExecCompletionContext;
use crate::unified_exec::async_watcher::emit_exec_end_for_unified_exec;
use crate::unified_exec::async_watcher::emit_failed_exec_end_for_unified_exec;
use crate::unified_exec::async_watcher::spawn_exit_watcher;
use crate::unified_exec::async_watcher::start_streaming_output;
use crate::unified_exec::clamp_yield_time;
use crate::unified_exec::generate_chunk_id;
use crate::unified_exec::head_tail_buffer::HeadTailBuffer;
use crate::unified_exec::monitor_watcher::spawn_monitor_watcher;
use crate::unified_exec::monitors::MonitorHandle;
use crate::unified_exec::process::OutputHandles;
use crate::unified_exec::process::SpawnLifecycleHandle;
use crate::unified_exec::process::UnifiedExecProcess;
use crate::unified_exec::shell_snapshot::shell_snapshot_request;
use crate::unified_exec::take_plugin_metrics_sidecar;
use codex_core_plugins::PLUGIN_METRICS_OUTPUT_ENV_VAR;
use codex_core_plugins::PluginCommandAttribution;
use codex_core_plugins::PluginMetricsSidecar;
use codex_core_plugins::strip_output_env;
use codex_network_proxy::NetworkPolicyDecider;
use codex_network_proxy::NetworkProxy;
use codex_protocol::config_types::ShellEnvironmentPolicy;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::SandboxErr;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ExecCommandSource;
use codex_protocol::protocol::TerminalInteractionEvent;
use codex_protocol::shell_environment::is_non_inheritable_env_var;
use codex_sandboxing::SandboxCommand;
use codex_tools::ToolName;
use codex_utils_output_truncation::approx_tokens_from_byte_count;
use codex_utils_path_uri::PathUri;

const UNIFIED_EXEC_ENV: [(&str, &str); 10] = [
    ("NO_COLOR", "1"),
    ("TERM", "dumb"),
    ("LANG", "C.UTF-8"),
    ("LC_CTYPE", "C.UTF-8"),
    ("LC_ALL", "C.UTF-8"),
    ("COLORTERM", ""),
    ("PAGER", "cat"),
    ("GIT_PAGER", "cat"),
    ("GH_PAGER", "cat"),
    ("CODEX_CI", "1"),
];
const NETWORK_ACCESS_DENIED_MESSAGE: &str =
    "Network access was denied by the Codex sandbox network proxy.";
const LATE_NETWORK_DENIAL_GRACE_PERIOD: Duration = Duration::from_millis(100);
const INTERRUPT: &str = "\u{3}";

/// Test-only override for deterministic unified exec process IDs.
///
/// In production builds this value should remain at its default (`false`) and
/// must not be toggled.
static FORCE_DETERMINISTIC_PROCESS_IDS: AtomicBool = AtomicBool::new(false);

pub(super) fn set_deterministic_process_ids_for_tests(enabled: bool) {
    FORCE_DETERMINISTIC_PROCESS_IDS.store(enabled, Ordering::Relaxed);
}

fn deterministic_process_ids_forced_for_tests() -> bool {
    FORCE_DETERMINISTIC_PROCESS_IDS.load(Ordering::Relaxed)
}

fn should_use_deterministic_process_ids() -> bool {
    cfg!(test) || deterministic_process_ids_forced_for_tests()
}

impl InitialExecOutputPersistenceDecision {
    pub(crate) fn commit(mut self) {
        for pending_output in self.pending_outputs.drain(..) {
            pending_output.state.mark_yielded();
        }
    }
}

impl Drop for InitialExecOutputPersistenceDecision {
    fn drop(&mut self) {
        let abandoned = self
            .pending_outputs
            .drain(..)
            .map(|pending_output| {
                pending_output.state.mark_not_yielded();
                pending_output.process.terminate();
                (pending_output.process_id, pending_output.state)
            })
            .collect::<Vec<_>>();
        remove_abandoned_initial_exec_processes_from_store(
            Arc::clone(&self.process_store),
            abandoned,
        );
    }
}

fn apply_unified_exec_env(mut env: HashMap<String, String>) -> HashMap<String, String> {
    for (key, value) in UNIFIED_EXEC_ENV {
        env.insert(key.to_string(), value.to_string());
    }
    env
}

fn exec_env_policy_from_shell_policy(
    policy: &ShellEnvironmentPolicy,
) -> codex_exec_server::ExecEnvPolicy {
    let mut exclude = policy
        .exclude
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();
    exclude.extend([
        CODEX_PERMISSION_PROFILE_ENV_VAR.to_string(),
        codex_apply_patch::CODEX_APPLY_PATCH_PRESERVE_LINE_ENDINGS_ENV_VAR.to_string(),
        PLUGIN_METRICS_OUTPUT_ENV_VAR.to_string(),
    ]);
    let mut r#set = policy.r#set.clone();
    r#set.retain(|key, _| {
        ![
            CODEX_PERMISSION_PROFILE_ENV_VAR,
            codex_apply_patch::CODEX_APPLY_PATCH_PRESERVE_LINE_ENDINGS_ENV_VAR,
            PLUGIN_METRICS_OUTPUT_ENV_VAR,
        ]
        .iter()
        .any(|runtime_key| key.eq_ignore_ascii_case(runtime_key))
            && !is_non_inheritable_env_var(key)
    });
    codex_exec_server::ExecEnvPolicy {
        inherit: policy.inherit.clone(),
        ignore_default_excludes: policy.ignore_default_excludes,
        exclude,
        r#set,
        include_only: policy
            .include_only
            .iter()
            .map(std::string::ToString::to_string)
            .collect(),
    }
}

fn env_overlay_for_exec_server(
    request_env: &HashMap<String, String>,
    local_policy_env: &HashMap<String, String>,
) -> HashMap<String, String> {
    request_env
        .iter()
        .filter(|(key, value)| {
            !is_non_inheritable_env_var(key)
                && (matches!(
                    key.as_str(),
                    CODEX_PERMISSION_PROFILE_ENV_VAR
                        | codex_apply_patch::CODEX_APPLY_PATCH_PRESERVE_LINE_ENDINGS_ENV_VAR
                ) || local_policy_env.get(*key) != Some(*value))
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn exec_server_env_for_request(
    request: &ExecRequest,
) -> (
    Option<codex_exec_server::ExecEnvPolicy>,
    HashMap<String, String>,
) {
    if let Some(exec_server_env_config) = &request.exec_server_env_config {
        let mut env =
            env_overlay_for_exec_server(&request.env, &exec_server_env_config.local_policy_env);
        if request.exec_server_shell_snapshot.is_some()
            && !exec_server_env_config.policy.r#set.contains_key("PATH")
        {
            env.remove("PATH");
        }
        if request.exec_server_managed_network.is_some() {
            for (key, value) in &request.env {
                if is_managed_proxy_env_var(key, value) {
                    env.insert(key.clone(), value.clone());
                }
            }
        }
        (Some(exec_server_env_config.policy.clone()), env)
    } else {
        (None, request.env.clone())
    }
}

fn exec_server_params_for_request(
    process_id: i32,
    request: &ExecRequest,
    windows_sandbox_proxy_settings_mode: codex_sandboxing::WindowsSandboxProxySettingsMode,
    tty: bool,
) -> codex_exec_server::ExecParams {
    let (env_policy, env) = exec_server_env_for_request(request);
    let sandbox = request.exec_server_sandbox.clone().map(|mut sandbox| {
        sandbox.windows_sandbox_proxy_settings_mode = Some(windows_sandbox_proxy_settings_mode);
        sandbox
    });
    // Sandbox retries and memory-backed local launches can reuse a unified-exec
    // ID while the executor still retains the previous process.
    let exec_server_process_id =
        if request.exec_server_sandbox.is_some() || request.exec_server_shell_snapshot.is_some() {
            format!("{process_id}-{}", Uuid::new_v4())
        } else {
            process_id.to_string()
        };
    codex_exec_server::ExecParams {
        process_id: exec_server_process_id.into(),
        argv: request.command.clone(),
        cwd: request.cwd.clone(),
        env_policy,
        shell_snapshot: request.exec_server_shell_snapshot.clone(),
        env,
        tty,
        pipe_stdin: false,
        arg0: request.arg0.clone(),
        sandbox,
        enforce_managed_network: request.exec_server_enforce_managed_network,
        managed_network: request.exec_server_managed_network.clone(),
        network_proxy: request.exec_server_network_proxy.clone(),
    }
}

/// Borrowed process state prepared for a `write_stdin` or poll operation.
struct PreparedProcessHandles {
    process: Arc<UnifiedExecProcess>,
    output: OutputHandles,
    pause_state: Option<watch::Receiver<bool>>,
    session: Option<Arc<crate::session::session::Session>>,
    network_approval: Option<DeferredNetworkApproval>,
    call_id: String,
    hook_command: String,
    process_id: i32,
    tty: bool,
}

struct InitialExecCommandGuard {
    state: Option<Arc<InitialExecCommandState>>,
    metrics_sidecar: Option<PluginMetricsSidecar>,
}

impl InitialExecCommandGuard {
    async fn finish_plugin_metrics(&mut self, context: &UnifiedExecContext, exit_code: i32) {
        finish_and_track_measurements(
            self.metrics_sidecar.take(),
            exit_code,
            &context.session,
            &context.step_context.turn,
            &context.call_id,
        )
        .await;
    }

    fn take_state(&mut self) -> Option<Arc<InitialExecCommandState>> {
        self.state.take()
    }
}

impl Drop for InitialExecCommandGuard {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            state.mark_not_yielded();
        }
    }
}

struct ProcessSetupGuard {
    process: Arc<UnifiedExecProcess>,
    process_store: Arc<tokio::sync::Mutex<ProcessStore>>,
    process_id: i32,
    session: std::sync::Weak<crate::session::session::Session>,
    network_approval: Option<DeferredNetworkApproval>,
    armed: bool,
}

struct UnboundProcessReservationGuard {
    process_store: Arc<tokio::sync::Mutex<ProcessStore>>,
    process_id: i32,
    armed: bool,
}

impl UnboundProcessReservationGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for UnboundProcessReservationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let process_store = Arc::clone(&self.process_store);
        let process_id = self.process_id;
        tokio::spawn(async move {
            let mut store = process_store.lock().await;
            store.reserved_process_ids.remove(&process_id);
            store.reservation_owners.remove(&process_id);
        });
    }
}

impl ProcessSetupGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ProcessSetupGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.process.terminate();
        self.process.output_drained_notify().notify_one();
        let process = Arc::clone(&self.process);
        let process_store = Arc::clone(&self.process_store);
        let process_id = self.process_id;
        let session = self.session.clone();
        let network_approval = self.network_approval.clone();
        tokio::spawn(async move {
            let removed = {
                let mut store = process_store.lock().await;
                let process_matches = store
                    .processes
                    .get(&process_id)
                    .is_some_and(|entry| Arc::ptr_eq(&entry.process, &process));
                if process_matches {
                    store.remove(process_id)
                } else {
                    let reservation_matches = store
                        .reservation_owners
                        .get(&process_id)
                        .and_then(std::sync::Weak::upgrade)
                        .is_some_and(|owner| Arc::ptr_eq(&owner, &process));
                    if reservation_matches {
                        store.reserved_process_ids.remove(&process_id);
                        store.reservation_owners.remove(&process_id);
                    }
                    None
                }
            };
            if let Some(entry) = removed {
                unregister_network_approval_for_entry(&entry).await;
            } else if let Some(network_approval) = network_approval
                && let Some(session) = session.upgrade()
            {
                session
                    .services
                    .network_approval
                    .unregister_call(network_approval.registration_id())
                    .await;
            }
        });
    }
}

async fn unregister_network_approval_for_entry(entry: &ProcessEntry) {
    if let Some(network_approval) = entry.network_approval.as_ref()
        && let Some(session) = entry.session.upgrade()
    {
        session
            .services
            .network_approval
            .unregister_call(network_approval.registration_id())
            .await;
    }
}

async fn finish_network_approval_after_process_exit_for_entry(
    entry: &ProcessEntry,
) -> Result<(), String> {
    let session = entry.session.upgrade();
    finish_deferred_network_approval_after_process_exit_for_session(
        session.as_ref(),
        entry.network_approval.clone(),
    )
    .await
}

async fn finish_deferred_network_approval_for_session(
    session: Option<&Arc<crate::session::session::Session>>,
    deferred: Option<DeferredNetworkApproval>,
) -> Result<(), String> {
    let Some(session) = session else {
        return Ok(());
    };
    finish_deferred_network_approval(session.as_ref(), deferred)
        .await
        .map_err(network_approval_error_message)
}

fn network_approval_error_message(err: ToolError) -> String {
    match err {
        ToolError::Rejected(message) => message,
        ToolError::Codex(err) => err.to_string(),
    }
}

async fn network_denial_message_for_session(
    session: Option<&Arc<crate::session::session::Session>>,
    deferred: Option<DeferredNetworkApproval>,
) -> String {
    let Some(session) = session else {
        return NETWORK_ACCESS_DENIED_MESSAGE.to_string();
    };
    match finish_deferred_network_approval(session.as_ref(), deferred).await {
        Ok(()) => NETWORK_ACCESS_DENIED_MESSAGE.to_string(),
        Err(err) => network_approval_error_message(err),
    }
}

async fn wait_for_late_network_denial(network_cancelled: Option<CancellationToken>) -> bool {
    let Some(network_cancelled) = network_cancelled else {
        return false;
    };
    if network_cancelled.is_cancelled() {
        return true;
    }

    tokio::select! {
        _ = network_cancelled.cancelled() => true,
        _ = tokio::time::sleep(LATE_NETWORK_DENIAL_GRACE_PERIOD) => false,
    }
}

async fn finish_deferred_network_approval_after_process_exit_for_session(
    session: Option<&Arc<crate::session::session::Session>>,
    deferred: Option<DeferredNetworkApproval>,
) -> Result<(), String> {
    wait_for_late_network_denial(
        deferred
            .as_ref()
            .map(DeferredNetworkApproval::cancellation_token),
    )
    .await;
    finish_deferred_network_approval_for_session(session, deferred).await
}

fn fail_process_with_message(process: &UnifiedExecProcess, message: String) -> UnifiedExecError {
    if let Some(message) = process.failure_message() {
        process.terminate();
        return UnifiedExecError::process_failed(message);
    }

    process.fail_and_terminate(message.clone());
    UnifiedExecError::process_failed(process.failure_message().unwrap_or(message))
}

#[allow(clippy::too_many_arguments)]
async fn emit_failed_initial_exec_end_if_unstored(
    process_started_alive: bool,
    context: &UnifiedExecContext,
    request: &ExecCommandRequest,
    cwd: PathUri,
    plugin_attribution: Option<PluginCommandAttribution>,
    transcript: Arc<tokio::sync::Mutex<HeadTailBuffer>>,
    fallback_output: String,
    message: String,
    wall_time: Duration,
) {
    if process_started_alive {
        return;
    }

    emit_failed_exec_end_for_unified_exec(
        Arc::clone(&context.session),
        Arc::clone(&context.step_context.turn),
        context.call_id.clone(),
        request.command.clone(),
        cwd,
        Some(request.process_id.to_string()),
        plugin_attribution,
        transcript,
        fallback_output,
        message,
        wall_time,
    )
    .await;
}

fn terminate_process_on_network_denial(
    process: Arc<UnifiedExecProcess>,
    session: std::sync::Weak<crate::session::session::Session>,
    deferred: DeferredNetworkApproval,
) -> tokio::task::JoinHandle<()> {
    let network_cancelled = deferred.cancellation_token();
    let process_exited = process.cancellation_token();
    tokio::spawn(async move {
        let denied = tokio::select! {
            _ = network_cancelled.cancelled() => true,
            _ = process_exited.cancelled() => {
                wait_for_late_network_denial(Some(network_cancelled.clone())).await
            }
        };
        if !denied {
            return;
        }
        let session = session.upgrade();
        let message = network_denial_message_for_session(session.as_ref(), Some(deferred)).await;
        process.fail_and_terminate(message);
    })
}

fn remove_abandoned_initial_exec_processes_from_store(
    process_store: Arc<tokio::sync::Mutex<ProcessStore>>,
    abandoned: Vec<(i32, Arc<InitialExecCommandState>)>,
) {
    if abandoned.is_empty() {
        return;
    }
    tokio::spawn(async move {
        let removed = {
            let mut store = process_store.lock().await;
            abandoned
                .into_iter()
                .filter_map(|(process_id, state)| {
                    let state_matches = store.processes.get(&process_id).is_some_and(|entry| {
                        Arc::ptr_eq(&entry.initial_exec_command_state, &state)
                    });
                    if !state_matches {
                        return None;
                    }
                    store.remove(process_id)
                })
                .collect::<Vec<_>>()
        };
        for entry in removed {
            entry.process.terminate();
            unregister_network_approval_for_entry(&entry).await;
        }
    });
}

impl UnifiedExecProcessManager {
    pub(crate) async fn begin_shutdown(&self) {
        let _watcher_lifecycle_guard = self.watcher_lifecycle_lock.lock().await;
        self.shutdown_started.store(true, Ordering::Release);
    }

    /// Reset each monitor's model-visible notification budget while compaction
    /// publishes a new in-memory history window.
    pub(crate) async fn begin_notification_window(&self) {
        self.monitor_store.lock().await.begin_notification_window();
    }

    pub(crate) async fn allocate_process_id(&self) -> i32 {
        loop {
            let mut store = self.process_store.lock().await;

            let process_id = if should_use_deterministic_process_ids() {
                // test or deterministic mode
                store
                    .reserved_process_ids
                    .iter()
                    .copied()
                    .max()
                    .map(|m| std::cmp::max(m, 999) + 1)
                    .unwrap_or(1000)
            } else {
                // production mode → random
                rand::rng().random_range(1_000..100_000)
            };

            if store.reserved_process_ids.contains(&process_id) {
                continue;
            }

            store.reserved_process_ids.insert(process_id);
            return process_id;
        }
    }

    pub(crate) async fn release_process_id(&self, process_id: i32) {
        let removed = {
            let mut store = self.process_store.lock().await;
            store.remove(process_id)
        };
        if let Some(entry) = removed {
            unregister_network_approval_for_entry(&entry).await;
        }
    }

    pub(crate) async fn exec_command(
        &self,
        request: ExecCommandRequest,
        context: &UnifiedExecContext,
    ) -> Result<ExecCommandToolOutput, UnifiedExecError> {
        self.exec_command_inner(request, context, /*monitor*/ None)
            .await
    }

    /// Start a monitored process.
    ///
    /// This is an ordinary `exec_command` with a watcher attached: the process
    /// lands in the same process store, gets the same process id, the same
    /// bounded retained output, the same `write_stdin` and `list`/`terminate`
    /// operations, and the same survival across turn interruption. What the
    /// attachment adds is the model-facing notification pump and the watcher
    /// metadata needed to reason about a long-lived process.
    pub(crate) async fn start_monitor(
        &self,
        request: ExecCommandRequest,
        context: &UnifiedExecContext,
        attachment: MonitorAttachment,
    ) -> Result<ExecCommandToolOutput, UnifiedExecError> {
        self.exec_command_inner(request, context, Some(attachment))
            .await
    }

    async fn exec_command_inner(
        &self,
        request: ExecCommandRequest,
        context: &UnifiedExecContext,
        monitor: Option<MonitorAttachment>,
    ) -> Result<ExecCommandToolOutput, UnifiedExecError> {
        let mut reservation_guard = UnboundProcessReservationGuard {
            process_store: Arc::clone(&self.process_store),
            process_id: request.process_id,
            armed: true,
        };
        let cwd = request.cwd.clone();
        let process = self
            .open_session_with_sandbox(&request, cwd.clone(), context)
            .await;

        let (attempt, mut deferred_network_approval) = match process {
            Ok((attempt, deferred_network_approval)) => (attempt, deferred_network_approval),
            Err(err) => return Err(err),
        };
        let UnifiedExecAttempt {
            process,
            metrics_sidecar,
        } = attempt;
        let process = Arc::new(process);
        let mut process_setup_guard = ProcessSetupGuard {
            process: Arc::clone(&process),
            process_store: Arc::clone(&self.process_store),
            process_id: request.process_id,
            session: Arc::downgrade(&context.session),
            network_approval: deferred_network_approval.clone(),
            armed: true,
        };
        let reservation_bound = {
            let mut store = self.process_store.lock().await;
            if store.reserved_process_ids.contains(&request.process_id) {
                store
                    .reservation_owners
                    .insert(request.process_id, Arc::downgrade(&process));
                true
            } else {
                false
            }
        };
        if !reservation_bound {
            return Err(UnifiedExecError::process_failed(
                "process ID reservation was lost before startup".to_string(),
            ));
        }
        reservation_guard.disarm();
        let network_denial_monitor = deferred_network_approval.as_ref().map(|deferred| {
            terminate_process_on_network_denial(
                Arc::clone(&process),
                Arc::downgrade(&context.session),
                deferred.clone(),
            )
        });

        let transcript = Arc::new(tokio::sync::Mutex::new(HeadTailBuffer::default()));
        let event_ctx = ToolEventCtx::new(
            context.session.as_ref(),
            context.step_context.turn.as_ref(),
            &context.call_id,
            /*turn_diff_tracker*/ None,
        );
        let plugin_attribution = if request.turn_environment.environment.is_remote() {
            let file_system = request.turn_environment.environment.get_filesystem();
            context
                .step_context
                .turn
                .plugin_attribution_for_executor_command(
                    &request.command,
                    &cwd,
                    file_system.as_ref(),
                )
                .await
        } else {
            cwd.to_abs_path().ok().and_then(|cwd| {
                context
                    .step_context
                    .turn
                    .plugin_attribution_for_command(&request.command, &cwd)
            })
        };
        let emitter = ToolEmitter::unified_exec(
            &request.command,
            cwd.clone(),
            ExecCommandSource::UnifiedExecStartup,
            Some(request.process_id.to_string()),
            plugin_attribution.clone(),
        );
        emitter.emit(event_ctx, ToolEventStage::Begin).await;

        let watcher_lifecycle_guard = self.watcher_lifecycle_lock.lock().await;
        if self.shutdown_started.load(Ordering::Acquire) {
            drop(watcher_lifecycle_guard);
            return Err(UnifiedExecError::process_failed(
                "session is shutting down".to_string(),
            ));
        }

        let notify_yielded_completion = monitor.is_none();
        let start = Instant::now();
        // Persist live sessions before monitor attachment so cancellation can
        // never leave a monitor as the process's only owner.
        let process_started_alive = !process.has_exited() && process.exit_code().is_none();
        let (mut initial_exec_command_guard, pruned_entry) = if process_started_alive {
            let initial_exec_command_state = Arc::new(InitialExecCommandState::new());
            let initial_exec_command_guard = InitialExecCommandGuard {
                state: Some(Arc::clone(&initial_exec_command_state)),
                metrics_sidecar: None,
            };
            let pruned_entry = self
                .store_process_locked(
                    Arc::clone(&process),
                    context,
                    &request.command,
                    request.hook_command.clone(),
                    cwd.clone(),
                    plugin_attribution.clone(),
                    start,
                    request.process_id,
                    request.tty,
                    deferred_network_approval.clone(),
                    network_denial_monitor,
                    metrics_sidecar,
                    Arc::clone(&transcript),
                    Arc::clone(&initial_exec_command_state),
                    notify_yielded_completion,
                )
                .await;
            (initial_exec_command_guard, pruned_entry)
        } else {
            (
                InitialExecCommandGuard {
                    state: None,
                    metrics_sidecar,
                },
                None,
            )
        };
        // Bytes the process produced before the monitor could subscribe belong
        // to the monitor now, so put them back in front of the initial yield.
        let monitor_seed = match monitor {
            Some(attachment) => {
                self.attach_monitor_locked(&request, context, &process, &transcript, attachment)
                    .await
            }
            None => {
                process.monitor_output_buffer().lock().await.take();
                Vec::new()
            }
        };
        start_streaming_output(&process, context, Arc::clone(&transcript));
        if process_started_alive {
            process_setup_guard.disarm();
        }
        drop(watcher_lifecycle_guard);
        if let Some(pruned_entry) = pruned_entry {
            unregister_network_approval_for_entry(&pruned_entry).await;
        }

        let yield_time_ms = clamp_yield_time(request.yield_time_ms);
        // For the initial exec_command call, we both stream output to events
        // (via start_streaming_output above) and collect a snapshot here for
        // the tool response body.
        let deadline = start + Duration::from_millis(yield_time_ms);
        let collected_output = Self::collect_output_until_deadline(
            process.output_handles(),
            Some(context.session.subscribe_elicitation_pause_state()),
            deadline,
        )
        .await;
        let wall_time = Instant::now().saturating_duration_since(start);

        let original_token_count = usize::try_from(approx_tokens_from_byte_count(
            collected_output.total_bytes(),
        ))
        .unwrap_or(usize::MAX);
        let output_omitted_bytes = NonZeroUsize::new(collected_output.omitted_bytes());
        let mut collected = monitor_seed;
        collected.extend(collected_output.to_bytes_with_omission_marker());
        let text = String::from_utf8_lossy(&collected).to_string();
        let chunk_id = generate_chunk_id();
        if deferred_network_approval
            .as_ref()
            .is_some_and(DeferredNetworkApproval::is_cancelled)
        {
            let message = network_denial_message_for_session(
                Some(&context.session),
                deferred_network_approval.take(),
            )
            .await;
            emit_failed_initial_exec_end_if_unstored(
                process_started_alive,
                context,
                &request,
                cwd.clone(),
                plugin_attribution.clone(),
                Arc::clone(&transcript),
                text.clone(),
                message.clone(),
                wall_time,
            )
            .await;
            self.release_process_id(request.process_id).await;
            return Err(fail_process_with_message(process.as_ref(), message));
        }
        if let Some(message) = process.failure_message() {
            let finish_result = finish_deferred_network_approval_for_session(
                Some(&context.session),
                deferred_network_approval.take(),
            )
            .await;
            emit_failed_initial_exec_end_if_unstored(
                process_started_alive,
                context,
                &request,
                cwd.clone(),
                plugin_attribution.clone(),
                Arc::clone(&transcript),
                text.clone(),
                message.clone(),
                wall_time,
            )
            .await;
            self.release_process_id(request.process_id).await;
            if let Err(message) = finish_result {
                return Err(fail_process_with_message(process.as_ref(), message));
            }
            return Err(UnifiedExecError::process_failed(message));
        }
        let process_id = request.process_id;
        let (mut response_process_id, mut exit_code) = if process_started_alive {
            match self.refresh_process_state(process_id).await {
                ProcessStatus::Alive {
                    exit_code,
                    process_id,
                    ..
                } => (Some(process_id), exit_code),
                ProcessStatus::Exited { exit_code, entry } => {
                    if let Err(message) =
                        finish_deferred_network_approval_after_process_exit_for_session(
                            Some(&context.session),
                            deferred_network_approval.take(),
                        )
                        .await
                    {
                        return Err(fail_process_with_message(entry.process.as_ref(), message));
                    }
                    process
                        .check_for_sandbox_denial_with_text(&text)
                        .await
                        .map_err(|err| {
                            err.with_output_collection_metadata(
                                original_token_count,
                                output_omitted_bytes,
                            )
                        })?;
                    let metrics_sidecar = entry
                        .plugin_metrics_sidecar
                        .as_ref()
                        .and_then(take_plugin_metrics_sidecar);
                    finish_and_track_measurements(
                        metrics_sidecar,
                        exit_code.unwrap_or(-1),
                        &context.session,
                        &context.step_context.turn,
                        &context.call_id,
                    )
                    .await;
                    (None, exit_code)
                }
                ProcessStatus::Unknown => {
                    return Err(UnifiedExecError::UnknownProcessId { process_id });
                }
            }
        } else {
            // Short-lived command: emit the completed command item immediately
            // using the same helper as the background watcher.
            let finish_result = finish_deferred_network_approval_after_process_exit_for_session(
                Some(&context.session),
                deferred_network_approval.take(),
            )
            .await;
            if let Err(message) = finish_result {
                emit_failed_initial_exec_end_if_unstored(
                    process_started_alive,
                    context,
                    &request,
                    cwd.clone(),
                    plugin_attribution.clone(),
                    Arc::clone(&transcript),
                    text.clone(),
                    message.clone(),
                    wall_time,
                )
                .await;
                self.release_process_id(request.process_id).await;
                return Err(fail_process_with_message(process.as_ref(), message));
            }
            let exit_code = process.exit_code();
            let exit = exit_code.unwrap_or(-1);
            initial_exec_command_guard
                .finish_plugin_metrics(context, exit)
                .await;
            emit_exec_end_for_unified_exec(
                Arc::clone(&context.session),
                Arc::clone(&context.step_context.turn),
                context.call_id.clone(),
                request.command.clone(),
                cwd.clone(),
                Some(process_id.to_string()),
                plugin_attribution.clone(),
                Arc::clone(&transcript),
                text.clone(),
                exit,
                wall_time,
            )
            .await;

            self.release_process_id(request.process_id).await;
            process
                .check_for_sandbox_denial_with_text(&text)
                .await
                .map_err(|err| {
                    err.with_output_collection_metadata(original_token_count, output_omitted_bytes)
                })?;
            (None, exit_code)
        };

        if response_process_id.is_some()
            && context.initial_output_destination != InitialExecCommandOutputDestination::Untracked
        {
            let store = self.process_store.lock().await;
            let process_is_still_stored = store
                .processes
                .get(&process_id)
                .is_some_and(|entry| Arc::ptr_eq(&entry.process, &process));
            if process_is_still_stored {
                let mut pending = self.initial_exec_outputs_pending_recording.lock().await;
                if let Some(state) = initial_exec_command_guard.take_state() {
                    pending.entry(context.call_id.clone()).or_default().push(
                        PendingInitialExecCommandOutput {
                            process_id,
                            state: Arc::clone(&state),
                            process: Arc::clone(&process),
                            destination: context.initial_output_destination,
                        },
                    );
                    state.mark_returned();
                }
            } else {
                response_process_id = None;
                exit_code = process.exit_code();
            }
        }

        let response = ExecCommandToolOutput {
            event_call_id: context.call_id.clone(),
            chunk_id,
            wall_time,
            raw_output: collected,
            truncation_policy: context
                .step_context
                .turn
                .model_info
                .truncation_policy
                .into(),
            max_output_tokens: request.max_output_tokens,
            process_id: response_process_id,
            exit_code,
            original_token_count: Some(original_token_count),
            output_omitted_bytes,
            hook_command: Some(request.hook_command.clone()),
        };

        Ok(response)
    }

    pub(crate) async fn write_stdin(
        &self,
        request: WriteStdinRequest<'_>,
    ) -> Result<ExecCommandToolOutput, UnifiedExecError> {
        let process_id = request.process_id;

        // Different terminal sessions can be polled concurrently, but reads and
        // writes against one terminal must not overlap because they share a
        // draining output buffer and process lifecycle.
        let locked_process = {
            let store = self.process_store.lock().await;
            let entry = store
                .processes
                .get(&process_id)
                .ok_or(UnifiedExecError::UnknownProcessId { process_id })?;
            Arc::clone(&entry.process)
        };
        let _interaction_guard = locked_process.interaction_lock().lock_owned().await;

        let PreparedProcessHandles {
            process,
            output,
            pause_state,
            session,
            network_approval,
            call_id,
            hook_command,
            process_id,
            tty,
            ..
        } = self
            .prepare_process_handles(process_id, &locked_process)
            .await?;
        let mut status_after_write = None;

        if !request.input.is_empty() {
            if !tty {
                if request.input == INTERRUPT {
                    process.interrupt().await?;
                } else {
                    return Err(UnifiedExecError::StdinClosed);
                }
            } else {
                match process.write(request.input.as_bytes()).await {
                    Ok(()) => {
                        // Give the remote process a brief window to react so that we are
                        // more likely to capture its output in the poll below.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(err) => {
                        let status = self.refresh_process_state(process_id).await;
                        if matches!(status, ProcessStatus::Exited { .. }) {
                            status_after_write = Some(status);
                        } else if matches!(err, UnifiedExecError::ProcessFailed { .. }) {
                            process.terminate();
                            self.release_process_id(process_id).await;
                            return Err(err);
                        } else {
                            return Err(err);
                        }
                    }
                }
            }
        }

        let yield_time_ms = {
            // Empty polls use configurable background timeout bounds. Non-empty
            // writes keep a fixed max cap so interactive stdin remains responsive.
            let time_ms = request.yield_time_ms.max(MIN_YIELD_TIME_MS);
            if request.input.is_empty() {
                time_ms.clamp(MIN_EMPTY_YIELD_TIME_MS, self.max_write_stdin_yield_time_ms)
            } else {
                time_ms.min(MAX_YIELD_TIME_MS)
            }
        };
        let start = Instant::now();
        let deadline = start + Duration::from_millis(yield_time_ms);
        let collected_output =
            Self::collect_output_until_deadline(&output, pause_state, deadline).await;
        let wall_time = Instant::now().saturating_duration_since(start);

        let original_token_count = usize::try_from(approx_tokens_from_byte_count(
            collected_output.total_bytes(),
        ))
        .unwrap_or(usize::MAX);
        let output_omitted_bytes = NonZeroUsize::new(collected_output.omitted_bytes());
        let collected = collected_output.to_bytes_with_omission_marker();
        let chunk_id = generate_chunk_id();
        if network_approval
            .as_ref()
            .is_some_and(DeferredNetworkApproval::is_cancelled)
        {
            let message =
                network_denial_message_for_session(session.as_ref(), network_approval.clone())
                    .await;
            let error = fail_process_with_message(process.as_ref(), message)
                .with_collected_process_output(&collected);
            self.release_process_id(process_id).await;
            return Err(error);
        }
        if let Some(message) = process.failure_message() {
            let finish_result = finish_deferred_network_approval_for_session(
                session.as_ref(),
                network_approval.clone(),
            )
            .await;
            self.release_process_id(process_id).await;
            if let Err(message) = finish_result {
                return Err(fail_process_with_message(process.as_ref(), message)
                    .with_collected_process_output(&collected));
            }
            return Err(
                UnifiedExecError::process_failed(message).with_collected_process_output(&collected)
            );
        }

        // After polling, refresh_process_state tells us whether the PTY is
        // still alive or has exited and been removed from the store; we thread
        // that through so the handler can tag or suppress TerminalInteraction
        // with an appropriate process_id and exit_code.
        let status = if let Some(status) = status_after_write {
            status
        } else {
            self.refresh_process_state(process_id).await
        };
        let (process_id, exit_code, event_call_id) = match status {
            ProcessStatus::Alive {
                exit_code,
                call_id,
                process_id,
            } => (Some(process_id), exit_code, call_id),
            ProcessStatus::Exited { exit_code, entry } => {
                let call_id = entry.call_id.clone();
                if let Err(message) =
                    finish_network_approval_after_process_exit_for_entry(&entry).await
                {
                    return Err(fail_process_with_message(entry.process.as_ref(), message)
                        .with_collected_process_output(&collected));
                }
                (None, exit_code, call_id)
            }
            ProcessStatus::Unknown => {
                if process.has_exited() {
                    (None, process.exit_code(), call_id)
                } else {
                    return Err(UnifiedExecError::UnknownProcessId {
                        process_id: request.process_id,
                    });
                }
            }
        };

        let response = ExecCommandToolOutput {
            event_call_id,
            chunk_id,
            wall_time,
            raw_output: collected,
            truncation_policy: request.truncation_policy,
            max_output_tokens: request.max_output_tokens,
            process_id,
            exit_code,
            original_token_count: Some(original_token_count),
            output_omitted_bytes,
            hook_command: Some(hook_command),
        };

        let should_emit_interaction = !request.input.is_empty() || response.process_id.is_some();
        if should_emit_interaction
            && let Some(WriteStdinInteractionEvent { session, turn }) = request.interaction_event
        {
            let interaction = TerminalInteractionEvent {
                call_id: response.event_call_id.clone(),
                process_id: response
                    .process_id
                    .unwrap_or(request.process_id)
                    .to_string(),
                stdin: request.input.to_string(),
            };
            session
                .send_event(turn.as_ref(), EventMsg::TerminalInteraction(interaction))
                .await;
        }

        Ok(response)
    }

    async fn refresh_process_state(&self, process_id: i32) -> ProcessStatus {
        let mut store = self.process_store.lock().await;
        let Some(entry) = store.processes.get_mut(&process_id) else {
            return ProcessStatus::Unknown;
        };

        let exit_code = entry.process.exit_code();
        let process_id = entry.process_id;

        if entry.process.has_exited() {
            let Some(entry) = store.remove(process_id) else {
                return ProcessStatus::Unknown;
            };
            ProcessStatus::Exited {
                exit_code,
                entry: Box::new(entry),
            }
        } else {
            ProcessStatus::Alive {
                exit_code,
                call_id: entry.call_id.clone(),
                process_id,
            }
        }
    }

    async fn prepare_process_handles(
        &self,
        process_id: i32,
        expected_process: &Arc<UnifiedExecProcess>,
    ) -> Result<PreparedProcessHandles, UnifiedExecError> {
        let mut store = self.process_store.lock().await;
        let entry = store
            .processes
            .get_mut(&process_id)
            .ok_or(UnifiedExecError::UnknownProcessId { process_id })?;
        if !Arc::ptr_eq(&entry.process, expected_process) {
            return Err(UnifiedExecError::UnknownProcessId { process_id });
        }
        entry.last_used = Instant::now();
        let output = entry.process.output_handles().clone();
        let pause_state = entry
            .session
            .upgrade()
            .map(|session| session.subscribe_elicitation_pause_state());
        let session = entry.session.upgrade();

        Ok(PreparedProcessHandles {
            process: Arc::clone(&entry.process),
            output,
            pause_state,
            session,
            network_approval: entry.network_approval.clone(),
            call_id: entry.call_id.clone(),
            hook_command: entry.hook_command.clone(),
            process_id: entry.process_id,
            tty: entry.tty,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn store_process_locked(
        &self,
        process: Arc<UnifiedExecProcess>,
        context: &UnifiedExecContext,
        command: &[String],
        hook_command: String,
        cwd: PathUri,
        plugin_attribution: Option<PluginCommandAttribution>,
        started_at: Instant,
        process_id: i32,
        tty: bool,
        network_approval: Option<DeferredNetworkApproval>,
        network_denial_monitor: Option<tokio::task::JoinHandle<()>>,
        metrics_sidecar: Option<PluginMetricsSidecar>,
        transcript: Arc<tokio::sync::Mutex<HeadTailBuffer>>,
        initial_exec_command_state: Arc<InitialExecCommandState>,
        notify_yielded_completion: bool,
    ) -> Option<ProcessEntry> {
        let plugin_metrics_sidecar =
            metrics_sidecar.map(|sidecar| Arc::new(std::sync::Mutex::new(Some(sidecar))));
        let completion_notification = notify_yielded_completion.then(|| {
            YieldedExecCompletionContext::new(
                Arc::clone(&initial_exec_command_state),
                hook_command.clone(),
            )
        });
        let entry = ProcessEntry {
            process: Arc::clone(&process),
            plugin_metrics_sidecar: plugin_metrics_sidecar.clone(),
            call_id: context.call_id.clone(),
            process_id,
            cwd: cwd.clone(),
            initial_exec_command_state,
            hook_command,
            tty,
            network_approval,
            session: Arc::downgrade(&context.session),
            last_used: started_at,
        };
        let mut exec_watcher_tasks = self.exec_watcher_tasks.lock().await;
        exec_watcher_tasks.retain(|watcher| !watcher.is_finished());
        let pruned_entry = {
            let mut store = self.process_store.lock().await;
            let pruned_entry = Self::prune_processes_if_needed(&mut store);
            store.processes.insert(process_id, entry);
            pruned_entry
        };
        let watcher = spawn_exit_watcher(
            Arc::clone(&process),
            Arc::clone(&context.session),
            Arc::clone(&context.step_context.turn),
            context.call_id.clone(),
            command.to_vec(),
            cwd,
            process_id,
            plugin_attribution,
            transcript,
            started_at,
            network_denial_monitor,
            plugin_metrics_sidecar,
            completion_notification,
        );
        exec_watcher_tasks.push(watcher);
        if let Some(pruned_entry) = pruned_entry.as_ref() {
            pruned_entry.process.terminate();
        }
        pruned_entry
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn open_session_with_exec_env(
        &self,
        process_id: i32,
        command: SandboxCommand,
        options: ExecOptions,
        attempt: &SandboxAttempt<'_>,
        network: Option<&NetworkProxy>,
        network_proxy_launch: Option<codex_network_proxy::RemoteNetworkProxyLaunchConfig>,
        environment_id: Option<&str>,
        exec_server_env_config: Option<ExecServerEnvConfig>,
        shell_snapshot: Option<codex_exec_server::ShellSnapshotRequest>,
        windows_sandbox_proxy_settings_mode: codex_sandboxing::WindowsSandboxProxySettingsMode,
        tty: bool,
        spawn_lifecycle: SpawnLifecycleHandle,
        environment: &codex_exec_server::Environment,
    ) -> Result<UnifiedExecProcess, ToolError> {
        let mut request = if environment.is_remote() || shell_snapshot.is_some() {
            attempt.env_for_exec_server(command, options)
        } else {
            attempt.env_for(command, options, network, environment_id)
        }
        .map_err(ToolError::Codex)?;
        let network_policy_decider = network_proxy_launch
            .as_ref()
            .filter(|launch| launch.policy_decision_timeout_ms.is_some())
            .and_then(|_| network.and_then(NetworkProxy::remote_policy_decider));
        request.exec_server_network_proxy = network_proxy_launch;
        request.exec_server_env_config = exec_server_env_config;
        request.exec_server_shell_snapshot = shell_snapshot;
        self.open_session_with_prepared_exec_env(
            process_id,
            &request,
            windows_sandbox_proxy_settings_mode,
            network_policy_decider,
            tty,
            spawn_lifecycle,
            environment,
        )
        .await
        .map_err(|err| match err {
            UnifiedExecError::SandboxDenied { output, .. } => {
                ToolError::Codex(CodexErr::Sandbox(SandboxErr::Denied {
                    output: Box::new(output),
                    network_policy_decision: None,
                }))
            }
            other => ToolError::Rejected(other.to_string()),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn open_session_with_prepared_exec_env(
        &self,
        process_id: i32,
        request: &ExecRequest,
        windows_sandbox_proxy_settings_mode: codex_sandboxing::WindowsSandboxProxySettingsMode,
        network_policy_decider: Option<Arc<dyn NetworkPolicyDecider>>,
        tty: bool,
        mut spawn_lifecycle: SpawnLifecycleHandle,
        environment: &codex_exec_server::Environment,
    ) -> Result<UnifiedExecProcess, UnifiedExecError> {
        let inherited_fds = spawn_lifecycle.inherited_fds();

        if environment.is_remote() || request.exec_server_shell_snapshot.is_some() {
            if !inherited_fds.is_empty() {
                return Err(UnifiedExecError::create_process(
                    "remote exec-server does not support inherited file descriptors".to_string(),
                ));
            }

            let backend = environment.get_exec_backend();
            let params = exec_server_params_for_request(
                process_id,
                request,
                windows_sandbox_proxy_settings_mode,
                tty,
            );
            let started = match network_policy_decider {
                Some(decider) => {
                    backend
                        .start_with_network_policy_decider(params, decider)
                        .await
                }
                None => backend.start(params).await,
            }
            .map_err(|err| UnifiedExecError::create_process(err.to_string()))?;
            spawn_lifecycle.after_spawn();
            return UnifiedExecProcess::from_exec_server_started(started).await;
        }

        // TODO(anp): Keep PathUri through the local PTY/process launch boundary.
        let native_cwd = request
            .cwd
            .to_abs_path()
            .map_err(|_| UnifiedExecError::ForeignPath {
                path: request.cwd.clone(),
            })?;

        if request.command.is_empty() {
            return Err(UnifiedExecError::MissingCommandLine);
        }
        let network_proxy_restricting_sid = {
            #[cfg(target_os = "windows")]
            {
                if request.sandbox == codex_sandboxing::SandboxType::WindowsRestrictedToken {
                    request
                        .network
                        .as_ref()
                        .map(|network| {
                            network
                                .network_proxy_restricting_sid(
                                    request.network_environment_id.as_deref(),
                                )
                                .ok_or_else(|| {
                                    UnifiedExecError::create_process(
                                        "managed Windows proxy route is missing its restricting SID"
                                            .to_string(),
                                    )
                                })
                        })
                        .transpose()?
                } else {
                    None
                }
            }
            #[cfg(not(target_os = "windows"))]
            {
                None::<String>
            }
        };
        let windows_sandbox =
            if request.sandbox == codex_sandboxing::SandboxType::WindowsRestrictedToken {
                Some(codex_sandboxing::WindowsSandboxSpawnRequest {
                    permission_profile: &request.permission_profile,
                    workspace_roots: &request.windows_sandbox_workspace_roots,
                    windows_sandbox_level: request.windows_sandbox_level,
                    proxy_enforced: request.network.is_some(),
                    network_proxy_restricting_sid: network_proxy_restricting_sid.as_deref(),
                    proxy_settings_mode: windows_sandbox_proxy_settings_mode,
                    filesystem_overrides: request.windows_sandbox_filesystem_overrides.as_ref(),
                    use_private_desktop: request.windows_sandbox_private_desktop,
                })
            } else {
                None
            };
        let spawn_result = codex_sandboxing::spawn_process(codex_sandboxing::SpawnRequest {
            command: &request.command,
            cwd: native_cwd.as_path(),
            env: &request.env,
            arg0: &request.arg0,
            sandbox: request.sandbox,
            windows_sandbox,
            tty,
            stdin_open: tty,
            inherited_fds: &inherited_fds,
        })
        .await;
        spawn_lifecycle.after_spawn();
        let spawned =
            spawn_result.map_err(|err| UnifiedExecError::create_process(err.to_string()))?;
        UnifiedExecProcess::from_spawned(spawned, request.sandbox, spawn_lifecycle).await
    }

    pub(super) async fn open_session_with_sandbox(
        &self,
        request: &ExecCommandRequest,
        cwd: PathUri,
        context: &UnifiedExecContext,
    ) -> Result<(UnifiedExecAttempt, Option<DeferredNetworkApproval>), UnifiedExecError> {
        let turn = &context.step_context.turn;
        let shell_environment_policy = request.turn_environment.shell_environment_policy();
        let local_policy_env = create_env(shell_environment_policy, /*thread_id*/ None);
        let mut env = local_policy_env.clone();
        env.insert(
            CODEX_THREAD_ID_ENV_VAR.to_string(),
            context.session.thread_id.to_string(),
        );
        inject_session_id_env(&mut env, context.session.session_id());
        inject_apply_patch_env(&mut env, &turn.config.features);
        let active_permission_profile = request.turn_environment.active_permission_profile();
        inject_permission_profile_env(&mut env, active_permission_profile.as_ref());
        let mut env = apply_unified_exec_env(env);
        strip_output_env(&mut env);
        let mut explicit_env_overrides = shell_environment_policy.r#set.clone();
        strip_output_env(&mut explicit_env_overrides);
        let exec_server_env_config = ExecServerEnvConfig {
            policy: exec_env_policy_from_shell_policy(shell_environment_policy),
            local_policy_env,
        };
        let shell_snapshot = shell_snapshot_request(request, &cwd, context);
        let mut orchestrator = ToolOrchestrator::new();
        let mut runtime = UnifiedExecRuntime::new(self, request.shell_mode.clone());
        let session_shell = context.session.user_shell();
        let configured_shell = request
            .turn_environment
            .shell
            .as_ref()
            .unwrap_or(session_shell.as_ref());
        let exec_approval_requirement = context
            .session
            .services
            .exec_policy
            .create_exec_approval_requirement_for_shell(
                ExecApprovalRequest {
                    command: &request.command,
                    approval_policy: turn.approval_policy(),
                    permission_profile: request.turn_environment.permission_profile().clone(),
                    environment_policy: request.turn_environment.config().exec_policy.as_ref(),
                    windows_sandbox_level: turn.windows_sandbox_level,
                    sandbox_permissions: if request.additional_permissions_preapproved {
                        crate::sandboxing::SandboxPermissions::UseDefault
                    } else {
                        request.sandbox_permissions
                    },
                    prefix_rule: request.prefix_rule.clone(),
                    allow_prefix_rules: context.step_context.turn.allow_prefix_rules(),
                },
                configured_shell,
                &request.shell_mode,
            )
            .await;
        let req = UnifiedExecToolRequest {
            command: request.command.clone(),
            shell_type: request.shell_type,
            hook_command: request.hook_command.clone(),
            process_id: request.process_id,
            cwd,
            sandbox_cwd: request.sandbox_cwd.clone(),
            turn_environment: request.turn_environment.clone(),
            env,
            exec_server_env_config: Some(exec_server_env_config),
            shell_snapshot,
            explicit_env_overrides,
            network: request.network.clone(),
            tty: request.tty,
            sandbox_permissions: request.sandbox_permissions,
            additional_permissions: request.additional_permissions.clone(),
            #[cfg(unix)]
            additional_permissions_preapproved: request.additional_permissions_preapproved,
            justification: request.justification.clone(),
            exec_approval_requirement,
        };
        let tool_ctx = ToolCtx {
            session: context.session.clone(),
            step_context: Arc::clone(&context.step_context),
            cancellation_token: context.cancellation_token.clone(),
            call_id: context.call_id.clone(),
            tool_name: ToolName::plain("exec_command"),
        };
        orchestrator
            .run(&mut runtime, &req, &tool_ctx, turn, turn.approval_policy())
            .await
            .map(|result| (result.output, result.deferred_network_approval))
            .map_err(|err| match err {
                ToolError::Codex(err) => match err.details() {
                    CodexErrorDetails::Sandbox(SandboxErr::Denied { output, .. }) => {
                        let output = output.as_ref().clone();
                        let message = if output.aggregated_output.text.is_empty() {
                            let exit_code = output.exit_code;
                            format!("Process exited with code {exit_code}")
                        } else {
                            output.aggregated_output.text.clone()
                        };
                        UnifiedExecError::sandbox_denied(message, output)
                    }
                    _ => UnifiedExecError::create_process(format!("{err:?}")),
                },
                other => UnifiedExecError::create_process(format!("{other:?}")),
            })
    }

    pub(super) async fn collect_output_until_deadline<const MAX_BYTES: usize>(
        output: &OutputHandles<MAX_BYTES>,
        mut pause_state: Option<watch::Receiver<bool>>,
        mut deadline: Instant,
    ) -> HeadTailBuffer<MAX_BYTES> {
        const POST_EXIT_CLOSE_WAIT_CAP: Duration = Duration::from_millis(50);

        let OutputHandles {
            output_buffer,
            output_notify,
            output_closed,
            output_closed_notify,
            cancellation_token,
        } = output;
        let mut collected = HeadTailBuffer::default();
        let mut exit_signal_received = cancellation_token.is_cancelled();
        let mut post_exit_deadline: Option<Instant> = None;
        loop {
            Self::extend_deadlines_while_paused(
                &mut pause_state,
                &mut deadline,
                &mut post_exit_deadline,
                cancellation_token,
            )
            .await;
            let drained_output: HeadTailBuffer<MAX_BYTES>;
            let has_drained_output: bool;
            let mut wait_for_output = None;
            {
                let mut guard = output_buffer.lock().await;
                drained_output = std::mem::take(&mut *guard);
                has_drained_output =
                    drained_output.retained_bytes() > 0 || drained_output.omitted_bytes() > 0;
                if !has_drained_output {
                    wait_for_output = Some(output_notify.notified());
                }
            }

            if !has_drained_output {
                exit_signal_received |= cancellation_token.is_cancelled();
                if exit_signal_received && output_closed.load(std::sync::atomic::Ordering::Acquire)
                {
                    break;
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining == Duration::ZERO {
                    break;
                }

                if exit_signal_received {
                    let now = Instant::now();
                    let close_wait_deadline = *post_exit_deadline
                        .get_or_insert_with(|| now + remaining.min(POST_EXIT_CLOSE_WAIT_CAP));
                    let close_wait_remaining = close_wait_deadline.saturating_duration_since(now);
                    if close_wait_remaining == Duration::ZERO {
                        break;
                    }
                    let notified = wait_for_output.unwrap_or_else(|| output_notify.notified());
                    let closed = output_closed_notify.notified();
                    tokio::pin!(notified);
                    tokio::pin!(closed);
                    tokio::select! {
                        _ = &mut notified => {}
                        _ = &mut closed => {}
                        _ = tokio::time::sleep(close_wait_remaining) => break,
                        _ = Self::wait_for_pause_change(pause_state.as_ref()) => {}
                    }
                    continue;
                }

                let notified = wait_for_output.unwrap_or_else(|| output_notify.notified());
                tokio::pin!(notified);
                let exit_notified = cancellation_token.cancelled();
                tokio::pin!(exit_notified);
                tokio::select! {
                    _ = &mut notified => {}
                    _ = &mut exit_notified => exit_signal_received = true,
                    _ = tokio::time::sleep(remaining) => break,
                    _ = Self::wait_for_pause_change(pause_state.as_ref()) => {}
                }
                continue;
            }

            collected.push_buffer(drained_output);

            exit_signal_received |= cancellation_token.is_cancelled();
            if Instant::now() >= deadline {
                break;
            }
        }

        collected
    }

    async fn extend_deadlines_while_paused(
        pause_state: &mut Option<watch::Receiver<bool>>,
        deadline: &mut Instant,
        post_exit_deadline: &mut Option<Instant>,
        cancellation_token: &CancellationToken,
    ) {
        let Some(receiver) = pause_state.as_mut() else {
            return;
        };
        if !*receiver.borrow() {
            return;
        }

        let paused_at = Instant::now();
        let mut process_exited = false;
        while *receiver.borrow() {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    process_exited = true;
                    break;
                }
                changed = receiver.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
            }
        }

        let paused_for = paused_at.elapsed();
        *deadline += paused_for;
        if let Some(post_exit_deadline) = post_exit_deadline.as_mut() {
            *post_exit_deadline += paused_for;
        }
        if process_exited {
            *pause_state = None;
        }
    }

    async fn wait_for_pause_change(pause_state: Option<&watch::Receiver<bool>>) {
        match pause_state {
            Some(pause_state) => {
                let mut receiver = pause_state.clone();
                let _ = receiver.changed().await;
            }
            None => std::future::pending::<()>().await,
        }
    }

    fn prune_processes_if_needed(store: &mut ProcessStore) -> Option<ProcessEntry> {
        if store.processes.len() < MAX_UNIFIED_EXEC_PROCESSES {
            return None;
        }

        let mut meta: Vec<(i32, Instant, bool)> = store
            .processes
            .iter()
            .filter(|(_, entry)| !entry.initial_exec_command_state.is_unrecorded())
            .map(|(id, entry)| (*id, entry.last_used, entry.process.has_exited()))
            .collect();
        let mut found_locked_exited_process = false;

        while let Some(process_id) = Self::process_id_to_prune_from_meta(&meta) {
            let candidate_process = store
                .processes
                .get(&process_id)
                .map(|entry| Arc::clone(&entry.process));
            let candidate_has_exited = candidate_process
                .as_ref()
                .is_some_and(|process| process.has_exited());
            if found_locked_exited_process && !candidate_has_exited {
                // The store may temporarily exceed its soft cap while an exited
                // process is publishing its terminal event. Do not evict a live
                // process just because that exited process is briefly locked.
                return None;
            }

            // Do not prune processes while write_stdin or terminal event
            // publication holds their interaction lock.
            if let Some(interaction_lock) = candidate_process
                .as_ref()
                .map(|process| process.interaction_lock())
                && let Ok(_interaction_guard) = interaction_lock.try_lock_owned()
            {
                return store.remove(process_id);
            }
            found_locked_exited_process |= candidate_has_exited
                || candidate_process.is_some_and(|process| process.has_exited());
            meta.retain(|(id, _, _)| *id != process_id);
        }

        None
    }

    // Centralized pruning policy so we can easily swap strategies later.
    fn process_id_to_prune_from_meta(meta: &[(i32, Instant, bool)]) -> Option<i32> {
        if meta.is_empty() {
            return None;
        }

        let mut by_recency = meta.to_vec();
        by_recency.sort_by_key(|(_, last_used, _)| Reverse(*last_used));
        let protected: HashSet<i32> = by_recency
            .iter()
            .take(8)
            .map(|(process_id, _, _)| *process_id)
            .collect();

        let mut lru = meta.to_vec();
        lru.sort_by_key(|(_, last_used, _)| *last_used);

        if let Some((process_id, _, _)) = lru
            .iter()
            .find(|(process_id, _, exited)| !protected.contains(process_id) && *exited)
        {
            return Some(*process_id);
        }

        lru.into_iter()
            .find(|(process_id, _, _)| !protected.contains(process_id))
            .map(|(process_id, _, _)| process_id)
    }

    pub(crate) async fn terminate_all_processes(&self) {
        let _watcher_lifecycle_guard = self.watcher_lifecycle_lock.lock().await;
        // Session teardown is a deliberate stop, not a crash: mark monitors
        // before killing so each watcher's terminal notification says so.
        for handle in self.monitor_store.lock().await.all() {
            handle.request_stop();
        }
        let (entries, processes) = {
            let mut processes = self.process_store.lock().await;
            let process_handles = processes
                .processes
                .values()
                .map(|entry| Arc::clone(&entry.process))
                .collect::<Vec<_>>();
            (processes.drain_unclaimed(), process_handles)
        };

        for entry in entries {
            unregister_network_approval_for_entry(&entry).await;
        }
        for process in processes {
            process.terminate();
        }
        self.join_exec_watchers_locked().await;
        self.join_monitor_watchers_locked().await;
    }

    pub(crate) async fn list_processes(&self) -> Vec<BackgroundTerminalInfo> {
        let store = self.process_store.lock().await;
        let mut entries = store
            .processes
            .values()
            .filter(|entry| !entry.process.has_exited())
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.process_id);
        entries
            .into_iter()
            .map(|entry| BackgroundTerminalInfo {
                item_id: entry.call_id.clone(),
                process_id: entry.process_id.to_string(),
                command: entry.hook_command.clone(),
                cwd: entry.cwd.clone(),
            })
            .collect()
    }

    pub(crate) async fn prepare_initial_exec_command_output_persistence(
        &self,
        outputs: &[(String, String)],
    ) -> InitialExecOutputPersistenceDecision {
        let mut extracted = Vec::new();
        if outputs.is_empty() {
            return InitialExecOutputPersistenceDecision {
                pending_outputs: extracted,
                process_store: Arc::clone(&self.process_store),
            };
        }
        let mut pending = self.initial_exec_outputs_pending_recording.lock().await;
        let call_ids = outputs
            .iter()
            .map(|(call_id, _)| call_id)
            .collect::<HashSet<_>>();
        for call_id in call_ids {
            let Some(mut states) = pending.remove(call_id) else {
                continue;
            };
            let recorded_process_ids = outputs
                .iter()
                .filter(|(output_call_id, _)| output_call_id == call_id)
                .filter_map(|(_, output)| {
                    output
                        .lines()
                        .take_while(|line| *line != "Output:")
                        .find_map(|line| line.strip_prefix("Process running with session ID "))
                        .and_then(|process_id| process_id.parse::<i32>().ok())
                })
                .collect::<HashSet<_>>();
            for pending_output in states.drain(..) {
                if pending_output.destination == InitialExecCommandOutputDestination::Rollout
                    && recorded_process_ids.contains(&pending_output.process_id)
                {
                    extracted.push(pending_output);
                } else {
                    pending
                        .entry(call_id.clone())
                        .or_default()
                        .push(pending_output);
                }
            }
        }
        InitialExecOutputPersistenceDecision {
            pending_outputs: extracted,
            process_store: Arc::clone(&self.process_store),
        }
    }

    pub(crate) async fn discard_unrecorded_initial_exec_command_outputs(&self) {
        let mut pending = self.initial_exec_outputs_pending_recording.lock().await;
        let mut abandoned = Vec::new();
        let mut retained = HashMap::new();
        for (call_id, states) in pending.drain() {
            for pending_output in states {
                if pending_output.destination == InitialExecCommandOutputDestination::Rollout {
                    pending_output.state.mark_not_yielded();
                    pending_output.process.terminate();
                    abandoned.push((pending_output.process_id, pending_output.state));
                } else {
                    retained
                        .entry(call_id.clone())
                        .or_insert_with(Vec::new)
                        .push(pending_output);
                }
            }
        }
        *pending = retained;
        drop(pending);
        self.remove_abandoned_initial_exec_processes(abandoned);
    }

    pub(crate) async fn acknowledge_code_mode_initial_exec_output(
        &self,
        call_id: &str,
        process_id: i32,
    ) {
        let mut pending = self.initial_exec_outputs_pending_recording.lock().await;
        let Some(mut states) = pending.remove(call_id) else {
            return;
        };
        for pending_output in states.drain(..) {
            if pending_output.destination == InitialExecCommandOutputDestination::CodeMode
                && pending_output.process_id == process_id
            {
                pending_output.state.mark_yielded();
            } else {
                pending
                    .entry(call_id.to_string())
                    .or_default()
                    .push(pending_output);
            }
        }
    }

    pub(crate) async fn discard_code_mode_initial_exec_outputs(&self, call_id: &str) {
        let mut pending = self.initial_exec_outputs_pending_recording.lock().await;
        let Some(states) = pending.remove(call_id) else {
            return;
        };
        let mut abandoned = Vec::new();
        for pending_output in states {
            if pending_output.destination == InitialExecCommandOutputDestination::CodeMode {
                pending_output.state.mark_not_yielded();
                pending_output.process.terminate();
                abandoned.push((pending_output.process_id, pending_output.state));
            } else {
                pending
                    .entry(call_id.to_string())
                    .or_default()
                    .push(pending_output);
            }
        }
        drop(pending);
        self.remove_abandoned_initial_exec_processes(abandoned);
    }

    fn remove_abandoned_initial_exec_processes(
        &self,
        abandoned: Vec<(i32, Arc<InitialExecCommandState>)>,
    ) {
        remove_abandoned_initial_exec_processes_from_store(
            Arc::clone(&self.process_store),
            abandoned,
        );
    }

    pub(crate) async fn terminate_process(&self, process_id: i32) -> bool {
        let (process, already_exited, network_approval, session) = {
            let store = self.process_store.lock().await;
            let Some(entry) = store.processes.get(&process_id) else {
                return false;
            };
            (
                Arc::clone(&entry.process),
                entry.process.has_exited(),
                entry.network_approval.clone(),
                entry.session.clone(),
            )
        };

        if !already_exited && process.terminate_confirmed().await.is_err() {
            return false;
        }

        // Keep the process result available until cancellation-prone cleanup
        // completes. If this future is dropped while unregistering, the exit
        // watcher can still claim and durably announce the terminal result.
        if let Some(network_approval) = network_approval
            && let Some(session) = session.upgrade()
        {
            session
                .services
                .network_approval
                .unregister_call(network_approval.registration_id())
                .await;
        }

        {
            let mut store = self.process_store.lock().await;
            let Some(entry) = store.processes.get(&process_id) else {
                return true;
            };
            if !Arc::ptr_eq(&entry.process, &process) {
                return true;
            }
            if entry.initial_exec_command_state.is_unrecorded() {
                return true;
            }
            // Once the watcher has announced a terminal result, retain the
            // entry so the notification-directed write_stdin poll can collect
            // it. Capacity pruning and session shutdown remain the bounded
            // cleanup paths.
            if entry
                .initial_exec_command_state
                .terminal_notification_claimed()
            {
                return true;
            }
            if store.remove_unclaimed(process_id).is_none() {
                return true;
            }
        }

        true
    }

    /// Register watcher metadata for a freshly spawned process and start its
    /// notification pump. Returns the output the process had already produced,
    /// which the caller must still account for in the tool result.
    ///
    /// A process starts writing the moment it is spawned, well before this runs
    /// — approvals and sandbox selection sit in between. A broadcast receiver
    /// only sees what is sent after it subscribes, so subscribing alone drops
    /// the head of the output, and the head is usually the line that says what
    /// started or why it failed. The complete bytes are in the process's own
    /// output buffer, so the seed is taken from there and the subscription is
    /// made under the same lock the reader takes to append: the reader cannot
    /// publish between the two.
    async fn attach_monitor_locked(
        &self,
        request: &ExecCommandRequest,
        context: &UnifiedExecContext,
        process: &Arc<UnifiedExecProcess>,
        transcript: &Arc<tokio::sync::Mutex<HeadTailBuffer>>,
        attachment: MonitorAttachment,
    ) -> Vec<u8> {
        let (transcript_seed, notification_seed, receiver) = {
            let mut output_buffer = process.output_handles().output_buffer.lock().await;
            let mut monitor_output_buffer = process.monitor_output_buffer().lock().await;
            let transcript_seed = output_buffer.drain();
            let notification_seed = monitor_output_buffer.take().unwrap_or_default();
            (
                transcript_seed.to_bytes_with_omission_marker(),
                notification_seed.to_bytes_with_omission_marker(),
                process.monitor_output_receiver(),
            )
        };

        // The transcript backs `monitor` `read`, so it retains both stdout and
        // stderr even though only the stdout seed is sent to the model.
        if !transcript_seed.is_empty() {
            transcript.lock().await.push_chunk(&transcript_seed);
        }

        let handle = Arc::new(MonitorHandle::new(
            request.process_id,
            attachment.command_display,
            request.cwd.to_string(),
            attachment.kind,
            attachment.owner,
            Arc::clone(process),
            Arc::clone(transcript),
        ));
        let mut monitor_store = self.monitor_store.lock().await;
        let mut monitor_watcher_tasks = self.monitor_watcher_tasks.lock().await;
        monitor_watcher_tasks.retain(|watcher| !watcher.is_finished());
        monitor_store.insert(Arc::clone(&handle));
        let watcher = spawn_monitor_watcher(
            handle,
            Arc::clone(&context.session),
            Arc::clone(&context.step_context.turn),
            attachment.timeout,
            notification_seed,
            receiver,
        );
        monitor_watcher_tasks.push(watcher);
        transcript_seed
    }

    async fn join_exec_watchers_locked(&self) {
        let watchers = std::mem::take(&mut *self.exec_watcher_tasks.lock().await);
        for watcher in watchers {
            if let Err(err) = watcher.await
                && !err.is_cancelled()
            {
                tracing::warn!("exec watcher task failed during shutdown: {err}");
            }
        }
    }

    async fn join_monitor_watchers_locked(&self) {
        let watchers = std::mem::take(&mut *self.monitor_watcher_tasks.lock().await);
        for watcher in watchers {
            if let Err(err) = watcher.await
                && !err.is_cancelled()
            {
                tracing::warn!("monitor watcher task failed during shutdown: {err}");
            }
        }
    }

    async fn monitor(&self, process_id: i32) -> Option<Arc<MonitorHandle>> {
        self.monitor_store.lock().await.get(process_id)
    }

    /// Every monitor this session started, including finished ones whose
    /// retained output is still readable.
    pub(crate) async fn list_monitors(&self) -> Vec<MonitorInfo> {
        let handles = self.monitor_store.lock().await.all();
        let mut infos = Vec::with_capacity(handles.len());
        for handle in handles {
            infos.push(handle.info().await);
        }
        infos
    }

    /// Read a monitor's bounded retained output, optionally acknowledging the
    /// notifications it has delivered.
    pub(crate) async fn read_monitor_output(
        &self,
        process_id: i32,
        acknowledgement: MonitorAcknowledgement,
    ) -> Option<MonitorOutput> {
        let handle = self.monitor(process_id).await?;
        Some(handle.output(acknowledgement).await)
    }

    /// Stop a monitor. The watcher still delivers the terminal notification —
    /// classified as `stopped` rather than as a bare exit code.
    pub(crate) async fn stop_monitor(&self, process_id: i32) -> Option<bool> {
        let handle = self.monitor(process_id).await?;
        if handle.state().is_terminal() {
            return Some(false);
        }
        handle.request_stop();
        if !self.terminate_process(process_id).await {
            // A short-lived monitor is never stored in the process store, and a
            // pruned one has already been removed; terminate the handle we hold.
            handle.process().terminate();
        }
        Some(true)
    }

    /// Wait for a monitor to reach a terminal state, giving up after `timeout`.
    pub(crate) async fn wait_for_monitor(
        &self,
        process_id: i32,
        timeout: Duration,
    ) -> Option<MonitorWaitOutcome> {
        let handle = self.monitor(process_id).await?;
        let completed = tokio::time::timeout(timeout, handle.wait_for_terminal())
            .await
            .is_ok();
        Some(MonitorWaitOutcome {
            completed,
            info: handle.info().await,
        })
    }
}

enum ProcessStatus {
    Alive {
        exit_code: Option<i32>,
        call_id: String,
        process_id: i32,
    },
    Exited {
        exit_code: Option<i32>,
        entry: Box<ProcessEntry>,
    },
    Unknown,
}

#[cfg(test)]
#[path = "process_manager_tests.rs"]
mod tests;
