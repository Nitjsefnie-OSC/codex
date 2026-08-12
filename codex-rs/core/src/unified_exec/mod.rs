//! Unified Exec: interactive process execution orchestrated with approvals + sandboxing.
//!
//! Responsibilities
//! - Manages interactive processes (create, reuse, buffer output with caps).
//! - Uses the shared ToolOrchestrator to handle approval, sandbox selection, and
//!   retry semantics in a single, descriptive flow.
//! - Spawns the PTY from a sandbox-transformed `ExecRequest`; on sandbox denial,
//!   retries without sandbox when policy allows (no re‑prompt thanks to caching).
//! - Uses the shared `is_likely_sandbox_denied` heuristic to keep denial messages
//!   consistent with other exec paths.
//!
//! Flow at a glance (open process)
//! 1) Build a small request `{ command, cwd }`.
//! 2) Orchestrator: approval (bypass/cache/prompt) → select sandbox → run.
//! 3) Runtime: transform `SandboxTransformRequest` -> `ExecRequest` -> spawn PTY.
//! 4) If denial, orchestrator retries with `SandboxType::None`.
//! 5) Process handle is returned with streaming output + metadata.
//!
//! This keeps policy logic and user interaction centralized while the PTY/process
//! concerns remain isolated here. The implementation is split between:
//! - `process.rs`: PTY process lifecycle + output buffering.
//! - `process_state.rs`: shared exit/failure state for local and remote processes.
//! - `process_manager.rs`: orchestration (approvals, sandboxing, reuse) and request handling.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;

use codex_network_proxy::NetworkProxy;
use codex_protocol::models::AdditionalPermissionProfile;
use codex_tools::UnifiedExecShellMode;
use codex_utils_output_truncation::TruncationPolicy;
use codex_utils_path_uri::PathUri;
use rand::Rng;
use rand::rng;
use tokio::sync::Mutex;
use tokio::sync::watch;

use crate::sandboxing::SandboxPermissions;
use crate::session::session::Session;
use crate::session::step_context::StepContext;
use crate::session::turn_context::TurnContext;
use crate::session::turn_context::TurnEnvironment;
use crate::shell::ShellType;
use crate::tools::network_approval::DeferredNetworkApproval;
use codex_core_plugins::PluginMetricsSidecar;

mod async_watcher;
mod errors;
mod head_tail_buffer;
mod monitor_watcher;
mod monitors;
mod process;
mod process_manager;
mod process_state;

pub(crate) fn set_deterministic_process_ids_for_tests(enabled: bool) {
    process_manager::set_deterministic_process_ids_for_tests(enabled);
}

pub(crate) use errors::UnifiedExecError;
#[cfg(test)]
pub(crate) use monitors::MAX_MONITOR_NOTIFICATIONS;
pub use monitors::MonitorAcknowledgement;
pub(crate) use monitors::MonitorAttachment;
pub use monitors::MonitorInfo;
pub use monitors::MonitorKind;
pub use monitors::MonitorOutput;
pub use monitors::MonitorOwner;
pub use monitors::MonitorState;
pub use monitors::MonitorWaitOutcome;
pub(crate) use process::NoopSpawnLifecycle;
#[cfg(unix)]
pub(crate) use process::SpawnLifecycle;
pub(crate) use process::SpawnLifecycleHandle;
pub(crate) use process::UnifiedExecProcess;

pub(crate) const MIN_YIELD_TIME_MS: u64 = 250;
pub(crate) const WINDOWS_INITIAL_EXEC_YIELD_TIME_FLOOR_MS: u64 = 10_000;
// Minimum yield time for an empty `write_stdin`.
pub(crate) const MIN_EMPTY_YIELD_TIME_MS: u64 = 5_000;
pub(crate) const MAX_YIELD_TIME_MS: u64 = 30_000;
pub(crate) const DEFAULT_MAX_BACKGROUND_TERMINAL_TIMEOUT_MS: u64 = 300_000;
pub(crate) const DEFAULT_MAX_OUTPUT_TOKENS: usize = 10_000;
pub(crate) const UNIFIED_EXEC_OUTPUT_MAX_BYTES: usize = 1024 * 1024; // 1 MiB
pub(crate) const UNIFIED_EXEC_OUTPUT_MAX_TOKENS: usize = UNIFIED_EXEC_OUTPUT_MAX_BYTES / 4;
pub(crate) const MAX_UNIFIED_EXEC_PROCESSES: usize = 64;

pub(crate) struct UnifiedExecContext {
    pub session: Arc<Session>,
    pub step_context: Arc<StepContext>,
    pub call_id: String,
    pub initial_output_destination: InitialExecCommandOutputDestination,
}

impl UnifiedExecContext {
    pub fn new(
        session: Arc<Session>,
        step_context: Arc<StepContext>,
        call_id: String,
        initial_output_destination: InitialExecCommandOutputDestination,
    ) -> Self {
        Self {
            session,
            step_context,
            call_id,
            initial_output_destination,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InitialExecCommandOutputDestination {
    Rollout,
    CodeMode,
    Untracked,
}

#[derive(Debug)]
pub(crate) struct ExecCommandRequest {
    pub command: Vec<String>,
    pub shell_type: ShellType,
    pub hook_command: String,
    pub process_id: i32,
    pub yield_time_ms: u64,
    pub max_output_tokens: Option<usize>,
    pub cwd: PathUri,
    pub sandbox_cwd: PathUri,
    pub turn_environment: TurnEnvironment,
    pub shell_mode: UnifiedExecShellMode,
    pub network: Option<NetworkProxy>,
    pub tty: bool,
    pub sandbox_permissions: SandboxPermissions,
    pub additional_permissions: Option<AdditionalPermissionProfile>,
    pub additional_permissions_preapproved: bool,
    pub justification: Option<String>,
    pub prefix_rule: Option<Vec<String>>,
}

#[derive(Debug)]
pub(crate) struct WriteStdinRequest<'a> {
    pub process_id: i32,
    pub input: &'a str,
    pub yield_time_ms: u64,
    pub max_output_tokens: Option<usize>,
    pub truncation_policy: TruncationPolicy,
    pub interaction_event: Option<WriteStdinInteractionEvent<'a>>,
}

pub(crate) struct WriteStdinInteractionEvent<'a> {
    pub session: &'a Arc<Session>,
    pub turn: &'a Arc<TurnContext>,
}

impl std::fmt::Debug for WriteStdinInteractionEvent<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("WriteStdinInteractionEvent")
    }
}

#[derive(Default)]
pub(crate) struct ProcessStore {
    processes: HashMap<i32, ProcessEntry>,
    reserved_process_ids: HashSet<i32>,
    reservation_owners: HashMap<i32, Weak<UnifiedExecProcess>>,
}

impl ProcessStore {
    /// Remove entries that have not already promised a retrievable terminal
    /// result. Claimed notifications and initial results awaiting recording
    /// keep their entries until `write_stdin` or later cleanup.
    fn drain_unclaimed(&mut self) -> Vec<ProcessEntry> {
        let process_ids = self.processes.keys().copied().collect::<Vec<_>>();
        let mut removed = Vec::with_capacity(process_ids.len());
        for process_id in process_ids {
            let result_is_unrecorded = self
                .processes
                .get(&process_id)
                .is_some_and(|entry| entry.initial_exec_command_state.is_unrecorded());
            if result_is_unrecorded {
                continue;
            }
            if let Some(entry) = self.remove_unclaimed(process_id) {
                removed.push(entry);
            }
        }
        removed
    }

    fn remove_unclaimed(&mut self, process_id: i32) -> Option<ProcessEntry> {
        let removal_reserved = self.processes.get(&process_id).is_some_and(|entry| {
            entry
                .initial_exec_command_state
                .reserve_unclaimed_terminal_result_removal()
        });
        if !removal_reserved {
            return None;
        }
        self.remove(process_id)
    }

    fn remove(&mut self, process_id: i32) -> Option<ProcessEntry> {
        self.reserved_process_ids.remove(&process_id);
        self.reservation_owners.remove(&process_id);
        let entry = self.processes.remove(&process_id)?;
        entry
            .initial_exec_command_state
            .mark_terminal_result_unavailable();
        Some(entry)
    }
}

pub(crate) struct UnifiedExecProcessManager {
    process_store: Arc<Mutex<ProcessStore>>,
    initial_exec_outputs_pending_recording:
        Mutex<HashMap<String, Vec<PendingInitialExecCommandOutput>>>,
    /// Watcher metadata for the subset of `process_store` started as monitors.
    /// Not a second process registry — see [`monitors`].
    monitor_store: Mutex<monitors::MonitorStore>,
    /// Serializes watcher registration with shutdown so a newly spawned
    /// watcher cannot be missed by the shutdown join.
    watcher_lifecycle_lock: Mutex<()>,
    shutdown_started: AtomicBool,
    exec_watcher_tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    monitor_watcher_tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    max_write_stdin_yield_time_ms: u64,
}

impl UnifiedExecProcessManager {
    pub(crate) fn new(max_write_stdin_yield_time_ms: u64) -> Self {
        Self {
            process_store: Arc::new(Mutex::new(ProcessStore::default())),
            initial_exec_outputs_pending_recording: Mutex::new(HashMap::new()),
            monitor_store: Mutex::new(monitors::MonitorStore::default()),
            watcher_lifecycle_lock: Mutex::new(()),
            shutdown_started: AtomicBool::new(false),
            exec_watcher_tasks: Mutex::new(Vec::new()),
            monitor_watcher_tasks: Mutex::new(Vec::new()),
            max_write_stdin_yield_time_ms: max_write_stdin_yield_time_ms
                .max(MIN_EMPTY_YIELD_TIME_MS),
        }
    }
}

impl Default for UnifiedExecProcessManager {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_BACKGROUND_TERMINAL_TIMEOUT_MS)
    }
}

struct ProcessEntry {
    process: Arc<UnifiedExecProcess>,
    plugin_metrics_sidecar: Option<SharedPluginMetricsSidecar>,
    call_id: String,
    process_id: i32,
    cwd: PathUri,
    initial_exec_command_state: Arc<InitialExecCommandState>,
    hook_command: String,
    tty: bool,
    network_approval: Option<DeferredNetworkApproval>,
    session: Weak<Session>,
    last_used: tokio::time::Instant,
}

type SharedPluginMetricsSidecar = Arc<std::sync::Mutex<Option<PluginMetricsSidecar>>>;

fn take_plugin_metrics_sidecar(
    sidecar: &SharedPluginMetricsSidecar,
) -> Option<PluginMetricsSidecar> {
    sidecar
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

struct PendingInitialExecCommandOutput {
    process_id: i32,
    state: Arc<InitialExecCommandState>,
    process: Arc<UnifiedExecProcess>,
    destination: InitialExecCommandOutputDestination,
}

/// Owns the rollout exec results whose persistence outcome has not yet been
/// decided. Dropping an uncommitted decision suppresses their completion
/// notifications and terminates their processes.
pub(crate) struct InitialExecOutputPersistenceDecision {
    pending_outputs: Vec<PendingInitialExecCommandOutput>,
    process_store: Arc<Mutex<ProcessStore>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InitialExecCommandOutcome {
    Pending,
    Returned,
    Yielded,
    NotYielded,
}

/// Coordinates process exit with the initial `exec_command` result.
///
/// A process may exit after the manager decides to return a session id but
/// before that result is recorded. The exit watcher waits for this state until
/// rollout recording acknowledges the result, then atomically claims the
/// single model-visible terminal notification.
struct InitialExecCommandState {
    outcome_tx: watch::Sender<InitialExecCommandOutcome>,
    terminal_state: AtomicU8,
}

const TERMINAL_NOTIFICATION_CLAIMED: u8 = 1 << 0;
const TERMINAL_RESULT_AVAILABLE: u8 = 1 << 1;

impl InitialExecCommandState {
    fn new() -> Self {
        let (outcome_tx, _) = watch::channel(InitialExecCommandOutcome::Pending);
        Self {
            outcome_tx,
            terminal_state: AtomicU8::new(TERMINAL_RESULT_AVAILABLE),
        }
    }

    #[cfg(test)]
    fn resolved(outcome: InitialExecCommandOutcome) -> Self {
        let state = Self::new();
        state.resolve(outcome);
        state
    }

    fn is_unrecorded(&self) -> bool {
        matches!(
            *self.outcome_tx.borrow(),
            InitialExecCommandOutcome::Pending | InitialExecCommandOutcome::Returned
        )
    }

    fn mark_yielded(&self) {
        self.resolve_returned(InitialExecCommandOutcome::Yielded);
    }

    fn mark_returned(&self) {
        self.resolve_pending(InitialExecCommandOutcome::Returned);
    }

    fn mark_not_yielded(&self) {
        let _ = self.outcome_tx.send_if_modified(|current| {
            if matches!(
                current,
                InitialExecCommandOutcome::Pending | InitialExecCommandOutcome::Returned
            ) {
                *current = InitialExecCommandOutcome::NotYielded;
                true
            } else {
                false
            }
        });
    }

    async fn claim_terminal_notification(&self) -> Option<bool> {
        let mut outcome_rx = self.outcome_tx.subscribe();
        let outcome = *outcome_rx.borrow_and_update();
        let outcome = if matches!(
            outcome,
            InitialExecCommandOutcome::Pending | InitialExecCommandOutcome::Returned
        ) {
            match outcome_rx
                .wait_for(|outcome| {
                    matches!(
                        outcome,
                        InitialExecCommandOutcome::Yielded | InitialExecCommandOutcome::NotYielded
                    )
                })
                .await
            {
                Ok(outcome) => *outcome,
                Err(_) => InitialExecCommandOutcome::NotYielded,
            }
        } else {
            outcome
        };
        if outcome != InitialExecCommandOutcome::Yielded {
            return None;
        }
        let previous = self
            .terminal_state
            .fetch_or(TERMINAL_NOTIFICATION_CLAIMED, Ordering::AcqRel);
        (previous & TERMINAL_NOTIFICATION_CLAIMED == 0)
            .then_some(previous & TERMINAL_RESULT_AVAILABLE != 0)
    }

    #[cfg(test)]
    fn terminal_result_available(&self) -> bool {
        self.terminal_state.load(Ordering::Acquire) & TERMINAL_RESULT_AVAILABLE != 0
    }

    fn terminal_notification_claimed(&self) -> bool {
        self.terminal_state.load(Ordering::Acquire) & TERMINAL_NOTIFICATION_CLAIMED != 0
    }

    fn mark_terminal_result_unavailable(&self) {
        self.terminal_state
            .fetch_and(!TERMINAL_RESULT_AVAILABLE, Ordering::AcqRel);
    }

    fn reserve_unclaimed_terminal_result_removal(&self) -> bool {
        let mut current = self.terminal_state.load(Ordering::Acquire);
        loop {
            if current & TERMINAL_NOTIFICATION_CLAIMED != 0 {
                return false;
            }
            match self.terminal_state.compare_exchange_weak(
                current,
                current & !TERMINAL_RESULT_AVAILABLE,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    #[cfg(test)]
    fn resolve(&self, outcome: InitialExecCommandOutcome) {
        self.resolve_pending(outcome);
    }

    fn resolve_pending(&self, outcome: InitialExecCommandOutcome) {
        let _ = self.outcome_tx.send_if_modified(|current| {
            if *current != InitialExecCommandOutcome::Pending {
                return false;
            }
            *current = outcome;
            true
        });
    }

    fn resolve_returned(&self, outcome: InitialExecCommandOutcome) {
        let _ = self.outcome_tx.send_if_modified(|current| {
            if *current != InitialExecCommandOutcome::Returned {
                return false;
            }
            *current = outcome;
            true
        });
    }
}

pub(crate) fn clamp_yield_time(yield_time_ms: u64) -> u64 {
    let yield_time_ms = if cfg!(windows) {
        yield_time_ms.max(WINDOWS_INITIAL_EXEC_YIELD_TIME_FLOOR_MS)
    } else {
        yield_time_ms
    };
    yield_time_ms.clamp(MIN_YIELD_TIME_MS, MAX_YIELD_TIME_MS)
}

pub(crate) fn resolve_max_tokens(max_tokens: Option<usize>) -> usize {
    max_tokens.unwrap_or(DEFAULT_MAX_OUTPUT_TOKENS)
}

pub(crate) fn format_output_omission_marker(omitted_bytes: usize) -> String {
    format!("... {omitted_bytes} bytes omitted ...")
}

pub(crate) fn generate_chunk_id() -> String {
    let mut rng = rng();
    (0..6)
        .map(|_| format!("{:x}", rng.random_range(0..16)))
        .collect()
}

#[cfg(test)]
#[cfg(unix)]
#[path = "process_tests.rs"]
mod process_tests;
#[cfg(test)]
#[cfg(unix)]
#[path = "mod_tests.rs"]
mod tests;
