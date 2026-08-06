//! Watcher metadata for monitored unified-exec processes.
//!
//! The processes themselves live in [`UnifiedExecProcessManager`]'s process
//! store — this module deliberately keeps **no** second process registry. What
//! it adds is the metadata unified exec has no opinion about: whether a process
//! is a persistent watcher or a job that will finish, which agent started it,
//! what its terminal state was, how many model-visible notifications it has
//! produced, and how far the model has acknowledged reading them.
//!
//! A record outlives its process entry on purpose. `terminate_process` removes
//! the entry from the process store and `list_processes` hides exited ones, but
//! a monitor's retained output has to stay readable after the fact — so a
//! record holds an `Arc` of the *same* bounded transcript buffer unified exec
//! fills, not a copy of it.
//!
//! [`UnifiedExecProcessManager`]: super::UnifiedExecProcessManager

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use serde::Serialize;
use tokio::sync::watch;
use tokio::time::Instant;

use super::head_tail_buffer::HeadTailBuffer;
use super::process::UnifiedExecProcess;

/// Retained monitor records, including finished ones. Older terminal records are
/// evicted first so a long session cannot accumulate transcripts without bound.
pub(crate) const MAX_RETAINED_MONITORS: usize = 64;

/// Non-terminal notifications a single monitor may deliver to the model. Past
/// this, batches are counted and dropped; the retained output still has them,
/// and the terminal notification reports how many were suppressed.
pub(crate) const MAX_MONITOR_NOTIFICATIONS: u64 = 20;

/// Complete lines carried by one notification. A firehose batch is truncated to
/// this and reports the remainder as `omitted_lines`.
pub(crate) const MAX_LINES_PER_NOTIFICATION: usize = 40;

/// Byte ceiling for the lines carried by one notification.
pub(crate) const MAX_NOTIFICATION_BYTES: usize = 4096;

/// Whether a monitor is expected to finish on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorKind {
    /// Will terminate on its own: a build, a test run, a deploy.
    Job,
    /// Runs until it is stopped or the session ends: a log tail, a dev server.
    Watcher,
}

impl MonitorKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Job => "job",
            Self::Watcher => "watcher",
        }
    }
}

/// Terminal classification of a monitor. `Running` is the only non-terminal one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum MonitorState {
    Running,
    Exited {
        exit_code: i32,
    },
    Failed {
        message: String,
    },
    /// Terminated by an explicit `stop`, or by session teardown.
    Stopped,
    TimedOut,
}

impl MonitorState {
    pub fn is_terminal(&self) -> bool {
        !matches!(self, Self::Running)
    }

    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Running => "running".to_string(),
            Self::Exited { exit_code } => format!("exited with code {exit_code}"),
            Self::Failed { message } => format!("failed: {message}"),
            Self::Stopped => "stopped".to_string(),
            Self::TimedOut => "timed out".to_string(),
        }
    }
}

/// Which agent started a monitor, so a caller can tell its own watchers from
/// another turn's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MonitorOwner {
    /// Model slug of the turn that started the monitor.
    pub model_slug: String,
    /// Submission id of that turn.
    pub sub_id: String,
    /// Tool call id of the `monitor` invocation that started it.
    pub call_id: String,
}

/// Snapshot of one monitor, for `list`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MonitorInfo {
    pub process_id: i32,
    pub command: String,
    pub cwd: String,
    pub kind: MonitorKind,
    pub owner: MonitorOwner,
    pub state: MonitorState,
    /// Seconds since the monitor started.
    pub age_seconds: f64,
    pub notifications_delivered: u64,
    pub notifications_suppressed: u64,
    /// Sequence number of the newest notification delivered, 0 if none.
    pub last_notification_seq: u64,
    /// Newest sequence number the caller has acknowledged reading.
    pub acknowledged_seq: u64,
    /// Delivered notifications the caller has not acknowledged.
    pub unacknowledged_notifications: u64,
    /// Bytes currently retained for `read`.
    pub retained_bytes: usize,
    /// Bytes the process has produced, including any dropped by the cap.
    pub total_bytes: usize,
}

/// How far a `read` acknowledges the notifications it consumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonitorAcknowledgement {
    /// Read without acknowledging.
    None,
    /// Acknowledge everything delivered so far.
    All,
    /// Acknowledge up to and including this sequence number.
    Through(u64),
}

/// Retained output of one monitor, for `read`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MonitorOutput {
    pub process_id: i32,
    pub state: MonitorState,
    /// Bounded retained output, with an omission marker where bytes were
    /// dropped by unified exec's output cap.
    pub output: String,
    /// Whether the cap dropped any bytes.
    pub truncated: bool,
    pub acknowledged_seq: u64,
    pub unacknowledged_notifications: u64,
}

/// Result of waiting on a monitor.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MonitorWaitOutcome {
    /// Whether the monitor reached a terminal state before the wait expired.
    pub completed: bool,
    pub info: MonitorInfo,
}

/// Everything a monitored process needs beyond an ordinary unified exec.
pub(crate) struct MonitorAttachment {
    pub kind: MonitorKind,
    pub owner: MonitorOwner,
    /// Human-readable command, used in notifications and `list`.
    pub command_display: String,
    /// Lifetime ceiling. `None` for a watcher that runs until the session ends.
    pub timeout: Option<std::time::Duration>,
}

/// Mutable notification bookkeeping. Guarded by a std mutex because every
/// operation on it is a handful of integer updates and never awaits.
#[derive(Debug, Default)]
pub(crate) struct MonitorCounters {
    next_seq: u64,
    delivered: u64,
    suppressed: u64,
    last_seq: u64,
    acknowledged_seq: u64,
}

impl MonitorCounters {
    /// Reserve a sequence number for a batch notification, or report that the
    /// monitor's notification cap has already been spent.
    fn reserve(&mut self) -> NotificationSlot {
        if self.delivered >= MAX_MONITOR_NOTIFICATIONS {
            self.suppressed += 1;
            return NotificationSlot::Suppressed;
        }
        NotificationSlot::Allowed {
            seq: self.advance(),
        }
    }

    /// Reserve the sequence number for the single terminal notification. The
    /// terminal notification is never capped — it is the one message that must
    /// always arrive.
    fn reserve_terminal(&mut self) -> u64 {
        self.advance()
    }

    /// Record how far the caller has consumed the notifications. Acknowledgement
    /// only moves forward, and never past what was delivered.
    fn acknowledge(&mut self, acknowledgement: MonitorAcknowledgement) -> u64 {
        let target = match acknowledgement {
            MonitorAcknowledgement::None => return self.acknowledged_seq,
            MonitorAcknowledgement::All => self.last_seq,
            MonitorAcknowledgement::Through(seq) => seq.min(self.last_seq),
        };
        self.acknowledged_seq = self.acknowledged_seq.max(target);
        self.acknowledged_seq
    }

    fn unacknowledged(&self) -> u64 {
        self.last_seq.saturating_sub(self.acknowledged_seq)
    }

    fn advance(&mut self) -> u64 {
        self.next_seq += 1;
        self.delivered += 1;
        self.last_seq = self.next_seq;
        self.next_seq
    }
}

/// A monitor's metadata plus shared handles to the unified-exec process and its
/// bounded transcript.
pub(crate) struct MonitorHandle {
    pub(crate) process_id: i32,
    command: String,
    cwd: String,
    kind: MonitorKind,
    owner: MonitorOwner,
    started_at: Instant,
    process: Arc<UnifiedExecProcess>,
    transcript: Arc<tokio::sync::Mutex<HeadTailBuffer>>,
    state_tx: watch::Sender<MonitorState>,
    terminal_emitted: AtomicBool,
    stop_requested: AtomicBool,
    counters: Mutex<MonitorCounters>,
}

/// Outcome of reserving the next notification sequence number.
pub(crate) enum NotificationSlot {
    /// Deliver the notification under this sequence number.
    Allowed { seq: u64 },
    /// The monitor is past its cap; the batch was counted, not sent.
    Suppressed,
}

impl MonitorHandle {
    pub(crate) fn new(
        process_id: i32,
        command: String,
        cwd: String,
        kind: MonitorKind,
        owner: MonitorOwner,
        process: Arc<UnifiedExecProcess>,
        transcript: Arc<tokio::sync::Mutex<HeadTailBuffer>>,
    ) -> Self {
        let (state_tx, _) = watch::channel(MonitorState::Running);
        Self {
            process_id,
            command,
            cwd,
            kind,
            owner,
            started_at: Instant::now(),
            process,
            transcript,
            state_tx,
            terminal_emitted: AtomicBool::new(false),
            stop_requested: AtomicBool::new(false),
            counters: Mutex::new(MonitorCounters::default()),
        }
    }

    /// Record that the monitor is being stopped on purpose, so the watcher
    /// classifies the termination as `Stopped` rather than a bare exit code.
    pub(crate) fn request_stop(&self) {
        self.stop_requested.store(true, Ordering::Release);
    }

    pub(crate) fn stop_requested(&self) -> bool {
        self.stop_requested.load(Ordering::Acquire)
    }

    pub(crate) fn command(&self) -> &str {
        &self.command
    }

    pub(crate) fn kind(&self) -> MonitorKind {
        self.kind
    }

    pub(crate) fn process(&self) -> &Arc<UnifiedExecProcess> {
        &self.process
    }

    pub(crate) fn state(&self) -> MonitorState {
        self.state_tx.borrow().clone()
    }

    pub(crate) fn reserve_notification(&self) -> NotificationSlot {
        self.counters().reserve()
    }

    pub(crate) fn reserve_terminal_notification(&self) -> u64 {
        self.counters().reserve_terminal()
    }

    pub(crate) fn suppressed_notifications(&self) -> u64 {
        self.counters().suppressed
    }

    /// Record a terminal state. Returns `false` when one was already recorded,
    /// which is how the watcher guarantees exactly one terminal notification.
    pub(crate) fn claim_terminal(&self, state: MonitorState) -> bool {
        if self
            .terminal_emitted
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        let _ = self.state_tx.send(state);
        true
    }

    pub(crate) fn acknowledge(&self, acknowledgement: MonitorAcknowledgement) -> u64 {
        self.counters().acknowledge(acknowledgement)
    }

    pub(crate) async fn info(&self) -> MonitorInfo {
        let (retained_bytes, total_bytes) = {
            let transcript = self.transcript.lock().await;
            (transcript.retained_bytes(), transcript.total_bytes())
        };
        let counters = self.counters();
        MonitorInfo {
            process_id: self.process_id,
            command: self.command.clone(),
            cwd: self.cwd.clone(),
            kind: self.kind,
            owner: self.owner.clone(),
            state: self.state(),
            age_seconds: Instant::now()
                .saturating_duration_since(self.started_at)
                .as_secs_f64(),
            notifications_delivered: counters.delivered,
            notifications_suppressed: counters.suppressed,
            last_notification_seq: counters.last_seq,
            acknowledged_seq: counters.acknowledged_seq,
            unacknowledged_notifications: counters.unacknowledged(),
            retained_bytes,
            total_bytes,
        }
    }

    pub(crate) async fn output(&self, acknowledgement: MonitorAcknowledgement) -> MonitorOutput {
        let (output, truncated) = {
            let transcript = self.transcript.lock().await;
            (
                String::from_utf8_lossy(&transcript.to_bytes_with_omission_marker()).to_string(),
                transcript.omitted_bytes() > 0,
            )
        };
        self.acknowledge(acknowledgement);
        let counters = self.counters();
        MonitorOutput {
            process_id: self.process_id,
            state: self.state(),
            output,
            truncated,
            acknowledged_seq: counters.acknowledged_seq,
            unacknowledged_notifications: counters.unacknowledged(),
        }
    }

    /// Wait until the monitor reaches a terminal state.
    pub(crate) async fn wait_for_terminal(&self) -> MonitorState {
        let mut receiver = self.state_tx.subscribe();
        match receiver
            .wait_for(MonitorState::is_terminal)
            .await
            .map(|state| state.clone())
        {
            Ok(state) => state,
            // The sender lives on the handle, so it cannot be dropped while
            // this borrow is alive; fall back to the last known state anyway.
            Err(_) => self.state(),
        }
    }

    fn counters(&self) -> std::sync::MutexGuard<'_, MonitorCounters> {
        self.counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Monitor metadata keyed by the unified-exec process id that owns the process.
#[derive(Default)]
pub(crate) struct MonitorStore {
    monitors: HashMap<i32, Arc<MonitorHandle>>,
    /// Insertion order, used to evict the oldest record when full.
    order: Vec<i32>,
}

impl MonitorStore {
    pub(crate) fn insert(&mut self, handle: Arc<MonitorHandle>) {
        let process_id = handle.process_id;
        self.evict_if_needed();
        if self.monitors.insert(process_id, handle).is_none() {
            self.order.push(process_id);
        }
    }

    pub(crate) fn get(&self, process_id: i32) -> Option<Arc<MonitorHandle>> {
        self.monitors.get(&process_id).map(Arc::clone)
    }

    pub(crate) fn all(&self) -> Vec<Arc<MonitorHandle>> {
        self.order
            .iter()
            .filter_map(|process_id| self.monitors.get(process_id).map(Arc::clone))
            .collect()
    }

    /// Evict the oldest terminal record, falling back to the oldest record of
    /// any state when every retained monitor is still running.
    fn evict_if_needed(&mut self) {
        if self.monitors.len() < MAX_RETAINED_MONITORS {
            return;
        }
        let victim = self
            .order
            .iter()
            .copied()
            .find(|process_id| {
                self.monitors
                    .get(process_id)
                    .is_some_and(|handle| handle.state().is_terminal())
            })
            .or_else(|| self.order.first().copied());
        if let Some(victim) = victim {
            self.monitors.remove(&victim);
            self.order.retain(|process_id| *process_id != victim);
        }
    }
}

#[cfg(test)]
#[path = "monitors_tests.rs"]
mod tests;
