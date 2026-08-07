//! The model-facing half of a monitor.
//!
//! [`super::async_watcher`] streams a unified-exec process's output to the
//! *client* as `ExecCommandOutputDelta` events and emits one `ExecCommandEnd`
//! item. The model sees none of that. This watcher runs beside it on the same
//! process, batching **complete lines** into bounded, sequenced notifications
//! injected into the active turn, and delivering exactly one terminal
//! notification whatever the outcome — clean exit, non-zero exit, failure,
//! stop, or timeout.
//!
//! It reuses the process's own broadcast output channel and cancellation token,
//! so it adds no polling and no second copy of the output: the retained bytes
//! stay in unified exec's bounded transcript buffer, readable afterwards
//! through `monitor` `read`.

use std::sync::Arc;

use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::error::TryRecvError;
use tokio::time::Duration;
use tokio::time::Instant;

use super::async_watcher::TRAILING_OUTPUT_GRACE;
use super::monitors::MAX_LINES_PER_NOTIFICATION;
use super::monitors::MAX_NOTIFICATION_BYTES;
use super::monitors::MonitorHandle;
use super::monitors::MonitorState;
use super::monitors::NotificationSlot;
use crate::context::MonitorNotification;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;

/// How long complete lines accumulate before they are delivered as one
/// notification. Long enough that a chatty build does not become one
/// notification per line, short enough to stay useful while work is in flight.
const BATCH_INTERVAL: Duration = Duration::from_millis(500);

/// A line that never terminates (a progress bar redrawing with `\r`) is flushed
/// once it reaches this many bytes so it cannot buffer without bound.
const MAX_PARTIAL_LINE_BYTES: usize = MAX_NOTIFICATION_BYTES;

/// Spawn the notification pump for a monitored process.
///
/// The task outlives the turn that started the monitor: a watcher runs until it
/// is stopped or the session tears its processes down.
/// `seed` is the output the process produced before `receiver` existed; the
/// caller takes both under the process's output lock so nothing falls between
/// them. `receiver` is passed in rather than subscribed here because a receiver
/// only sees chunks published after it is created.
pub(crate) fn spawn_monitor_watcher(
    handle: Arc<MonitorHandle>,
    session: Arc<Session>,
    turn: Arc<TurnContext>,
    timeout: Option<Duration>,
    seed: Vec<u8>,
    receiver: tokio::sync::broadcast::Receiver<Vec<u8>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        run_monitor_watcher(handle, session, turn, timeout, seed, receiver).await;
    })
}

async fn run_monitor_watcher(
    handle: Arc<MonitorHandle>,
    session: Arc<Session>,
    turn: Arc<TurnContext>,
    timeout: Option<Duration>,
    seed: Vec<u8>,
    mut receiver: tokio::sync::broadcast::Receiver<Vec<u8>>,
) {
    let process = Arc::clone(handle.process());
    let exit_token = process.cancellation_token();

    let mut pending = seed;
    let mut batch = Vec::<String>::new();
    let mut flush_deadline: Option<Instant> = None;
    let mut timeout_deadline = timeout.map(|timeout| Instant::now() + timeout);
    let mut exit_grace: Option<Instant> = None;
    let mut timed_out = false;
    let mut lagged = false;

    // The head of the output is already in `pending`; it becomes the first
    // notification exactly as if it had arrived over the channel.
    if ingest(&mut pending, &mut batch) {
        flush_deadline = None;
        deliver_batch(&handle, &session, &turn, std::mem::take(&mut batch)).await;
    } else if !batch.is_empty() {
        flush_deadline = Some(Instant::now() + BATCH_INTERVAL);
    }

    loop {
        let next_deadline = [flush_deadline, timeout_deadline, exit_grace]
            .into_iter()
            .flatten()
            .min();

        tokio::select! {
            _ = exit_token.cancelled(), if exit_grace.is_none() => {
                // Give the reader a moment to publish the last chunk the
                // process wrote before it exited.
                exit_grace = Some(Instant::now() + TRAILING_OUTPUT_GRACE);
            }

            () = sleep_until_optional(next_deadline) => {
                let now = Instant::now();
                if timeout_deadline.is_some_and(|deadline| deadline <= now) {
                    timeout_deadline = None;
                    timed_out = true;
                    process.terminate();
                }
                if flush_deadline.is_some_and(|deadline| deadline <= now) {
                    flush_deadline = None;
                    deliver_batch(&handle, &session, &turn, std::mem::take(&mut batch)).await;
                }
                if exit_grace.is_some_and(|deadline| deadline <= now) {
                    break;
                }
            }

            received = receiver.recv() => {
                match received {
                    Ok(chunk) => {
                        pending.extend_from_slice(&chunk);
                        if ingest(&mut pending, &mut batch) {
                            flush_deadline = None;
                            deliver_batch(&handle, &session, &turn, std::mem::take(&mut batch)).await;
                        } else if !batch.is_empty() && flush_deadline.is_none() {
                            flush_deadline = Some(Instant::now() + BATCH_INTERVAL);
                        }
                    }
                    // The retained transcript still holds the dropped bytes, so
                    // note the gap and keep going rather than tearing down.
                    Err(RecvError::Lagged(_)) => lagged = true,
                    Err(RecvError::Closed) => break,
                }
            }
        }
    }

    // Output producers publish every chunk before closing the channel, so a
    // non-blocking drain here is the last safe read.
    loop {
        match receiver.try_recv() {
            Ok(chunk) => pending.extend_from_slice(&chunk),
            Err(TryRecvError::Lagged(_)) => lagged = true,
            Err(TryRecvError::Empty | TryRecvError::Closed) => break,
        }
    }
    take_complete_lines(&mut pending, &mut batch);
    if !pending.is_empty() {
        batch.push(String::from_utf8_lossy(&pending).to_string());
        pending.clear();
    }
    if !batch.is_empty() {
        deliver_batch(&handle, &session, &turn, std::mem::take(&mut batch)).await;
    }

    let state = terminal_state(&handle, timed_out);
    deliver_terminal(&handle, &session, &turn, state, lagged).await;
}

/// Move complete lines from `pending` into `batch`, reporting whether the batch
/// is now full enough to send without waiting for the batching interval.
fn ingest(pending: &mut Vec<u8>, batch: &mut Vec<String>) -> bool {
    take_complete_lines(pending, batch);
    batch.len() >= MAX_LINES_PER_NOTIFICATION
}

/// Sleep until `deadline`, or forever when there is no deadline to wait for.
async fn sleep_until_optional(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

fn terminal_state(handle: &MonitorHandle, timed_out: bool) -> MonitorState {
    if timed_out {
        return MonitorState::TimedOut;
    }
    if let Some(message) = handle.process().failure_message() {
        return MonitorState::Failed { message };
    }
    if handle.stop_requested() {
        return MonitorState::Stopped;
    }
    MonitorState::Exited {
        exit_code: handle.process().exit_code().unwrap_or(-1),
    }
}

/// Move every complete line out of `pending` and into `batch`, holding back a
/// trailing partial line until it terminates — unless it has grown past the
/// point where waiting is worse than emitting it unaligned.
fn take_complete_lines(pending: &mut Vec<u8>, batch: &mut Vec<String>) {
    while let Some(index) = pending.iter().position(|byte| *byte == b'\n') {
        let mut line: Vec<u8> = pending.drain(..=index).collect();
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        batch.push(String::from_utf8_lossy(&line).to_string());
    }
    if pending.len() >= MAX_PARTIAL_LINE_BYTES {
        batch.push(String::from_utf8_lossy(pending).to_string());
        pending.clear();
    }
}

async fn deliver_batch(
    handle: &Arc<MonitorHandle>,
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    lines: Vec<String>,
) {
    if lines.is_empty() {
        return;
    }
    let NotificationSlot::Allowed { seq } = handle.reserve_notification() else {
        return;
    };
    let (lines, omitted_lines) = cap_lines(lines);
    inject(
        session,
        turn,
        MonitorNotification {
            process_id: handle.process_id,
            seq,
            command: handle.command().to_string(),
            kind: handle.kind().as_str(),
            terminal_state: None,
            lines,
            omitted_lines,
            suppressed_notifications: 0,
            note: None,
        },
    )
    .await;
}

async fn deliver_terminal(
    handle: &Arc<MonitorHandle>,
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    state: MonitorState,
    lagged: bool,
) {
    if !handle.claim_terminal(state.clone()) {
        return;
    }
    let seq = handle.reserve_terminal_notification();
    let suppressed = handle.suppressed_notifications();
    let mut note = format!(
        "Read the monitor's retained output with monitor(action=\"read\", process_id={}).",
        handle.process_id
    );
    if lagged {
        note.push_str(
            " Some output outran the notification channel; the retained output is authoritative.",
        );
    }
    inject(
        session,
        turn,
        MonitorNotification {
            process_id: handle.process_id,
            seq,
            command: handle.command().to_string(),
            kind: handle.kind().as_str(),
            terminal_state: Some(state.describe()),
            lines: Vec::new(),
            omitted_lines: 0,
            suppressed_notifications: suppressed,
            note: Some(note),
        },
    )
    .await;
}

/// Bound one notification by line count and by bytes, reporting what was left
/// out. The omitted lines stay in the retained output.
fn cap_lines(mut lines: Vec<String>) -> (Vec<String>, usize) {
    let mut omitted = lines.len().saturating_sub(MAX_LINES_PER_NOTIFICATION);
    lines.truncate(MAX_LINES_PER_NOTIFICATION);

    let mut budget = MAX_NOTIFICATION_BYTES;
    let mut kept = Vec::with_capacity(lines.len());
    for line in lines {
        if budget == 0 {
            omitted += 1;
        } else if line.len() <= budget {
            budget -= line.len();
            kept.push(line);
        } else {
            // Spend the rest of the budget on a prefix rather than dropping a
            // long line whole: a truncated first line beats an empty batch.
            kept.push(truncate_on_char_boundary(&line, budget));
            budget = 0;
        }
    }
    (kept, omitted)
}

fn truncate_on_char_boundary(line: &str, max_bytes: usize) -> String {
    let mut end = max_bytes.min(line.len());
    while end > 0 && !line.is_char_boundary(end) {
        end -= 1;
    }
    line[..end].to_string()
}

async fn inject(
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    notification: MonitorNotification,
) {
    session
        .deliver_monitor_notification(notification, turn.as_ref())
        .await;
}

#[cfg(test)]
#[path = "monitor_watcher_tests.rs"]
mod tests;
