use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use codex_code_mode_protocol::CellId;
use codex_code_mode_protocol::CodeModeSessionDelegate;
use codex_code_mode_protocol::host::EncodedFrame;
use codex_code_mode_protocol::host::HostToClient;
use codex_code_mode_protocol::host::MAX_PENDING_DELEGATE_CALLS;
use codex_code_mode_protocol::host::RequestId;
use codex_code_mode_protocol::host::TransportLane;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub(in crate::remote_session) use self::cleanup::SessionCleanup;
use self::delegate_runtime::DelegateRuntime;
use self::request_tracker::RequestTracker;
use self::session_registry::SessionRegistry;
pub(super) use self::types::DriverCommand;
pub(super) use self::types::DriverEvent;
pub(in crate::remote_session) use self::types::RemoteSession;

mod cell_ids;
mod cleanup;
mod commands;
mod delegate_runtime;
mod request_tracker;
mod responses;
mod session_registry;
mod types;

// Keep one bounded event-channel's worth of work plus the cancel-and-terminate pair
// that one abandoned execution can emit before the driver observes backpressure.
const MAX_DEFERRED_OUTGOING_FRAMES: usize = super::IPC_CHANNEL_CAPACITY + 2;

pub(super) struct DriverLifecycle {
    pub(super) alive: Arc<AtomicBool>,
    pub(super) failure: Arc<std::sync::Mutex<Option<String>>>,
    pub(super) cancellation: CancellationToken,
}

pub(super) struct ConnectionDriver {
    command_rx: mpsc::Receiver<DriverCommand>,
    event_rx: mpsc::Receiver<DriverEvent>,
    event_tx: mpsc::Sender<DriverEvent>,
    execute_claim_rx: mpsc::UnboundedReceiver<RequestId>,
    outgoing_tx: mpsc::Sender<EncodedFrame>,
    bulk_tx: Option<mpsc::Sender<EncodedFrame>>,
    deferred_outgoing: VecDeque<(EncodedFrame, TransportLane)>,
    delegate_response_permits: Arc<Semaphore>,
    receipt_response_permits: Arc<Semaphore>,
    requests: RequestTracker,
    deferred_host_messages: VecDeque<HostToClient>,
    sessions: SessionRegistry,
    delegates: DelegateRuntime,
    alive: Arc<AtomicBool>,
    failure: Arc<std::sync::Mutex<Option<String>>>,
    cancellation: CancellationToken,
    failed: bool,
}

impl ConnectionDriver {
    pub(super) fn new(
        command_rx: mpsc::Receiver<DriverCommand>,
        event_rx: mpsc::Receiver<DriverEvent>,
        event_tx: mpsc::Sender<DriverEvent>,
        outgoing_tx: mpsc::Sender<EncodedFrame>,
        lifecycle: DriverLifecycle,
    ) -> (Self, mpsc::UnboundedSender<RequestId>) {
        let (execute_claim_tx, execute_claim_rx) = mpsc::unbounded_channel();
        (
            Self {
                command_rx,
                event_rx,
                event_tx: event_tx.clone(),
                execute_claim_rx,
                outgoing_tx,
                bulk_tx: None,
                deferred_outgoing: VecDeque::new(),
                delegate_response_permits: Arc::new(Semaphore::new(MAX_PENDING_DELEGATE_CALLS)),
                receipt_response_permits: Arc::new(Semaphore::new(1)),
                requests: RequestTracker::new(),
                deferred_host_messages: VecDeque::new(),
                sessions: SessionRegistry::new(),
                delegates: DelegateRuntime::new(event_tx),
                alive: lifecycle.alive,
                failure: lifecycle.failure,
                cancellation: lifecycle.cancellation,
                failed: false,
            },
            execute_claim_tx,
        )
    }

    pub(super) async fn run(mut self) {
        loop {
            if !self.deferred_outgoing.is_empty() {
                if !self.run_while_outgoing_is_blocked().await {
                    return;
                }
                continue;
            }
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => {
                    self.fail("code-mode host connection closed".to_string());
                    return;
                }
                event = self.event_rx.recv() => {
                    let Some(event) = event else {
                        self.fail("code-mode host event stream closed".to_string());
                        return;
                    };
                    if !self.cancel_dropped_callers() || !self.handle_event(event) {
                        return;
                    }
                }
                claim = self.execute_claim_rx.recv() => {
                    let Some(request_id) = claim else {
                        self.fail("code-mode execute claim stream closed".to_string());
                        return;
                    };
                    self.requests.claim_execute(request_id);
                }
                command = self.command_rx.recv() => {
                    let Some(command) = command else {
                        self.fail("code-mode host command stream closed".to_string());
                        return;
                    };
                    if !self.cancel_dropped_callers() || !self.handle_command(command) {
                        return;
                    }
                }
            }
        }
    }

    fn handle_event(&mut self, event: DriverEvent) -> bool {
        let keep_running = match event {
            DriverEvent::HostMessage(message) => self.handle_host_message(message),
            DriverEvent::DelegateCompleted { id, result } => self.complete_delegate(id, result),
            DriverEvent::RequestCancelled(id) => {
                if self.deferred_outgoing.is_empty() {
                    self.cancel_request(id)
                } else {
                    true
                }
            }
            DriverEvent::Failed(reason) => {
                self.fail(reason);
                false
            }
        };
        if keep_running && self.deferred_outgoing.is_empty() {
            self.flush_deferred_waits()
        } else {
            keep_running
        }
    }

    async fn run_while_outgoing_is_blocked(&mut self) -> bool {
        let lane = self
            .deferred_outgoing
            .front()
            .expect("checked non-empty deferred outgoing queue")
            .1;
        let sender = match lane {
            TransportLane::Control => self.outgoing_tx.clone(),
            TransportLane::Bulk => self.bulk_tx.as_ref().unwrap_or(&self.outgoing_tx).clone(),
        };
        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => {
                self.fail("code-mode host connection closed".to_string());
                false
            }
            permit = sender.reserve_owned() => {
                let Ok(permit) = permit else {
                    self.fail("code-mode host writer closed".to_string());
                    return false;
                };
                let (frame, queued_lane) = self
                    .deferred_outgoing
                    .pop_front()
                    .expect("reserved capacity for queued outgoing frame");
                debug_assert_eq!(queued_lane, lane);
                permit.send(frame);
                if self.deferred_outgoing.is_empty() {
                    if !self.cancel_dropped_callers() {
                        return false;
                    }
                    if self.deferred_outgoing.is_empty() {
                        self.flush_deferred_waits()
                    } else {
                        true
                    }
                } else {
                    true
                }
            }
            event = self.event_rx.recv(), if self.deferred_outgoing.len() < MAX_DEFERRED_OUTGOING_FRAMES => {
                let Some(event) = event else {
                    self.fail("code-mode host event stream closed".to_string());
                    return false;
                };
                self.handle_event(event)
            }
            claim = self.execute_claim_rx.recv() => {
                let Some(request_id) = claim else {
                    self.fail("code-mode execute claim stream closed".to_string());
                    return false;
                };
                self.requests.claim_execute(request_id);
                true
            }
        }
    }

    pub(super) fn with_bulk_sender(mut self, sender: mpsc::Sender<EncodedFrame>) -> Self {
        self.bulk_tx = Some(sender);
        self
    }

    fn queue_frame(&mut self, frame: EncodedFrame, lane: TransportLane) -> bool {
        if !self.deferred_outgoing.is_empty() {
            debug_assert!(self.deferred_outgoing.len() < MAX_DEFERRED_OUTGOING_FRAMES);
            self.deferred_outgoing.push_back((frame, lane));
            return true;
        }
        let sender = match lane {
            TransportLane::Control => &self.outgoing_tx,
            TransportLane::Bulk => self.bulk_tx.as_ref().unwrap_or(&self.outgoing_tx),
        };
        match sender.try_send(frame) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(frame)) => {
                self.deferred_outgoing.push_back((frame, lane));
                true
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.fail("code-mode host writer closed".to_string());
                false
            }
        }
    }

    fn fail(&mut self, reason: String) {
        if self.failed {
            return;
        }
        self.failed = true;
        self.alive.store(false, Ordering::Release);
        let reason = {
            let mut failure = self
                .failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            failure.get_or_insert(reason).clone()
        };
        self.requests.fail_all(&reason);
        let failed_sessions = self.sessions.drain();
        self.delegates.fail_all(failed_sessions);
        self.cancellation.cancel();
    }
}

impl Drop for ConnectionDriver {
    fn drop(&mut self) {
        self.fail("code-mode connection driver stopped unexpectedly".to_string());
    }
}

fn notify_cell_closed(delegate: &Arc<dyn CodeModeSessionDelegate>, cell_id: &CellId) {
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| delegate.cell_closed(cell_id)));
}

#[cfg(test)]
#[path = "driver_tests.rs"]
mod tests;
