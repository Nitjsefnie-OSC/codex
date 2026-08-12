use crate::context::SubagentNotification;
use crate::context::is_background_notification;
use crate::state::ActiveTurn;
use crate::state::MailboxDeliveryPhase;
use crate::state::TaskKind;
use crate::state::TurnState;
use codex_diagnostics::Gauge;
use codex_diagnostics::GaugeGuard;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::user_input::UserInput;
use serde::Deserialize;
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::watch;

static PENDING_MAILBOX_MESSAGES: Gauge = Gauge::new("core.mailbox.pending");

/// Input consumed by a regular turn.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TurnInput {
    UserInput {
        content: Vec<UserInput>,
        client_id: Option<String>,
    },
    ResponseItem(ResponseItem),
    InterAgentCommunication(InterAgentCommunication),
    /// A terminal child result delivered through the durable background-wake path.
    AgentCompletion(InterAgentCommunication),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InputQueueActivity {
    Mailbox,
    Steer,
}

#[derive(Debug, PartialEq)]
pub(crate) enum PendingInputBatch {
    Foreground(Vec<TurnInput>),
    Background {
        items: Vec<TurnInput>,
        parent_turn: PendingParentTurn,
    },
}

/// Turn-local pending input storage owned by the input queue flow.
#[derive(Default)]
pub(crate) struct TurnInputQueue {
    items: VecDeque<QueuedTurnInput>,
}

struct QueuedTurnInput {
    input: TurnInput,
    parent_turn: PendingParentTurn,
}

/// Session-scoped pending input storage and active-turn mailbox delivery coordination.
pub(crate) struct InputQueue {
    activity_tx: watch::Sender<InputQueueActivity>,
    mailbox_pending_mails: Mutex<VecDeque<PendingMailboxCommunication>>,
    turn_start_lock: Mutex<()>,
    background_notification_delivery_lock: Arc<Mutex<()>>,
    background_wake_state: std::sync::Mutex<BackgroundWakeState>,
    background_wake_requested: AtomicBool,
    agent_completion_pending: AtomicBool,
    background_wake_notify: Notify,
}

#[derive(Default)]
struct BackgroundWakeState {
    next_generation: u64,
    pending: VecDeque<BackgroundWakeEntry>,
}

struct BackgroundWakeEntry {
    generation: u64,
    parent_turn: PendingParentTurn,
    agent_completion: bool,
}

pub(crate) struct BackgroundWakeReceipt {
    through_generation: Option<u64>,
    parent_turn: PendingParentTurn,
}

impl BackgroundWakeReceipt {
    pub(crate) fn apply_parent_turn(
        &self,
        metadata: &mut crate::responses_metadata::CodexResponsesMetadata,
    ) {
        match &self.parent_turn {
            PendingParentTurn::Empty if self.through_generation.is_some() => {
                metadata.parent_turn_id = None;
            }
            PendingParentTurn::Empty => {}
            PendingParentTurn::Unique(id) => metadata.parent_turn_id = Some(id.clone()),
            PendingParentTurn::Conflict => metadata.parent_turn_id = None,
        }
    }
}

struct PendingMailboxCommunication {
    communication: InterAgentCommunication,
    parent_turn_id: Option<String>,
    _diagnostics_guard: GaugeGuard,
}

#[derive(Default)]
pub(crate) struct DrainedMailboxInputs {
    pub(crate) items: Vec<TurnInput>,
    pub(crate) parent_turn: PendingParentTurn,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) enum PendingParentTurn {
    #[default]
    Empty,
    Unique(String),
    Conflict,
}

impl InputQueue {
    pub(crate) fn new() -> Self {
        let (activity_tx, _) = watch::channel(InputQueueActivity::Mailbox);
        Self {
            activity_tx,
            mailbox_pending_mails: Mutex::new(VecDeque::new()),
            turn_start_lock: Mutex::new(()),
            background_notification_delivery_lock: Arc::new(Mutex::new(())),
            background_wake_state: std::sync::Mutex::new(BackgroundWakeState::default()),
            background_wake_requested: AtomicBool::new(false),
            agent_completion_pending: AtomicBool::new(false),
            background_wake_notify: Notify::new(),
        }
    }

    pub(crate) async fn lock_turn_start(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.turn_start_lock.lock().await
    }

    pub(crate) async fn lock_background_notification_delivery(
        &self,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.background_notification_delivery_lock)
            .lock_owned()
            .await
    }

    pub(crate) fn request_background_wake(
        &self,
        parent_turn: PendingParentTurn,
        agent_completion: bool,
    ) {
        let mut state = self
            .background_wake_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.next_generation = state.next_generation.wrapping_add(1);
        let generation = state.next_generation;
        state.pending.push_back(BackgroundWakeEntry {
            generation,
            parent_turn,
            agent_completion,
        });
        self.background_wake_requested
            .store(true, Ordering::Release);
    }

    /// Wake the submission loop after the background notification represented
    /// by the durable flag has been persisted and flushed.
    pub(crate) fn notify_background_wake(&self) {
        if self.background_wake_requested() {
            self.background_wake_notify.notify_one();
        }
    }

    pub(crate) async fn background_wake_notified(&self) {
        self.background_wake_notify.notified().await;
    }

    pub(crate) fn background_wake_requested(&self) -> bool {
        self.background_wake_requested.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn claim_background_wake(&self) -> bool {
        let claimed = self.background_wake_requested.swap(false, Ordering::AcqRel);
        if claimed {
            self.background_wake_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pending
                .clear();
            self.agent_completion_pending
                .store(false, Ordering::Release);
        }
        claimed
    }

    pub(crate) fn snapshot_background_wake(&self) -> BackgroundWakeReceipt {
        let state = self
            .background_wake_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut parent_turn = PendingParentTurn::Empty;
        for entry in &state.pending {
            parent_turn.merge_state(entry.parent_turn.clone());
        }
        BackgroundWakeReceipt {
            through_generation: state.pending.back().map(|entry| entry.generation),
            parent_turn,
        }
    }

    pub(crate) fn acknowledge_background_wake(&self, receipt: BackgroundWakeReceipt) {
        let Some(through_generation) = receipt.through_generation else {
            return;
        };
        let mut state = self
            .background_wake_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state
            .pending
            .front()
            .is_some_and(|entry| entry.generation <= through_generation)
        {
            state.pending.pop_front();
        }
        let has_pending = !state.pending.is_empty();
        let has_agent_completion = state.pending.iter().any(|entry| entry.agent_completion);
        self.background_wake_requested
            .store(has_pending, Ordering::Release);
        self.agent_completion_pending
            .store(has_agent_completion, Ordering::Release);
    }

    pub(crate) fn request_agent_completion_activity(&self) {
        self.agent_completion_pending.store(true, Ordering::Release);
        self.activity_tx.send_replace(InputQueueActivity::Mailbox);
    }

    /// Add a background notification to an active regular turn only while that
    /// turn still accepts same-turn work. The active-turn lock and turn-state
    /// lock make this decision atomic with task finalization.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "active turn and turn state must be checked and updated atomically"
    )]
    pub(crate) async fn inject_background_inputs_if_running(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
        inputs: Vec<TurnInput>,
        parent_turn: PendingParentTurn,
        activity: Option<InputQueueActivity>,
    ) -> Result<(), (Vec<TurnInput>, PendingParentTurn)> {
        let mut active = active_turn.lock().await;
        let Some(active_turn) = active.as_mut() else {
            return Err((inputs, parent_turn));
        };
        if let Some(task) = active_turn.task.as_ref()
            && (task.kind != TaskKind::Regular || !task.task.accepts_background_notifications())
        {
            return Err((inputs, parent_turn));
        }
        let mut turn_state = active_turn.turn_state.lock().await;
        if !turn_state.accepts_mailbox_delivery_for_current_turn() {
            return Err((inputs, parent_turn));
        }
        turn_state.pending_input.extend(inputs, parent_turn);
        if let Some(activity) = activity {
            self.activity_tx.send_replace(activity);
        }
        Ok(())
    }

    #[cfg(test)]
    fn background_input_activity(input: &TurnInput) -> Option<InputQueueActivity> {
        match input {
            TurnInput::AgentCompletion(_) => Some(InputQueueActivity::Mailbox),
            TurnInput::ResponseItem(item) if SubagentNotification::is_response_item(item) => {
                Some(InputQueueActivity::Mailbox)
            }
            TurnInput::UserInput { .. } | TurnInput::InterAgentCommunication(_) => {
                Some(InputQueueActivity::Steer)
            }
            TurnInput::ResponseItem(_) => None,
        }
    }

    pub(crate) async fn subscribe_activity(
        &self,
        turn_state: Option<&Mutex<TurnState>>,
    ) -> (
        watch::Receiver<InputQueueActivity>,
        Option<InputQueueActivity>,
    ) {
        let activity_rx = self.activity_tx.subscribe();
        let (has_pending_steer, has_pending_completion) = match turn_state {
            Some(turn_state) => {
                let pending_input = &turn_state.lock().await.pending_input;
                (
                    pending_input.has_user_input(),
                    pending_input.has_agent_completion(),
                )
            }
            None => (false, false),
        };
        let pending_activity = if has_pending_steer {
            Some(InputQueueActivity::Steer)
        } else if has_pending_completion
            || self.agent_completion_pending.load(Ordering::Acquire)
            || self.has_pending_mailbox_items().await
        {
            Some(InputQueueActivity::Mailbox)
        } else {
            None
        };
        (activity_rx, pending_activity)
    }

    pub(crate) async fn enqueue_mailbox_communication(
        &self,
        communication: InterAgentCommunication,
        parent_turn_id: Option<String>,
    ) {
        self.mailbox_pending_mails
            .lock()
            .await
            .push_back(PendingMailboxCommunication {
                communication,
                parent_turn_id,
                _diagnostics_guard: PENDING_MAILBOX_MESSAGES.track(),
            });
        self.activity_tx.send_replace(InputQueueActivity::Mailbox);
    }

    pub(crate) async fn enqueue_or_inject_mailbox_communication(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
        communication: InterAgentCommunication,
        parent_turn_id: Option<String>,
    ) {
        let parent_turn = if communication.trigger_turn {
            match parent_turn_id {
                Some(parent_turn_id) => PendingParentTurn::Unique(parent_turn_id),
                None => PendingParentTurn::Conflict,
            }
        } else {
            PendingParentTurn::Empty
        };
        let input = TurnInput::InterAgentCommunication(communication);
        match self
            .inject_background_inputs_if_running(
                active_turn,
                vec![input],
                parent_turn,
                Some(InputQueueActivity::Mailbox),
            )
            .await
        {
            Ok(()) => {}
            Err((mut inputs, parent_turn)) => {
                let TurnInput::InterAgentCommunication(communication) =
                    inputs.pop().expect("mailbox injection returns its input")
                else {
                    unreachable!("mailbox injection preserves the communication variant")
                };
                let parent_turn_id = match parent_turn {
                    PendingParentTurn::Unique(parent_turn_id) => Some(parent_turn_id),
                    PendingParentTurn::Empty | PendingParentTurn::Conflict => None,
                };
                self.enqueue_mailbox_communication(communication, parent_turn_id)
                    .await;
            }
        }
    }

    pub(super) async fn drain_mailbox_into_turn_state_before_input(
        &self,
        turn_state: &Mutex<TurnState>,
        input: Vec<TurnInput>,
    ) {
        let mailbox = self.drain_mailbox_inputs().await;
        let mut turn_state = turn_state.lock().await;
        turn_state
            .pending_input
            .extend(mailbox.items, mailbox.parent_turn);
        turn_state
            .pending_input
            .extend(input, PendingParentTurn::Empty);
        turn_state.accept_mailbox_delivery_for_current_turn();
        self.activity_tx.send_replace(InputQueueActivity::Steer);
    }

    pub(crate) async fn has_pending_mailbox_items(&self) -> bool {
        !self.mailbox_pending_mails.lock().await.is_empty()
    }

    pub(crate) async fn has_trigger_turn_mailbox_items(&self) -> bool {
        self.mailbox_pending_mails
            .lock()
            .await
            .iter()
            .any(|mail| mail.communication.trigger_turn)
    }

    #[cfg(test)]
    pub(crate) async fn drain_mailbox_input_items(&self) -> (Vec<TurnInput>, Option<String>) {
        let drained = self.drain_mailbox_inputs().await;
        (drained.items, drained.parent_turn.into_option())
    }

    pub(crate) async fn drain_mailbox_inputs(&self) -> DrainedMailboxInputs {
        let pending_mails = self
            .mailbox_pending_mails
            .lock()
            .await
            .drain(..)
            .collect::<Vec<_>>();
        let mut parent_turn = PendingParentTurn::Empty;
        for mail in pending_mails
            .iter()
            .filter(|mail| mail.communication.trigger_turn)
        {
            parent_turn.merge(mail.parent_turn_id.as_deref());
        }
        let items = pending_mails
            .into_iter()
            .map(|mail| TurnInput::InterAgentCommunication(mail.communication))
            .collect();
        DrainedMailboxInputs { items, parent_turn }
    }

    pub(crate) async fn ordered_background_inputs(
        &self,
        completion: TurnInput,
    ) -> (Vec<TurnInput>, PendingParentTurn) {
        let mut drained = self.drain_mailbox_inputs().await;
        drained.items.push(completion);
        (drained.items, drained.parent_turn)
    }

    pub(crate) async fn turn_state_for_sub_id(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
        sub_id: &str,
    ) -> Option<Arc<Mutex<TurnState>>> {
        let active = active_turn.lock().await;
        active.as_ref().and_then(|active_turn| {
            active_turn
                .task
                .as_ref()
                .is_some_and(|task| task.turn_context.sub_id == sub_id)
                .then(|| Arc::clone(&active_turn.turn_state))
        })
    }

    /// Clear any pending waiters and input buffered for the current turn.
    pub(crate) async fn clear_pending(&self, active_turn: &ActiveTurn) {
        let mut turn_state = active_turn.turn_state.lock().await;
        turn_state.clear_pending_waiters();
        turn_state.pending_input.items.clear();
    }

    pub(crate) async fn defer_mailbox_delivery_to_next_turn(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
        sub_id: &str,
    ) {
        let turn_state = self.turn_state_for_sub_id(active_turn, sub_id).await;
        let Some(turn_state) = turn_state else {
            return;
        };
        let mut turn_state = turn_state.lock().await;
        // Explicit same-turn work still needs a follow-up. Queue-only child mail does not: keep
        // it pending so task completion records it for the next turn without sampling again.
        if turn_state.pending_input.items.iter().any(|queued| {
            !matches!(
                &queued.input,
                TurnInput::InterAgentCommunication(communication) if !communication.trigger_turn
            )
        }) {
            return;
        }
        turn_state.set_mailbox_delivery_phase(MailboxDeliveryPhase::NextTurn);
    }

    pub(crate) async fn accept_mailbox_delivery_for_current_turn(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
        sub_id: &str,
    ) {
        let turn_state = self.turn_state_for_sub_id(active_turn, sub_id).await;
        let Some(turn_state) = turn_state else {
            return;
        };
        self.accept_mailbox_delivery_for_turn_state(turn_state.as_ref())
            .await;
    }

    pub(super) async fn accept_mailbox_delivery_for_turn_state(
        &self,
        turn_state: &Mutex<TurnState>,
    ) {
        turn_state
            .lock()
            .await
            .accept_mailbox_delivery_for_current_turn();
    }

    #[cfg(test)]
    pub(super) async fn extend_pending_input_and_accept_mailbox_delivery_for_turn_state(
        &self,
        turn_state: &Mutex<TurnState>,
        input: Vec<TurnInput>,
    ) {
        {
            let mut turn_state = turn_state.lock().await;
            turn_state
                .pending_input
                .extend(input, PendingParentTurn::Empty);
            turn_state.accept_mailbox_delivery_for_current_turn();
        }
        self.activity_tx.send_replace(InputQueueActivity::Steer);
    }

    #[cfg(test)]
    pub(crate) async fn extend_pending_input_for_turn_state(
        &self,
        turn_state: &Mutex<TurnState>,
        input: Vec<TurnInput>,
    ) {
        turn_state
            .lock()
            .await
            .pending_input
            .extend(input, PendingParentTurn::Empty);
    }

    pub(crate) async fn extend_pending_input_batch_for_turn_state(
        &self,
        turn_state: &Mutex<TurnState>,
        input: Vec<TurnInput>,
        parent_turn: PendingParentTurn,
    ) {
        turn_state
            .lock()
            .await
            .pending_input
            .extend(input, parent_turn);
    }

    #[cfg(test)]
    pub(crate) async fn take_pending_input_for_turn_state(
        &self,
        turn_state: &Mutex<TurnState>,
    ) -> Vec<TurnInput> {
        self.take_pending_input_batch_for_turn_state(turn_state)
            .await
            .0
    }

    pub(crate) async fn take_pending_input_batch_for_turn_state(
        &self,
        turn_state: &Mutex<TurnState>,
    ) -> (Vec<TurnInput>, PendingParentTurn) {
        let mut turn_state = turn_state.lock().await;
        turn_state.pending_input.take_all()
    }

    pub(crate) async fn pending_input_summary_for_turn_state(
        &self,
        turn_state: &Mutex<TurnState>,
    ) -> (bool, bool, bool) {
        let turn_state = turn_state.lock().await;
        (
            !turn_state.pending_input.items.is_empty(),
            turn_state.pending_input.has_user_input(),
            turn_state
                .pending_input
                .items
                .iter()
                .any(|queued| !Self::input_is_background(&queued.input)),
        )
    }

    pub(crate) async fn refresh_injected_agent_completion_activity(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
        sub_id: &str,
    ) {
        let has_pending_completion = match self.turn_state_for_sub_id(active_turn, sub_id).await {
            Some(turn_state) => turn_state.lock().await.pending_input.has_agent_completion(),
            None => false,
        };
        let durable_agent_completion = self
            .background_wake_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending
            .iter()
            .any(|entry| entry.agent_completion);
        self.agent_completion_pending.store(
            has_pending_completion || durable_agent_completion,
            Ordering::Release,
        );
    }

    pub(crate) async fn take_pending_background_inputs_for_turn_state(
        &self,
        turn_state: &Mutex<TurnState>,
    ) -> (Vec<TurnInput>, PendingParentTurn) {
        let mut turn_state = turn_state.lock().await;
        let pending_items = std::mem::take(&mut turn_state.pending_input.items);
        let mut notification_items = Vec::new();
        let mut notification_parent_turn = PendingParentTurn::Empty;
        let mut remaining_items = VecDeque::with_capacity(pending_items.len());
        for queued in pending_items {
            match queued.input {
                TurnInput::ResponseItem(response_item)
                    if is_background_notification(&response_item) =>
                {
                    notification_parent_turn.merge_state(queued.parent_turn);
                    notification_items.push(TurnInput::ResponseItem(response_item));
                }
                item @ (TurnInput::InterAgentCommunication(_) | TurnInput::AgentCompletion(_)) => {
                    notification_parent_turn.merge_state(queued.parent_turn);
                    notification_items.push(item);
                }
                input => remaining_items.push_back(QueuedTurnInput {
                    input,
                    parent_turn: queued.parent_turn,
                }),
            }
        }
        turn_state.pending_input.items = remaining_items;
        (notification_items, notification_parent_turn)
    }

    pub(crate) async fn take_pending_input_and_mailbox_for_turn_state(
        &self,
        turn_state: &Mutex<TurnState>,
    ) -> (Vec<TurnInput>, PendingParentTurn) {
        let (mut items, mut parent_turn) = self
            .take_pending_input_batch_for_turn_state(turn_state)
            .await;
        let mailbox = self.drain_mailbox_inputs().await;
        items.extend(mailbox.items);
        parent_turn.merge_state(mailbox.parent_turn);
        (items, parent_turn)
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "active turn checks and turn state updates must remain atomic"
    )]
    pub(crate) async fn take_next_pending_input_batch(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
    ) -> PendingInputBatch {
        let (pending_input, pending_parent_turn, accepts_mailbox_delivery) = {
            let mut active = active_turn.lock().await;
            match active.as_mut() {
                Some(active_turn) => {
                    let mut turn_state = active_turn.turn_state.lock().await;
                    let accepts_mailbox_delivery =
                        turn_state.accepts_mailbox_delivery_for_current_turn();
                    let (pending_input, parent_turn) = if accepts_mailbox_delivery
                        && let Some(first) = turn_state.pending_input.items.front()
                    {
                        let first_is_background = Self::input_is_background(&first.input);
                        let prefix_len = turn_state
                            .pending_input
                            .items
                            .iter()
                            .take_while(|queued| {
                                Self::input_is_background(&queued.input) == first_is_background
                            })
                            .count();
                        let mut parent_turn = PendingParentTurn::Empty;
                        let items = turn_state
                            .pending_input
                            .items
                            .drain(..prefix_len)
                            .map(|queued| {
                                parent_turn.merge_state(queued.parent_turn);
                                queued.input
                            })
                            .collect();
                        (items, parent_turn)
                    } else {
                        (Vec::new(), PendingParentTurn::Empty)
                    };
                    (pending_input, parent_turn, accepts_mailbox_delivery)
                }
                None => (Vec::new(), PendingParentTurn::Empty, true),
            }
        };
        if !accepts_mailbox_delivery {
            return PendingInputBatch::Foreground(pending_input);
        }
        if let Some(first) = pending_input.first() {
            return if Self::input_is_background(first) {
                PendingInputBatch::Background {
                    items: pending_input,
                    parent_turn: pending_parent_turn,
                }
            } else {
                PendingInputBatch::Foreground(pending_input)
            };
        }
        let mailbox = self.drain_mailbox_inputs().await;
        if mailbox.items.is_empty() {
            PendingInputBatch::Foreground(Vec::new())
        } else {
            PendingInputBatch::Background {
                items: mailbox.items,
                parent_turn: mailbox.parent_turn,
            }
        }
    }

    fn input_is_background(input: &TurnInput) -> bool {
        match input {
            TurnInput::InterAgentCommunication(_) | TurnInput::AgentCompletion(_) => true,
            TurnInput::ResponseItem(item) => is_background_notification(item),
            TurnInput::UserInput { .. } => false,
        }
    }

    #[cfg(test)]
    pub(crate) async fn get_pending_input(
        &self,
        active_turn: &Mutex<Option<ActiveTurn>>,
    ) -> (Vec<TurnInput>, Option<String>) {
        let mut all_items = Vec::new();
        let mut all_parent_turn = PendingParentTurn::Empty;
        loop {
            match self.take_next_pending_input_batch(active_turn).await {
                PendingInputBatch::Foreground(items) => {
                    if items.is_empty() {
                        break;
                    }
                    all_items.extend(items);
                }
                PendingInputBatch::Background { items, parent_turn } => {
                    if items.is_empty() {
                        break;
                    }
                    all_items.extend(items);
                    all_parent_turn.merge_state(parent_turn);
                }
            }
        }
        (all_items, all_parent_turn.into_option())
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "active turn checks and turn state reads must remain atomic"
    )]
    pub(crate) async fn has_pending_input(&self, active_turn: &Mutex<Option<ActiveTurn>>) -> bool {
        let (has_turn_pending_input, accepts_mailbox_delivery) = {
            let active = active_turn.lock().await;
            match active.as_ref() {
                Some(active_turn) => {
                    let turn_state = active_turn.turn_state.lock().await;
                    (
                        !turn_state.pending_input.items.is_empty(),
                        turn_state.accepts_mailbox_delivery_for_current_turn(),
                    )
                }
                None => (false, true),
            }
        };
        if !accepts_mailbox_delivery {
            return false;
        }
        if has_turn_pending_input {
            return true;
        }
        self.has_pending_mailbox_items().await
    }
}

impl PendingParentTurn {
    fn merge(&mut self, candidate: Option<&str>) {
        let candidate = candidate.filter(|id| !id.trim().is_empty());
        match (&*self, candidate) {
            (Self::Empty, Some(candidate)) => *self = Self::Unique(candidate.to_string()),
            (Self::Unique(expected), Some(candidate)) if expected == candidate => {}
            (Self::Conflict, _) => {}
            _ => *self = Self::Conflict,
        }
    }

    fn merge_state(&mut self, other: Self) {
        match other {
            Self::Empty => {}
            Self::Unique(candidate) => self.merge(Some(&candidate)),
            Self::Conflict => *self = Self::Conflict,
        }
    }

    #[cfg(test)]
    fn into_option(self) -> Option<String> {
        match self {
            Self::Unique(id) => Some(id),
            Self::Empty | Self::Conflict => None,
        }
    }
}

impl TurnInputQueue {
    fn extend(&mut self, input: Vec<TurnInput>, parent_turn: PendingParentTurn) {
        self.items
            .extend(input.into_iter().map(|input| QueuedTurnInput {
                input,
                parent_turn: parent_turn.clone(),
            }));
    }

    fn take_all(&mut self) -> (Vec<TurnInput>, PendingParentTurn) {
        let mut parent_turn = PendingParentTurn::Empty;
        let items = self
            .items
            .drain(..)
            .map(|queued| {
                parent_turn.merge_state(queued.parent_turn);
                queued.input
            })
            .collect();
        (items, parent_turn)
    }

    fn has_user_input(&self) -> bool {
        self.items
            .iter()
            .any(|queued| matches!(&queued.input, TurnInput::UserInput { .. }))
    }

    fn has_agent_completion(&self) -> bool {
        self.items.iter().any(|queued| match &queued.input {
            TurnInput::AgentCompletion(_) => true,
            TurnInput::ResponseItem(item) => SubagentNotification::is_response_item(item),
            TurnInput::UserInput { .. } | TurnInput::InterAgentCommunication(_) => false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ContextualUserFragment;
    use crate::context::ExecCommandCompletion;
    use crate::context::ExecCommandCompletionNotification;
    use crate::context::MonitorNotification;
    use codex_protocol::AgentPath;
    use pretty_assertions::assert_eq;

    fn make_mail(
        author: AgentPath,
        recipient: AgentPath,
        content: &str,
        trigger_turn: bool,
    ) -> InterAgentCommunication {
        InterAgentCommunication::new(
            author,
            recipient,
            Vec::new(),
            content.to_string(),
            trigger_turn,
        )
    }

    #[tokio::test]
    async fn input_queue_notifies_mailbox_subscribers() {
        let input_queue = InputQueue::new();
        let (mut activity_rx, pending_activity) =
            input_queue.subscribe_activity(/*turn_state*/ None).await;
        assert_eq!(pending_activity, None);

        let mail_one = make_mail(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            "one",
            /*trigger_turn*/ false,
        );
        input_queue
            .enqueue_mailbox_communication(mail_one, /*parent_turn_id*/ None)
            .await;
        let mail_two = make_mail(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            "two",
            /*trigger_turn*/ false,
        );
        input_queue
            .enqueue_mailbox_communication(mail_two, /*parent_turn_id*/ None)
            .await;

        activity_rx.changed().await.expect("mailbox update");
        assert_eq!(
            *activity_rx.borrow_and_update(),
            InputQueueActivity::Mailbox
        );
    }

    #[tokio::test]
    async fn input_queue_notifies_steer_subscribers() {
        let input_queue = InputQueue::new();
        let turn_state = Mutex::new(TurnState::default());
        let (mut activity_rx, pending_activity) =
            input_queue.subscribe_activity(Some(&turn_state)).await;
        assert_eq!(pending_activity, None);

        input_queue
            .extend_pending_input_and_accept_mailbox_delivery_for_turn_state(
                &turn_state,
                vec![TurnInput::UserInput {
                    content: vec![UserInput::Text {
                        text: "steer".to_string(),
                        text_elements: Vec::new(),
                    }],
                    client_id: None,
                }],
            )
            .await;

        activity_rx.changed().await.expect("steer update");
        assert_eq!(*activity_rx.borrow_and_update(), InputQueueActivity::Steer);
    }

    #[tokio::test]
    async fn input_queue_reports_already_pending_steer() {
        let input_queue = InputQueue::new();
        let turn_state = Mutex::new(TurnState::default());
        input_queue
            .extend_pending_input_and_accept_mailbox_delivery_for_turn_state(
                &turn_state,
                vec![TurnInput::UserInput {
                    content: vec![UserInput::Text {
                        text: "already pending".to_string(),
                        text_elements: Vec::new(),
                    }],
                    client_id: None,
                }],
            )
            .await;

        let (_activity_rx, pending_activity) =
            input_queue.subscribe_activity(Some(&turn_state)).await;

        assert_eq!(pending_activity, Some(InputQueueActivity::Steer));
    }

    #[tokio::test]
    async fn input_queue_drains_mailbox_in_delivery_order() {
        let input_queue = InputQueue::new();
        let mail_one = make_mail(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            "one",
            /*trigger_turn*/ false,
        );
        let mail_two = make_mail(
            AgentPath::try_from("/root/worker").expect("agent path"),
            AgentPath::root(),
            "two",
            /*trigger_turn*/ true,
        );

        input_queue
            .enqueue_mailbox_communication(mail_one.clone(), /*parent_turn_id*/ None)
            .await;
        input_queue
            .enqueue_mailbox_communication(mail_two.clone(), /*parent_turn_id*/ None)
            .await;

        assert_eq!(
            input_queue.drain_mailbox_input_items().await.0,
            vec![
                TurnInput::InterAgentCommunication(mail_one),
                TurnInput::InterAgentCommunication(mail_two)
            ]
        );
        assert!(!input_queue.has_pending_mailbox_items().await);
    }

    #[tokio::test]
    async fn input_queue_requires_one_unambiguous_trigger_parent() {
        for (pending_mails, expected_parent_turn_id) in [
            (Vec::new(), None),
            (vec![(false, Some("q"))], None),
            (vec![(true, Some(""))], None),
            (vec![(true, Some("   "))], None),
            (vec![(true, None)], None),
            (vec![(true, Some("a")), (true, Some("b"))], None),
            (vec![(true, Some("a")), (true, None)], None),
            (vec![(true, Some("a")), (true, Some("a"))], Some("a")),
            (vec![(false, Some("q")), (true, Some("a"))], Some("a")),
        ] {
            let input_queue = InputQueue::new();
            for (trigger_turn, parent_turn_id) in pending_mails {
                input_queue
                    .enqueue_mailbox_communication(
                        make_mail(AgentPath::root(), AgentPath::root(), "task", trigger_turn),
                        parent_turn_id.map(str::to_string),
                    )
                    .await;
            }
            let (_, parent_turn_id) = input_queue.drain_mailbox_input_items().await;
            assert_eq!(parent_turn_id.as_deref(), expected_parent_turn_id);
        }
    }

    #[tokio::test]
    async fn completion_batch_preserves_earlier_mailbox_order_and_parent() {
        let input_queue = InputQueue::new();
        let mail = make_mail(
            AgentPath::try_from("/root/worker").expect("agent path"),
            AgentPath::root(),
            "progress",
            /*trigger_turn*/ true,
        );
        let completion = make_mail(
            AgentPath::try_from("/root/worker").expect("agent path"),
            AgentPath::root(),
            "done",
            /*trigger_turn*/ false,
        );
        input_queue
            .enqueue_mailbox_communication(mail.clone(), Some("parent-turn".to_string()))
            .await;

        let (items, parent_turn) = input_queue
            .ordered_background_inputs(TurnInput::AgentCompletion(completion.clone()))
            .await;

        assert_eq!(
            items,
            vec![
                TurnInput::InterAgentCommunication(mail),
                TurnInput::AgentCompletion(completion),
            ]
        );
        assert_eq!(parent_turn.into_option().as_deref(), Some("parent-turn"));
    }

    #[test]
    fn background_wake_receipts_partition_parent_provenance() {
        let input_queue = InputQueue::new();
        input_queue.request_background_wake(
            PendingParentTurn::Unique("first".to_string()),
            /*agent_completion*/ true,
        );
        let first = input_queue.snapshot_background_wake();
        input_queue.request_background_wake(
            PendingParentTurn::Unique("second".to_string()),
            /*agent_completion*/ true,
        );

        input_queue.acknowledge_background_wake(first);

        let remaining = input_queue.snapshot_background_wake();
        assert_eq!(
            remaining.parent_turn,
            PendingParentTurn::Unique("second".to_string())
        );
        assert!(input_queue.background_wake_requested());
    }

    #[test]
    fn represented_empty_parent_clears_only_that_request_metadata() {
        let input_queue = InputQueue::new();
        let mut metadata = crate::responses_metadata::CodexResponsesMetadata::new(
            "installation".to_string(),
            "session".to_string(),
            "thread".to_string(),
            "window".to_string(),
        );
        metadata.parent_turn_id = Some("old-parent".to_string());

        input_queue
            .snapshot_background_wake()
            .apply_parent_turn(&mut metadata);
        assert_eq!(metadata.parent_turn_id.as_deref(), Some("old-parent"));

        input_queue
            .request_background_wake(PendingParentTurn::Empty, /*agent_completion*/ true);
        input_queue
            .snapshot_background_wake()
            .apply_parent_turn(&mut metadata);
        assert_eq!(metadata.parent_turn_id, None);
    }

    #[tokio::test]
    async fn foreground_input_remains_ahead_of_later_completion() {
        let input_queue = InputQueue::new();
        let active_turn = ActiveTurn::default();
        let turn_state = Arc::clone(&active_turn.turn_state);
        let active_turn = Mutex::new(Some(active_turn));
        let user_input = TurnInput::UserInput {
            content: vec![UserInput::Text {
                text: "steer first".to_string(),
                text_elements: Vec::new(),
            }],
            client_id: None,
        };
        let completion = TurnInput::AgentCompletion(make_mail(
            AgentPath::try_from("/root/worker").expect("agent path"),
            AgentPath::root(),
            "done",
            /*trigger_turn*/ false,
        ));
        input_queue
            .extend_pending_input_batch_for_turn_state(
                turn_state.as_ref(),
                vec![user_input.clone(), completion.clone()],
                PendingParentTurn::Unique("parent-turn".to_string()),
            )
            .await;

        assert_eq!(
            input_queue
                .take_next_pending_input_batch(&active_turn)
                .await,
            PendingInputBatch::Foreground(vec![user_input])
        );
        assert_eq!(
            input_queue
                .take_next_pending_input_batch(&active_turn)
                .await,
            PendingInputBatch::Background {
                items: vec![completion],
                parent_turn: PendingParentTurn::Unique("parent-turn".to_string()),
            }
        );
    }

    #[tokio::test]
    async fn interleaved_background_batches_keep_separate_parent_turns() {
        let input_queue = InputQueue::new();
        let active_turn = ActiveTurn::default();
        let turn_state = Arc::clone(&active_turn.turn_state);
        let active_turn = Mutex::new(Some(active_turn));
        let completion_a = TurnInput::AgentCompletion(make_mail(
            AgentPath::try_from("/root/a").expect("agent path"),
            AgentPath::root(),
            "a done",
            /*trigger_turn*/ false,
        ));
        let foreground = TurnInput::UserInput {
            content: vec![UserInput::Text {
                text: "steer".to_string(),
                text_elements: Vec::new(),
            }],
            client_id: None,
        };
        let completion_b = TurnInput::AgentCompletion(make_mail(
            AgentPath::try_from("/root/b").expect("agent path"),
            AgentPath::root(),
            "b done",
            /*trigger_turn*/ false,
        ));
        input_queue
            .extend_pending_input_batch_for_turn_state(
                turn_state.as_ref(),
                vec![completion_a.clone()],
                PendingParentTurn::Unique("parent-a".to_string()),
            )
            .await;
        input_queue
            .extend_pending_input_batch_for_turn_state(
                turn_state.as_ref(),
                vec![foreground.clone()],
                PendingParentTurn::Empty,
            )
            .await;
        input_queue
            .extend_pending_input_batch_for_turn_state(
                turn_state.as_ref(),
                vec![completion_b.clone()],
                PendingParentTurn::Unique("parent-b".to_string()),
            )
            .await;

        assert_eq!(
            input_queue
                .take_next_pending_input_batch(&active_turn)
                .await,
            PendingInputBatch::Background {
                items: vec![completion_a],
                parent_turn: PendingParentTurn::Unique("parent-a".to_string()),
            }
        );
        assert_eq!(
            input_queue
                .take_next_pending_input_batch(&active_turn)
                .await,
            PendingInputBatch::Foreground(vec![foreground])
        );
        assert_eq!(
            input_queue
                .take_next_pending_input_batch(&active_turn)
                .await,
            PendingInputBatch::Background {
                items: vec![completion_b],
                parent_turn: PendingParentTurn::Unique("parent-b".to_string()),
            }
        );
    }

    #[tokio::test]
    async fn input_queue_tracks_pending_trigger_turn_mail() {
        let input_queue = InputQueue::new();

        let queued_mail = make_mail(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            "queued",
            /*trigger_turn*/ false,
        );
        input_queue
            .enqueue_mailbox_communication(queued_mail, /*parent_turn_id*/ None)
            .await;
        assert!(!input_queue.has_trigger_turn_mailbox_items().await);

        let trigger_mail = make_mail(
            AgentPath::root(),
            AgentPath::try_from("/root/worker").expect("agent path"),
            "wake",
            /*trigger_turn*/ true,
        );
        input_queue
            .enqueue_mailbox_communication(trigger_mail, /*parent_turn_id*/ None)
            .await;
        assert!(input_queue.has_trigger_turn_mailbox_items().await);
    }

    #[tokio::test]
    async fn background_wake_requests_are_coalesced_until_a_turn_claims_them() {
        let input_queue = InputQueue::new();

        input_queue
            .request_background_wake(PendingParentTurn::Empty, /*agent_completion*/ false);
        input_queue
            .request_background_wake(PendingParentTurn::Empty, /*agent_completion*/ false);
        assert!(input_queue.background_wake_requested());

        assert!(input_queue.claim_background_wake());
        assert!(!input_queue.background_wake_requested());
        assert!(!input_queue.claim_background_wake());
    }

    #[tokio::test]
    async fn background_wake_notification_waits_for_explicit_durable_wake() {
        let input_queue = InputQueue::new();
        input_queue
            .request_background_wake(PendingParentTurn::Empty, /*agent_completion*/ false);

        let mut notified = Box::pin(input_queue.background_wake_notified());
        tokio::select! {
            biased;
            _ = &mut notified => panic!("a flag alone must not wake the submission loop"),
            _ = tokio::task::yield_now() => {}
        }

        input_queue.notify_background_wake();
        notified.await;
    }

    #[tokio::test]
    async fn background_notifications_can_be_recovered_before_turn_state_cleanup() {
        let input_queue = InputQueue::new();
        let turn_state = Mutex::new(TurnState::default());
        let monitor_item = ContextualUserFragment::into(MonitorNotification {
            process_id: 1,
            seq: 1,
            command: "watch".to_string(),
            kind: "watcher",
            terminal_state: None,
            lines: vec!["output".to_string()],
            omitted_lines: 0,
            suppressed_notifications: 0,
            note: None,
        });
        let exec_completion_item =
            ContextualUserFragment::into(ExecCommandCompletionNotification {
                session_id: 2,
                command: "build".to_string(),
                completion: ExecCommandCompletion::Exited { exit_code: 0 },
                output_may_be_available: true,
            });
        let ordinary_item = ResponseItem::Message {
            id: None,
            role: "developer".to_string(),
            content: vec![codex_protocol::models::ContentItem::InputText {
                text: "ordinary".to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        };
        let agent_completion = make_mail(
            AgentPath::try_from("/root/worker").expect("agent path"),
            AgentPath::root(),
            "done",
            /*trigger_turn*/ false,
        );
        input_queue
            .extend_pending_input_for_turn_state(
                &turn_state,
                vec![
                    TurnInput::ResponseItem(monitor_item.clone()),
                    TurnInput::ResponseItem(exec_completion_item.clone()),
                    TurnInput::AgentCompletion(agent_completion.clone()),
                    TurnInput::ResponseItem(ordinary_item),
                ],
            )
            .await;

        assert_eq!(
            (
                vec![
                    TurnInput::ResponseItem(monitor_item),
                    TurnInput::ResponseItem(exec_completion_item),
                    TurnInput::AgentCompletion(agent_completion),
                ],
                PendingParentTurn::Empty,
            ),
            input_queue
                .take_pending_background_inputs_for_turn_state(&turn_state)
                .await
        );
        assert_eq!(
            vec![TurnInput::ResponseItem(ResponseItem::Message {
                id: None,
                role: "developer".to_string(),
                content: vec![codex_protocol::models::ContentItem::InputText {
                    text: "ordinary".to_string(),
                }],
                phase: None,
                internal_chat_message_metadata_passthrough: None,
            })],
            input_queue
                .take_pending_input_for_turn_state(&turn_state)
                .await
        );
    }

    #[tokio::test]
    async fn pending_agent_completion_is_reported_as_mailbox_activity() {
        let input_queue = InputQueue::new();
        let turn_state = Mutex::new(TurnState::default());
        input_queue
            .extend_pending_input_for_turn_state(
                &turn_state,
                vec![TurnInput::AgentCompletion(make_mail(
                    AgentPath::try_from("/root/worker").expect("agent path"),
                    AgentPath::root(),
                    "done",
                    /*trigger_turn*/ false,
                ))],
            )
            .await;

        let (_activity_rx, pending_activity) =
            input_queue.subscribe_activity(Some(&turn_state)).await;

        assert_eq!(pending_activity, Some(InputQueueActivity::Mailbox));
    }

    #[tokio::test]
    async fn pending_agent_completion_activity_is_reported_as_mailbox_activity() {
        let input_queue = InputQueue::new();
        let turn_state = Mutex::new(TurnState::default());
        input_queue.request_agent_completion_activity();

        let (_activity_rx, pending_activity) =
            input_queue.subscribe_activity(Some(&turn_state)).await;

        assert_eq!(pending_activity, Some(InputQueueActivity::Mailbox));
    }

    #[tokio::test]
    async fn pending_legacy_completion_is_reported_as_mailbox_activity() {
        let input_queue = InputQueue::new();
        let turn_state = Mutex::new(TurnState::default());
        let notification = ContextualUserFragment::into(crate::context::SubagentNotification::new(
            "worker",
            codex_protocol::protocol::AgentStatus::Completed(Some("done".to_string())),
        ));
        input_queue
            .extend_pending_input_for_turn_state(
                &turn_state,
                vec![TurnInput::ResponseItem(notification)],
            )
            .await;

        let (_activity_rx, pending_activity) =
            input_queue.subscribe_activity(Some(&turn_state)).await;

        assert_eq!(pending_activity, Some(InputQueueActivity::Mailbox));
    }

    #[tokio::test]
    async fn pending_process_notification_is_not_reported_as_mailbox_activity() {
        let input_queue = InputQueue::new();
        let turn_state = Mutex::new(TurnState::default());
        let notification = ContextualUserFragment::into(MonitorNotification {
            process_id: 1,
            seq: 1,
            command: "watch".to_string(),
            kind: "watcher",
            terminal_state: None,
            lines: Vec::new(),
            omitted_lines: 0,
            suppressed_notifications: 0,
            note: None,
        });
        input_queue
            .extend_pending_input_for_turn_state(
                &turn_state,
                vec![TurnInput::ResponseItem(notification)],
            )
            .await;

        let (_activity_rx, pending_activity) =
            input_queue.subscribe_activity(Some(&turn_state)).await;

        assert_eq!(pending_activity, None);
    }

    #[test]
    fn injected_agent_completion_publishes_mailbox_activity() {
        let completion = TurnInput::AgentCompletion(make_mail(
            AgentPath::try_from("/root/worker").expect("agent path"),
            AgentPath::root(),
            "done",
            /*trigger_turn*/ false,
        ));

        assert_eq!(
            InputQueue::background_input_activity(&completion),
            Some(InputQueueActivity::Mailbox)
        );
    }
}
