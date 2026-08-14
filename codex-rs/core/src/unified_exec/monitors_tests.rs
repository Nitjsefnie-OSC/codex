use super::*;
use pretty_assertions::assert_eq;

fn allowed_seq(slot: NotificationSlot) -> Option<u64> {
    match slot {
        NotificationSlot::Allowed { seq } => Some(seq),
        NotificationSlot::Suppressed => None,
    }
}

#[test]
fn notifications_are_numbered_from_one_without_gaps() {
    let mut counters = MonitorCounters::default();

    let sequence: Vec<Option<u64>> = (0..3)
        .map(|_| allowed_seq(counters.reserve()))
        .collect::<Vec<_>>();

    assert_eq!(sequence, vec![Some(1), Some(2), Some(3)]);
}

#[test]
fn a_noisy_monitor_stops_at_its_cap_and_counts_what_it_dropped() {
    let mut counters = MonitorCounters::default();
    for _ in 0..MAX_MONITOR_NOTIFICATIONS {
        assert!(allowed_seq(counters.reserve()).is_some());
    }

    let over_cap: Vec<Option<u64>> = (0..5).map(|_| allowed_seq(counters.reserve())).collect();

    assert_eq!(over_cap, vec![None; 5]);
    assert_eq!(counters.delivered, MAX_MONITOR_NOTIFICATIONS);
    assert_eq!(counters.suppressed, 5);
}

#[test]
fn compaction_starts_a_new_notification_window_without_resetting_lifetime_stats() {
    let mut counters = MonitorCounters::default();
    for _ in 0..MAX_MONITOR_NOTIFICATIONS {
        assert!(allowed_seq(counters.reserve()).is_some());
    }
    assert_eq!(allowed_seq(counters.reserve()), None);

    counters.begin_notification_window();

    assert_eq!(allowed_seq(counters.reserve()), Some(21));
    assert_eq!(counters.delivered, 21);
    assert_eq!(counters.suppressed, 1);
    assert_eq!(counters.last_seq, 21);
}

#[test]
fn the_terminal_notification_is_never_capped() {
    let mut counters = MonitorCounters::default();
    for _ in 0..MAX_MONITOR_NOTIFICATIONS + 10 {
        counters.reserve();
    }

    let terminal = counters.reserve_terminal();

    assert_eq!(terminal, MAX_MONITOR_NOTIFICATIONS + 1);
    assert_eq!(counters.last_seq, terminal);
    assert_eq!(allowed_seq(counters.reserve()), None);
}

#[test]
fn acknowledgement_moves_forward_only_and_never_past_what_was_delivered() {
    let mut counters = MonitorCounters::default();
    for _ in 0..3 {
        counters.reserve();
    }

    assert_eq!(counters.acknowledge(MonitorAcknowledgement::Through(2)), 2);
    assert_eq!(counters.unacknowledged(), 1);
    // An older acknowledgement cannot walk the watermark back.
    assert_eq!(counters.acknowledge(MonitorAcknowledgement::Through(1)), 2);
    // Nor can one reach past the last notification actually delivered.
    assert_eq!(counters.acknowledge(MonitorAcknowledgement::Through(99)), 3);
    assert_eq!(counters.unacknowledged(), 0);
}

#[test]
fn reading_without_acknowledging_leaves_the_watermark_alone() {
    let mut counters = MonitorCounters::default();
    counters.reserve();
    counters.reserve();

    assert_eq!(counters.acknowledge(MonitorAcknowledgement::None), 0);
    assert_eq!(counters.unacknowledged(), 2);

    assert_eq!(counters.acknowledge(MonitorAcknowledgement::All), 2);
    assert_eq!(counters.unacknowledged(), 0);
}

#[test]
fn a_terminal_state_describes_itself_for_the_final_notification() {
    assert_eq!(
        MonitorState::Exited { exit_code: 3 }.describe(),
        "exited with code 3"
    );
    assert_eq!(MonitorState::Stopped.describe(), "stopped");
    assert_eq!(MonitorState::TimedOut.describe(), "timed out");
    assert!(!MonitorState::Running.is_terminal());
    assert!(MonitorState::Exited { exit_code: 0 }.is_terminal());
}
