use super::*;
use pretty_assertions::assert_eq;

fn notification() -> MonitorNotification {
    MonitorNotification {
        process_id: 1234,
        seq: 2,
        command: "cargo build".to_string(),
        kind: "job",
        terminal_state: None,
        lines: vec!["compiling".to_string(), "linking".to_string()],
        omitted_lines: 0,
        suppressed_notifications: 0,
        note: None,
    }
}

#[test]
fn a_batch_notification_carries_its_lines_and_sequence() {
    let body: serde_json::Value =
        serde_json::from_str(notification().body().trim()).expect("body is JSON");

    assert_eq!(
        body,
        serde_json::json!({
            "process_id": 1234,
            "seq": 2,
            "command": "cargo build",
            "kind": "job",
            "final": false,
            "lines": ["compiling", "linking"],
        })
    );
}

#[test]
fn the_terminal_notification_is_flagged_and_names_the_state() {
    let mut fragment = notification();
    fragment.seq = 7;
    fragment.terminal_state = Some("exited with code 0".to_string());
    fragment.lines = Vec::new();
    fragment.suppressed_notifications = 3;

    let body: serde_json::Value =
        serde_json::from_str(fragment.body().trim()).expect("body is JSON");

    assert_eq!(body["final"], serde_json::json!(true));
    assert_eq!(body["state"], serde_json::json!("exited with code 0"));
    assert_eq!(body["suppressed_notifications"], serde_json::json!(3));
}

#[test]
fn notifications_are_marked_and_never_merged_with_a_neighbour() {
    let fragment = notification();

    assert!(fragment.requires_separate_message());
    assert!(MonitorNotification::matches_text(&fragment.render()));
}

#[test]
fn response_item_classifier_recognizes_only_monitor_fragments() {
    let monitor_item = ContextualUserFragment::into(notification());
    let ordinary_item = codex_protocol::models::ResponseItem::Message {
        id: None,
        role: "developer".to_string(),
        content: vec![codex_protocol::models::ContentItem::InputText {
            text: "ordinary developer context".to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    };

    assert!(MonitorNotification::is_response_item(&monitor_item));
    assert!(!MonitorNotification::is_response_item(&ordinary_item));
}
