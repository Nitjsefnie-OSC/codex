use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

#[test]
fn notification_fields_are_bounded_for_model_context() {
    let notification = ExecCommandCompletionNotification {
        session_id: 42,
        command: "c".repeat(MAX_NOTIFICATION_FIELD_BYTES + 1),
        completion: ExecCommandCompletion::Failed {
            message: "m".repeat(MAX_NOTIFICATION_FIELD_BYTES + 1),
        },
        output_may_be_available: true,
    };

    let payload: serde_json::Value =
        serde_json::from_str(notification.body().trim()).expect("body should contain valid JSON");

    assert_eq!(
        payload,
        json!({
            "session_id": 42,
            "command": format!("{}…", "c".repeat(MAX_NOTIFICATION_FIELD_BYTES)),
            "status": "failed",
            "message": format!("{}…", "m".repeat(MAX_NOTIFICATION_FIELD_BYTES)),
            "output_may_be_available": true,
            "note": "Final output may still be available. Call write_stdin(session_id=42) to collect it; an unknown session means it was already collected, evicted, or released.",
        })
    );
}
