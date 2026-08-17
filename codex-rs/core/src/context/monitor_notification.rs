use super::ContextualUserFragment;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_utils_output_truncation::approx_bytes_for_tokens;
use codex_utils_string::take_bytes_at_char_boundary;

// Keep the complete marked fragment around the same size as the existing
// small context fragments. The individual budgets below are for serialized
// JSON, rather than source-string bytes, because quotes and control characters
// can expand substantially during JSON encoding.
const MAX_NOTIFICATION_TOKENS: usize = 1_000;
const MAX_COMMAND_JSON_BYTES: usize = 512;
const MAX_TERMINAL_STATE_JSON_BYTES: usize = 256;
const MAX_NOTE_JSON_BYTES: usize = 384;
const MAX_LINE_JSON_BYTES: usize = 56;
const MAX_LINES_JSON_BYTES: usize = 2_300;
const TRUNCATION_SUFFIX: &str = "…[truncated]";

/// A model-visible notification produced by a `monitor` job or watcher.
///
/// Monitor output reaches the client as `ExecCommandOutputDelta` events, which
/// the model never sees. This fragment is the model's channel: the watcher
/// batches complete lines into one of these and injects it into the active
/// turn. Every monitor delivers exactly one notification with `final` set,
/// whatever the outcome, so silence is never mistaken for success.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MonitorNotification {
    pub(crate) process_id: i32,
    pub(crate) seq: u64,
    pub(crate) command: String,
    pub(crate) kind: &'static str,
    /// Terminal state description; `Some` on the single final notification.
    pub(crate) terminal_state: Option<String>,
    pub(crate) lines: Vec<String>,
    /// Lines dropped from the lines field because this notification hit one of
    /// its model-visible caps. They remain in the monitor's retained output.
    pub(crate) omitted_lines: usize,
    /// Batches never sent because the monitor exhausted its notification cap.
    pub(crate) suppressed_notifications: u64,
    pub(crate) note: Option<String>,
}

impl ContextualUserFragment for MonitorNotification {
    fn role(&self) -> &'static str {
        "developer"
    }

    /// Each notification is its own message so the sequence stays legible and a
    /// later batch cannot be merged into an earlier one.
    fn requires_separate_message(&self) -> bool {
        true
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        ("<monitor_notification>", "</monitor_notification>")
    }

    fn body(&self) -> String {
        let (lines, omitted_lines) = bounded_lines(&self.lines, self.omitted_lines);
        let mut payload = serde_json::Map::new();
        payload.insert("process_id".to_string(), self.process_id.into());
        payload.insert("seq".to_string(), self.seq.into());
        payload.insert(
            "command".to_string(),
            bounded_json_string(&self.command, MAX_COMMAND_JSON_BYTES).into(),
        );
        payload.insert("kind".to_string(), self.kind.into());
        payload.insert("final".to_string(), self.terminal_state.is_some().into());
        payload.insert("lines".to_string(), lines.into());
        if let Some(state) = &self.terminal_state {
            payload.insert(
                "state".to_string(),
                bounded_json_string(state, MAX_TERMINAL_STATE_JSON_BYTES).into(),
            );
        }
        if omitted_lines > 0 {
            payload.insert("omitted_lines".to_string(), omitted_lines.into());
        }
        if self.suppressed_notifications > 0 {
            payload.insert(
                "suppressed_notifications".to_string(),
                self.suppressed_notifications.into(),
            );
        }
        if let Some(note) = &self.note {
            payload.insert(
                "note".to_string(),
                bounded_json_string(note, MAX_NOTE_JSON_BYTES).into(),
            );
        }
        let mut payload = serde_json::Value::Object(payload);
        let mut body =
            serde_json::to_string(&payload).expect("monitor notification payload should serialize");
        let (start_marker, end_marker) = Self::type_markers();
        let max_body_bytes = approx_bytes_for_tokens(MAX_NOTIFICATION_TOKENS)
            .saturating_sub(start_marker.len() + end_marker.len() + 2);
        if body.len() > max_body_bytes {
            // The per-field budgets above make this unreachable for the
            // current schema. Keep a runtime fallback so a future field cannot
            // turn this fragment into an unbounded context item.
            let payload = payload
                .as_object_mut()
                .expect("monitor notification payload should be an object");
            payload.insert("command".to_string(), TRUNCATION_SUFFIX.to_string().into());
            payload.insert(
                "kind".to_string(),
                bounded_json_string(self.kind, 64).into(),
            );
            payload.insert("lines".to_string(), Vec::<String>::new().into());
            if payload.contains_key("state") {
                payload.insert("state".to_string(), TRUNCATION_SUFFIX.to_string().into());
            }
            if payload.contains_key("note") {
                payload.insert("note".to_string(), TRUNCATION_SUFFIX.to_string().into());
            }
            body = serde_json::to_string(&serde_json::Value::Object(payload.clone()))
                .expect("bounded monitor notification payload should serialize");
        }
        debug_assert!(
            body.len() <= max_body_bytes,
            "monitor notification body exceeded its context budget"
        );
        format!("\n{body}\n")
    }
}

/// Bound each JSON string by its encoded size, preserving a useful prefix and
/// an explicit marker. This is deliberately based on serialized bytes so a
/// command containing quotes, backslashes, or control characters cannot evade
/// the context budget through JSON escaping.
fn bounded_json_string(value: &str, max_json_bytes: usize) -> String {
    let serialized_len = |candidate: &str| {
        serde_json::to_string(candidate)
            .expect("monitor notification string should serialize")
            .len()
    };

    if serialized_len(value) <= max_json_bytes {
        return value.to_string();
    }

    let mut prefix_end = take_bytes_at_char_boundary(value, max_json_bytes).len();
    loop {
        let prefix = &value[..prefix_end];
        let candidate = format!("{prefix}{TRUNCATION_SUFFIX}");
        if serialized_len(&candidate) <= max_json_bytes {
            return candidate;
        }
        if prefix_end == 0 {
            return TRUNCATION_SUFFIX.to_string();
        }
        prefix_end = value[..prefix_end]
            .char_indices()
            .next_back()
            .map_or(0, |(index, _)| index);
    }
}

/// Keep the line array bounded after JSON encoding as well as preserving the
/// upstream line cap. The omitted count includes lines dropped here so the
/// model can still distinguish a partial preview from an empty result.
fn bounded_lines(lines: &[String], omitted_lines: usize) -> (Vec<String>, usize) {
    let mut encoded_bytes = 2; // [ and ]
    let mut bounded =
        Vec::with_capacity(lines.len().min(MAX_LINES_JSON_BYTES / MAX_LINE_JSON_BYTES));

    for line in lines {
        let value = bounded_json_string(line, MAX_LINE_JSON_BYTES);
        let value_bytes = serde_json::to_string(&value)
            .expect("monitor notification line should serialize")
            .len();
        let separator_bytes = usize::from(!bounded.is_empty());
        if encoded_bytes + separator_bytes + value_bytes > MAX_LINES_JSON_BYTES {
            break;
        }
        encoded_bytes += separator_bytes + value_bytes;
        bounded.push(value);
    }

    let omitted = omitted_lines.saturating_add(lines.len().saturating_sub(bounded.len()));
    (bounded, omitted)
}

impl MonitorNotification {
    /// Returns whether a response item is one of the monitor fragments owned
    /// by the monitor delivery state machine.
    pub(crate) fn is_response_item(item: &ResponseItem) -> bool {
        let ResponseItem::Message { role, content, .. } = item else {
            return false;
        };
        role == "developer"
            && content.iter().any(|item| match item {
                ContentItem::InputText { text } => Self::matches_text(text),
                ContentItem::InputImage { .. }
                | ContentItem::InputAudio { .. }
                | ContentItem::OutputText { .. } => false,
            })
    }
}

#[cfg(test)]
#[path = "monitor_notification_tests.rs"]
mod tests;
