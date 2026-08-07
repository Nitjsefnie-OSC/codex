use super::ContextualUserFragment;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;

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
    /// Lines dropped from `lines` because this notification hit its own cap.
    /// They remain in the monitor's retained output.
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
        let mut payload = serde_json::Map::new();
        payload.insert("process_id".to_string(), self.process_id.into());
        payload.insert("seq".to_string(), self.seq.into());
        payload.insert("command".to_string(), self.command.clone().into());
        payload.insert("kind".to_string(), self.kind.into());
        payload.insert("final".to_string(), self.terminal_state.is_some().into());
        payload.insert("lines".to_string(), self.lines.clone().into());
        if let Some(state) = &self.terminal_state {
            payload.insert("state".to_string(), state.clone().into());
        }
        if self.omitted_lines > 0 {
            payload.insert("omitted_lines".to_string(), self.omitted_lines.into());
        }
        if self.suppressed_notifications > 0 {
            payload.insert(
                "suppressed_notifications".to_string(),
                self.suppressed_notifications.into(),
            );
        }
        if let Some(note) = &self.note {
            payload.insert("note".to_string(), note.clone().into());
        }
        format!("\n{}\n", serde_json::Value::Object(payload))
    }
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
