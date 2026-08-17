use super::ContextualUserFragment;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_utils_string::take_bytes_at_char_boundary;

const MAX_NOTIFICATION_FIELD_BYTES: usize = 512;
const TRUNCATION_SUFFIX: &str = "…";

/// Terminal result announced after an `exec_command` returned a live session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExecCommandCompletion {
    Exited { exit_code: i32 },
    Failed { message: String },
}

/// Model-visible notification that a yielded `exec_command` reached a terminal state.
///
/// The notification deliberately carries no process output. The retained output
/// remains owned by unified exec and must still be retrieved with `write_stdin`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecCommandCompletionNotification {
    pub(crate) session_id: i32,
    pub(crate) command: String,
    pub(crate) completion: ExecCommandCompletion,
    pub(crate) output_may_be_available: bool,
}

impl ContextualUserFragment for ExecCommandCompletionNotification {
    fn role(&self) -> &'static str {
        "developer"
    }

    fn requires_separate_message(&self) -> bool {
        true
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        ("<exec_command_completion>", "</exec_command_completion>")
    }

    fn body(&self) -> String {
        let mut payload = serde_json::Map::new();
        payload.insert("session_id".to_string(), self.session_id.into());
        payload.insert("command".to_string(), bounded_field(&self.command).into());
        match &self.completion {
            ExecCommandCompletion::Exited { exit_code } => {
                payload.insert("status".to_string(), "exited".into());
                payload.insert("exit_code".to_string(), (*exit_code).into());
            }
            ExecCommandCompletion::Failed { message } => {
                payload.insert("status".to_string(), "failed".into());
                payload.insert("message".to_string(), bounded_field(message).into());
            }
        }
        payload.insert(
            "output_may_be_available".to_string(),
            self.output_may_be_available.into(),
        );
        payload.insert(
            "note".to_string(),
            if self.output_may_be_available {
                format!(
                    "Final output may still be available. Call write_stdin(session_id={}) to collect it; an unknown session means it was already collected, evicted, or released.",
                    self.session_id
                )
            } else {
                "The final output is no longer retained; do not call write_stdin for this session."
                    .to_string()
            }
            .into(),
        );
        format!("\n{}\n", serde_json::Value::Object(payload))
    }
}

fn bounded_field(value: &str) -> String {
    let prefix = take_bytes_at_char_boundary(value, MAX_NOTIFICATION_FIELD_BYTES);
    if prefix.len() == value.len() {
        value.to_string()
    } else {
        format!("{prefix}{TRUNCATION_SUFFIX}")
    }
}

impl ExecCommandCompletionNotification {
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
#[path = "exec_command_completion_notification_tests.rs"]
mod tests;
