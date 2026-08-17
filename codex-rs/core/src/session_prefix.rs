use codex_protocol::AgentPath;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::InterAgentCommunication;
use codex_utils_output_truncation::TruncationPolicy;
use codex_utils_output_truncation::truncate_text;

use crate::context::ContextualUserFragment;
use crate::context::InterAgentCompletionMessage;
use crate::context_manager::estimate_item_token_count;

const COMPLETION_MESSAGE_MAX_TOKENS: usize = 1_000;
const COMPLETION_MESSAGE_ENVELOPE_TOKEN_RESERVE: usize = 250;
const ERROR_MAX_TOKENS: usize =
    COMPLETION_MESSAGE_MAX_TOKENS - COMPLETION_MESSAGE_ENVELOPE_TOKEN_RESERVE;
const LEGACY_STATUS_MAX_TOKENS: usize = ERROR_MAX_TOKENS / 6;
const ERROR_NEXT_ACTION: &str = "This agent's turn failed. If you still need this agent, use the available collaboration tools to give it another task.";

// Helpers for model-visible session state markers that are stored in user-role
// messages but are not user intent.

// TODO(jif) unify with structured schema
pub(crate) fn bounded_completion_status(status: &AgentStatus) -> AgentStatus {
    match status {
        AgentStatus::Completed(Some(message)) => AgentStatus::Completed(Some(truncate_text(
            message,
            TruncationPolicy::Tokens(LEGACY_STATUS_MAX_TOKENS),
        ))),
        AgentStatus::Errored(error) => AgentStatus::Errored(truncate_text(
            error,
            TruncationPolicy::Tokens(LEGACY_STATUS_MAX_TOKENS),
        )),
        status => status.clone(),
    }
}

pub(crate) fn bounded_completion_agent_reference(reference: &str) -> String {
    codex_utils_string::take_bytes_at_char_boundary(reference, AgentPath::MAX_NEW_PATH_BYTES)
        .to_string()
}

pub(crate) fn bounded_completion_turn_id(turn_id: &str) -> String {
    codex_utils_string::take_bytes_at_char_boundary(turn_id, AgentPath::MAX_NEW_PATH_BYTES)
        .to_string()
}

#[derive(Clone)]
pub(crate) struct CompletionAgentIdentity {
    pub(crate) model_path: AgentPath,
    pub(crate) reference: String,
}

pub(crate) fn completion_agent_identity(
    path: &AgentPath,
    thread_id: codex_protocol::ThreadId,
) -> CompletionAgentIdentity {
    if path.len() <= AgentPath::MAX_NEW_PATH_BYTES {
        return CompletionAgentIdentity {
            model_path: path.clone(),
            reference: path.to_string(),
        };
    }
    let model_path = AgentPath::root()
        .join(&format!(
            "thread_{}",
            thread_id.to_string().replace('-', "_")
        ))
        .unwrap_or_else(|_| AgentPath::root());
    CompletionAgentIdentity {
        model_path,
        reference: thread_id.to_string(),
    }
}

pub(crate) fn format_inter_agent_completion_message(
    task: &CompletionAgentIdentity,
    sender: &CompletionAgentIdentity,
    status: &AgentStatus,
    turn_id: Option<&str>,
) -> Option<String> {
    if matches!(
        status,
        AgentStatus::PendingInit | AgentStatus::Running | AgentStatus::Interrupted
    ) {
        return None;
    }

    let mut minimum = 0;
    let mut maximum = ERROR_MAX_TOKENS;
    let mut bounded_message = None;
    while minimum <= maximum {
        let budget = minimum + (maximum - minimum) / 2;
        let payload = match status {
            AgentStatus::Completed(Some(message)) => {
                truncate_text(message, TruncationPolicy::Tokens(budget))
            }
            AgentStatus::Errored(error) => {
                let error = truncate_text(error, TruncationPolicy::Tokens(budget));
                format!("Agent errored: {error}\n\n{ERROR_NEXT_ACTION}")
            }
            AgentStatus::Completed(None) => String::new(),
            AgentStatus::Shutdown => "Agent shut down.".to_string(),
            AgentStatus::NotFound => "Agent was not found.".to_string(),
            AgentStatus::PendingInit | AgentStatus::Running | AgentStatus::Interrupted => {
                unreachable!("non-terminal status returned before completion rendering")
            }
        };
        let message = InterAgentCompletionMessage::new(
            task.reference.clone(),
            sender.reference.clone(),
            payload,
        )
        .render();
        let mut communication = InterAgentCommunication::new(
            sender.model_path.clone(),
            task.model_path.clone(),
            Vec::new(),
            message.clone(),
            /*trigger_turn*/ false,
        );
        communication
            .set_turn_id_if_missing(turn_id.unwrap_or("00000000-0000-0000-0000-000000000000"));
        communication.id = Some(codex_protocol::ResponseItemId::with_suffix(
            "amsg",
            "00000000-0000-0000-0000-000000000000",
        ));
        let item = communication.to_model_input_item();
        if estimate_item_token_count(&item) < COMPLETION_MESSAGE_MAX_TOKENS as i64 {
            bounded_message = Some(message);
            minimum = budget.saturating_add(1);
        } else if budget == 0 {
            break;
        } else {
            maximum = budget - 1;
        }
    }
    bounded_message
}

#[cfg(test)]
#[path = "session_prefix_tests.rs"]
mod tests;

pub(crate) fn format_subagent_context_line(
    agent_reference: &str,
    agent_nickname: Option<&str>,
) -> String {
    match agent_nickname.filter(|nickname| !nickname.is_empty()) {
        Some(agent_nickname) => format!("- {agent_reference}: {agent_nickname}"),
        None => format!("- {agent_reference}"),
    }
}
