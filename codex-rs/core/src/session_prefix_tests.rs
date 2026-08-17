use codex_protocol::AgentPath;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::InterAgentCommunication;
use codex_utils_output_truncation::approx_token_count;

use crate::context::ContextualUserFragment;
use crate::context::SubagentNotification;

use super::COMPLETION_MESSAGE_MAX_TOKENS;
use super::ERROR_NEXT_ACTION;
use super::bounded_completion_status;
use super::format_inter_agent_completion_message;

#[test]
fn error_completion_message_stays_below_manual_review_threshold() {
    let task =
        super::completion_agent_identity(&AgentPath::root(), codex_protocol::ThreadId::new());
    let sender = super::completion_agent_identity(
        &AgentPath::try_from("/root/worker").expect("valid agent path"),
        codex_protocol::ThreadId::new(),
    );
    let message = format_inter_agent_completion_message(
        &task,
        &sender,
        &AgentStatus::Errored("stream disconnected ".repeat(1_000)),
        None,
    )
    .expect("error status should produce a completion message");

    assert!(approx_token_count(&message) < COMPLETION_MESSAGE_MAX_TOKENS);
    assert!(message.contains(ERROR_NEXT_ACTION));
}

#[test]
fn successful_completion_messages_are_bounded() {
    let status = AgentStatus::Completed(Some("\"\\\n\r\tchild output ".repeat(10_000)));
    let task =
        super::completion_agent_identity(&AgentPath::root(), codex_protocol::ThreadId::new());
    let sender = super::completion_agent_identity(
        &AgentPath::try_from("/root/worker").expect("valid agent path"),
        codex_protocol::ThreadId::new(),
    );
    let message = format_inter_agent_completion_message(&task, &sender, &status, None)
        .expect("completed status should produce a completion message");
    let legacy = SubagentNotification::new("worker", bounded_completion_status(&status)).render();

    assert!(approx_token_count(&message) < COMPLETION_MESSAGE_MAX_TOKENS);
    assert!(approx_token_count(&legacy) < COMPLETION_MESSAGE_MAX_TOKENS);
}

#[test]
fn completion_item_is_bounded_with_maximum_path_and_status() {
    let long_path = AgentPath::root()
        .join(&"a".repeat(AgentPath::MAX_NEW_PATH_BYTES - AgentPath::ROOT.len() - 1))
        .expect("maximum agent path should be valid");
    let turn_id = super::bounded_completion_turn_id(&"turn".repeat(10_000));
    let message = format_inter_agent_completion_message(
        &super::completion_agent_identity(&long_path, codex_protocol::ThreadId::new()),
        &super::completion_agent_identity(&long_path, codex_protocol::ThreadId::new()),
        &AgentStatus::Completed(Some("\"\\\n\r\tchild output ".repeat(10_000))),
        Some(&turn_id),
    )
    .expect("completed status should produce a completion message");
    let mut communication = InterAgentCommunication::new(
        long_path.clone(),
        long_path,
        Vec::new(),
        message,
        /*trigger_turn*/ false,
    );
    communication.set_turn_id_if_missing(&turn_id);
    communication.id = Some(codex_protocol::ResponseItemId::with_suffix(
        "amsg",
        "00000000-0000-0000-0000-000000000000",
    ));
    let item = communication.to_model_input_item();

    assert!(
        crate::context_manager::estimate_item_token_count(&item)
            < COMPLETION_MESSAGE_MAX_TOKENS as i64
    );
}
