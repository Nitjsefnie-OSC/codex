use super::*;
use codex_extension_api::ExtensionData;
use codex_extension_api::TurnItemContributor;
use codex_protocol::ResponseItemId;
use codex_protocol::items::AgentMessageContent;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use tracing_subscriber::prelude::*;

struct RewriteAgentMessageContributor;

impl TurnItemContributor for RewriteAgentMessageContributor {
    fn contribute<'a>(
        &'a self,
        _thread_store: &'a ExtensionData,
        _turn_store: &'a ExtensionData,
        item: &'a mut TurnItem,
    ) -> codex_extension_api::ExtensionFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if let TurnItem::AgentMessage(agent_message) = item {
                agent_message.content = vec![AgentMessageContent::Text {
                    text: "plan contributed assistant text".to_string(),
                }];
            }
            Ok(())
        })
    }
}

fn assistant_output_text(text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: Some(ResponseItemId::with_suffix("msg", "1")),
        role: "assistant".to_string(),
        content: vec![ContentItem::OutputText {
            text: text.to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

#[test]
fn post_sampling_token_estimate_is_disabled_by_always_on_sinks() {
    let feedback = codex_feedback::CodexFeedback::new();
    let subscriber = tracing_subscriber::registry()
        .with(feedback.logger_layer())
        .with(tracing_subscriber::fmt::layer().with_filter(codex_state::log_db::default_filter()));

    tracing::subscriber::with_default(subscriber, || {
        tracing::callsite::rebuild_interest_cache();
        assert!(!tracing::event_enabled!(
            target: POST_SAMPLING_TOKEN_ESTIMATE_TARGET,
            tracing::Level::TRACE,
            turn_id,
            estimated_token_count,
            message
        ));
    });
}

#[tokio::test]
async fn plan_mode_uses_contributed_turn_item_for_last_agent_message() {
    let (mut session, turn_context) = crate::session::tests::make_session_and_context().await;
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    builder.turn_item_contributor(Arc::new(RewriteAgentMessageContributor));
    session.services.extensions = Arc::new(builder.build());
    let turn_store = ExtensionData::new(turn_context.sub_id.clone());
    let mut state = PlanModeStreamState::new(&turn_context.sub_id);
    let mut last_agent_message = None;
    let item = assistant_output_text("original assistant text");

    let handled = handle_assistant_item_done_in_plan_mode(
        &session,
        &turn_context,
        &turn_store,
        &item,
        &mut state,
        /*previously_active_item*/ None,
        &mut last_agent_message,
    )
    .await;

    assert!(handled);
    assert_eq!(
        last_agent_message.as_deref(),
        Some("plan contributed assistant text")
    );
}

#[tokio::test]
async fn sampling_request_future_boundary_is_pointer_sized() {
    let (session, turn_context) = crate::session::tests::make_session_and_context().await;
    let sess = Arc::new(session);
    let turn_context = Arc::new(turn_context);
    let step_context = StepContext::for_test(Arc::clone(&turn_context));
    let turn_diff_tracker = Arc::new(tokio::sync::Mutex::new(TurnDiffTracker::new()));
    let mut client_session = crate::session::tests::test_model_client_session();
    let responses_metadata = turn_context.turn_metadata_state.to_responses_metadata(
        sess.installation_id.clone(),
        sess.current_window_id().await,
        CodexResponsesRequestKind::Turn,
    );

    let future = run_sampling_request(
        Arc::clone(&sess),
        step_context,
        Arc::clone(&turn_context.extension_data),
        turn_diff_tracker,
        &mut client_session,
        &responses_metadata,
        Vec::new(),
        CancellationToken::new(),
    );
    let future_size = std::mem::size_of_val(&future);

    assert!(
        future_size <= 2 * std::mem::size_of::<usize>(),
        "sampling request API future boundary is {future_size} bytes"
    );
}

#[tokio::test]
async fn try_sampling_request_future_boundary_is_pointer_sized() {
    let (session, turn_context) = crate::session::tests::make_session_and_context().await;
    let sess = Arc::new(session);
    let turn_context = Arc::new(turn_context);
    let step_context = StepContext::for_test(Arc::clone(&turn_context));
    let turn_diff_tracker = Arc::new(tokio::sync::Mutex::new(TurnDiffTracker::new()));
    let tool_runtime = ToolCallRuntime::new(
        Arc::clone(&sess),
        Arc::clone(&step_context),
        Arc::clone(&turn_diff_tracker),
    );
    let mut client_session = crate::session::tests::test_model_client_session();
    let responses_metadata = turn_context.turn_metadata_state.to_responses_metadata(
        sess.installation_id.clone(),
        sess.current_window_id().await,
        CodexResponsesRequestKind::Turn,
    );
    let prompt = build_prompt(
        Vec::new(),
        step_context.tool_router.as_ref(),
        turn_context.as_ref(),
        sess.get_base_instructions().await,
    );

    let future = try_run_sampling_request(
        tool_runtime,
        Arc::clone(&sess),
        Arc::clone(&turn_context),
        Arc::clone(&step_context.response_identity),
        Arc::clone(&turn_context.extension_data),
        &mut client_session,
        &responses_metadata,
        turn_diff_tracker,
        &prompt,
        CancellationToken::new(),
    );
    let future_size = std::mem::size_of_val(&future);

    assert!(
        future_size <= 2 * std::mem::size_of::<usize>(),
        "try sampling request API future boundary is {future_size} bytes"
    );
}

#[tokio::test]
async fn model_stream_future_boundary_is_pointer_sized() {
    let (session, turn_context) = crate::session::tests::make_session_and_context().await;
    let sess = Arc::new(session);
    let turn_context = Arc::new(turn_context);
    let mut client_session = crate::session::tests::test_model_client_session();
    let responses_metadata = turn_context.turn_metadata_state.to_responses_metadata(
        sess.installation_id.clone(),
        sess.current_window_id().await,
        CodexResponsesRequestKind::Turn,
    );
    let step_context = StepContext::for_test(Arc::clone(&turn_context));
    let prompt = build_prompt(
        Vec::new(),
        step_context.tool_router.as_ref(),
        turn_context.as_ref(),
        sess.get_base_instructions().await,
    );
    let inference_trace = sess.services.rollout_thread_trace.inference_trace_context(
        turn_context.sub_id.as_str(),
        turn_context.model_info.slug.as_str(),
        turn_context.provider.info().name.as_str(),
    );

    let future = client_session.stream(
        &prompt,
        &turn_context.model_info,
        &turn_context.session_telemetry,
        turn_context.reasoning_effort.clone(),
        turn_context.reasoning_summary,
        turn_context.config.service_tier.clone(),
        &responses_metadata,
        &inference_trace,
    );
    let future_size = std::mem::size_of_val(&future);

    assert!(
        future_size <= 2 * std::mem::size_of::<usize>(),
        "model stream API future boundary is {future_size} bytes"
    );
}
