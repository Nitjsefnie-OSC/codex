use crate::function_tool::FunctionCallError;
use crate::tools::context::FunctionToolOutput;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::whoami_spec::WHOAMI_TOOL_NAME;
use crate::tools::handlers::whoami_spec::create_whoami_tool;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;
use codex_protocol::models::ResponseInputItem;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use serde_json::Value as JsonValue;
use serde_json::json;

#[derive(Debug, Clone)]
struct WhoamiOutput {
    slug: String,
    display_name: String,
    reasoning_effort: Option<String>,
    context_window: Option<i64>,
}

impl WhoamiOutput {
    fn fragment(&self) -> String {
        let mut lines = vec![
            format!("model slug: {}", self.slug),
            format!("model display name: {}", self.display_name),
        ];
        if let Some(reasoning_effort) = &self.reasoning_effort {
            lines.push(format!("reasoning effort: {reasoning_effort}"));
        }
        if let Some(context_window) = self.context_window {
            lines.push(format!("context window: {context_window} tokens"));
        }
        lines.join("\n")
    }
}

impl ToolOutput for WhoamiOutput {
    fn log_preview(&self) -> String {
        self.fragment()
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        FunctionToolOutput::from_text(self.fragment(), Some(true))
            .to_response_item(call_id, payload)
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        json!({
            "slug": self.slug,
            "display_name": self.display_name,
            "reasoning_effort": self.reasoning_effort,
            "context_window": self.context_window,
        })
    }
}

pub struct WhoamiHandler;

impl ToolExecutor<ToolInvocation> for WhoamiHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain(WHOAMI_TOOL_NAME)
    }

    fn spec(&self) -> ToolSpec {
        create_whoami_tool()
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(async move {
            if !matches!(invocation.payload, ToolPayload::Function { .. }) {
                return Err(FunctionCallError::RespondToModel(
                    "whoami handler received unsupported payload".to_string(),
                ));
            }

            let model_info = &invocation.turn.model_info;

            Ok(boxed_tool_output(WhoamiOutput {
                slug: model_info.slug.clone(),
                display_name: model_info.display_name.clone(),
                reasoning_effort: invocation
                    .turn
                    .reasoning_effort
                    .as_ref()
                    .map(|effort| effort.as_str().to_string()),
                context_window: model_info.context_window,
            }))
        })
    }
}

impl CoreToolRuntime for WhoamiHandler {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::session::Session;
    use crate::session::step_context::StepContext;
    use crate::session::tests::make_session_and_context;
    use crate::tools::context::ToolCallSource;
    use crate::turn_diff_tracker::TurnDiffTracker;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn make_invocation(session: Session, turn: Arc<crate::TurnContext>) -> ToolInvocation {
        ToolInvocation {
            session: Arc::new(session),
            step_context: StepContext::for_test(Arc::clone(&turn)),
            turn,
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            tracker: Arc::new(Mutex::new(TurnDiffTracker::new())),
            call_id: "call-whoami".to_string(),
            tool_name: ToolName::plain(WHOAMI_TOOL_NAME),
            source: ToolCallSource::Direct,
            payload: ToolPayload::Function {
                arguments: "{}".to_string(),
            },
        }
    }

    #[tokio::test]
    async fn handle_returns_the_executing_models_slug_and_display_name() {
        let (session, turn) = make_session_and_context().await;
        let expected_slug = turn.model_info.slug.clone();
        let expected_display_name = turn.model_info.display_name.clone();
        let expected_context_window = turn.model_info.context_window;
        let expected_reasoning_effort = turn
            .reasoning_effort
            .as_ref()
            .map(|effort| effort.as_str().to_string());
        let turn = Arc::new(turn);

        let result = WhoamiHandler
            .handle(make_invocation(session, turn))
            .await
            .expect("whoami should succeed");

        let payload = ToolPayload::Function {
            arguments: "{}".to_string(),
        };
        assert_eq!(
            result.code_mode_result(&payload),
            json!({
                "slug": expected_slug,
                "display_name": expected_display_name,
                "reasoning_effort": expected_reasoning_effort,
                "context_window": expected_context_window,
            })
        );
    }

    #[tokio::test]
    async fn handle_rejects_unsupported_payload() {
        let (session, turn) = make_session_and_context().await;
        let turn = Arc::new(turn);
        let mut invocation = make_invocation(session, turn);
        invocation.payload = ToolPayload::Custom {
            input: "not json".to_string(),
        };

        let result = WhoamiHandler.handle(invocation).await;

        let Err(FunctionCallError::RespondToModel(message)) = result else {
            panic!("expected unsupported payload error");
        };
        assert_eq!(message, "whoami handler received unsupported payload");
    }
}
