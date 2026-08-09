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

const SERVER_REPORTED_IDENTITY_PROVENANCE: &str = "server_reported";
const UNVERIFIED_REQUEST_IDENTITY_PROVENANCE: &str = "request_metadata_unverified";
const REQUEST_METADATA_PROVENANCE: &str = "request_metadata";
const MODEL_CATALOG_PROVENANCE: &str = "model_catalog_configuration";
const SERVER_MODEL_SLUG_PROVENANCE: &str = "server_reported_slug";
const SERVER_MODEL_METADATA_UNAVAILABLE_PROVENANCE: &str = "unavailable_for_reported_model";

#[derive(Debug, Clone)]
struct WhoamiOutput {
    slug: String,
    display_name: String,
    reasoning_effort: Option<String>,
    context_window: Option<i64>,
    requested_model: String,
    requested_display_name: String,
    requested_context_window: Option<i64>,
    server_reported_model: Option<String>,
    model_identity_provenance: &'static str,
    model_identity_verified: bool,
    display_name_provenance: &'static str,
    reasoning_effort_provenance: &'static str,
    context_window_provenance: &'static str,
}

impl WhoamiOutput {
    fn fragment(&self) -> String {
        let mut lines = vec![
            format!("model slug: {}", self.slug),
            format!("model display name: {}", self.display_name),
            format!(
                "model identity provenance: {} (requested model: {}, server reported: {}, verified: {})",
                self.model_identity_provenance,
                self.requested_model,
                self.server_reported_model.as_deref().unwrap_or("none"),
                self.model_identity_verified
            ),
            format!(
                "model display name provenance: {} (requested display name: {})",
                self.display_name_provenance, self.requested_display_name
            ),
        ];
        if let Some(reasoning_effort) = &self.reasoning_effort {
            lines.push(format!(
                "reasoning effort: {reasoning_effort} ({})",
                self.reasoning_effort_provenance
            ));
        } else {
            lines.push(format!(
                "reasoning effort: unavailable ({})",
                self.reasoning_effort_provenance
            ));
        }
        if let Some(context_window) = self.context_window {
            lines.push(format!(
                "context window: {context_window} tokens ({})",
                self.context_window_provenance
            ));
        } else {
            lines.push(format!(
                "context window: unavailable ({})",
                self.context_window_provenance
            ));
        }
        lines.join("\n")
    }

    fn structured_output(&self) -> JsonValue {
        json!({
            "slug": self.slug,
            "display_name": self.display_name,
            "reasoning_effort": self.reasoning_effort,
            "context_window": self.context_window,
            "requested_model": self.requested_model,
            "requested_display_name": self.requested_display_name,
            "requested_context_window": self.requested_context_window,
            "server_reported_model": self.server_reported_model,
            "model_identity_provenance": self.model_identity_provenance,
            "model_identity_verified": self.model_identity_verified,
            "display_name_provenance": self.display_name_provenance,
            "reasoning_effort_provenance": self.reasoning_effort_provenance,
            "context_window_provenance": self.context_window_provenance,
        })
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
        FunctionToolOutput::from_text(self.structured_output().to_string(), Some(true))
            .to_response_item(call_id, payload)
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        self.structured_output()
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
            let requested_model = model_info.slug.clone();
            let requested_display_name = model_info.display_name.clone();
            let requested_context_window = model_info.context_window;
            let server_reported_model = invocation
                .step_context
                .response_identity
                .latest_server_model()
                .await;
            let model_identity_provenance = if server_reported_model.is_some() {
                SERVER_REPORTED_IDENTITY_PROVENANCE
            } else {
                UNVERIFIED_REQUEST_IDENTITY_PROVENANCE
            };
            let slug = server_reported_model
                .clone()
                .unwrap_or_else(|| requested_model.clone());
            // `get_model_info` only reads the model manager's in-memory catalog; it
            // never refreshes or mutates model configuration while a tool runs.
            let reported_model_info = match server_reported_model.as_deref() {
                Some(server_reported_model) => Some(
                    invocation
                        .session
                        .services
                        .models_manager
                        .get_model_info(
                            server_reported_model,
                            &invocation.turn.config.to_models_manager_config(),
                        )
                        .await,
                ),
                None => None,
            };
            let reported_model_has_catalog_metadata = reported_model_info
                .as_ref()
                .is_some_and(|model_info| !model_info.used_fallback_model_metadata);
            let display_name = reported_model_info
                .as_ref()
                .filter(|model_info| !model_info.used_fallback_model_metadata)
                .map(|model_info| model_info.display_name.clone())
                .unwrap_or_else(|| {
                    server_reported_model
                        .clone()
                        .unwrap_or_else(|| requested_display_name.clone())
                });
            let context_window = match server_reported_model.as_ref() {
                Some(_) if reported_model_has_catalog_metadata => {
                    reported_model_info
                        .as_ref()
                        .and_then(|model_info| model_info.context_window)
                }
                Some(_) => None,
                None => requested_context_window,
            };
            let display_name_provenance = if server_reported_model.is_none()
                || reported_model_has_catalog_metadata
            {
                MODEL_CATALOG_PROVENANCE
            } else {
                SERVER_MODEL_SLUG_PROVENANCE
            };
            let context_window_provenance = if server_reported_model.is_none()
                || reported_model_has_catalog_metadata
            {
                MODEL_CATALOG_PROVENANCE
            } else {
                SERVER_MODEL_METADATA_UNAVAILABLE_PROVENANCE
            };

            Ok(boxed_tool_output(WhoamiOutput {
                slug,
                display_name,
                reasoning_effort: invocation
                    .turn
                    .request_reasoning_effort()
                    .map(|effort| effort.as_str().to_string()),
                context_window,
                requested_model,
                server_reported_model,
                model_identity_provenance,
                model_identity_verified: model_identity_provenance
                    == SERVER_REPORTED_IDENTITY_PROVENANCE,
                display_name_provenance,
                reasoning_effort_provenance: REQUEST_METADATA_PROVENANCE,
                context_window_provenance,
                requested_display_name,
                requested_context_window,
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
    use codex_protocol::models::FunctionCallOutputBody;
    use codex_protocol::models::ResponseInputItem;
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
    async fn handle_reports_request_metadata_as_unverified_without_server_model() {
        let (session, turn) = make_session_and_context().await;
        let requested_model = turn.model_info.slug.clone();
        let requested_display_name = turn.model_info.display_name.clone();
        let requested_context_window = turn.model_info.context_window;
        let requested_reasoning_effort = turn
            .request_reasoning_effort()
            .map(|effort| effort.as_str().to_string());
        let turn = Arc::new(turn);

        let result = WhoamiHandler
            .handle(make_invocation(session, turn))
            .await
            .expect("whoami should succeed");

        let payload = ToolPayload::Function {
            arguments: "{}".to_string(),
        };
        let expected = json!({
            "slug": requested_model.clone(),
            "display_name": requested_display_name.clone(),
            "reasoning_effort": requested_reasoning_effort,
            "context_window": requested_context_window,
            "requested_model": requested_model,
            "requested_display_name": requested_display_name,
            "requested_context_window": requested_context_window,
            "server_reported_model": null,
            "model_identity_provenance": "request_metadata_unverified",
            "model_identity_verified": false,
            "display_name_provenance": "model_catalog_configuration",
            "reasoning_effort_provenance": "request_metadata",
            "context_window_provenance": "model_catalog_configuration",
        });
        assert_eq!(result.code_mode_result(&payload), expected);

        let ResponseInputItem::FunctionCallOutput { output, .. } =
            result.to_response_item("call-whoami", &payload)
        else {
            panic!("whoami should return a function-call output");
        };
        let FunctionCallOutputBody::Text(text) = output.body else {
            panic!("whoami should return structured JSON text");
        };
        let serialized: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(serialized, expected);
    }

    #[tokio::test]
    async fn handle_prefers_latest_server_reported_model_for_legacy_slug() {
        let (session, turn) = make_session_and_context().await;
        let requested_model = turn.model_info.slug.clone();
        let requested_display_name = turn.model_info.display_name.clone();
        let requested_context_window = turn.model_info.context_window;
        let requested_reasoning_effort = turn
            .request_reasoning_effort()
            .map(|effort| effort.as_str().to_string());
        let turn = Arc::new(turn);
        let mut invocation = make_invocation(session, Arc::clone(&turn));
        let response_generation = invocation
            .step_context
            .response_identity
            .begin_response()
            .await;
        invocation
            .step_context
            .response_identity
            .record_server_model_for_response(
                response_generation,
                "server-routed-model".to_string(),
            )
            .await;

        let result = WhoamiHandler
            .handle(invocation)
            .await
            .expect("whoami should succeed");

        let payload = ToolPayload::Function {
            arguments: "{}".to_string(),
        };
        assert_eq!(
            result.code_mode_result(&payload),
            json!({
                "slug": "server-routed-model",
                "display_name": "server-routed-model",
                "reasoning_effort": requested_reasoning_effort,
                "context_window": null,
                "requested_model": requested_model,
                "requested_display_name": requested_display_name,
                "requested_context_window": requested_context_window,
                "server_reported_model": "server-routed-model",
                "model_identity_provenance": "server_reported",
                "model_identity_verified": true,
                "display_name_provenance": "server_reported_slug",
                "reasoning_effort_provenance": "request_metadata",
                "context_window_provenance": "unavailable_for_reported_model",
            })
        );
    }

    #[tokio::test]
    async fn response_identity_discards_late_events_from_prior_sampling_attempt() {
        let response_identity = crate::session::step_context::ResponseIdentityState::default();
        let first_generation = response_identity.begin_response().await;
        response_identity
            .record_server_model_for_response(first_generation, "first-model".to_string())
            .await;

        let second_generation = response_identity.begin_response().await;
        response_identity
            .record_server_model_for_response(first_generation, "stale-model".to_string())
            .await;
        assert_eq!(response_identity.latest_server_model().await, None);

        response_identity
            .record_server_model_for_response(second_generation, "second-model".to_string())
            .await;
        assert_eq!(
            response_identity.latest_server_model().await,
            Some("second-model".to_string())
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
