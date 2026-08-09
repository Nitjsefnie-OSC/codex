use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolSpec;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeMap;

pub(crate) const WHOAMI_TOOL_NAME: &str = "whoami";

pub fn create_whoami_tool() -> ToolSpec {
    ToolSpec::Function(ResponsesApiTool {
        name: WHOAMI_TOOL_NAME.to_string(),
        description: "Report the model requested for this sampling step and, when the transport \
            provides it, the latest server-reported model identity. The legacy slug is the \
            server-reported model when available and otherwise is explicitly unverified request \
            metadata. Reasoning effort is the request value; context-window metadata comes from \
            the local model catalog. Use this before writing a model name into a git commit \
            trailer, changelog entry, or anywhere else self-identification must be accurate."
            .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(BTreeMap::new(), /*required*/ None, Some(false.into())),
        output_schema: Some(whoami_output_schema()),
    })
}

fn whoami_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "slug": {
                "type": "string",
                "description": "The server-reported model slug when available; otherwise the requested slug, explicitly unverified."
            },
            "display_name": {
                "type": "string",
                "description": "Human-readable display name corresponding to slug; it is catalog metadata when available, otherwise the server-reported slug."
            },
            "reasoning_effort": {
                "anyOf": [
                    { "type": "string" },
                    { "type": "null" }
                ],
                "description": "Effective reasoning effort sent in request metadata, or null when not applicable."
            },
            "context_window": {
                "anyOf": [
                    { "type": "integer" },
                    { "type": "null" }
                ],
                "description": "Catalog/configured context-window metadata for slug in tokens, or null when unavailable for a server-reported slug."
            },
            "requested_model": {
                "type": "string",
                "description": "Model slug selected for this request before server routing."
            },
            "requested_display_name": {
                "type": "string",
                "description": "Display name for the requested model from local catalog metadata."
            },
            "requested_context_window": {
                "anyOf": [
                    { "type": "integer" },
                    { "type": "null" }
                ],
                "description": "Requested model's local catalog/configuration context-window metadata, or null when unknown."
            },
            "server_reported_model": {
                "anyOf": [
                    { "type": "string" },
                    { "type": "null" }
                ],
                "description": "Latest model slug reported by the response transport for this sampling step, or null when absent."
            },
            "model_identity_provenance": {
                "type": "string",
                "enum": ["server_reported", "request_metadata_unverified"],
                "description": "Whether slug is sourced from a server response or is an unverified request fallback."
            },
            "model_identity_verified": {
                "type": "boolean",
                "description": "True only when the response transport reported a model for this sampling step."
            },
            "display_name_provenance": {
                "type": "string",
                "enum": ["model_catalog_configuration", "server_reported_slug"],
                "description": "Provenance for display_name, which always corresponds to slug."
            },
            "reasoning_effort_provenance": {
                "type": "string",
                "enum": ["request_metadata"],
                "description": "Reasoning effort is the effective value sent in request metadata, not server attestation."
            },
            "context_window_provenance": {
                "type": "string",
                "enum": ["model_catalog_configuration", "unavailable_for_reported_model"],
                "description": "Context window is local model-catalog/configuration metadata, never server attestation; it is unavailable when the reported slug is not in the catalog."
            }
        },
        "required": [
            "slug",
            "display_name",
            "reasoning_effort",
            "context_window",
            "requested_model",
            "requested_display_name",
            "requested_context_window",
            "server_reported_model",
            "model_identity_provenance",
            "model_identity_verified",
            "display_name_provenance",
            "reasoning_effort_provenance",
            "context_window_provenance"
        ],
        "additionalProperties": false
    })
}
