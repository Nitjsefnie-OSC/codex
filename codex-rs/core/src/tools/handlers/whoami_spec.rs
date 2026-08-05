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
        description: "Report the identity of the model currently executing this turn \
            (its model slug and related metadata). Use this before writing a model name \
            into a git commit trailer, changelog entry, or anywhere else self-identification \
            must be accurate, rather than guessing from training data."
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
                "description": "The exact model slug for the model executing this turn."
            },
            "display_name": {
                "type": "string",
                "description": "Human-readable display name for the model."
            },
            "reasoning_effort": {
                "anyOf": [
                    { "type": "string" },
                    { "type": "null" }
                ],
                "description": "Reasoning effort configured for this turn, or null when not applicable."
            },
            "context_window": {
                "anyOf": [
                    { "type": "integer" },
                    { "type": "null" }
                ],
                "description": "Size of the model's context window in tokens, or null when unknown."
            }
        },
        "required": ["slug", "display_name", "reasoning_effort", "context_window"],
        "additionalProperties": false
    })
}
