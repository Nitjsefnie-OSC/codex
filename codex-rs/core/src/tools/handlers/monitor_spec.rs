use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolSpec;
use std::collections::BTreeMap;

pub(crate) const TOOL_NAME: &str = "monitor";

/// Default lifetime of a monitored command. Long enough for a build or a test
/// suite, short enough that an unattended `tail -f` cannot outlive the session.
pub(crate) const DEFAULT_MONITOR_TIMEOUT_MS: u64 = 10 * 60 * 1000;

/// Hard ceiling on `timeout_ms`.
pub(crate) const MAX_MONITOR_TIMEOUT_MS: u64 = 60 * 60 * 1000;

pub(crate) fn create_monitor_tool() -> ToolSpec {
    let properties = BTreeMap::from([
        (
            "command".to_string(),
            JsonSchema::array(
                JsonSchema::string(None),
                Some(
                    "Program and arguments to run, e.g. [\"cargo\", \"build\"]. Not a shell string."
                        .to_string(),
                ),
            ),
        ),
        (
            "workdir".to_string(),
            JsonSchema::string(Some(
                "Directory to run the command in. Relative paths resolve against the session working directory."
                    .to_string(),
            )),
        ),
        (
            "timeout_ms".to_string(),
            JsonSchema::number(Some(format!(
                "How long the command may run before it is terminated. Defaults to {DEFAULT_MONITOR_TIMEOUT_MS} ms, maximum {MAX_MONITOR_TIMEOUT_MS} ms."
            ))),
        ),
    ]);

    ToolSpec::Function(ResponsesApiTool {
        name: TOOL_NAME.to_string(),
        description: r#"Start a long-running command and stream its output back as it is produced.

Use this for work you want to watch rather than wait for: a build, a test run, a deploy, a log tail. The call returns as soon as the command has started; complete output lines are delivered to the user's client as they arrive, and a final event reports the exit code when the command ends.

Because the call returns immediately, the returned text is a confirmation that monitoring started, not the command's output or result. Use the ordinary shell tool when you need the output yourself.
"#
        .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(
            properties,
            Some(vec!["command".to_string()]),
            /*additional_properties*/ Some(false.into()),
        ),
        output_schema: None,
    })
}
