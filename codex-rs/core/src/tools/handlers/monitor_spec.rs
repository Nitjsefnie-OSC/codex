use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolSpec;
use std::collections::BTreeMap;

pub(crate) const TOOL_NAME: &str = "monitor";

/// Default lifetime of a monitored *job*. Long enough for a build or a test
/// suite. Watchers have no default ceiling — they run until stopped or until
/// the session tears its processes down.
pub(crate) const DEFAULT_JOB_TIMEOUT_MS: u64 = 10 * 60 * 1000;

/// Hard ceiling on an explicit `timeout_ms`.
pub(crate) const MAX_MONITOR_TIMEOUT_MS: u64 = 60 * 60 * 1000;

/// Default and maximum ceiling for a `wait`. A wait blocks the turn, so it is
/// bounded much more tightly than the monitor it waits on.
pub(crate) const DEFAULT_WAIT_TIMEOUT_MS: u64 = 30 * 1000;
pub(crate) const MAX_WAIT_TIMEOUT_MS: u64 = 10 * 60 * 1000;

pub(crate) fn create_monitor_tool() -> ToolSpec {
    let properties = BTreeMap::from([
        (
            "action".to_string(),
            JsonSchema::string_enum(
                vec![
                    serde_json::json!("start"),
                    serde_json::json!("list"),
                    serde_json::json!("read"),
                    serde_json::json!("stop"),
                    serde_json::json!("wait"),
                ],
                Some(
                    "What to do. Defaults to `start`. `list` reports every monitor and whether its output is unread; `read` returns retained output; `stop` terminates one; `wait` blocks until one finishes."
                        .to_string(),
                ),
            ),
        ),
        (
            "command".to_string(),
            JsonSchema::array(
                JsonSchema::string(None),
                Some(
                    "start: program and arguments, e.g. [\"cargo\", \"build\"]. Not a shell string."
                        .to_string(),
                ),
            ),
        ),
        (
            "workdir".to_string(),
            JsonSchema::string(Some(
                "start: directory to run in. Relative paths resolve against the session working directory."
                    .to_string(),
            )),
        ),
        (
            "kind".to_string(),
            JsonSchema::string_enum(
                vec![serde_json::json!("job"), serde_json::json!("watcher")],
                Some(
                    "start: `job` (default) is expected to finish on its own; `watcher` is persistent and runs until it is stopped or the session ends."
                        .to_string(),
                ),
            ),
        ),
        (
            "timeout_ms".to_string(),
            JsonSchema::number(Some(format!(
                "start: how long the command may run before it is terminated. Jobs default to {DEFAULT_JOB_TIMEOUT_MS} ms; watchers have no default ceiling. Maximum {MAX_MONITOR_TIMEOUT_MS} ms."
            ))),
        ),
        (
            "process_id".to_string(),
            JsonSchema::number(Some(
                "read/stop/wait: the process id reported when the monitor started.".to_string(),
            )),
        ),
        (
            "acknowledge".to_string(),
            JsonSchema::boolean(Some(
                "read: record the monitor's notifications as consumed. Defaults to true."
                    .to_string(),
            )),
        ),
        (
            "acknowledge_through".to_string(),
            JsonSchema::number(Some(
                "read: acknowledge only up to this notification sequence number, instead of all of them."
                    .to_string(),
            )),
        ),
        (
            "wait_timeout_ms".to_string(),
            JsonSchema::number(Some(format!(
                "wait: how long to block. Defaults to {DEFAULT_WAIT_TIMEOUT_MS} ms, maximum {MAX_WAIT_TIMEOUT_MS} ms."
            ))),
        ),
    ]);

    ToolSpec::Function(ResponsesApiTool {
        name: TOOL_NAME.to_string(),
        description: r#"Run a long-running command in the background and get told about its output as it arrives.

`start` returns as soon as the command is running, with a process id. From then on you receive `monitor_notification` messages carrying batches of complete stdout lines, and exactly one final notification reporting how the command ended — success, failure, stop, or timeout. Stderr stays quiet but remains available with `read`. Notifications are capped per monitor; the full retained output is always available with `read`.

Classify the work: a `job` is expected to finish (a build, a test run, a deploy); a `watcher` is persistent and runs until you stop it or the session ends (a log tail, a dev server). Use `list` to see what is running, who started it, and whether its notifications are unread; `wait` to block until a job finishes; `stop` to end one.

Because `start` returns immediately, its return value is not the command's result. Use the ordinary shell tool when you need output synchronously.
"#
        .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(
            properties,
            /*required*/ None,
            /*additional_properties*/ Some(false.into()),
        ),
        output_schema: None,
    })
}
