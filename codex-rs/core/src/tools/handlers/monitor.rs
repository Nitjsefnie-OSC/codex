use std::sync::Arc;
use std::time::Duration;

use codex_shell_command::parse_command::shlex_join;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use serde::Deserialize;

use crate::function_tool::FunctionCallError;
use crate::sandboxing::SandboxPermissions;
use crate::tools::context::FunctionToolOutput;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::monitor_spec::DEFAULT_JOB_TIMEOUT_MS;
use crate::tools::handlers::monitor_spec::DEFAULT_WAIT_TIMEOUT_MS;
use crate::tools::handlers::monitor_spec::MAX_MONITOR_TIMEOUT_MS;
use crate::tools::handlers::monitor_spec::MAX_WAIT_TIMEOUT_MS;
use crate::tools::handlers::monitor_spec::TOOL_NAME;
use crate::tools::handlers::monitor_spec::create_monitor_tool;
use crate::tools::handlers::parse_arguments;
use crate::tools::handlers::unified_exec::resolve_shell_command;
use crate::tools::handlers::unified_exec::shell_mode_for_environment;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;
use crate::unified_exec::ExecCommandRequest;
use crate::unified_exec::MonitorAcknowledgement;
use crate::unified_exec::MonitorAttachment;
use crate::unified_exec::MonitorKind;
use crate::unified_exec::MonitorOwner;
use crate::unified_exec::UnifiedExecContext;

/// How long `start` waits before returning. Unified exec clamps a yield below
/// its floor, so this is the shortest wait that still lets a command that fails
/// instantly report its failure in the start result.
const START_YIELD_TIME_MS: u64 = 250;

/// Runs long-lived work on the native unified-exec process manager and reports
/// on it to the model.
///
/// The process is an ordinary unified-exec process: it has a process id, it is
/// listed and terminated through the same manager, its output is retained in
/// the same bounded buffer, `write_stdin` still reaches it, and it survives the
/// interruption of the turn that started it. What `monitor` adds on top is the
/// model-facing half — batched line notifications, a guaranteed terminal
/// notification, watcher classification and ownership, and read
/// acknowledgement — plus the control surface to list, read, stop and wait.
pub struct MonitorHandler;

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MonitorAction {
    #[default]
    Start,
    List,
    Read,
    Stop,
    Wait,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MonitorKindArg {
    Job,
    Watcher,
}

impl From<MonitorKindArg> for MonitorKind {
    fn from(kind: MonitorKindArg) -> Self {
        match kind {
            MonitorKindArg::Job => Self::Job,
            MonitorKindArg::Watcher => Self::Watcher,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MonitorArgs {
    #[serde(default)]
    action: MonitorAction,
    #[serde(default)]
    command: Option<Vec<String>>,
    #[serde(default)]
    workdir: Option<String>,
    #[serde(default)]
    kind: Option<MonitorKindArg>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    process_id: Option<i32>,
    #[serde(default)]
    acknowledge: Option<bool>,
    #[serde(default)]
    acknowledge_through: Option<u64>,
    #[serde(default)]
    wait_timeout_ms: Option<u64>,
}

impl ToolExecutor<ToolInvocation> for MonitorHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain(TOOL_NAME)
    }

    fn spec(&self) -> ToolSpec {
        create_monitor_tool()
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(handle_call(invocation))
    }
}

impl CoreToolRuntime for MonitorHandler {}

async fn handle_call(invocation: ToolInvocation) -> Result<Box<dyn ToolOutput>, FunctionCallError> {
    let ToolPayload::Function { arguments } = &invocation.payload else {
        return Err(FunctionCallError::RespondToModel(format!(
            "{TOOL_NAME} handler received unsupported payload"
        )));
    };
    let args: MonitorArgs = parse_arguments(arguments)?;

    match args.action {
        MonitorAction::Start => start(invocation, args).await,
        MonitorAction::List => list(invocation).await,
        MonitorAction::Read => read(invocation, args).await,
        MonitorAction::Stop => stop(invocation, args).await,
        MonitorAction::Wait => wait(invocation, args).await,
    }
}

async fn start(
    invocation: ToolInvocation,
    args: MonitorArgs,
) -> Result<Box<dyn ToolOutput>, FunctionCallError> {
    let ToolInvocation {
        session,
        turn,
        step_context,
        call_id,
        ..
    } = invocation;

    let command = args.command.unwrap_or_default();
    if command.is_empty() {
        return Err(FunctionCallError::RespondToModel(
            "`command` must contain at least the program to run".to_string(),
        ));
    }
    if let Some(timeout_ms) = args.timeout_ms
        && !(1..=MAX_MONITOR_TIMEOUT_MS).contains(&timeout_ms)
    {
        return Err(FunctionCallError::RespondToModel(format!(
            "`timeout_ms` must be between 1 and {MAX_MONITOR_TIMEOUT_MS}"
        )));
    }
    let kind: MonitorKind = args.kind.map_or(MonitorKind::Job, MonitorKind::from);
    let timeout = match (kind, args.timeout_ms) {
        (_, Some(timeout_ms)) => Some(Duration::from_millis(timeout_ms)),
        // A watcher is persistent by definition; only a job gets a default
        // ceiling it did not ask for.
        (MonitorKind::Watcher, None) => None,
        (MonitorKind::Job, None) => Some(Duration::from_millis(DEFAULT_JOB_TIMEOUT_MS)),
    };

    let Some(turn_environment) = step_context.environments.primary().cloned() else {
        return Err(FunctionCallError::RespondToModel(format!(
            "{TOOL_NAME} is unavailable in this session"
        )));
    };
    let environment_cwd = turn_environment.cwd().clone();
    let cwd = match args.workdir.as_deref().filter(|dir| !dir.is_empty()) {
        Some(workdir) => environment_cwd
            .join(workdir)
            .map_err(|err| FunctionCallError::RespondToModel(err.to_string()))?,
        None => environment_cwd.clone(),
    };

    let shell_mode = shell_mode_for_environment(
        &turn.unified_exec_shell_mode,
        turn_environment.environment.as_ref(),
    );
    let shell = turn_environment
        .shell
        .clone()
        .map(Arc::new)
        .unwrap_or_else(|| session.user_shell());
    let display_command = shlex_join(&command);
    let resolved = resolve_shell_command(
        &display_command,
        shell.as_ref(),
        &shell_mode,
        /*use_login_shell*/ false,
    );

    let manager = &session.services.unified_exec_manager;
    let process_id = manager.allocate_process_id().await;
    let context = UnifiedExecContext::new(Arc::clone(&session), Arc::clone(&turn), call_id.clone());
    let request = ExecCommandRequest {
        command: resolved.command,
        shell_type: resolved.shell_type,
        hook_command: display_command.clone(),
        process_id,
        yield_time_ms: START_YIELD_TIME_MS,
        max_output_tokens: None,
        cwd,
        sandbox_cwd: environment_cwd,
        turn_environment,
        shell_mode,
        network: turn.network.clone(),
        // Line-oriented output is the point; a PTY would interleave the
        // terminal's own control sequences into the batches.
        tty: false,
        sandbox_permissions: SandboxPermissions::UseDefault,
        additional_permissions: None,
        additional_permissions_preapproved: false,
        justification: None,
        prefix_rule: None,
    };
    let attachment = MonitorAttachment {
        kind,
        owner: MonitorOwner {
            model_slug: turn.model_info.slug.clone(),
            sub_id: turn.sub_id.clone(),
            call_id,
        },
        command_display: display_command.clone(),
        timeout,
    };

    match manager.start_monitor(request, &context, attachment).await {
        Ok(output) => Ok(json_output(serde_json::json!({
            "started": true,
            "process_id": process_id,
            "kind": kind.as_str(),
            "command": display_command,
            "initial_output": String::from_utf8_lossy(&output.raw_output),
            "note": "Output arrives as monitor_notification messages; exactly one final notification reports how this ended.",
        }))),
        Err(err) => Err(FunctionCallError::RespondToModel(format!(
            "{TOOL_NAME} failed to start `{display_command}`: {err}"
        ))),
    }
}

async fn list(invocation: ToolInvocation) -> Result<Box<dyn ToolOutput>, FunctionCallError> {
    let monitors = invocation
        .session
        .services
        .unified_exec_manager
        .list_monitors()
        .await;
    Ok(json_output(serde_json::json!({ "monitors": monitors })))
}

async fn read(
    invocation: ToolInvocation,
    args: MonitorArgs,
) -> Result<Box<dyn ToolOutput>, FunctionCallError> {
    let process_id = required_process_id(&args)?;
    let acknowledgement = match (args.acknowledge.unwrap_or(true), args.acknowledge_through) {
        (false, _) => MonitorAcknowledgement::None,
        (true, Some(seq)) => MonitorAcknowledgement::Through(seq),
        (true, None) => MonitorAcknowledgement::All,
    };
    let output = invocation
        .session
        .services
        .unified_exec_manager
        .read_monitor_output(process_id, acknowledgement)
        .await
        .ok_or_else(|| unknown_monitor(process_id))?;
    Ok(json_output(serde_json::json!(output)))
}

async fn stop(
    invocation: ToolInvocation,
    args: MonitorArgs,
) -> Result<Box<dyn ToolOutput>, FunctionCallError> {
    let process_id = required_process_id(&args)?;
    let stopped = invocation
        .session
        .services
        .unified_exec_manager
        .stop_monitor(process_id)
        .await
        .ok_or_else(|| unknown_monitor(process_id))?;
    Ok(json_output(serde_json::json!({
        "process_id": process_id,
        "stopped": stopped,
        "note": if stopped {
            "The final notification will report the stop."
        } else {
            "This monitor had already reached a terminal state."
        },
    })))
}

async fn wait(
    invocation: ToolInvocation,
    args: MonitorArgs,
) -> Result<Box<dyn ToolOutput>, FunctionCallError> {
    let process_id = required_process_id(&args)?;
    let timeout_ms = args.wait_timeout_ms.unwrap_or(DEFAULT_WAIT_TIMEOUT_MS);
    if !(1..=MAX_WAIT_TIMEOUT_MS).contains(&timeout_ms) {
        return Err(FunctionCallError::RespondToModel(format!(
            "`wait_timeout_ms` must be between 1 and {MAX_WAIT_TIMEOUT_MS}"
        )));
    }
    let outcome = invocation
        .session
        .services
        .unified_exec_manager
        .wait_for_monitor(process_id, Duration::from_millis(timeout_ms))
        .await
        .ok_or_else(|| unknown_monitor(process_id))?;
    Ok(json_output(serde_json::json!(outcome)))
}

fn required_process_id(args: &MonitorArgs) -> Result<i32, FunctionCallError> {
    args.process_id.ok_or_else(|| {
        FunctionCallError::RespondToModel(
            "`process_id` is required for this action; call monitor(action=\"list\") to find it"
                .to_string(),
        )
    })
}

fn unknown_monitor(process_id: i32) -> FunctionCallError {
    FunctionCallError::RespondToModel(format!(
        "no monitor with process_id {process_id}; call monitor(action=\"list\") to see what exists"
    ))
}

fn json_output(payload: serde_json::Value) -> Box<dyn ToolOutput> {
    boxed_tool_output(FunctionToolOutput::from_text(
        payload.to_string(),
        /*success*/ Some(true),
    ))
}

#[cfg(test)]
#[path = "monitor_tests.rs"]
mod tests;
