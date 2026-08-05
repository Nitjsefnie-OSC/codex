use std::sync::Arc;
use std::time::Duration;

use codex_protocol::protocol::ExecCommandSource;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use serde::Deserialize;

use crate::exec::ExecCapturePolicy;
use crate::exec::ExecExpiration;
use crate::exec::ExecParams;
use crate::exec::StdoutStream;
use crate::exec::process_exec_tool_call;
use crate::exec_env::create_env;
use crate::exec_output_deltas::ExecDeltaChunking;
use crate::function_tool::FunctionCallError;
use crate::sandboxing::SandboxPermissions;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use crate::tools::context::FunctionToolOutput;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::events::ToolEmitter;
use crate::tools::events::ToolEventCtx;
use crate::tools::events::ToolEventFailure;
use crate::tools::events::ToolEventStage;
use crate::tools::handlers::monitor_spec::DEFAULT_MONITOR_TIMEOUT_MS;
use crate::tools::handlers::monitor_spec::MAX_MONITOR_TIMEOUT_MS;
use crate::tools::handlers::monitor_spec::TOOL_NAME;
use crate::tools::handlers::monitor_spec::create_monitor_tool;
use crate::tools::handlers::parse_arguments;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;

/// Runs a command whose output is worth watching while it runs.
///
/// Unlike the shell tools, the handler future does not wait for the command:
/// it emits the command-execution "started" item, hands the child to a detached
/// task, and returns immediately. The task streams complete output lines to the
/// client as `ExecCommandOutputDelta` events and, when the command ends, emits
/// the matching "completed" item carrying the exit code. That final event is
/// the only thing that distinguishes a finished command from a quiet one, so it
/// is emitted on every exit path — success, non-zero exit, timeout, interrupt,
/// and spawn failure alike.
pub struct MonitorHandler;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MonitorArgs {
    command: Vec<String>,
    #[serde(default)]
    workdir: Option<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
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

async fn handle_call(
    invocation: ToolInvocation,
) -> Result<Box<dyn crate::tools::context::ToolOutput>, FunctionCallError> {
    let ToolInvocation {
        session,
        turn,
        step_context,
        cancellation_token,
        call_id,
        payload,
        ..
    } = invocation;

    let ToolPayload::Function { arguments } = payload else {
        return Err(FunctionCallError::RespondToModel(format!(
            "{TOOL_NAME} handler received unsupported payload"
        )));
    };
    let args: MonitorArgs = parse_arguments(&arguments)?;
    if args.command.is_empty() {
        return Err(FunctionCallError::RespondToModel(
            "`command` must contain at least the program to run".to_string(),
        ));
    }
    let timeout_ms = args.timeout_ms.unwrap_or(DEFAULT_MONITOR_TIMEOUT_MS);
    if !(1..=MAX_MONITOR_TIMEOUT_MS).contains(&timeout_ms) {
        return Err(FunctionCallError::RespondToModel(format!(
            "`timeout_ms` must be between 1 and {MAX_MONITOR_TIMEOUT_MS}"
        )));
    }

    let Some(turn_environment) = step_context.environments.primary().cloned() else {
        return Err(FunctionCallError::RespondToModel(format!(
            "{TOOL_NAME} is unavailable in this session"
        )));
    };
    let environment_cwd = turn_environment.cwd().to_abs_path().map_err(|err| {
        FunctionCallError::RespondToModel(format!(
            "{TOOL_NAME} cwd `{}` is not native to the Codex host: {err}",
            turn_environment.cwd()
        ))
    })?;
    let cwd = match args.workdir.as_deref().filter(|dir| !dir.is_empty()) {
        Some(workdir) => environment_cwd.join(workdir),
        None => environment_cwd.clone(),
    };

    let exec_params = ExecParams {
        command: args.command.clone(),
        cwd: cwd.clone(),
        // The command outlives the handler, so interrupting the turn is the only
        // way a user can stop it early; the timeout bounds it otherwise.
        expiration: ExecExpiration::TimeoutOrCancellation {
            timeout: Duration::from_millis(timeout_ms),
            cancellation: cancellation_token,
        },
        capture_policy: ExecCapturePolicy::ShellTool,
        env: create_env(
            &turn.config.permissions.shell_environment_policy,
            Some(session.thread_id),
        ),
        network: turn.network.clone(),
        network_environment_id: Some(turn_environment.environment_id.clone()),
        sandbox_permissions: SandboxPermissions::UseDefault,
        windows_sandbox_level: turn.windows_sandbox_level,
        windows_sandbox_private_desktop: turn.config.permissions.windows_sandbox_private_desktop,
        justification: None,
        arg0: None,
    };
    let stdout_stream = StdoutStream {
        sub_id: turn.sub_id.clone(),
        call_id: call_id.clone(),
        tx_event: session.get_tx_event(),
        chunking: ExecDeltaChunking::Lines,
    };

    let emitter = ToolEmitter::shell(
        args.command.clone(),
        cwd.clone(),
        ExecCommandSource::Agent,
        /*plugin_attribution*/ None,
    );
    emitter
        .emit(
            ToolEventCtx::new(
                session.as_ref(),
                turn.as_ref(),
                &call_id,
                /*turn_diff_tracker*/ None,
            ),
            ToolEventStage::Begin,
        )
        .await;

    spawn_monitor(
        Arc::clone(&session),
        Arc::clone(&turn),
        turn_environment.permission_profile_with_workspace_roots(),
        environment_cwd,
        call_id,
        emitter,
        exec_params,
        stdout_stream,
    );

    let command = args.command.join(" ");
    Ok(boxed_tool_output(FunctionToolOutput::from_text(
        format!(
            "Monitoring `{command}` in {cwd}. Output lines stream to the user as they arrive, and a final event reports the exit code.",
            cwd = cwd.to_string_lossy()
        ),
        /*success*/ Some(true),
    )))
}

/// Detach the command onto a task that outlives this turn's handler future.
#[allow(clippy::too_many_arguments)]
fn spawn_monitor(
    session: Arc<Session>,
    turn: Arc<TurnContext>,
    permission_profile: codex_protocol::models::PermissionProfile,
    sandbox_cwd: codex_utils_absolute_path::AbsolutePathBuf,
    call_id: String,
    emitter: ToolEmitter,
    exec_params: ExecParams,
    stdout_stream: StdoutStream,
) {
    let workspace_roots = turn.config.effective_workspace_roots();
    let codex_linux_sandbox_exe = turn.config.codex_linux_sandbox_exe.clone();
    let use_legacy_landlock = turn.config.features.get().use_legacy_landlock();

    tokio::spawn(async move {
        let result = process_exec_tool_call(
            exec_params,
            &permission_profile,
            &sandbox_cwd,
            &workspace_roots,
            &codex_linux_sandbox_exe,
            use_legacy_landlock,
            Some(stdout_stream),
        )
        .await;

        let ctx = ToolEventCtx::new(
            session.as_ref(),
            turn.as_ref(),
            &call_id,
            /*turn_diff_tracker*/ None,
        );
        let stage = match result {
            // A non-zero exit is still a completed command; the emitter maps the
            // exit code onto the terminal status.
            Ok(output) => ToolEventStage::Success {
                output,
                applied_patch_delta: None,
            },
            Err(err) => ToolEventStage::Failure(ToolEventFailure::Message(format!(
                "{TOOL_NAME} failed to run the command: {err}"
            ))),
        };
        emitter.emit(ctx, stage).await;
    });
}

#[cfg(test)]
#[path = "monitor_tests.rs"]
mod tests;
