use std::path::Path;
use std::sync::Arc;

use crate::function_tool::FunctionCallError;
use crate::maybe_emit_implicit_skill_invocation;
use crate::session::turn_context::TurnContext;
use crate::skills::record_skill_activation;
use crate::skills::retain_pending_skill_activation;
use crate::tools::context::ExecCommandToolOutput;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::apply_granted_turn_permissions;
use crate::tools::handlers::apply_patch::intercept_apply_patch;
use crate::tools::handlers::implicit_granted_permissions;
use crate::tools::handlers::normalize_and_validate_additional_permissions;
use crate::tools::handlers::parse_arguments;
use crate::tools::handlers::parse_arguments_with_base_path;
use crate::tools::handlers::resolve_sandbox_permissions;
use crate::tools::handlers::resolve_tool_environment;
use crate::tools::handlers::rewrite_function_string_argument;
use crate::tools::handlers::updated_hook_command;
use crate::tools::hook_names::HookToolName;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::PostToolUsePayload;
use crate::tools::registry::PreToolUsePayload;
use crate::tools::registry::ToolExecutor;
use crate::unified_exec::ExecCommandRequest;
use crate::unified_exec::UnifiedExecContext;
use crate::unified_exec::UnifiedExecError;
use crate::unified_exec::UnifiedExecProcessManager;
use crate::unified_exec::generate_chunk_id;
use codex_features::Feature;
use codex_hooks::SkillActivation;
use codex_otel::SessionTelemetry;
use codex_otel::TOOL_CALL_UNIFIED_EXEC_METRIC;
use codex_sandboxing::SandboxManager;
use codex_sandboxing::SandboxType;
use codex_sandboxing::SandboxablePreference;
use codex_shell_command::shell_detect::detect_shell_type;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use codex_utils_output_truncation::approx_token_count;
use codex_utils_path_uri::PathConvention;

use super::super::shell_spec::CommandToolOptions;
use super::super::shell_spec::create_exec_command_tool_with_environment_id;
use super::ExecCommandArgs;
use super::ExecCommandEnvironmentArgs;
use super::get_command;
use super::post_unified_exec_tool_use_payload;
use super::shell_mode_for_environment;

#[derive(Clone, Copy)]
pub(crate) struct ExecCommandHandlerOptions {
    pub(crate) allow_login_shell: bool,
    pub(crate) exec_permission_approvals_enabled: bool,
    pub(crate) include_environment_id: bool,
    pub(crate) include_shell_parameter: bool,
}

pub struct ExecCommandHandler {
    options: ExecCommandHandlerOptions,
}

impl Default for ExecCommandHandler {
    fn default() -> Self {
        Self {
            options: ExecCommandHandlerOptions {
                allow_login_shell: false,
                exec_permission_approvals_enabled: false,
                include_environment_id: false,
                include_shell_parameter: true,
            },
        }
    }
}

impl ExecCommandHandler {
    pub(crate) fn new(options: ExecCommandHandlerOptions) -> Self {
        Self { options }
    }
}

impl ToolExecutor<ToolInvocation> for ExecCommandHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("exec_command")
    }

    fn spec(&self) -> ToolSpec {
        create_exec_command_tool_with_environment_id(
            CommandToolOptions {
                allow_login_shell: self.options.allow_login_shell,
                exec_permission_approvals_enabled: self.options.exec_permission_approvals_enabled,
            },
            self.options.include_environment_id,
            self.options.include_shell_parameter,
        )
    }

    fn supports_parallel_tool_calls(&self) -> bool {
        true
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(self.handle_call(invocation))
    }
}

impl ExecCommandHandler {
    async fn handle_call(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn crate::tools::context::ToolOutput>, FunctionCallError> {
        let ToolInvocation {
            session,
            turn,
            step_context,
            tracker,
            call_id,
            payload,
            ..
        } = invocation;

        let arguments = match payload {
            ToolPayload::Function { arguments } => arguments,
            _ => {
                return Err(FunctionCallError::RespondToModel(
                    "exec_command handler received unsupported payload".to_string(),
                ));
            }
        };

        let manager: &UnifiedExecProcessManager = &session.services.unified_exec_manager;
        let context =
            UnifiedExecContext::new(session.clone(), step_context.clone(), call_id.clone());
        let environment_args: ExecCommandEnvironmentArgs = parse_arguments(&arguments)?;
        let Some(turn_environment) = resolve_tool_environment(
            &step_context.environments,
            environment_args.environment_id.as_deref(),
        )?
        else {
            return Err(FunctionCallError::RespondToModel(
                "unified exec is unavailable in this session".to_string(),
            ));
        };
        let native_environment_cwd = turn_environment.cwd().clone();
        let cwd = environment_args
            .workdir
            .as_deref()
            .filter(|workdir| !workdir.is_empty())
            .map_or_else(
                || Ok(native_environment_cwd.clone()),
                |workdir| native_environment_cwd.join(workdir),
            )
            .map_err(|err| FunctionCallError::RespondToModel(err.to_string()))?;
        let environment = Arc::clone(&turn_environment.environment);
        let fs = environment.get_filesystem();

        // Remote executors enforce URI-native sandbox policy themselves. Only a host-local
        // sandbox needs a native cwd for resolving paths nested in the permissions config.
        let requires_host_native_cwd = !environment.is_remote()
            && SandboxManager::new().select_initial(
                turn_environment.permission_profile(),
                SandboxablePreference::Auto,
                turn.windows_sandbox_level,
                turn.network.is_some(),
            ) != SandboxType::None;
        // `to_abs_path()` alone cannot identify foreign drive paths: `file:///C:/repo` is
        // representable as `/C:/repo` on POSIX. Require the inferred convention to match too.
        let cwd_uses_native_convention =
            cwd.infer_path_convention() == Some(PathConvention::native());
        let native_cwd = match cwd.to_abs_path() {
            Ok(cwd) if cwd_uses_native_convention => Some(cwd),
            _ if !requires_host_native_cwd => None,
            Err(err) => return Err(FunctionCallError::RespondToModel(err.to_string())),
            Ok(_) => {
                return Err(FunctionCallError::RespondToModel(format!(
                    "path URI `{cwd}` does not use the host's native {} path convention",
                    PathConvention::native()
                )));
            }
        };
        let mut args: ExecCommandArgs = match native_cwd.as_ref() {
            Some(native_cwd) => {
                // The base path only resolves paths nested in the permissions config types.
                parse_arguments_with_base_path(&arguments, native_cwd)?
            }
            None => {
                // Foreign executor cwd values cannot seed this host's AbsolutePathBufGuard.
                // Sandbox intent and URI-native roots are still sent to the executor.
                parse_arguments(&arguments)?
            }
        };
        let sandbox_permissions =
            resolve_sandbox_permissions(args.sandbox_permissions, args.justification.as_deref())?;
        let hook_command = args.cmd.clone();
        let implicit_skill_activation = maybe_emit_implicit_skill_invocation(
            session.as_ref(),
            context.step_context.turn.as_ref(),
            &hook_command,
            &cwd,
            native_cwd.as_ref(),
            &turn_environment.environment_id,
        )
        .await;
        let shell_mode =
            shell_mode_for_environment(&turn.unified_exec_shell_mode, environment.as_ref());
        // Remote environments may use a different OS and must build commands with their native
        // shell; fall back to the session shell when the environment did not report one.
        let shell = turn_environment
            .shell
            .clone()
            .map(Arc::new)
            .unwrap_or_else(|| session.user_shell());
        // TODO(anp): Resolve requested shells in remote environments instead of restricting
        // commands to the reported default shell.
        if environment.is_remote()
            && let Some(requested_shell) = args.shell.take()
        {
            let Some(remote_shell) = turn_environment.shell.as_ref() else {
                return Err(FunctionCallError::RespondToModel(format!(
                    "environment `{}` does not report a shell",
                    turn_environment.environment_id
                )));
            };
            if detect_shell_type(Path::new(&requested_shell)) != Some(remote_shell.shell_type) {
                return Err(FunctionCallError::RespondToModel(format!(
                    "environment `{}` only supports `{}`",
                    turn_environment.environment_id,
                    remote_shell.name()
                )));
            }
        }
        let process_id = manager.allocate_process_id().await;
        let resolved_command = get_command(
            &args,
            shell,
            &shell_mode,
            turn_environment.config.allow_login_shell,
        )
        .map_err(FunctionCallError::RespondToModel)?;
        let command = resolved_command.command;
        let shell_type = resolved_command.shell_type;
        let command_for_display = codex_shell_command::parse_command::shlex_join(&command);

        let ExecCommandArgs {
            tty,
            yield_time_ms,
            max_output_tokens,
            sandbox_permissions: _,
            additional_permissions,
            justification,
            prefix_rule,
            ..
        } = args;

        let exec_permission_approvals_enabled =
            session.features().enabled(Feature::ExecPermissionApprovals);
        let requested_additional_permissions = additional_permissions.clone();
        // TODO(anp): Make permission matching operate on PathUri for remote environments.
        let permission_cwd = native_cwd.as_ref().unwrap_or(&turn.config.cwd);
        let effective_additional_permissions = apply_granted_turn_permissions(
            context.session.as_ref(),
            &turn_environment.environment_id,
            permission_cwd.as_path(),
            sandbox_permissions,
            additional_permissions,
        )
        .await;
        let additional_permissions_allowed = exec_permission_approvals_enabled
            || (session.features().enabled(Feature::RequestPermissionsTool)
                && effective_additional_permissions.permissions_preapproved);

        // Sticky turn permissions have already been approved, so they should
        // continue through the normal exec approval flow for the command.
        if effective_additional_permissions
            .sandbox_permissions
            .requests_sandbox_override()
            && !effective_additional_permissions.permissions_preapproved
            && !matches!(
                context.step_context.turn.approval_policy(),
                codex_protocol::protocol::AskForApproval::OnRequest
            )
        {
            let approval_policy = context.step_context.turn.approval_policy();
            manager.release_process_id(process_id).await;
            return Err(FunctionCallError::RespondToModel(format!(
                "approval policy is {approval_policy:?}; reject command — you cannot ask for escalated permissions if the approval policy is {approval_policy:?}"
            )));
        }

        let normalized_additional_permissions = match implicit_granted_permissions(
            sandbox_permissions,
            requested_additional_permissions.as_ref(),
            &effective_additional_permissions,
        )
        .map_or_else(
            || {
                normalize_and_validate_additional_permissions(
                    additional_permissions_allowed,
                    context.step_context.turn.approval_policy(),
                    effective_additional_permissions.sandbox_permissions,
                    effective_additional_permissions.additional_permissions,
                    effective_additional_permissions.permissions_preapproved,
                    permission_cwd,
                )
            },
            |permissions| Ok(Some(permissions)),
        ) {
            Ok(normalized) => normalized,
            Err(err) => {
                manager.release_process_id(process_id).await;
                return Err(FunctionCallError::RespondToModel(err));
            }
        };

        let intercepted_patch = intercept_apply_patch(
            &command,
            &cwd,
            fs.as_ref(),
            turn_environment.clone(),
            context.session.clone(),
            Arc::clone(&context.step_context),
            Some(&tracker),
            &context.call_id,
            "exec_command",
        )
        .await;
        // Keep the reservation when interception returns `Ok(None)`: the normal command below
        // still needs this process ID.
        if intercepted_patch.is_err() {
            manager.release_process_id(process_id).await;
        }
        if let Some(output) = intercepted_patch? {
            manager.release_process_id(process_id).await;
            return Ok(boxed_tool_output(ExecCommandToolOutput {
                event_call_id: String::new(),
                chunk_id: String::new(),
                wall_time: std::time::Duration::ZERO,
                raw_output: output.into_text().into_bytes(),
                truncation_policy: turn.model_info.truncation_policy.into(),
                max_output_tokens,
                process_id: None,
                exit_code: None,
                original_token_count: None,
                output_omitted_bytes: None,
                hook_command: None,
            }));
        }

        emit_unified_exec_tty_metric(&turn.session_telemetry, tty);
        let result = manager
            .exec_command(
                ExecCommandRequest {
                    command,
                    shell_type,
                    hook_command: hook_command.clone(),
                    process_id,
                    yield_time_ms,
                    max_output_tokens,
                    cwd,
                    sandbox_cwd: native_environment_cwd,
                    turn_environment: turn_environment.clone(),
                    shell_mode,
                    network: context.step_context.turn.network.clone(),
                    tty,
                    sandbox_permissions: effective_additional_permissions.sandbox_permissions,
                    additional_permissions: normalized_additional_permissions,
                    additional_permissions_preapproved: effective_additional_permissions
                        .permissions_preapproved,
                    justification,
                    prefix_rule,
                },
                &context,
            )
            .await;
        settle_unified_exec_implicit_skill_activation(
            context.step_context.turn.as_ref(),
            implicit_skill_activation,
            result.as_ref(),
        );
        match result {
            Ok(response) => Ok(boxed_tool_output(response)),
            Err(UnifiedExecError::SandboxDenied {
                output,
                original_token_count,
                output_omitted_bytes,
                ..
            }) => {
                let output_text = output.aggregated_output.text;
                let original_token_count =
                    original_token_count.unwrap_or_else(|| approx_token_count(&output_text));
                Ok(boxed_tool_output(ExecCommandToolOutput {
                    event_call_id: context.call_id.clone(),
                    chunk_id: generate_chunk_id(),
                    wall_time: output.duration,
                    raw_output: output_text.into_bytes(),
                    truncation_policy: turn.model_info.truncation_policy.into(),
                    max_output_tokens,
                    // Sandbox denial is terminal, so there is no live
                    // process for write_stdin to resume.
                    process_id: None,
                    exit_code: Some(output.exit_code),
                    original_token_count: Some(original_token_count),
                    output_omitted_bytes,
                    hook_command: Some(hook_command),
                }))
            }
            Err(err) => Err(FunctionCallError::RespondToModel(format!(
                "exec_command failed for `{command_for_display}`: {err:?}"
            ))),
        }
    }
}

fn settle_unified_exec_implicit_skill_activation(
    turn: &TurnContext,
    candidate: Option<SkillActivation>,
    result: Result<&ExecCommandToolOutput, &UnifiedExecError>,
) {
    let (Some(candidate), Ok(response)) = (candidate, result) else {
        return;
    };
    if let Some(process_id) = response.process_id {
        retain_pending_skill_activation(turn, process_id, candidate);
    } else if response.exit_code == Some(0) {
        record_skill_activation(turn, candidate);
    }
}

impl CoreToolRuntime for ExecCommandHandler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }

    fn pre_tool_use_payload(&self, invocation: &ToolInvocation) -> Option<PreToolUsePayload> {
        let ToolPayload::Function { arguments } = &invocation.payload else {
            return None;
        };

        parse_arguments::<ExecCommandArgs>(arguments)
            .ok()
            .map(|args| PreToolUsePayload {
                tool_name: HookToolName::bash(),
                tool_input: serde_json::json!({ "command": args.cmd }),
            })
    }

    fn with_updated_hook_input(
        &self,
        mut invocation: ToolInvocation,
        updated_input: serde_json::Value,
    ) -> Result<ToolInvocation, FunctionCallError> {
        let ToolPayload::Function { arguments } = invocation.payload else {
            return Err(FunctionCallError::RespondToModel(
                "hook input rewrite received unsupported exec_command payload".to_string(),
            ));
        };
        invocation.payload = ToolPayload::Function {
            arguments: rewrite_function_string_argument(
                &arguments,
                "exec_command",
                "cmd",
                updated_hook_command(&updated_input)?,
            )?,
        };
        Ok(invocation)
    }

    fn post_tool_use_payload(
        &self,
        invocation: &ToolInvocation,
        result: &dyn crate::tools::context::ToolOutput,
    ) -> Option<PostToolUsePayload> {
        post_unified_exec_tool_use_payload(invocation, result)
    }
}

fn emit_unified_exec_tty_metric(session_telemetry: &SessionTelemetry, tty: bool) {
    session_telemetry.counter(
        TOOL_CALL_UNIFIED_EXEC_METRIC,
        /*inc*/ 1,
        &[("tty", if tty { "true" } else { "false" })],
    );
}

#[cfg(test)]
mod implicit_activation_tests {
    use std::sync::Arc;

    use codex_hooks::SkillActivation;
    use codex_hooks::SkillActivationKind;
    use codex_hooks::SkillActivationScope;
    use codex_protocol::exec_output::ExecToolCallOutput;
    use codex_protocol::models::PermissionProfile;
    use codex_utils_output_truncation::TruncationPolicy;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use tokio::sync::Mutex;

    use super::*;
    use crate::session::step_context::StepContext;
    use crate::session::tests::make_session_and_context;
    use crate::skills::promote_pending_skill_activation;
    use crate::skills::skill_activation_snapshot;
    use crate::skills::tests::assert_implicit_skill_candidate;
    use crate::skills::tests::configure_implicit_skill_fixture_for_exec;
    use crate::skills::tests::implicit_skill_fixture;
    use crate::skills::tests::quote_skill_test_path;
    use crate::tools::context::ToolCallSource;
    use crate::turn_diff_tracker::TurnDiffTracker;

    fn activation(name: &str, digest: char) -> SkillActivation {
        SkillActivation::new(
            name.to_string(),
            format!("/repo/{name}/SKILL.md"),
            SkillActivationScope::Repo,
            SkillActivationKind::Implicit,
            "turn-1".to_string(),
            digest.to_string().repeat(64),
        )
        .expect("valid activation")
    }

    fn response(process_id: Option<i32>, exit_code: Option<i32>) -> ExecCommandToolOutput {
        ExecCommandToolOutput {
            event_call_id: "call-1".to_string(),
            chunk_id: "chunk-1".to_string(),
            wall_time: std::time::Duration::ZERO,
            raw_output: Vec::new(),
            truncation_policy: TruncationPolicy::Tokens(10_000),
            max_output_tokens: None,
            process_id,
            exit_code,
            original_token_count: None,
            output_omitted_bytes: None,
            hook_command: Some("cat SKILL.md".to_string()),
        }
    }

    fn invocation(
        session: Arc<crate::session::session::Session>,
        turn: Arc<TurnContext>,
        arguments: serde_json::Value,
        call_id: &str,
    ) -> ToolInvocation {
        ToolInvocation {
            session,
            step_context: StepContext::for_test(Arc::clone(&turn)),
            turn,
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            tracker: Arc::new(Mutex::new(TurnDiffTracker::new())),
            call_id: call_id.to_string(),
            tool_name: ToolName::plain("exec_command"),
            source: ToolCallSource::Direct,
            payload: ToolPayload::Function {
                arguments: arguments.to_string(),
            },
        }
    }

    #[tokio::test]
    async fn unified_exec_implicit_skill_activation_actual_nonzero_does_not_record() {
        let mut fixture = implicit_skill_fixture(codex_protocol::protocol::SkillScope::Repo).await;
        configure_implicit_skill_fixture_for_exec(&mut fixture, PermissionProfile::Disabled);
        let command = format!(
            "cat {} ; exit 7",
            quote_skill_test_path(&fixture.skill_path)
        );
        assert_implicit_skill_candidate(&fixture, &command).await;
        let turn = Arc::new(fixture.turn);

        ExecCommandHandler::default()
            .handle(invocation(
                Arc::new(fixture.session),
                Arc::clone(&turn),
                json!({ "cmd": command }),
                "failed-skill-read",
            ))
            .await
            .expect("nonzero command should return structured output");

        assert_eq!(skill_activation_snapshot(&turn), Vec::new());
    }

    #[tokio::test]
    async fn unified_exec_implicit_skill_activation_actual_rewritten_terminal_zero_records() {
        let mut fixture = implicit_skill_fixture(codex_protocol::protocol::SkillScope::Admin).await;
        configure_implicit_skill_fixture_for_exec(&mut fixture, PermissionProfile::Disabled);
        let rewritten_command = format!("cat {}", quote_skill_test_path(&fixture.skill_path));
        let turn = Arc::new(fixture.turn);
        let handler = ExecCommandHandler::default();
        let original = invocation(
            Arc::new(fixture.session),
            Arc::clone(&turn),
            json!({ "cmd": "exit 99" }),
            "rewritten-skill-read",
        );
        let rewritten = handler
            .with_updated_hook_input(original, json!({ "command": rewritten_command }))
            .expect("rewrite exec_command input");

        handler
            .handle(rewritten)
            .await
            .expect("rewritten command should execute successfully");

        let activations = skill_activation_snapshot(&turn);
        assert_eq!(activations.len(), 1);
        assert_eq!(activations[0].name(), "audit-skill");
        assert_eq!(activations[0].scope(), SkillActivationScope::Admin);
    }

    #[tokio::test]
    async fn unified_exec_implicit_skill_activation_actual_escalation_rejection_does_not_record() {
        let mut fixture =
            implicit_skill_fixture(codex_protocol::protocol::SkillScope::System).await;
        configure_implicit_skill_fixture_for_exec(&mut fixture, PermissionProfile::Disabled);
        let command = format!("cat {}", quote_skill_test_path(&fixture.skill_path));
        assert_implicit_skill_candidate(&fixture, &command).await;
        let turn = Arc::new(fixture.turn);

        let Err(error) = ExecCommandHandler::default()
            .handle(invocation(
                Arc::new(fixture.session),
                Arc::clone(&turn),
                json!({
                    "cmd": command,
                    "sandbox_permissions": "require_escalated",
                    "justification": "exercise approval rejection"
                }),
                "denied-skill-read",
            ))
            .await
        else {
            panic!("Never policy must reject explicit escalation");
        };

        assert!(error.to_string().contains("approval policy is Never"));
        assert_eq!(skill_activation_snapshot(&turn), Vec::new());
    }

    #[tokio::test]
    async fn unified_exec_implicit_skill_activation_actual_sandbox_denial_does_not_record() {
        let mut fixture = implicit_skill_fixture(codex_protocol::protocol::SkillScope::Repo).await;
        configure_implicit_skill_fixture_for_exec(&mut fixture, PermissionProfile::read_only());
        let denied_path = fixture.workdir.join("sandbox-denied.txt");
        let command = format!(
            "cat {} ; echo denied > {}",
            quote_skill_test_path(&fixture.skill_path),
            quote_skill_test_path(&denied_path)
        );
        assert_implicit_skill_candidate(&fixture, &command).await;
        let turn = Arc::new(fixture.turn);

        let result = ExecCommandHandler::default()
            .handle(invocation(
                Arc::new(fixture.session),
                Arc::clone(&turn),
                json!({ "cmd": command }),
                "sandbox-denied-skill-read",
            ))
            .await;
        let denial = match result {
            Ok(output) => output.log_preview(),
            Err(error) => error.to_string(),
        }
        .to_ascii_lowercase();

        assert!(
            denial.contains("permission denied")
                || denial.contains("operation not permitted")
                || denial.contains("read-only file system")
                || denial.contains("sandbox")
                || denial.contains("landlocksandboxexecutablenotprovided"),
            "unexpected sandbox-denial output: {denial}"
        );
        assert!(!denied_path.exists());
        assert_eq!(skill_activation_snapshot(&turn), Vec::new());
    }

    #[tokio::test]
    async fn unified_exec_implicit_skill_activation_records_terminal_zero_and_hides_yielded() {
        let (_session, turn) = make_session_and_context().await;
        let terminal = activation("terminal", 'a');
        settle_unified_exec_implicit_skill_activation(
            &turn,
            Some(terminal.clone()),
            Ok(&response(None, Some(0))),
        );
        assert_eq!(skill_activation_snapshot(&turn), vec![terminal.clone()]);

        let yielded = activation("yielded", 'b');
        settle_unified_exec_implicit_skill_activation(
            &turn,
            Some(yielded.clone()),
            Ok(&response(Some(31), Some(0))),
        );
        assert_eq!(skill_activation_snapshot(&turn).len(), 1);
        assert!(promote_pending_skill_activation(&turn, 31));
        assert_eq!(skill_activation_snapshot(&turn), vec![terminal, yielded]);
    }

    #[tokio::test]
    async fn unified_exec_implicit_skill_activation_drops_nonzero_missing_exit_and_sandbox_denial()
    {
        let (_session, turn) = make_session_and_context().await;
        settle_unified_exec_implicit_skill_activation(
            &turn,
            Some(activation("nonzero", 'a')),
            Ok(&response(None, Some(7))),
        );
        settle_unified_exec_implicit_skill_activation(
            &turn,
            Some(activation("unknown", 'b')),
            Ok(&response(None, None)),
        );
        let denied =
            UnifiedExecError::sandbox_denied("denied".to_string(), ExecToolCallOutput::default());
        settle_unified_exec_implicit_skill_activation(
            &turn,
            Some(activation("denied", 'c')),
            Err(&denied),
        );

        assert_eq!(skill_activation_snapshot(&turn), Vec::new());
    }
}
