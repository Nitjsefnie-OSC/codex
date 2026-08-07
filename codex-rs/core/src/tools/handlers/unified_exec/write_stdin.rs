use crate::function_tool::FunctionCallError;
use crate::session::turn_context::TurnContext;
use crate::skills::discard_pending_skill_activation;
use crate::skills::promote_pending_skill_activation;
use crate::tools::context::ExecCommandToolOutput;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::parse_arguments;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::PostToolUsePayload;
use crate::tools::registry::PreToolUsePayload;
use crate::tools::registry::ToolExecutor;
use crate::unified_exec::UnifiedExecError;
use crate::unified_exec::WriteStdinInteractionEvent;
use crate::unified_exec::WriteStdinRequest;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use serde::Deserialize;

use super::super::shell_spec::create_write_stdin_tool;
use super::post_unified_exec_tool_use_payload;

#[derive(Debug, Deserialize)]
struct WriteStdinArgs {
    // The model is trained on `session_id`.
    session_id: i32,
    #[serde(default)]
    chars: String,
    #[serde(default = "super::default_write_stdin_yield_time_ms")]
    yield_time_ms: u64,
    #[serde(default)]
    max_output_tokens: Option<usize>,
}

pub struct WriteStdinHandler;

impl ToolExecutor<ToolInvocation> for WriteStdinHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("write_stdin")
    }

    fn spec(&self) -> ToolSpec {
        create_write_stdin_tool()
    }

    fn supports_parallel_tool_calls(&self) -> bool {
        true
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(self.handle_call(invocation))
    }
}

impl WriteStdinHandler {
    async fn handle_call(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn crate::tools::context::ToolOutput>, FunctionCallError> {
        let ToolInvocation {
            session,
            turn,
            payload,
            ..
        } = invocation;

        let arguments = match payload {
            ToolPayload::Function { arguments } => arguments,
            _ => {
                return Err(FunctionCallError::RespondToModel(
                    "write_stdin handler received unsupported payload".to_string(),
                ));
            }
        };

        let args: WriteStdinArgs = parse_arguments(&arguments)?;
        let result = session
            .services
            .unified_exec_manager
            .write_stdin(WriteStdinRequest {
                process_id: args.session_id,
                input: &args.chars,
                yield_time_ms: args.yield_time_ms,
                max_output_tokens: args.max_output_tokens,
                truncation_policy: turn.model_info.truncation_policy.into(),
                interaction_event: Some(WriteStdinInteractionEvent {
                    session: &session,
                    turn: &turn,
                }),
            })
            .await;
        settle_write_stdin_implicit_skill_activation(&turn, args.session_id, result.as_ref());
        let response = result.map_err(|err| {
            FunctionCallError::RespondToModel(format!("write_stdin failed: {err}"))
        })?;

        Ok(boxed_tool_output(response))
    }
}

fn settle_write_stdin_implicit_skill_activation(
    turn: &TurnContext,
    requested_process_id: i32,
    result: Result<&ExecCommandToolOutput, &UnifiedExecError>,
) {
    match result {
        Ok(response) if response.process_id.is_some() => {}
        Ok(response) if response.exit_code == Some(0) => {
            promote_pending_skill_activation(turn, requested_process_id);
        }
        Ok(_) => {
            discard_pending_skill_activation(turn, requested_process_id);
        }
        Err(UnifiedExecError::UnknownProcessId { .. } | UnifiedExecError::ProcessFailed { .. }) => {
            discard_pending_skill_activation(turn, requested_process_id);
        }
        Err(_) => {}
    }
}

impl CoreToolRuntime for WriteStdinHandler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }

    fn pre_tool_use_payload(&self, _invocation: &ToolInvocation) -> Option<PreToolUsePayload> {
        // `write_stdin` is transport for an existing exec session. Empty writes
        // are background polls, and non-empty writes continue a command that
        // already ran PreToolUse as Bash, so do not emit a second pre hook here.
        None
    }

    fn post_tool_use_payload(
        &self,
        invocation: &ToolInvocation,
        result: &dyn crate::tools::context::ToolOutput,
    ) -> Option<PostToolUsePayload> {
        // A `write_stdin` poll can observe final completion for the original
        // `exec_command`; emit that command's matching Bash PostToolUse.
        post_unified_exec_tool_use_payload(invocation, result)
    }
}

#[cfg(test)]
mod implicit_activation_tests {
    use std::sync::Arc;

    use codex_hooks::SkillActivation;
    use codex_hooks::SkillActivationKind;
    use codex_hooks::SkillActivationScope;
    use codex_protocol::models::PermissionProfile;
    use codex_utils_output_truncation::TruncationPolicy;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use tokio::sync::Mutex;

    use super::*;
    use crate::session::step_context::StepContext;
    use crate::session::tests::make_session_and_context;
    use crate::session::turn_context::TurnContext;
    use crate::skills::promote_pending_skill_activation;
    use crate::skills::retain_pending_skill_activation;
    use crate::skills::skill_activation_snapshot;
    use crate::skills::tests::configure_implicit_skill_fixture_for_exec;
    use crate::skills::tests::implicit_skill_fixture;
    use crate::tools::context::ExecCommandToolOutput;
    use crate::tools::context::ToolCallSource;
    use crate::tools::registry::ToolExecutor;
    use crate::turn_diff_tracker::TurnDiffTracker;
    use crate::unified_exec::UnifiedExecError;

    use super::super::ExecCommandHandler;

    fn activation() -> SkillActivation {
        SkillActivation::new(
            "yielded".to_string(),
            "/repo/yielded/SKILL.md".to_string(),
            SkillActivationScope::Repo,
            SkillActivationKind::Implicit,
            "turn-1".to_string(),
            "a".repeat(64),
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
        tool_name: &str,
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
            tool_name: ToolName::plain(tool_name),
            source: ToolCallSource::Direct,
            payload: ToolPayload::Function {
                arguments: arguments.to_string(),
            },
        }
    }

    async fn start_yielded_skill_read(
        session: Arc<crate::session::session::Session>,
        turn: Arc<TurnContext>,
        command: String,
        call_id: &str,
    ) -> i32 {
        let output = ExecCommandHandler::default()
            .handle(invocation(
                session,
                turn,
                "exec_command",
                json!({ "cmd": command, "yield_time_ms": 250 }),
                call_id,
            ))
            .await
            .expect("long-running skill read should yield");
        output
            .code_mode_result(&ToolPayload::Function {
                arguments: String::new(),
            })
            .get("session_id")
            .and_then(serde_json::Value::as_i64)
            .and_then(|id| i32::try_from(id).ok())
            .expect("yielded command should return a session id")
    }

    async fn poll_process_until_terminal(
        session: Arc<crate::session::session::Session>,
        turn: Arc<TurnContext>,
        process_id: i32,
        call_id: &str,
    ) {
        for attempt in 0..20 {
            let output = WriteStdinHandler
                .handle(invocation(
                    Arc::clone(&session),
                    Arc::clone(&turn),
                    "write_stdin",
                    json!({ "session_id": process_id, "yield_time_ms": 250 }),
                    &format!("{call_id}-{attempt}"),
                ))
                .await
                .expect("polling yielded process should succeed");
            let result = output.code_mode_result(&ToolPayload::Function {
                arguments: String::new(),
            });
            if result.get("session_id").is_none() {
                return;
            }
        }
        panic!("yielded process {process_id} did not terminate after repeated polls");
    }

    #[tokio::test]
    async fn write_stdin_implicit_skill_activation_actual_yielded_zero_promotes_requested_id() {
        let mut fixture = implicit_skill_fixture(codex_protocol::protocol::SkillScope::Repo).await;
        configure_implicit_skill_fixture_for_exec(&mut fixture, PermissionProfile::Disabled);
        let command = format!("cat {}; sleep 1; exit 0", fixture.skill_path.display());
        let session = Arc::new(fixture.session);
        let turn = Arc::new(fixture.turn);
        let process_id = start_yielded_skill_read(
            Arc::clone(&session),
            Arc::clone(&turn),
            command,
            "yielded-zero",
        )
        .await;
        assert_eq!(skill_activation_snapshot(&turn), Vec::new());

        poll_process_until_terminal(session, Arc::clone(&turn), process_id, "poll-yielded-zero")
            .await;

        let activations = skill_activation_snapshot(&turn);
        assert_eq!(activations.len(), 1);
        assert_eq!(activations[0].name(), "audit-skill");
    }

    #[tokio::test]
    async fn write_stdin_implicit_skill_activation_actual_yielded_nonzero_discards_requested_id() {
        let mut fixture = implicit_skill_fixture(codex_protocol::protocol::SkillScope::User).await;
        configure_implicit_skill_fixture_for_exec(&mut fixture, PermissionProfile::Disabled);
        let command = format!("cat {}; sleep 1; exit 6", fixture.skill_path.display());
        let session = Arc::new(fixture.session);
        let turn = Arc::new(fixture.turn);
        let process_id = start_yielded_skill_read(
            Arc::clone(&session),
            Arc::clone(&turn),
            command,
            "yielded-nonzero",
        )
        .await;

        poll_process_until_terminal(
            session,
            Arc::clone(&turn),
            process_id,
            "poll-yielded-nonzero",
        )
        .await;

        assert_eq!(skill_activation_snapshot(&turn), Vec::new());
        assert!(!promote_pending_skill_activation(&turn, process_id));
    }

    #[tokio::test]
    async fn write_stdin_implicit_skill_activation_actual_cross_turn_poll_cannot_promote() {
        let mut fixture = implicit_skill_fixture(codex_protocol::protocol::SkillScope::Repo).await;
        configure_implicit_skill_fixture_for_exec(&mut fixture, PermissionProfile::Disabled);
        let command = format!("cat {}; sleep 1; exit 0", fixture.skill_path.display());
        let session = Arc::new(fixture.session);
        let turn_a = Arc::new(fixture.turn);
        let process_id = start_yielded_skill_read(
            Arc::clone(&session),
            Arc::clone(&turn_a),
            command,
            "yielded-cross-turn",
        )
        .await;
        let (_unused_session, turn_b) = make_session_and_context().await;
        let turn_b = Arc::new(turn_b);

        poll_process_until_terminal(session, Arc::clone(&turn_b), process_id, "poll-cross-turn")
            .await;

        assert_eq!(skill_activation_snapshot(&turn_a), Vec::new());
        assert_eq!(skill_activation_snapshot(&turn_b), Vec::new());
        assert!(promote_pending_skill_activation(&turn_a, process_id));
    }

    #[tokio::test]
    async fn write_stdin_implicit_skill_activation_same_turn_zero_promotes_and_live_stays_pending()
    {
        let (_session, turn) = make_session_and_context().await;
        retain_pending_skill_activation(&turn, 41, activation());
        settle_write_stdin_implicit_skill_activation(&turn, 41, Ok(&response(Some(41), None)));
        assert_eq!(skill_activation_snapshot(&turn), Vec::new());
        settle_write_stdin_implicit_skill_activation(&turn, 41, Ok(&response(None, Some(0))));
        assert_eq!(skill_activation_snapshot(&turn), vec![activation()]);
    }

    #[tokio::test]
    async fn write_stdin_implicit_skill_activation_nonzero_or_missing_exit_discards() {
        let (_session, turn) = make_session_and_context().await;
        retain_pending_skill_activation(&turn, 41, activation());
        settle_write_stdin_implicit_skill_activation(&turn, 41, Ok(&response(None, Some(9))));
        assert!(!promote_pending_skill_activation(&turn, 41));

        retain_pending_skill_activation(&turn, 42, activation());
        settle_write_stdin_implicit_skill_activation(&turn, 42, Ok(&response(None, None)));
        assert!(!promote_pending_skill_activation(&turn, 42));
        assert_eq!(skill_activation_snapshot(&turn), Vec::new());
    }

    #[tokio::test]
    async fn write_stdin_implicit_skill_activation_cross_turn_poll_cannot_promote() {
        let (_session_a, turn_a) = make_session_and_context().await;
        let (_session_b, turn_b) = make_session_and_context().await;
        retain_pending_skill_activation(&turn_a, 41, activation());

        settle_write_stdin_implicit_skill_activation(&turn_b, 41, Ok(&response(None, Some(0))));

        assert_eq!(skill_activation_snapshot(&turn_a), Vec::new());
        assert_eq!(skill_activation_snapshot(&turn_b), Vec::new());
        assert!(promote_pending_skill_activation(&turn_a, 41));
    }

    #[tokio::test]
    async fn write_stdin_implicit_skill_activation_preserves_pending_on_recoverable_write_error() {
        let (_session, turn) = make_session_and_context().await;
        retain_pending_skill_activation(&turn, 41, activation());

        settle_write_stdin_implicit_skill_activation(
            &turn,
            41,
            Err(&UnifiedExecError::StdinClosed),
        );

        assert!(promote_pending_skill_activation(&turn, 41));
    }
}
