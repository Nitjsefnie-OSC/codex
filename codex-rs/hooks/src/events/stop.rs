use std::path::PathBuf;

use codex_protocol::ThreadId;
use codex_protocol::items::HookPromptFragment;
use codex_protocol::protocol::HookCompletedEvent;
use codex_protocol::protocol::HookEventName;
use codex_protocol::protocol::HookOutputEntry;
use codex_protocol::protocol::HookOutputEntryKind;
use codex_protocol::protocol::HookRunStatus;
use codex_protocol::protocol::HookRunSummary;
use codex_utils_absolute_path::AbsolutePathBuf;

use super::common;
use crate::background_state::BackgroundState;
use crate::engine::CommandShell;
use crate::engine::ConfiguredHandler;
use crate::engine::command_runner::CommandRunResult;
use crate::engine::dispatcher;
use crate::engine::output_parser;
use crate::schema::NullableString;
use crate::schema::StopCommandInput;
use crate::schema::SubagentStopCommandInput;

#[derive(Debug, Clone)]
pub struct StopRequest {
    pub session_id: ThreadId,
    pub turn_id: String,
    pub cwd: AbsolutePathBuf,
    pub transcript_path: Option<PathBuf>,
    pub model: String,
    pub permission_mode: String,
    pub stop_hook_active: bool,
    pub last_assistant_message: Option<String>,
    /// Work the turn started that is still outstanding. Read from the process
    /// manager when the hook fires, so a hook can refuse to let a turn end on
    /// top of a running build or unread monitor output.
    pub background: BackgroundState,
    pub target: StopHookTarget,
}

#[derive(Debug, Clone)]
pub enum StopHookTarget {
    Stop,
    SubagentStop {
        agent_id: String,
        agent_type: String,
        agent_transcript_path: Option<PathBuf>,
    },
}

impl StopHookTarget {
    fn event_name(&self) -> HookEventName {
        match self {
            Self::Stop => HookEventName::Stop,
            Self::SubagentStop { .. } => HookEventName::SubagentStop,
        }
    }

    fn matcher_input(&self) -> Option<&str> {
        match self {
            Self::Stop => None,
            Self::SubagentStop { agent_type, .. } => Some(agent_type.as_str()),
        }
    }
}

#[derive(Debug, Default)]
pub struct StopOutcome {
    pub hook_events: Vec<HookCompletedEvent>,
    pub should_stop: bool,
    pub stop_reason: Option<String>,
    pub should_block: bool,
    pub block_reason: Option<String>,
    pub continuation_fragments: Vec<HookPromptFragment>,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct StopHandlerData {
    should_stop: bool,
    stop_reason: Option<String>,
    should_block: bool,
    block_reason: Option<String>,
    continuation_fragments: Vec<HookPromptFragment>,
}

pub(crate) fn preview(
    handlers: &[ConfiguredHandler],
    request: &StopRequest,
) -> Vec<HookRunSummary> {
    dispatcher::select_handlers(
        handlers,
        request.target.event_name(),
        request.target.matcher_input(),
    )
    .into_iter()
    .map(|handler| dispatcher::running_summary(&handler))
    .collect()
}

pub(crate) async fn run(
    handlers: &[ConfiguredHandler],
    shell: &CommandShell,
    request: StopRequest,
) -> StopOutcome {
    let matched = dispatcher::select_handlers(
        handlers,
        request.target.event_name(),
        request.target.matcher_input(),
    );
    if matched.is_empty() {
        return StopOutcome {
            hook_events: Vec::new(),
            should_stop: false,
            stop_reason: None,
            should_block: false,
            block_reason: None,
            continuation_fragments: Vec::new(),
        };
    }

    let input_json = match build_input_json(&request) {
        Ok(input_json) => input_json,
        Err(error) => {
            let label = match request.target {
                StopHookTarget::Stop => "stop hook input",
                StopHookTarget::SubagentStop { .. } => "subagent stop hook input",
            };
            return serialization_failure_outcome(common::serialization_failure_hook_events(
                matched,
                Some(request.turn_id),
                format!("failed to serialize {label}: {error}"),
            ));
        }
    };

    let results = dispatcher::execute_handlers(
        shell,
        matched,
        input_json,
        request.cwd.as_path(),
        Some(request.turn_id),
        parse_completed,
    )
    .await;

    let aggregate = aggregate_results(results.iter().map(|result| &result.data));

    StopOutcome {
        hook_events: results.into_iter().map(|result| result.completed).collect(),
        should_stop: aggregate.should_stop,
        stop_reason: aggregate.stop_reason,
        should_block: aggregate.should_block,
        block_reason: aggregate.block_reason,
        continuation_fragments: aggregate.continuation_fragments,
    }
}

/// Renders the JSON a stop hook reads on stdin.
///
/// Split out of `run` so the payload can be asserted without standing up a
/// shell and a handler: the fields a gate hook reads are a contract, and the
/// contract is what a test should pin.
fn build_input_json(request: &StopRequest) -> Result<String, serde_json::Error> {
    match &request.target {
        StopHookTarget::Stop => serde_json::to_string(&StopCommandInput {
            session_id: request.session_id.to_string(),
            turn_id: request.turn_id.clone(),
            transcript_path: NullableString::from_path(request.transcript_path.clone()),
            cwd: request.cwd.display().to_string(),
            hook_event_name: "Stop".to_string(),
            model: request.model.clone(),
            permission_mode: request.permission_mode.clone(),
            stop_hook_active: request.stop_hook_active,
            last_assistant_message: NullableString::from_string(
                request.last_assistant_message.clone(),
            ),
            background: request.background.clone(),
        }),
        StopHookTarget::SubagentStop {
            agent_id,
            agent_type,
            agent_transcript_path,
        } => serde_json::to_string(&SubagentStopCommandInput {
            session_id: request.session_id.to_string(),
            turn_id: request.turn_id.clone(),
            transcript_path: NullableString::from_path(request.transcript_path.clone()),
            agent_transcript_path: NullableString::from_path(agent_transcript_path.clone()),
            cwd: request.cwd.display().to_string(),
            hook_event_name: "SubagentStop".to_string(),
            model: request.model.clone(),
            permission_mode: request.permission_mode.clone(),
            stop_hook_active: request.stop_hook_active,
            agent_id: agent_id.clone(),
            agent_type: agent_type.clone(),
            last_assistant_message: NullableString::from_string(
                request.last_assistant_message.clone(),
            ),
            background: request.background.clone(),
        }),
    }
}

fn parse_completed(
    handler: &ConfiguredHandler,
    run_result: CommandRunResult,
    turn_id: Option<String>,
) -> dispatcher::ParsedHandler<StopHandlerData> {
    let mut entries = Vec::new();
    let mut status = HookRunStatus::Completed;
    let mut should_stop = false;
    let mut stop_reason = None;
    let mut should_block = false;
    let mut block_reason = None;
    let mut continuation_prompt = None;
    let hook_event_name = match handler.event_name {
        HookEventName::Stop | HookEventName::SubagentStop => handler.event_name,
        event_name => {
            panic!("expected stop hook event, got {event_name:?}");
        }
    };

    match run_result.error.as_deref() {
        Some(error) => {
            status = HookRunStatus::Failed;
            entries.push(HookOutputEntry {
                kind: HookOutputEntryKind::Error,
                text: error.to_string(),
            });
        }
        None => match run_result.exit_code {
            Some(0) => {
                let trimmed_stdout = run_result.stdout.trim();
                if trimmed_stdout.is_empty() {
                } else if let Some(parsed) = match hook_event_name {
                    HookEventName::Stop => output_parser::parse_stop(&run_result.stdout),
                    HookEventName::SubagentStop => {
                        output_parser::parse_subagent_stop(&run_result.stdout)
                    }
                    _ => unreachable!("validated stop hook event"),
                } {
                    if let Some(system_message) = parsed.universal.system_message {
                        entries.push(HookOutputEntry {
                            kind: HookOutputEntryKind::Warning,
                            text: system_message,
                        });
                    }
                    let _ = parsed.universal.suppress_output;
                    if !parsed.universal.continue_processing {
                        status = HookRunStatus::Stopped;
                        should_stop = true;
                        stop_reason = parsed.universal.stop_reason.clone();
                        if let Some(stop_reason_text) = parsed.universal.stop_reason {
                            entries.push(HookOutputEntry {
                                kind: HookOutputEntryKind::Stop,
                                text: stop_reason_text,
                            });
                        }
                    } else if let Some(invalid_block_reason) = parsed.invalid_block_reason {
                        status = HookRunStatus::Failed;
                        entries.push(HookOutputEntry {
                            kind: HookOutputEntryKind::Error,
                            text: invalid_block_reason,
                        });
                    } else if parsed.should_block {
                        if let Some(reason) =
                            parsed.reason.as_deref().and_then(common::trimmed_non_empty)
                        {
                            status = HookRunStatus::Blocked;
                            should_block = true;
                            block_reason = Some(reason.clone());
                            continuation_prompt = Some(reason.clone());
                            entries.push(HookOutputEntry {
                                kind: HookOutputEntryKind::Feedback,
                                text: reason,
                            });
                        } else {
                            status = HookRunStatus::Failed;
                            entries.push(HookOutputEntry {
                                kind: HookOutputEntryKind::Error,
                                text: match hook_event_name {
                                    HookEventName::Stop => "Stop hook returned decision:block without a non-empty reason",
                                    HookEventName::SubagentStop => "SubagentStop hook returned decision:block without a non-empty reason",
                                    _ => unreachable!("validated stop hook event"),
                                }
                                .to_string(),
                            });
                        }
                    }
                } else {
                    status = HookRunStatus::Failed;
                    entries.push(HookOutputEntry {
                        kind: HookOutputEntryKind::Error,
                        text: match hook_event_name {
                            HookEventName::Stop => "hook returned invalid stop hook JSON output",
                            HookEventName::SubagentStop => {
                                "hook returned invalid subagent stop hook JSON output"
                            }
                            _ => unreachable!("validated stop hook event"),
                        }
                        .to_string(),
                    });
                }
            }
            Some(2) => {
                if let Some(reason) = common::trimmed_non_empty(&run_result.stderr) {
                    status = HookRunStatus::Blocked;
                    should_block = true;
                    block_reason = Some(reason.clone());
                    continuation_prompt = Some(reason.clone());
                    entries.push(HookOutputEntry {
                        kind: HookOutputEntryKind::Feedback,
                        text: reason,
                    });
                } else {
                    status = HookRunStatus::Failed;
                    entries.push(HookOutputEntry {
                        kind: HookOutputEntryKind::Error,
                        text: match hook_event_name {
                            HookEventName::Stop => {
                                "Stop hook exited with code 2 but did not write a continuation prompt to stderr"
                            }
                            HookEventName::SubagentStop => {
                                "SubagentStop hook exited with code 2 but did not write a continuation prompt to stderr"
                            }
                            _ => unreachable!("validated stop hook event"),
                        }
                        .to_string(),
                    });
                }
            }
            Some(exit_code) => {
                status = HookRunStatus::Failed;
                entries.push(HookOutputEntry {
                    kind: HookOutputEntryKind::Error,
                    text: format!("hook exited with code {exit_code}"),
                });
            }
            None => {
                status = HookRunStatus::Failed;
                entries.push(HookOutputEntry {
                    kind: HookOutputEntryKind::Error,
                    text: "hook exited without a status code".to_string(),
                });
            }
        },
    }

    let completed = HookCompletedEvent {
        turn_id,
        run: dispatcher::completed_summary(handler, &run_result, status, entries),
    };
    let continuation_fragments = continuation_prompt
        .map(|prompt| {
            vec![HookPromptFragment::from_single_hook(
                prompt,
                completed.run.id.clone(),
            )]
        })
        .unwrap_or_default();

    dispatcher::ParsedHandler {
        completed,
        data: StopHandlerData {
            should_stop,
            stop_reason,
            should_block,
            block_reason,
            continuation_fragments,
        },
        completion_order: 0,
    }
}

fn aggregate_results<'a>(
    results: impl IntoIterator<Item = &'a StopHandlerData>,
) -> StopHandlerData {
    let results = results.into_iter().collect::<Vec<_>>();
    let should_stop = results.iter().any(|result| result.should_stop);
    let stop_reason = results.iter().find_map(|result| result.stop_reason.clone());
    let should_block = !should_stop && results.iter().any(|result| result.should_block);
    let block_reason = if should_block {
        common::join_text_chunks(
            results
                .iter()
                .filter_map(|result| result.block_reason.clone())
                .collect(),
        )
    } else {
        None
    };
    let continuation_fragments = if should_block {
        results
            .iter()
            .filter(|result| result.should_block)
            .flat_map(|result| result.continuation_fragments.clone())
            .collect()
    } else {
        Vec::new()
    };

    StopHandlerData {
        should_stop,
        stop_reason,
        should_block,
        block_reason,
        continuation_fragments,
    }
}

fn serialization_failure_outcome(hook_events: Vec<HookCompletedEvent>) -> StopOutcome {
    StopOutcome {
        hook_events,
        should_stop: false,
        stop_reason: None,
        should_block: false,
        block_reason: None,
        continuation_fragments: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use codex_protocol::protocol::HookEventName;
    use codex_protocol::protocol::HookOutputEntry;
    use codex_protocol::protocol::HookOutputEntryKind;
    use codex_protocol::protocol::HookRunStatus;
    use codex_utils_absolute_path::test_support::PathBufExt;
    use codex_utils_absolute_path::test_support::test_path_buf;
    use pretty_assertions::assert_eq;

    use codex_protocol::items::HookPromptFragment;

    use codex_protocol::ThreadId;

    use super::BackgroundState;
    use super::StopHandlerData;
    use super::StopHookTarget;
    use super::StopRequest;
    use super::aggregate_results;
    use super::build_input_json;
    use super::parse_completed;
    use crate::engine::ConfiguredHandler;
    use crate::engine::command_runner::CommandRunResult;

    #[test]
    fn block_decision_with_reason_sets_continuation_prompt() {
        let parsed = parse_completed(
            &handler(),
            run_result(
                Some(0),
                r#"{"decision":"block","reason":"retry with tests"}"#,
                "",
            ),
            Some("turn-1".to_string()),
        );

        assert_eq!(
            parsed.data,
            StopHandlerData {
                should_stop: false,
                stop_reason: None,
                should_block: true,
                block_reason: Some("retry with tests".to_string()),
                continuation_fragments: vec![HookPromptFragment {
                    text: "retry with tests".to_string(),
                    hook_run_id: parsed.completed.run.id.clone(),
                }],
            }
        );
        assert_eq!(parsed.completed.run.status, HookRunStatus::Blocked);
    }

    #[test]
    fn block_decision_without_reason_is_invalid() {
        let parsed = parse_completed(
            &handler(),
            run_result(Some(0), r#"{"decision":"block"}"#, ""),
            Some("turn-1".to_string()),
        );

        assert_eq!(parsed.data, StopHandlerData::default());
        assert_eq!(parsed.completed.run.status, HookRunStatus::Failed);
        assert_eq!(
            parsed.completed.run.entries,
            vec![HookOutputEntry {
                kind: HookOutputEntryKind::Error,
                text: "Stop hook returned decision:block without a non-empty reason".to_string(),
            }]
        );
    }

    #[test]
    fn continue_false_overrides_block_decision() {
        let parsed = parse_completed(
            &handler(),
            run_result(
                Some(0),
                r#"{"continue":false,"stopReason":"done","decision":"block","reason":"keep going"}"#,
                "",
            ),
            Some("turn-1".to_string()),
        );

        assert_eq!(
            parsed.data,
            StopHandlerData {
                should_stop: true,
                stop_reason: Some("done".to_string()),
                should_block: false,
                block_reason: None,
                continuation_fragments: Vec::new(),
            }
        );
        assert_eq!(parsed.completed.run.status, HookRunStatus::Stopped);
    }

    #[test]
    fn exit_code_two_uses_stderr_feedback_only() {
        let parsed = parse_completed(
            &handler(),
            run_result(Some(2), "ignored stdout", "retry with tests"),
            Some("turn-1".to_string()),
        );

        assert_eq!(
            parsed.data,
            StopHandlerData {
                should_stop: false,
                stop_reason: None,
                should_block: true,
                block_reason: Some("retry with tests".to_string()),
                continuation_fragments: vec![HookPromptFragment {
                    text: "retry with tests".to_string(),
                    hook_run_id: parsed.completed.run.id.clone(),
                }],
            }
        );
        assert_eq!(parsed.completed.run.status, HookRunStatus::Blocked);
    }

    #[test]
    fn exit_code_two_without_stderr_does_not_block() {
        let parsed = parse_completed(
            &handler(),
            run_result(Some(2), "", "   "),
            /*turn_id*/ None,
        );

        assert_eq!(parsed.data, StopHandlerData::default());
        assert_eq!(parsed.completed.run.status, HookRunStatus::Failed);
        assert_eq!(
            parsed.completed.run.entries,
            vec![HookOutputEntry {
                kind: HookOutputEntryKind::Error,
                text:
                    "Stop hook exited with code 2 but did not write a continuation prompt to stderr"
                        .to_string(),
            }]
        );
    }

    #[test]
    fn block_decision_with_blank_reason_fails_instead_of_blocking() {
        let parsed = parse_completed(
            &handler(),
            run_result(Some(0), "{\"decision\":\"block\",\"reason\":\"   \"}", ""),
            Some("turn-1".to_string()),
        );

        assert_eq!(parsed.data, StopHandlerData::default());
        assert_eq!(parsed.completed.run.status, HookRunStatus::Failed);
        assert_eq!(
            parsed.completed.run.entries,
            vec![HookOutputEntry {
                kind: HookOutputEntryKind::Error,
                text: "Stop hook returned decision:block without a non-empty reason".to_string(),
            }]
        );
    }

    #[test]
    fn invalid_stdout_fails_instead_of_silently_nooping() {
        let parsed = parse_completed(
            &handler(),
            run_result(Some(0), "not json", ""),
            Some("turn-1".to_string()),
        );

        assert_eq!(parsed.data, StopHandlerData::default());
        assert_eq!(parsed.completed.run.status, HookRunStatus::Failed);
        assert_eq!(
            parsed.completed.run.entries,
            vec![HookOutputEntry {
                kind: HookOutputEntryKind::Error,
                text: "hook returned invalid stop hook JSON output".to_string(),
            }]
        );
    }

    #[test]
    fn aggregate_results_concatenates_blocking_reasons_in_declaration_order() {
        let aggregate = aggregate_results([
            &StopHandlerData {
                should_stop: false,
                stop_reason: None,
                should_block: true,
                block_reason: Some("first".to_string()),
                continuation_fragments: vec![HookPromptFragment::from_single_hook(
                    "first", "run-1",
                )],
            },
            &StopHandlerData {
                should_stop: false,
                stop_reason: None,
                should_block: true,
                block_reason: Some("second".to_string()),
                continuation_fragments: vec![HookPromptFragment::from_single_hook(
                    "second", "run-2",
                )],
            },
        ]);

        assert_eq!(
            aggregate,
            StopHandlerData {
                should_stop: false,
                stop_reason: None,
                should_block: true,
                block_reason: Some("first\n\nsecond".to_string()),
                continuation_fragments: vec![
                    HookPromptFragment::from_single_hook("first", "run-1"),
                    HookPromptFragment::from_single_hook("second", "run-2"),
                ],
            }
        );
    }

    fn running_monitor() -> crate::background_state::MonitorSnapshot {
        crate::background_state::MonitorSnapshot {
            process_id: 4,
            command: "cargo build".to_string(),
            cwd: "/repo".to_string(),
            kind: "job".to_string(),
            state: "running".to_string(),
            running: true,
            exit_code: None,
            failure_message: None,
            age_seconds: 12.0,
            notifications_delivered: 2,
            notifications_suppressed: 0,
            unacknowledged_notifications: 2,
            owner_model_slug: "gpt-5".to_string(),
            owner_turn_id: "turn-1".to_string(),
            owner_call_id: "call-1".to_string(),
        }
    }

    fn request(target: StopHookTarget) -> StopRequest {
        StopRequest {
            session_id: ThreadId::new(),
            turn_id: "turn-1".to_string(),
            cwd: test_path_buf("/repo").abs(),
            transcript_path: None,
            model: "gpt-5".to_string(),
            permission_mode: "default".to_string(),
            stop_hook_active: false,
            last_assistant_message: None,
            background: BackgroundState {
                monitors: vec![running_monitor()],
                running_monitors: 1,
                unacknowledged_notifications: 2,
                background_terminals: vec![crate::background_state::BackgroundTerminalSnapshot {
                    process_id: "9".to_string(),
                    command: "bash".to_string(),
                    cwd: "/repo".to_string(),
                }],
                running_background_terminals: 1,
            },
            target,
        }
    }

    fn input_value(target: StopHookTarget) -> serde_json::Value {
        let json = build_input_json(&request(target)).expect("stop input should serialize");
        serde_json::from_str(&json).expect("stop input should be valid JSON")
    }

    #[test]
    fn stop_input_carries_running_background_work() {
        // Without this a Stop hook cannot tell a turn that finished from one
        // that walked away from a running build — the rest of the payload is
        // identical in both cases.
        let value = input_value(StopHookTarget::Stop);

        assert_eq!(value["hook_event_name"], "Stop");
        assert_eq!(value["background"]["running_monitors"], 1);
        assert_eq!(value["background"]["unacknowledged_notifications"], 2);
        assert_eq!(value["background"]["running_background_terminals"], 1);
        assert_eq!(value["background"]["monitors"][0]["process_id"], 4);
        assert_eq!(value["background"]["monitors"][0]["state"], "running");
        assert_eq!(value["background"]["monitors"][0]["kind"], "job");
        assert_eq!(
            value["background"]["monitors"][0]["owner_turn_id"],
            "turn-1"
        );
        assert_eq!(
            value["background"]["background_terminals"][0]["process_id"],
            "9"
        );
    }

    #[test]
    fn subagent_stop_input_carries_the_same_background_work() {
        // A subagent leaving a watcher running is the case this exists for: its
        // parent never sees the child's process manager.
        let value = input_value(StopHookTarget::SubagentStop {
            agent_id: "agent-1".to_string(),
            agent_type: "implementer".to_string(),
            agent_transcript_path: None,
        });

        assert_eq!(value["hook_event_name"], "SubagentStop");
        assert_eq!(value["agent_type"], "implementer");
        assert_eq!(value["background"]["running_monitors"], 1);
        assert_eq!(value["background"]["monitors"][0]["command"], "cargo build");
    }

    #[test]
    fn an_idle_turn_reports_empty_background_work() {
        let mut idle = request(StopHookTarget::Stop);
        idle.background = BackgroundState::default();
        let json = build_input_json(&idle).expect("stop input should serialize");
        let value: serde_json::Value =
            serde_json::from_str(&json).expect("stop input should be valid JSON");

        // Empty, not absent: a hook reads `background.running_monitors`
        // unconditionally and a missing key would read as an error, not as
        // "nothing running".
        assert_eq!(value["background"]["running_monitors"], 0);
        assert_eq!(value["background"]["running_background_terminals"], 0);
        assert_eq!(value["background"]["monitors"], serde_json::json!([]));
        assert_eq!(
            value["background"]["background_terminals"],
            serde_json::json!([])
        );
    }

    fn handler() -> ConfiguredHandler {
        ConfiguredHandler {
            event_name: HookEventName::Stop,
            matcher: None,
            command: "echo hook".to_string(),
            timeout_sec: 600,
            status_message: None,
            additional_context_limit: Default::default(),
            source_path: test_path_buf("/tmp/hooks.json").abs(),
            source: codex_protocol::protocol::HookSource::User,
            display_order: 0,
            env: std::collections::HashMap::new(),
        }
    }

    fn run_result(exit_code: Option<i32>, stdout: &str, stderr: &str) -> CommandRunResult {
        CommandRunResult {
            started_at: 1,
            completed_at: 2,
            duration_ms: 1,
            exit_code,
            stdout: stdout.to_string(),
            stderr: stderr.to_string(),
            error: None,
        }
    }
}
