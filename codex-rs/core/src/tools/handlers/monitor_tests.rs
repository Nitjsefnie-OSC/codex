use super::*;
use crate::config::PermissionProfileSnapshot;
use crate::environment_selection::TurnEnvironmentState;
use crate::session::step_context::StepContext;
use crate::session::tests::make_session_and_context_with_rx;
use crate::tools::context::ToolCallSource;
use crate::tools::context::ToolOutput;
use crate::turn_diff_tracker::TurnDiffTracker;
use codex_protocol::items::CommandExecutionStatus;
use codex_protocol::items::TurnItem;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ExecOutputStream;
use pretty_assertions::assert_eq;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Longest a monitored test command may take before the test gives up. Generous
/// on purpose: these spawn real child processes.
const EVENT_TIMEOUT: Duration = Duration::from_secs(30);

struct Harness {
    session: Arc<Session>,
    turn: Arc<TurnContext>,
    rx: async_channel::Receiver<Event>,
}

/// A session whose primary environment runs commands unsandboxed, so the tests
/// exercise the monitor's event protocol rather than the host's sandbox
/// availability.
async fn harness() -> Harness {
    let (session, mut turn, rx) = make_session_and_context_with_rx().await;
    let turn_mut =
        Arc::get_mut(&mut turn).expect("test turn context must be uniquely owned at setup");
    let mut config = (*turn_mut.config).clone();
    config
        .permissions
        .set_permission_profile(PermissionProfile::Disabled)
        .expect("test setup should allow updating the permission profile");
    turn_mut.config = Arc::new(config);
    let TurnEnvironmentState::Ready(environment) = turn_mut
        .environments
        .environments
        .first_mut()
        .expect("test session should have a primary environment")
    else {
        panic!("test session's primary environment should be ready");
    };
    environment.config.permission_profile =
        PermissionProfileSnapshot::legacy(PermissionProfile::Disabled);

    Harness { session, turn, rx }
}

fn invocation(harness: &Harness, call_id: &str, arguments: serde_json::Value) -> ToolInvocation {
    invocation_with_cancellation(harness, call_id, arguments, CancellationToken::new())
}

fn invocation_with_cancellation(
    harness: &Harness,
    call_id: &str,
    arguments: serde_json::Value,
    cancellation_token: CancellationToken,
) -> ToolInvocation {
    ToolInvocation {
        session: Arc::clone(&harness.session),
        step_context: StepContext::for_test(Arc::clone(&harness.turn)),
        turn: Arc::clone(&harness.turn),
        cancellation_token,
        tracker: Arc::new(tokio::sync::Mutex::new(TurnDiffTracker::new())),
        call_id: call_id.to_string(),
        tool_name: ToolName::plain(TOOL_NAME),
        source: ToolCallSource::Direct,
        payload: ToolPayload::Function {
            arguments: arguments.to_string(),
        },
    }
}

fn output_text(output: Box<dyn ToolOutput>) -> String {
    output.log_preview()
}

/// `ToolOutput` is not `Debug`, so `expect_err` is unavailable here.
fn expect_rejected(
    result: Result<Box<dyn ToolOutput>, FunctionCallError>,
    expected_fragment: &str,
) {
    match result {
        Ok(_) => panic!("expected the call to be rejected with `{expected_fragment}`"),
        Err(FunctionCallError::RespondToModel(message)) => assert!(
            message.contains(expected_fragment),
            "expected `{expected_fragment}` in: {message}"
        ),
        Err(other) => panic!("unexpected error: {other:?}"),
    }
}

/// The command-execution item carried by a terminal `ItemCompleted` event.
struct Termination {
    status: CommandExecutionStatus,
    exit_code: Option<i32>,
    stdout: String,
}

/// Collect stdout deltas until the monitored command's completion item arrives.
async fn collect_until_termination(
    rx: &async_channel::Receiver<Event>,
    call_id: &str,
) -> (Vec<Vec<u8>>, Termination) {
    let mut deltas = Vec::new();
    let deadline = tokio::time::Instant::now() + EVENT_TIMEOUT;
    loop {
        let event = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .expect("monitored command should terminate before the test deadline")
            .expect("event channel should stay open");
        match event.msg {
            EventMsg::ExecCommandOutputDelta(delta)
                if delta.call_id == call_id && delta.stream == ExecOutputStream::Stdout =>
            {
                deltas.push(delta.chunk);
            }
            EventMsg::ItemCompleted(completed) => {
                let TurnItem::CommandExecution(item) = completed.item else {
                    continue;
                };
                if item.id != call_id {
                    continue;
                }
                return (
                    deltas,
                    Termination {
                        status: item.status,
                        exit_code: item.exit_code,
                        stdout: item.stdout.unwrap_or_default(),
                    },
                );
            }
            _ => {}
        }
    }
}

/// Wait for the monitored command's `ItemStarted` event.
async fn expect_started(rx: &async_channel::Receiver<Event>, call_id: &str) {
    let deadline = tokio::time::Instant::now() + EVENT_TIMEOUT;
    loop {
        let event = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .expect("a started item should be emitted before the test deadline")
            .expect("event channel should stay open");
        if let EventMsg::ItemStarted(started) = event.msg
            && let TurnItem::CommandExecution(item) = started.item
            && item.id == call_id
        {
            assert_eq!(item.status, CommandExecutionStatus::InProgress);
            return;
        }
    }
}

#[cfg(unix)]
fn sh(script: &str) -> serde_json::Value {
    serde_json::json!(["/bin/sh", "-c", script])
}

#[cfg(unix)]
#[tokio::test]
async fn monitor_streams_each_line_as_its_own_event() {
    let harness = harness().await;
    let output = MonitorHandler
        .handle(invocation(
            &harness,
            "monitor-lines",
            serde_json::json!({
                "command": sh("echo first; sleep 0.3; echo second; sleep 0.3; echo third"),
            }),
        ))
        .await
        .expect("monitor should start the command");
    assert!(
        output_text(output).contains("Monitoring"),
        "the model-facing return acknowledges the start, it is not the output"
    );

    expect_started(&harness.rx, "monitor-lines").await;
    let (deltas, termination) = collect_until_termination(&harness.rx, "monitor-lines").await;

    // Lines separated in time arrive as separate events, each line-aligned.
    assert_eq!(
        deltas,
        vec![
            b"first\n".to_vec(),
            b"second\n".to_vec(),
            b"third\n".to_vec(),
        ]
    );
    assert_eq!(termination.status, CommandExecutionStatus::Completed);
    assert_eq!(termination.exit_code, Some(0));
}

#[cfg(unix)]
#[tokio::test]
async fn monitor_batches_lines_that_arrive_together() {
    let harness = harness().await;
    MonitorHandler
        .handle(invocation(
            &harness,
            "monitor-batch",
            serde_json::json!({
                "command": sh("for i in 1 2 3 4 5 6 7 8 9 10; do echo line-$i; done"),
            }),
        ))
        .await
        .expect("monitor should start the command");

    let (deltas, termination) = collect_until_termination(&harness.rx, "monitor-batch").await;

    assert_eq!(termination.exit_code, Some(0));
    assert!(
        deltas.len() < 10,
        "ten lines written at once must not become ten events, got {}",
        deltas.len()
    );
    for delta in &deltas {
        assert!(
            delta.ends_with(b"\n"),
            "every delta ends on a line boundary: {:?}",
            String::from_utf8_lossy(delta)
        );
    }
    let joined = deltas.concat();
    assert_eq!(
        String::from_utf8_lossy(&joined),
        (1..=10)
            .map(|i| format!("line-{i}\n"))
            .collect::<String>()
            .as_str()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn monitor_reports_a_nonzero_exit_as_failed() {
    let harness = harness().await;
    MonitorHandler
        .handle(invocation(
            &harness,
            "monitor-fail",
            serde_json::json!({ "command": sh("echo working; exit 3") }),
        ))
        .await
        .expect("monitor should start the command");

    let (deltas, termination) = collect_until_termination(&harness.rx, "monitor-fail").await;

    assert_eq!(deltas, vec![b"working\n".to_vec()]);
    assert_eq!(termination.status, CommandExecutionStatus::Failed);
    assert_eq!(termination.exit_code, Some(3));
    assert_eq!(termination.stdout, "working\n");
}

#[cfg(unix)]
#[tokio::test]
async fn monitor_terminates_a_command_that_outlives_its_timeout() {
    let harness = harness().await;
    MonitorHandler
        .handle(invocation(
            &harness,
            "monitor-timeout",
            serde_json::json!({
                "command": sh("echo starting; sleep 60"),
                "timeout_ms": 500,
            }),
        ))
        .await
        .expect("monitor should start the command");

    // Silence is not success: the timeout still produces a terminal event.
    let (_deltas, termination) = collect_until_termination(&harness.rx, "monitor-timeout").await;
    assert_eq!(termination.status, CommandExecutionStatus::Failed);
    assert_ne!(termination.exit_code, Some(0));
}

#[cfg(unix)]
#[tokio::test]
async fn monitor_returns_before_the_command_finishes() {
    let harness = harness().await;
    let started = std::time::Instant::now();
    MonitorHandler
        .handle(invocation(
            &harness,
            "monitor-async",
            serde_json::json!({ "command": sh("sleep 3; echo done") }),
        ))
        .await
        .expect("monitor should start the command");
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(2),
        "the handler must not wait for the command, but returned after {elapsed:?}"
    );

    let (deltas, termination) = collect_until_termination(&harness.rx, "monitor-async").await;
    assert_eq!(deltas, vec![b"done\n".to_vec()]);
    assert_eq!(termination.exit_code, Some(0));
}

#[cfg(unix)]
#[tokio::test]
async fn interrupting_the_turn_terminates_the_monitored_command() {
    let harness = harness().await;
    let cancellation_token = CancellationToken::new();
    MonitorHandler
        .handle(invocation_with_cancellation(
            &harness,
            "monitor-interrupt",
            serde_json::json!({
                "command": sh("echo watching; sleep 120"),
                "timeout_ms": MAX_MONITOR_TIMEOUT_MS,
            }),
            cancellation_token.clone(),
        ))
        .await
        .expect("monitor should start the command");

    expect_started(&harness.rx, "monitor-interrupt").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    cancellation_token.cancel();

    // The command dies with the turn instead of running out its timeout, and the
    // interrupt is still reported as a terminal event.
    let (_deltas, termination) = collect_until_termination(&harness.rx, "monitor-interrupt").await;
    assert_eq!(termination.status, CommandExecutionStatus::Failed);
    assert_ne!(termination.exit_code, Some(0));
}

#[tokio::test]
async fn monitor_rejects_an_empty_command() {
    let harness = harness().await;
    let result = MonitorHandler
        .handle(invocation(
            &harness,
            "monitor-empty",
            serde_json::json!({ "command": [] }),
        ))
        .await;

    expect_rejected(result, "at least the program");
}

#[tokio::test]
async fn monitor_rejects_a_timeout_above_the_ceiling() {
    let harness = harness().await;
    let result = MonitorHandler
        .handle(invocation(
            &harness,
            "monitor-long",
            serde_json::json!({
                "command": ["true"],
                "timeout_ms": MAX_MONITOR_TIMEOUT_MS + 1,
            }),
        ))
        .await;

    expect_rejected(result, "timeout_ms");
}

#[tokio::test]
async fn monitor_rejects_unknown_arguments() {
    let harness = harness().await;
    let result = MonitorHandler
        .handle(invocation(
            &harness,
            "monitor-unknown",
            serde_json::json!({ "command": ["true"], "follow": true }),
        ))
        .await;

    expect_rejected(result, "failed to parse");
}

#[test]
fn the_spec_advertises_the_tool_name_and_required_command() {
    let ToolSpec::Function(tool) = MonitorHandler.spec() else {
        panic!("monitor is a plain function tool");
    };
    assert_eq!(tool.name, TOOL_NAME);
    assert_eq!(
        tool.parameters.required,
        Some(vec!["command".to_string()]),
        "only `command` is required"
    );
}
