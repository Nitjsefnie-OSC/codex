use super::*;
use crate::config::PermissionProfileSnapshot;
use crate::context::ContextualUserFragment;
use crate::context::MonitorNotification;
use crate::environment_selection::TurnEnvironmentState;
use crate::session::session::Session;
use crate::session::step_context::StepContext;
use crate::session::tests::make_session_and_context_with_rx;
use crate::session::turn_context::TurnContext;
use crate::tools::context::ToolCallSource;
use crate::tools::handlers::monitor_spec::MAX_WAIT_TIMEOUT_MS;
use crate::turn_diff_tracker::TurnDiffTracker;
use crate::unified_exec::MAX_MONITOR_NOTIFICATIONS;
use codex_protocol::models::ContentItem;
use codex_protocol::models::FunctionCallOutputBody;
use codex_protocol::models::PermissionProfile;
use codex_protocol::models::ResponseInputItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::Event;
use pretty_assertions::assert_eq;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// Longest a monitored test command may take before the test gives up. Generous
/// on purpose: these spawn real child processes.
const TEST_TIMEOUT: Duration = Duration::from_secs(45);

struct Harness {
    session: Arc<Session>,
    turn: Arc<TurnContext>,
    #[allow(dead_code)]
    rx: async_channel::Receiver<Event>,
}

/// A session whose primary environment runs commands unsandboxed and never asks
/// for approval, so the tests exercise the monitor's contract rather than the
/// host's sandbox availability.
async fn harness() -> Harness {
    let (session, mut turn, rx) = make_session_and_context_with_rx().await;
    let turn_mut =
        Arc::get_mut(&mut turn).expect("test turn context must be uniquely owned at setup");
    let mut config = (*turn_mut.config).clone();
    config
        .permissions
        .set_permission_profile(PermissionProfile::Disabled)
        .expect("test setup should allow updating the permission profile");
    config.permissions.approval_policy =
        codex_config::Constrained::allow_any(codex_protocol::protocol::AskForApproval::Never);
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
    ToolInvocation {
        session: Arc::clone(&harness.session),
        step_context: StepContext::for_test(Arc::clone(&harness.turn)),
        turn: Arc::clone(&harness.turn),
        cancellation_token: CancellationToken::new(),
        tracker: Arc::new(tokio::sync::Mutex::new(TurnDiffTracker::new())),
        call_id: call_id.to_string(),
        tool_name: ToolName::plain(TOOL_NAME),
        source: ToolCallSource::Direct,
        payload: ToolPayload::Function {
            arguments: arguments.to_string(),
        },
    }
}

/// The full model-facing text of a tool result. `log_preview` truncates, so it
/// cannot be used to assert on a monitor's retained output.
fn output_text(output: &dyn ToolOutput) -> String {
    let item = output.to_response_item(
        "call",
        &ToolPayload::Function {
            arguments: String::new(),
        },
    );
    let ResponseInputItem::FunctionCallOutput { output, .. } = item else {
        panic!("monitor returns a function call output");
    };
    match output.body {
        FunctionCallOutputBody::Text(text) => text,
        other => panic!("monitor returns text, got {other:?}"),
    }
}

/// Call the tool and parse its JSON return value.
async fn call(harness: &Harness, call_id: &str, arguments: serde_json::Value) -> serde_json::Value {
    let output = MonitorHandler
        .handle(invocation(harness, call_id, arguments))
        .await
        .unwrap_or_else(|err| panic!("monitor call `{call_id}` should succeed: {err:?}"));
    let text = output_text(output.as_ref());
    serde_json::from_str(&text)
        .unwrap_or_else(|err| panic!("monitor returns JSON, got {text}: {err}"))
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

/// Every `monitor_notification` injected into the model's context so far, in
/// order, decoded from the fragment markers.
async fn notifications(harness: &Harness, process_id: i64) -> Vec<serde_json::Value> {
    let (start, end) = MonitorNotification::type_markers();
    harness
        .session
        .clone_history()
        .await
        .raw_items()
        .iter()
        .filter_map(|item| {
            let ResponseItem::Message { content, .. } = item else {
                return None;
            };
            content.iter().find_map(|part| match part {
                ContentItem::InputText { text } => text
                    .strip_prefix(start)
                    .and_then(|text| text.strip_suffix(end))
                    .and_then(|body| serde_json::from_str::<serde_json::Value>(body.trim()).ok()),
                _ => None,
            })
        })
        .filter(|notification| notification["process_id"] == serde_json::json!(process_id))
        .collect()
}

fn is_final(notification: &serde_json::Value) -> bool {
    notification["final"] == serde_json::json!(true)
}

/// Poll until the monitor's single terminal notification lands.
async fn await_terminal(harness: &Harness, process_id: i64) -> serde_json::Value {
    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        let delivered = notifications(harness, process_id).await;
        if let Some(terminal) = delivered.iter().find(|item| is_final(item)) {
            return terminal.clone();
        }
        assert!(
            Instant::now() < deadline,
            "no terminal notification for monitor {process_id}; got {delivered:#?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn process_id_of(started: &serde_json::Value) -> i64 {
    started["process_id"]
        .as_i64()
        .expect("start reports a process id")
}

#[cfg(unix)]
fn sh(script: &str) -> serde_json::Value {
    serde_json::json!(["/bin/sh", "-c", script])
}

#[cfg(unix)]
#[tokio::test]
async fn output_lines_arrive_as_separate_sequenced_notifications() {
    let harness = harness().await;
    let started = call(
        &harness,
        "monitor-lines",
        serde_json::json!({
            "command": sh("echo first; sleep 0.8; echo second; sleep 0.8; echo third"),
        }),
    )
    .await;
    let process_id = process_id_of(&started);

    await_terminal(&harness, process_id).await;
    let delivered = notifications(&harness, process_id).await;

    let batches: Vec<&serde_json::Value> =
        delivered.iter().filter(|item| !is_final(item)).collect();
    assert!(
        batches.len() >= 2,
        "lines separated in time are separate notifications, got {batches:#?}"
    );

    // Sequence numbers are dense and ordered across batches and the terminal.
    let sequence: Vec<u64> = delivered
        .iter()
        .map(|item| {
            item["seq"]
                .as_u64()
                .expect("every notification is sequenced")
        })
        .collect();
    assert_eq!(
        sequence,
        (1..=delivered.len() as u64).collect::<Vec<_>>(),
        "notifications are numbered from 1 without gaps"
    );

    let lines: Vec<String> = batches
        .iter()
        .flat_map(|item| {
            item["lines"]
                .as_array()
                .expect("a batch carries lines")
                .iter()
                .map(|line| line.as_str().unwrap_or_default().to_string())
        })
        .collect();
    assert_eq!(lines, vec!["first", "second", "third"]);
}

#[cfg(unix)]
#[tokio::test]
async fn a_successful_job_always_delivers_exactly_one_terminal_notification() {
    let harness = harness().await;
    let started = call(
        &harness,
        "monitor-exit-zero",
        serde_json::json!({ "command": sh("echo working") }),
    )
    .await;
    let process_id = process_id_of(&started);

    let terminal = await_terminal(&harness, process_id).await;

    assert_eq!(terminal["state"], serde_json::json!("exited with code 0"));
    // Give any second watcher pass time to misfire before asserting uniqueness.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let finals = notifications(&harness, process_id)
        .await
        .into_iter()
        .filter(is_final)
        .count();
    assert_eq!(finals, 1, "exactly one terminal notification, ever");
}

#[cfg(unix)]
#[tokio::test]
async fn a_failing_job_reports_its_exit_code_in_the_terminal_notification() {
    let harness = harness().await;
    let started = call(
        &harness,
        "monitor-exit-three",
        serde_json::json!({ "command": sh("echo working; exit 3") }),
    )
    .await;
    let process_id = process_id_of(&started);

    let terminal = await_terminal(&harness, process_id).await;

    assert_eq!(terminal["state"], serde_json::json!("exited with code 3"));
}

#[cfg(unix)]
#[tokio::test]
async fn a_job_that_outlives_its_timeout_still_terminates_and_says_so() {
    let harness = harness().await;
    let started = call(
        &harness,
        "monitor-timeout",
        serde_json::json!({
            "command": sh("echo starting; sleep 120"),
            "timeout_ms": 800,
        }),
    )
    .await;
    let process_id = process_id_of(&started);

    let terminal = await_terminal(&harness, process_id).await;

    assert_eq!(terminal["state"], serde_json::json!("timed out"));
}

#[cfg(unix)]
#[tokio::test]
async fn stopping_a_watcher_kills_the_process_rather_than_orphaning_it() {
    let harness = harness().await;
    let scratch = tempfile::tempdir().expect("create temp dir");
    let ticks = scratch.path().join("ticks");
    let ticks_path = ticks.to_string_lossy().to_string();

    let started = call(
        &harness,
        "monitor-stop",
        serde_json::json!({
            "kind": "watcher",
            "command": sh(&format!(
                "while :; do echo tick >> '{ticks_path}'; sleep 0.1; done"
            )),
        }),
    )
    .await;
    let process_id = process_id_of(&started);
    assert_eq!(started["kind"], serde_json::json!("watcher"));

    // Let it actually produce something before stopping it.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let stopped = call(
        &harness,
        "monitor-stop-call",
        serde_json::json!({ "action": "stop", "process_id": process_id }),
    )
    .await;
    assert_eq!(stopped["stopped"], serde_json::json!(true));

    let terminal = await_terminal(&harness, process_id).await;
    assert_eq!(terminal["state"], serde_json::json!("stopped"));

    // No orphan: the loop stops writing once the monitor is stopped.
    let after_stop = std::fs::metadata(&ticks).expect("tick file exists").len();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let later = std::fs::metadata(&ticks).expect("tick file exists").len();
    assert_eq!(
        after_stop, later,
        "a stopped watcher must leave no process still writing"
    );

    // And unified exec no longer lists it as a live background terminal.
    let live = harness.session.list_background_terminals().await;
    assert!(
        !live
            .iter()
            .any(|terminal| terminal.process_id == process_id.to_string()),
        "stopped monitor is gone from the process store, got {live:?}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn a_noisy_monitor_is_capped_but_its_output_stays_readable() {
    let harness = harness().await;
    let started = call(
        &harness,
        "monitor-firehose",
        serde_json::json!({ "command": sh("i=1; while [ $i -le 2000 ]; do echo line-$i; i=$((i+1)); done") }),
    )
    .await;
    let process_id = process_id_of(&started);

    await_terminal(&harness, process_id).await;
    let delivered = notifications(&harness, process_id).await;

    assert!(
        delivered.len() as u64 <= MAX_MONITOR_NOTIFICATIONS + 1,
        "a firehose must not outrun the notification cap, got {}",
        delivered.len()
    );

    // A command that finishes before the watcher can subscribe still reports
    // its first line: the head of the output is never dropped.
    let first_line = delivered
        .iter()
        .find(|item| !is_final(item))
        .and_then(|item| item["lines"].as_array())
        .and_then(|lines| lines.first())
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    assert_eq!(first_line.as_deref(), Some("line-1"));

    // The output the notifications did not carry is still readable after the
    // fact, and reading it acknowledges what was delivered.
    let output = call(
        &harness,
        "monitor-read",
        serde_json::json!({ "action": "read", "process_id": process_id }),
    )
    .await;
    let text = output["output"].as_str().expect("read returns text");
    assert!(
        text.contains("line-1\n") && text.contains("line-2000"),
        "retained output should span the run; got {} bytes",
        text.len()
    );
    assert_eq!(output["unacknowledged_notifications"], serde_json::json!(0));
}

#[cfg(unix)]
#[tokio::test]
async fn list_reports_classification_ownership_and_unread_notifications() {
    let harness = harness().await;
    let started = call(
        &harness,
        "monitor-list-start",
        serde_json::json!({ "command": sh("echo hello") }),
    )
    .await;
    let process_id = process_id_of(&started);
    await_terminal(&harness, process_id).await;

    let listed = call(
        &harness,
        "monitor-list",
        serde_json::json!({ "action": "list" }),
    )
    .await;
    let monitors = listed["monitors"]
        .as_array()
        .expect("list returns an array");
    let entry = monitors
        .iter()
        .find(|entry| entry["process_id"] == serde_json::json!(process_id))
        .expect("the monitor we started is listed");

    assert_eq!(entry["kind"], serde_json::json!("job"));
    assert_eq!(
        entry["owner"]["call_id"],
        serde_json::json!("monitor-list-start"),
        "the monitor records which call started it"
    );
    assert_eq!(entry["state"]["status"], serde_json::json!("exited"));
    assert!(
        entry["unacknowledged_notifications"]
            .as_u64()
            .expect("unacknowledged count")
            > 0,
        "nothing has been read yet"
    );

    // Reading acknowledges, and the proof shows up in the next list.
    let output = call(
        &harness,
        "monitor-list-read",
        serde_json::json!({ "action": "read", "process_id": process_id }),
    )
    .await;
    assert_eq!(
        output["output"].as_str(),
        Some("hello\n"),
        "a finished monitor's output is still readable"
    );
    let relisted = call(
        &harness,
        "monitor-list-again",
        serde_json::json!({ "action": "list" }),
    )
    .await;
    let entry = relisted["monitors"]
        .as_array()
        .expect("list returns an array")
        .iter()
        .find(|entry| entry["process_id"] == serde_json::json!(process_id))
        .expect("the monitor is still listed after it finished");
    assert_eq!(
        entry["unacknowledged_notifications"],
        serde_json::json!(0),
        "reading proves the output was consumed"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn wait_blocks_until_a_job_finishes_and_reports_it_did() {
    let harness = harness().await;
    let started = call(
        &harness,
        "monitor-wait-start",
        serde_json::json!({ "command": sh("sleep 1; echo done") }),
    )
    .await;
    let process_id = process_id_of(&started);

    let outcome = call(
        &harness,
        "monitor-wait",
        serde_json::json!({
            "action": "wait",
            "process_id": process_id,
            "wait_timeout_ms": 30_000,
        }),
    )
    .await;

    assert_eq!(outcome["completed"], serde_json::json!(true));
    assert_eq!(
        outcome["info"]["state"]["status"],
        serde_json::json!("exited")
    );
}

#[cfg(unix)]
#[tokio::test]
async fn wait_gives_up_on_a_persistent_watcher_without_stopping_it() {
    let harness = harness().await;
    let started = call(
        &harness,
        "monitor-wait-watcher",
        serde_json::json!({
            "kind": "watcher",
            "command": sh("while :; do sleep 1; done"),
        }),
    )
    .await;
    let process_id = process_id_of(&started);

    let outcome = call(
        &harness,
        "monitor-wait-timeout",
        serde_json::json!({
            "action": "wait",
            "process_id": process_id,
            "wait_timeout_ms": 300,
        }),
    )
    .await;

    assert_eq!(outcome["completed"], serde_json::json!(false));
    assert_eq!(
        outcome["info"]["state"]["status"],
        serde_json::json!("running"),
        "a watcher outlives a wait that gave up"
    );

    call(
        &harness,
        "monitor-wait-cleanup",
        serde_json::json!({ "action": "stop", "process_id": process_id }),
    )
    .await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_monitor_survives_the_interruption_of_the_turn_that_started_it() {
    let harness = harness().await;
    let cancellation_token = CancellationToken::new();
    let mut invocation = invocation(
        &harness,
        "monitor-survives",
        serde_json::json!({
            "kind": "watcher",
            "command": sh("echo watching; sleep 30"),
        }),
    );
    invocation.cancellation_token = cancellation_token.clone();
    let output = MonitorHandler
        .handle(invocation)
        .await
        .expect("monitor should start the command");
    let started: serde_json::Value =
        serde_json::from_str(&output_text(output.as_ref())).expect("start returns JSON");
    let process_id = process_id_of(&started);

    // Interrupting the turn is not a stop: the process manager owns the
    // process, so a watcher keeps running until it is stopped explicitly.
    cancellation_token.cancel();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let listed = call(
        &harness,
        "monitor-survives-list",
        serde_json::json!({ "action": "list" }),
    )
    .await;
    let entry = listed["monitors"]
        .as_array()
        .expect("list returns an array")
        .iter()
        .find(|entry| entry["process_id"] == serde_json::json!(process_id))
        .expect("the monitor is listed")
        .clone();
    assert_eq!(entry["state"]["status"], serde_json::json!("running"));

    call(
        &harness,
        "monitor-survives-cleanup",
        serde_json::json!({ "action": "stop", "process_id": process_id }),
    )
    .await;
}

#[tokio::test]
async fn start_rejects_an_empty_command() {
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
async fn start_rejects_a_timeout_above_the_ceiling() {
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
async fn wait_rejects_a_wait_above_the_ceiling() {
    let harness = harness().await;
    let result = MonitorHandler
        .handle(invocation(
            &harness,
            "monitor-long-wait",
            serde_json::json!({
                "action": "wait",
                "process_id": 1,
                "wait_timeout_ms": MAX_WAIT_TIMEOUT_MS + 1,
            }),
        ))
        .await;

    expect_rejected(result, "wait_timeout_ms");
}

#[tokio::test]
async fn control_actions_require_a_process_id() {
    let harness = harness().await;
    for action in ["read", "stop", "wait"] {
        let result = MonitorHandler
            .handle(invocation(
                &harness,
                "monitor-no-id",
                serde_json::json!({ "action": action }),
            ))
            .await;

        expect_rejected(result, "`process_id` is required");
    }
}

#[tokio::test]
async fn control_actions_reject_an_unknown_process_id() {
    let harness = harness().await;
    let result = MonitorHandler
        .handle(invocation(
            &harness,
            "monitor-unknown-id",
            serde_json::json!({ "action": "read", "process_id": 424_242 }),
        ))
        .await;

    expect_rejected(result, "no monitor with process_id 424242");
}

#[tokio::test]
async fn start_rejects_unknown_arguments() {
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
fn the_spec_advertises_the_tool_name_and_its_control_actions() {
    let ToolSpec::Function(tool) = MonitorHandler.spec() else {
        panic!("monitor is a plain function tool");
    };
    assert_eq!(tool.name, TOOL_NAME);

    let properties = tool
        .parameters
        .properties
        .expect("monitor takes named parameters");
    let actions = properties["action"]
        .enum_values
        .clone()
        .expect("action is an enum");
    assert_eq!(
        actions,
        vec![
            serde_json::json!("start"),
            serde_json::json!("list"),
            serde_json::json!("read"),
            serde_json::json!("stop"),
            serde_json::json!("wait"),
        ]
    );
    assert_eq!(
        tool.parameters.required, None,
        "`action` defaults to start, so nothing is unconditionally required"
    );
}
