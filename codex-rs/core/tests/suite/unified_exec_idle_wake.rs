use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use codex_core::compact::SUMMARIZATION_PROMPT;
use codex_features::Feature;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use core_test_support::TestTargetOs;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_custom_tool_call;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_once;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use core_test_support::test_target_os;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use serde_json::json;

fn yielded_session_id(output: &str) -> Result<i32> {
    output
        .lines()
        .find_map(|line| line.strip_prefix("Process running with session ID "))
        .context("exec_command did not return a running session id")?
        .parse()
        .context("exec_command returned an invalid session id")
}

const COMPLETION_GATE: &str = "yielded-exec-completion.ready";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn yielded_exec_completion_after_compaction_wakes_idle_session_once_and_remains_pollable()
-> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mut builder = test_codex().with_config(|config| {
        config
            .features
            .enable(Feature::UnifiedExec)
            .expect("test config should allow feature update");
        config.compact_prompt = Some(SUMMARIZATION_PROMPT.to_string());
    });
    let test = builder.build_with_auto_env(&server).await?;
    let completion_gate = test
        .executor_environment()
        .selection()
        .cwd
        .join(COMPLETION_GATE)?;
    let (shell, command) = match test_target_os() {
        TestTargetOs::Linux | TestTargetOs::MacOs => (
            "bash",
            format!(
                "while [ ! -f {COMPLETION_GATE} ]; do sleep 0.05; done; printf yielded-exec-complete"
            ),
        ),
        TestTargetOs::Windows => (
            "powershell",
            format!(
                "while (-not (Test-Path -LiteralPath '{COMPLETION_GATE}')) {{ Start-Sleep -Milliseconds 50 }}; [Console]::Out.Write('yielded-exec-complete')"
            ),
        ),
    };

    let initial_responses = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("yielded-exec-start"),
                ev_function_call(
                    "yielded-exec-call",
                    "exec_command",
                    &serde_json::to_string(&json!({
                        "cmd": command,
                        "shell": shell,
                        "login": false,
                        "yield_time_ms": 250,
                    }))?,
                ),
                ev_completed("yielded-exec-start"),
            ]),
            sse(vec![
                ev_assistant_message("yielded-exec-waiting", "waiting for completion"),
                ev_completed("yielded-exec-followup"),
            ]),
        ],
    )
    .await;

    test.submit_turn("start a command that will complete later")
        .await?;

    let initial_requests = initial_responses.requests();
    assert_eq!(2, initial_requests.len());
    let initial_output = initial_requests[1]
        .function_call_output_text("yielded-exec-call")
        .context("missing yielded exec_command output")?;
    let session_id = yielded_session_id(&initial_output)?;

    mount_sse_once(
        &server,
        sse(vec![
            ev_response_created("yielded-exec-compact"),
            ev_assistant_message("yielded-exec-compact-summary", "compacted exec context"),
            ev_completed("yielded-exec-compact"),
        ]),
    )
    .await;
    test.codex.submit(Op::Compact).await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::ContextCompacted(_))
    })
    .await;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;

    let completion_responses = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("yielded-exec-idle-wake"),
                ev_function_call(
                    "yielded-exec-poll",
                    "write_stdin",
                    &serde_json::to_string(&json!({
                        "session_id": session_id,
                        "yield_time_ms": 5000,
                    }))?,
                ),
                ev_completed("yielded-exec-idle-wake"),
            ]),
            sse(vec![
                ev_assistant_message("yielded-exec-observed", "completion observed"),
                ev_completed("yielded-exec-observed-response"),
            ]),
        ],
    )
    .await;

    test.fs()
        .write_file(&completion_gate, b"ready".to_vec(), /*sandbox*/ None)
        .await?;

    let completion_requests = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let requests = completion_responses.requests();
            if requests.len() == 2 {
                return requests;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await?;

    let notification = completion_requests[0]
        .message_input_texts("developer")
        .into_iter()
        .find(|text| text.contains("<exec_command_completion>"))
        .context("idle wake request did not contain an exec completion notification")?;
    assert!(
        notification.contains(&format!(r#""session_id":{session_id}"#)),
        "completion notification should identify the yielded session: {notification}"
    );
    assert!(
        notification.contains(r#""exit_code":0"#),
        "completion notification should report the exit code: {notification}"
    );
    assert!(
        notification.contains(r#""output_may_be_available":true"#),
        "completion notification should preserve write_stdin retrieval: {notification}"
    );

    let poll_output = completion_requests[1]
        .function_call_output_text("yielded-exec-poll")
        .context("missing write_stdin output after completion notification")?;
    assert!(
        poll_output.contains("Process exited with code 0"),
        "write_stdin should still return the terminal result: {poll_output}"
    );
    assert!(
        poll_output.ends_with("yielded-exec-complete"),
        "write_stdin should still return retained output: {poll_output}"
    );

    test.codex.shutdown_and_wait().await?;
    assert_eq!(2, completion_responses.requests().len());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn code_mode_nested_yield_survives_outer_turn_completion_and_wakes_idle_session() -> Result<()>
{
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mut builder = test_codex().with_config(|config| {
        config
            .features
            .enable(Feature::UnifiedExec)
            .expect("test config should allow feature update");
        config
            .features
            .enable(Feature::CodeMode)
            .expect("test config should allow feature update");
        config.code_mode.default_exec_yield_time_ms = 500;
    });
    let test = builder.build_with_auto_env(&server).await?;
    let completion_gate = test
        .executor_environment()
        .selection()
        .cwd
        .join(COMPLETION_GATE)?;
    let (shell, command) = match test_target_os() {
        TestTargetOs::Linux | TestTargetOs::MacOs => (
            "bash",
            format!(
                "while [ ! -f {COMPLETION_GATE} ]; do sleep 0.05; done; printf code-mode-yielded-exec-complete"
            ),
        ),
        TestTargetOs::Windows => (
            "powershell",
            format!(
                "while (-not (Test-Path -LiteralPath '{COMPLETION_GATE}')) {{ Start-Sleep -Milliseconds 50 }}; [Console]::Out.Write('code-mode-yielded-exec-complete')"
            ),
        ),
    };
    let code = format!(
        r#"const result = await tools.exec_command({{
  cmd: {},
  shell: {},
  login: false,
  yield_time_ms: 10000,
}});
text(JSON.stringify(result));"#,
        serde_json::to_string(&command)?,
        serde_json::to_string(shell)?,
    );

    let initial_responses = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("code-mode-yielded-exec-start"),
                ev_custom_tool_call("code-mode-yielded-exec-call", "exec", &code),
                ev_completed("code-mode-yielded-exec-start"),
            ]),
            sse(vec![
                ev_assistant_message("code-mode-yielded-exec-waiting", "waiting for completion"),
                ev_completed("code-mode-yielded-exec-followup"),
            ]),
        ],
    )
    .await;

    test.submit_turn("start a nested command that will complete later")
        .await?;

    let initial_requests = initial_responses.requests();
    assert_eq!(2, initial_requests.len());
    let initial_output = initial_requests[1].custom_tool_call_output("code-mode-yielded-exec-call");
    assert!(
        initial_output
            .to_string()
            .contains("Script running with cell ID"),
        "outer code cell should yield before nested exec delivery: {initial_output}"
    );

    let completion_responses = mount_sse_sequence(
        &server,
        vec![sse(vec![
            ev_assistant_message("code-mode-yielded-exec-observed", "completion observed"),
            ev_completed("code-mode-yielded-exec-observed-response"),
        ])],
    )
    .await;

    tokio::time::sleep(Duration::from_secs(15)).await;
    test.fs()
        .write_file(&completion_gate, b"ready".to_vec(), /*sandbox*/ None)
        .await?;

    let completion_requests = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let requests = completion_responses.requests();
            if requests.len() == 1 {
                return requests;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await?;

    let notification = completion_requests[0]
        .message_input_texts("developer")
        .into_iter()
        .find(|text| text.contains("<exec_command_completion>"))
        .context("idle wake request did not contain an exec completion notification")?;
    assert!(
        notification.contains(r#""session_id":"#),
        "completion notification should identify the nested session: {notification}"
    );
    assert!(
        notification.contains(r#""output_may_be_available":true"#),
        "completion notification should preserve nested write_stdin retrieval: {notification}"
    );

    test.codex.shutdown_and_wait().await?;
    assert_eq!(1, completion_responses.requests().len());

    Ok(())
}
