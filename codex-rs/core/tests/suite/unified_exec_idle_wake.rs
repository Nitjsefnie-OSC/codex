use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use codex_core::TurnInputRequest;
use codex_core::compact::SUMMARIZATION_PROMPT;
use codex_features::Feature;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::user_input::UserInput;
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
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use core_test_support::test_codex::test_codex;
use core_test_support::test_target_os;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use serde_json::json;
use tokio::sync::oneshot;
use tokio::time::timeout;

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
        let _ = config.features.disable(Feature::RemoteCompactionV2);
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
async fn monitor_notification_window_resets_after_compaction_and_wakes_idle_session_once()
-> Result<()> {
    skip_if_no_network!(Ok(()));

    const BEFORE_COMPACTION_GATE: &str = "monitor-compaction-before.ready";
    const AFTER_COMPACTION_GATE: &str = "monitor-compaction-after.ready";
    const PRE_COMPACTION_NOTIFICATION_COUNT: usize = 20;
    const LINES_PER_NOTIFICATION: usize = 40;

    let monitor_command = match test_target_os() {
        TestTargetOs::Linux | TestTargetOs::MacOs => {
            let command = format!(
                "i=1; while [ \"$i\" -le {PRE_COMPACTION_NOTIFICATION_COUNT} ]; do j=0; while [ \"$j\" -lt {LINES_PER_NOTIFICATION} ]; do printf 'monitor-before-%s-%s\\n' \"$i\" \"$j\"; j=$((j + 1)); done; sleep 0.75; i=$((i + 1)); done; sleep 0.5; touch {BEFORE_COMPACTION_GATE}; while [ ! -f {AFTER_COMPACTION_GATE} ]; do sleep 0.05; done; j=0; while [ \"$j\" -lt {LINES_PER_NOTIFICATION} ]; do printf 'monitor-after-%s\\n' \"$j\"; j=$((j + 1)); done; sleep 30"
            );
            vec!["bash".to_string(), "-c".to_string(), command]
        }
        TestTargetOs::Windows => {
            let command = format!(
                "$i=1; while ($i -le {PRE_COMPACTION_NOTIFICATION_COUNT}) {{ for ($j=0; $j -lt {LINES_PER_NOTIFICATION}; $j++) {{ [Console]::Out.WriteLine(\"monitor-before-$i-$j\") }}; Start-Sleep -Milliseconds 750; $i++ }}; Start-Sleep -Milliseconds 500; New-Item -ItemType File -Force -Path '{BEFORE_COMPACTION_GATE}' | Out-Null; while (-not (Test-Path -LiteralPath '{AFTER_COMPACTION_GATE}')) {{ Start-Sleep -Milliseconds 50 }}; for ($j=0; $j -lt {LINES_PER_NOTIFICATION}; $j++) {{ [Console]::Out.WriteLine(\"monitor-after-$j\") }}; Start-Sleep -Seconds 30"
            );
            vec![
                "powershell".to_string(),
                "-NoProfile".to_string(),
                "-Command".to_string(),
                command,
            ]
        }
    };
    let monitor_arguments = serde_json::to_string(&json!({
        "command": monitor_command,
        "kind": "watcher",
    }))?;

    let (release_followup_tx, release_followup_rx) = oneshot::channel();
    let (streaming_server, _completions) = start_streaming_sse_server(vec![
        vec![StreamingSseChunk {
            gate: None,
            body: sse(vec![
                ev_response_created("monitor-compaction-start"),
                ev_function_call("monitor-compaction-call", "monitor", &monitor_arguments),
                ev_completed("monitor-compaction-start"),
            ]),
        }],
        vec![
            StreamingSseChunk {
                gate: None,
                body: sse(vec![ev_response_created("monitor-compaction-followup")]),
            },
            StreamingSseChunk {
                gate: Some(release_followup_rx),
                body: sse(vec![
                    ev_assistant_message("monitor-compaction-followup-message", "monitor started"),
                    ev_completed("monitor-compaction-followup"),
                ]),
            },
        ],
        vec![StreamingSseChunk {
            gate: None,
            body: sse(vec![
                ev_response_created("monitor-compaction-before-response"),
                ev_assistant_message(
                    "monitor-compaction-before-message",
                    "received all pre-compaction monitor notifications",
                ),
                ev_completed("monitor-compaction-before-response"),
            ]),
        }],
        vec![StreamingSseChunk {
            gate: None,
            body: sse(vec![
                ev_response_created("monitor-compaction-summary-response"),
                ev_assistant_message(
                    "monitor-compaction-summary-message",
                    "summary after monitor compaction",
                ),
                ev_completed("monitor-compaction-summary-response"),
            ]),
        }],
        vec![StreamingSseChunk {
            gate: None,
            body: sse(vec![
                ev_response_created("monitor-compaction-after-response"),
                ev_assistant_message(
                    "monitor-compaction-after-message",
                    "received the post-compaction monitor notification",
                ),
                ev_completed("monitor-compaction-after-response"),
            ]),
        }],
    ])
    .await;

    let mut builder = test_codex().with_model("gpt-5.4").with_config(|config| {
        config
            .features
            .enable(Feature::UnifiedExec)
            .expect("test config should allow feature update");
        let _ = config.features.disable(Feature::RemoteCompactionV2);
        config.compact_prompt = Some(SUMMARIZATION_PROMPT.to_string());
        config.model_provider.supports_websockets = false;
    });
    let test = builder
        .build_with_streaming_server(&streaming_server)
        .await?;
    let before_compaction_gate = test
        .executor_environment()
        .selection()
        .cwd
        .join(BEFORE_COMPACTION_GATE)?;
    let after_compaction_gate = test
        .executor_environment()
        .selection()
        .cwd
        .join(AFTER_COMPACTION_GATE)?;

    test.codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "start a live monitor before compaction".to_string(),
            text_elements: Vec::new(),
        }]))
        .await?;

    timeout(Duration::from_secs(30), async {
        loop {
            if test
                .fs()
                .read_file(&before_compaction_gate, /*sandbox*/ None)
                .await
                .is_ok()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await?;

    release_followup_tx
        .send(())
        .map_err(|_| anyhow::anyhow!("monitor follow-up response gate closed unexpectedly"))?;
    timeout(
        Duration::from_secs(10),
        streaming_server.wait_for_request_count(3),
    )
    .await?;
    let requests = streaming_server.requests().await;
    let pre_compaction_request = String::from_utf8_lossy(&requests[2]);
    assert_eq!(
        PRE_COMPACTION_NOTIFICATION_COUNT,
        pre_compaction_request
            .matches("<monitor_notification>")
            .count(),
        "the live monitor should exhaust its twenty nonterminal notification slots"
    );
    for seq in 1..=PRE_COMPACTION_NOTIFICATION_COUNT {
        assert!(
            pre_compaction_request.contains(&format!(r#""seq":{seq}"#)),
            "pre-compaction request should contain monitor notification sequence {seq}"
        );
    }
    assert!(!pre_compaction_request.contains("monitor-after"));

    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
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

    test.fs()
        .write_file(
            &after_compaction_gate,
            b"ready".to_vec(),
            /*sandbox*/ None,
        )
        .await?;
    timeout(
        Duration::from_secs(10),
        streaming_server.wait_for_request_count(5),
    )
    .await?;
    let requests = streaming_server.requests().await;
    assert_eq!(5, requests.len());
    let post_compaction_requests = requests
        .iter()
        .filter(|request| String::from_utf8_lossy(request).contains("monitor-after"))
        .collect::<Vec<_>>();
    assert_eq!(
        1,
        post_compaction_requests.len(),
        "the post-compaction monitor batch should wake exactly one successor"
    );
    let post_compaction_request = String::from_utf8_lossy(post_compaction_requests[0]);
    assert_eq!(
        1,
        post_compaction_request
            .matches("<monitor_notification>")
            .count()
    );
    assert!(post_compaction_request.contains(r#""seq":21"#));

    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    test.codex.shutdown_and_wait().await?;
    assert_eq!(5, streaming_server.requests().await.len());
    streaming_server.shutdown().await;

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
