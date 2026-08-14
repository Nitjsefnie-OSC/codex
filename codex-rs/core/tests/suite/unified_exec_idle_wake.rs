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
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use core_test_support::test_codex::test_codex;
use core_test_support::test_target_os;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use tokio::time::timeout;

fn yielded_session_id(output: &str) -> Result<i32> {
    output
        .lines()
        .find_map(|line| line.strip_prefix("Process running with session ID "))
        .context("exec_command did not return a running session id")?
        .parse()
        .context("exec_command returned an invalid session id")
}

fn developer_input_texts(request: &[u8]) -> Result<Vec<String>> {
    let body: Value = serde_json::from_slice(request)
        .context("streaming Responses request body should be valid JSON")?;
    Ok(body
        .get("input")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| {
            item.get("type").and_then(Value::as_str) == Some("message")
                && item.get("role").and_then(Value::as_str) == Some("developer")
        })
        .filter_map(|item| item.get("content").and_then(Value::as_array).cloned())
        .flatten()
        .filter(|span| span.get("type").and_then(Value::as_str) == Some("input_text"))
        .filter_map(|span| span.get("text").and_then(Value::as_str).map(str::to_owned))
        .collect())
}

fn monitor_notification_payloads(request: &[u8]) -> Result<Vec<Value>> {
    const OPEN_TAG: &str = "<monitor_notification>";
    const CLOSE_TAG: &str = "</monitor_notification>";

    let mut payloads = Vec::new();
    for text in developer_input_texts(request)? {
        let mut remaining = text.as_str();
        while let Some(open_offset) = remaining.find(OPEN_TAG) {
            let payload_start = open_offset + OPEN_TAG.len();
            let Some(close_offset) = remaining[payload_start..].find(CLOSE_TAG) else {
                break;
            };
            let payload_end = payload_start + close_offset;
            let payload: Value = serde_json::from_str(&remaining[payload_start..payload_end])
                .context("monitor notification payload should be valid JSON")?;
            payloads.push(payload);
            remaining = &remaining[payload_end + CLOSE_TAG.len()..];
        }
    }

    Ok(payloads)
}

fn request_contains_monitor_sequence(request: &[u8], sequence: usize) -> Result<bool> {
    Ok(monitor_notification_payloads(request)?
        .iter()
        .any(|payload| payload.get("seq").and_then(Value::as_u64) == Some(sequence as u64)))
}

fn monitor_notification_sequences(payloads: &[Value], context: &str) -> Result<Vec<u64>> {
    payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| {
            payload
                .get("seq")
                .and_then(Value::as_u64)
                .with_context(|| format!("{context} payload {index} should have a numeric seq"))
        })
        .collect()
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
        // A non-OpenAI provider advertises Unsupported remote compaction, so this
        // test exercises the local compaction path rather than legacy remote compaction.
        config.model_provider.name = "Non-OpenAI Model provider".to_string();
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

    const START_GATE: &str = "monitor-compaction-start.ready";
    const BEFORE_COMPACTION_GATE: &str = "monitor-compaction-before.ready";
    const DURING_COMPACTION_GATE: &str = "monitor-compaction-during.ready";
    const BATCH_READY_PREFIX: &str = "monitor-compaction-batch-";
    const BATCH_ACK_PREFIX: &str = "monitor-compaction-batch-ack-";
    const PRE_COMPACTION_NOTIFICATION_COUNT: usize = 20;
    const LINES_PER_NOTIFICATION: usize = 40;

    let monitor_command = match test_target_os() {
        TestTargetOs::Linux | TestTargetOs::MacOs => {
            let command = format!(
                "while [ ! -f {START_GATE} ]; do sleep 0.05; done; i=1; while [ \"$i\" -le {PRE_COMPACTION_NOTIFICATION_COUNT} ]; do j=0; while [ \"$j\" -lt {LINES_PER_NOTIFICATION} ]; do printf 'monitor-before-%s-%s\\n' \"$i\" \"$j\"; j=$((j + 1)); done; touch {BATCH_READY_PREFIX}$i.ready; while [ ! -f {BATCH_ACK_PREFIX}$i ]; do sleep 0.05; done; i=$((i + 1)); done; touch {BEFORE_COMPACTION_GATE}; while [ ! -f {DURING_COMPACTION_GATE} ]; do sleep 0.05; done; j=0; while [ \"$j\" -lt {LINES_PER_NOTIFICATION} ]; do printf 'monitor-after-%s\\n' \"$j\"; j=$((j + 1)); done; sleep 30"
            );
            vec!["bash".to_string(), "-c".to_string(), command]
        }
        TestTargetOs::Windows => {
            let command = format!(
                "while (-not (Test-Path -LiteralPath '{START_GATE}')) {{ Start-Sleep -Milliseconds 50 }}; $i=1; while ($i -le {PRE_COMPACTION_NOTIFICATION_COUNT}) {{ for ($j=0; $j -lt {LINES_PER_NOTIFICATION}; $j++) {{ [Console]::Out.WriteLine(\"monitor-before-$i-$j\") }}; New-Item -ItemType File -Force -Path \"{BATCH_READY_PREFIX}$i.ready\" | Out-Null; while (-not (Test-Path -LiteralPath \"{BATCH_ACK_PREFIX}$i\")) {{ Start-Sleep -Milliseconds 50 }}; $i++ }}; New-Item -ItemType File -Force -Path '{BEFORE_COMPACTION_GATE}' | Out-Null; while (-not (Test-Path -LiteralPath '{DURING_COMPACTION_GATE}')) {{ Start-Sleep -Milliseconds 50 }}; for ($j=0; $j -lt {LINES_PER_NOTIFICATION}; $j++) {{ [Console]::Out.WriteLine(\"monitor-after-$j\") }}; Start-Sleep -Seconds 30"
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

    let (compact_release_tx, compact_release_rx) = tokio::sync::oneshot::channel();
    let mut response_streams = vec![
        vec![StreamingSseChunk {
            gate: None,
            body: sse(vec![
                ev_response_created("monitor-compaction-start"),
                ev_function_call("monitor-compaction-call", "monitor", &monitor_arguments),
                ev_completed("monitor-compaction-start"),
            ]),
        }],
        vec![StreamingSseChunk {
            gate: None,
            body: sse(vec![
                ev_response_created("monitor-compaction-followup"),
                ev_assistant_message("monitor-compaction-followup-message", "monitor started"),
                ev_completed("monitor-compaction-followup"),
            ]),
        }],
    ];
    for seq in 1..=PRE_COMPACTION_NOTIFICATION_COUNT {
        response_streams.push(vec![StreamingSseChunk {
            gate: None,
            body: sse(vec![
                ev_response_created(&format!("monitor-compaction-before-response-{seq}")),
                ev_assistant_message(
                    &format!("monitor-compaction-before-message-{seq}"),
                    "received one pre-compaction monitor notification",
                ),
                ev_completed(&format!("monitor-compaction-before-response-{seq}")),
            ]),
        }]);
    }
    response_streams.extend([
        vec![
            StreamingSseChunk {
                gate: None,
                body: sse(vec![ev_response_created(
                    "monitor-compaction-summary-response",
                )]),
            },
            StreamingSseChunk {
                gate: Some(compact_release_rx),
                body: sse(vec![
                    ev_assistant_message(
                        "monitor-compaction-summary-message",
                        "summary after monitor compaction",
                    ),
                    ev_completed("monitor-compaction-summary-response"),
                ]),
            },
        ],
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
    ]);
    let (streaming_server, mut completion_receivers) =
        start_streaming_sse_server(response_streams).await;

    let mut builder = test_codex().with_model("gpt-5.4").with_config(|config| {
        config
            .features
            .enable(Feature::UnifiedExec)
            .expect("test config should allow feature update");
        config.model_provider.name = "Non-OpenAI Model provider".to_string();
        config.compact_prompt = Some(SUMMARIZATION_PROMPT.to_string());
        config.model_provider.supports_websockets = false;
    });
    let test = builder
        .build_with_streaming_server(&streaming_server)
        .await?;
    let (_monitor_admission_guard, monitor_admitted) =
        codex_core::test_support::install_monitor_draft_admission_hook();
    let start_gate = test
        .executor_environment()
        .selection()
        .cwd
        .join(START_GATE)?;
    let before_compaction_gate = test
        .executor_environment()
        .selection()
        .cwd
        .join(BEFORE_COMPACTION_GATE)?;
    let during_compaction_gate = test
        .executor_environment()
        .selection()
        .cwd
        .join(DURING_COMPACTION_GATE)?;
    let batch_ready_gates = (1..=PRE_COMPACTION_NOTIFICATION_COUNT)
        .map(|seq| {
            let path = format!("{BATCH_READY_PREFIX}{seq}.ready");
            test.executor_environment()
                .selection()
                .cwd
                .join(&path)
                .map_err(anyhow::Error::from)
        })
        .collect::<Result<Vec<_>>>()?;
    let batch_ack_gates = (1..=PRE_COMPACTION_NOTIFICATION_COUNT)
        .map(|seq| {
            let path = format!("{BATCH_ACK_PREFIX}{seq}");
            test.executor_environment()
                .selection()
                .cwd
                .join(&path)
                .map_err(anyhow::Error::from)
        })
        .collect::<Result<Vec<_>>>()?;

    test.submit_turn("start a live monitor before compaction")
        .await?;
    completion_receivers
        .remove(0)
        .await
        .with_context(|| "initial monitor response stage did not complete")?;
    completion_receivers
        .remove(0)
        .await
        .with_context(|| "monitor follow-up response stage did not complete")?;
    let monitor_draft_admitted = monitor_admitted.notified();
    tokio::pin!(monitor_draft_admitted);
    test.fs()
        .write_file(&start_gate, b"ready".to_vec(), /*sandbox*/ None)
        .await?;

    for (index, (ready_gate, ack_gate)) in
        batch_ready_gates.iter().zip(&batch_ack_gates).enumerate()
    {
        let seq = index + 1;
        timeout(Duration::from_secs(30), async {
            loop {
                if test
                    .fs()
                    .read_file(ready_gate, /*sandbox*/ None)
                    .await
                    .is_ok()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .with_context(|| format!("timed out waiting for monitor batch {seq} ready gate"))?;
        timeout(Duration::from_secs(30), async {
            loop {
                let requests = streaming_server.requests().await;
                let has_sequence = requests
                    .iter()
                    .map(|request| request_contains_monitor_sequence(request, seq))
                    .collect::<Result<Vec<_>>>()
                    .with_context(|| {
                        format!("failed to inspect monitor batch {seq} request bodies")
                    })?
                    .into_iter()
                    .any(|contains_sequence| contains_sequence);
                if has_sequence {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .with_context(|| format!("timed out waiting for monitor batch {seq} request"))??;
        completion_receivers
            .remove(0)
            .await
            .with_context(|| format!("monitor batch {seq} response stage did not complete"))?;
        wait_for_event(&test.codex, |event| {
            matches!(event, EventMsg::TurnComplete(_))
        })
        .await;
        test.fs()
            .write_file(ack_gate, b"ready".to_vec(), /*sandbox*/ None)
            .await?;
    }

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
    .await
    .with_context(|| "timed out waiting for all pre-compaction monitor batches")?;

    timeout(
        Duration::from_secs(10),
        streaming_server.wait_for_request_count(2 + PRE_COMPACTION_NOTIFICATION_COUNT),
    )
    .await
    .with_context(|| "timed out waiting for pre-compaction monitor requests")?;
    let requests = streaming_server.requests().await;
    assert_eq!(
        2 + PRE_COMPACTION_NOTIFICATION_COUNT,
        requests.len(),
        "each pre-compaction batch should wake one successor request"
    );
    for (index, request) in requests[2..].iter().enumerate() {
        let seq = index + 1;
        let monitor_notifications = monitor_notification_payloads(request)
            .with_context(|| format!("failed to decode pre-compaction batch {seq} request"))?;
        let actual_sequences = monitor_notification_sequences(
            &monitor_notifications,
            &format!("pre-compaction batch {seq}"),
        )?;
        assert_eq!(
            (1..=seq)
                .map(|sequence| sequence as u64)
                .collect::<Vec<_>>(),
            actual_sequences,
            "pre-compaction batch {seq} should retain the monitor history frontier"
        );
        let expected_lines = Value::Array(
            (0..LINES_PER_NOTIFICATION)
                .map(|index| Value::String(format!("monitor-before-{seq}-{index}")))
                .collect(),
        );
        let newest_monitor_notification = monitor_notifications
            .last()
            .with_context(|| format!("pre-compaction batch {seq} monitor payloads are empty"))?;
        assert_eq!(
            Some(&expected_lines),
            newest_monitor_notification.get("lines"),
            "pre-compaction batch {seq} should contain the expected monitor lines"
        );
    }

    test.codex.submit(Op::Compact).await?;
    timeout(
        Duration::from_secs(10),
        streaming_server.wait_for_request_count(3 + PRE_COMPACTION_NOTIFICATION_COUNT),
    )
    .await
    .with_context(|| "timed out waiting for the active compaction request")?;
    test.fs()
        .write_file(
            &during_compaction_gate,
            b"ready".to_vec(),
            /*sandbox*/ None,
        )
        .await?;
    timeout(Duration::from_secs(30), &mut monitor_draft_admitted)
        .await
        .with_context(|| "timed out waiting for monitor draft admission during compaction")?;
    compact_release_tx
        .send(())
        .map_err(|_| anyhow::anyhow!("compaction response was dropped before release"))?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::ContextCompacted(_))
    })
    .await;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    completion_receivers
        .remove(0)
        .await
        .with_context(|| "local compaction response stage did not complete")?;

    timeout(
        Duration::from_secs(10),
        streaming_server.wait_for_request_count(4 + PRE_COMPACTION_NOTIFICATION_COUNT),
    )
    .await
    .with_context(|| "timed out waiting for the post-compaction monitor request")?;
    let requests = streaming_server.requests().await;
    assert_eq!(4 + PRE_COMPACTION_NOTIFICATION_COUNT, requests.len());
    let decoded_payloads = requests
        .iter()
        .map(|request| monitor_notification_payloads(request))
        .collect::<Result<Vec<_>>>()
        .context("failed to decode post-compaction request bodies")?;
    let expected_after_lines = Value::Array(
        (0..LINES_PER_NOTIFICATION)
            .map(|index| Value::String(format!("monitor-after-{index}")))
            .collect(),
    );
    let post_compaction_requests = requests
        .iter()
        .zip(&decoded_payloads)
        .filter(|(_, payloads)| {
            payloads
                .iter()
                .any(|payload| payload.get("lines") == Some(&expected_after_lines))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        1,
        post_compaction_requests.len(),
        "the post-compaction monitor batch should wake exactly one successor"
    );
    let (_, post_compaction_payloads) = post_compaction_requests[0];
    assert_eq!(1, post_compaction_payloads.len());
    let actual_sequences = monitor_notification_sequences(
        post_compaction_payloads,
        "post-compaction monitor request",
    )?;
    assert_eq!(
        vec![21_u64],
        actual_sequences,
        "post-compaction request should contain only the successor monitor notification"
    );
    assert_eq!(
        Some(&expected_after_lines),
        post_compaction_payloads[0].get("lines"),
        "post-compaction monitor notification should contain the expected lines"
    );

    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    completion_receivers
        .remove(0)
        .await
        .with_context(|| "post-compaction response stage did not complete")?;
    assert!(completion_receivers.is_empty());
    test.codex.shutdown_and_wait().await?;
    assert_eq!(
        4 + PRE_COMPACTION_NOTIFICATION_COUNT,
        streaming_server.requests().await.len()
    );
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
