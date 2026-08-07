#![cfg(unix)]

use anyhow::Result;
use codex_protocol::protocol::EventMsg;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use serde_json::json;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn monitor_output_wakes_an_idle_session_without_user_prompt() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let monitor_arguments = serde_json::to_string(&json!({
        "command": [
            "/bin/sh",
            "-c",
            "sleep 1; printf 'idle-monitor-output\\n'; sleep 30",
        ],
        "kind": "watcher",
    }))?;
    let responses = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("monitor-start"),
                ev_function_call("monitor-call", "monitor", &monitor_arguments),
                ev_completed("monitor-start"),
            ]),
            sse(vec![
                ev_response_created("monitor-start-followup"),
                ev_assistant_message("monitor-started", "monitor started"),
                ev_completed("monitor-start-followup"),
            ]),
            sse(vec![
                ev_response_created("monitor-idle-wake"),
                ev_assistant_message("monitor-observed", "monitor output observed"),
                ev_completed("monitor-idle-wake"),
            ]),
        ],
    )
    .await;
    let test = test_codex().build_with_auto_env(&server).await?;

    test.submit_turn("start a monitor and wait for its output").await?;

    let idle_request = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let requests = responses.requests();
            if requests.len() >= 3 {
                return requests[2].clone();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await?;

    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;

    let notification = idle_request
        .message_input_texts("developer")
        .into_iter()
        .find(|text| text.contains("<monitor_notification>"))
        .expect("the idle wake request should contain a monitor notification");
    assert!(
        notification.contains("idle-monitor-output"),
        "the idle wake request should contain monitor output: {notification}"
    );
    assert_eq!(3, responses.requests().len());
    test.codex.shutdown_and_wait().await?;
    assert_eq!(3, responses.requests().len());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn monitor_output_during_a_turn_is_injected_without_a_duplicate_turn() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let monitor_arguments = serde_json::to_string(&json!({
        "command": [
            "/bin/sh",
            "-c",
            "i=0; while [ \"$i\" -lt 40 ]; do printf 'active-monitor-output\\n'; i=$((i + 1)); done; sleep 30",
        ],
        "kind": "watcher",
    }))?;
    let responses = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("active-monitor-start"),
                ev_function_call("active-monitor-call", "monitor", &monitor_arguments),
                ev_completed("active-monitor-start"),
            ]),
            sse(vec![
                ev_response_created("active-monitor-followup"),
                ev_assistant_message("active-monitor-done", "monitor output observed"),
                ev_completed("active-monitor-followup"),
            ]),
        ],
    )
    .await;
    let test = test_codex().build_with_auto_env(&server).await?;

    test.submit_turn("start a monitor with active output").await?;

    let requests = responses.requests();
    assert_eq!(2, requests.len());
    let notification = requests[1]
        .message_input_texts("developer")
        .into_iter()
        .find(|text| text.contains("<monitor_notification>"))
        .expect("the active turn should receive a monitor notification");
    assert!(
        notification.contains("active-monitor-output"),
        "the active turn should receive monitor output: {notification}"
    );

    test.codex.shutdown_and_wait().await?;
    assert_eq!(2, responses.requests().len());

    Ok(())
}
