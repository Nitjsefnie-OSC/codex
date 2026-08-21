#![cfg(not(target_os = "windows"))]
#![allow(clippy::unwrap_used)]

use core_test_support::responses;
use core_test_support::test_codex_exec::test_codex_exec;
use serde_json::json;

const ROLE_MODEL: &str = "role-model";
const ROLE_DEVELOPER_MARKER: &str = "ROLE-DEVELOPER-MARKER";
const ROLE_EFFORT: &str = "low";
const CLI_MODEL: &str = "cli-model";
const CLI_EFFORT: &str = "high";

fn install_adversary_role(home: &std::path::Path) -> anyhow::Result<()> {
    let agents_dir = home.join("agents");
    std::fs::create_dir_all(&agents_dir)?;
    std::fs::write(
        agents_dir.join("adversary.toml"),
        format!(
            r#"name = "adversary"
description = "Adversarial reviewer"
model = "{ROLE_MODEL}"
model_reasoning_effort = "{ROLE_EFFORT}"
developer_instructions = "{ROLE_DEVELOPER_MARKER}"
"#,
        ),
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_agent_role_preserves_explicit_model_and_effort_overrides() -> anyhow::Result<()> {
    let test = test_codex_exec();
    install_adversary_role(test.home_path())?;
    let server = responses::start_mock_server().await;
    let body = responses::sse(vec![
        responses::ev_response_created("response_1"),
        responses::ev_assistant_message("message_1", "done"),
        responses::ev_completed("response_1"),
    ]);
    let response_mock = responses::mount_sse_once(&server, body).await;

    let output = test
        .cmd_with_server(&server)
        .arg("--agent")
        .arg("adversary")
        .arg("--model")
        .arg(CLI_MODEL)
        .arg("-c")
        .arg(format!("model_reasoning_effort=\"{CLI_EFFORT}\""))
        .arg("--dangerously-bypass-approvals-and-sandbox")
        .arg("--skip-git-repo-check")
        .arg("Review this claim")
        .output()?;

    assert!(output.status.success(), "exec run failed: {output:?}");
    let request = response_mock.single_request();
    assert_eq!(request.body_json()["model"], json!(CLI_MODEL));
    assert_eq!(
        request.body_json()["reasoning"]["effort"],
        json!(CLI_EFFORT)
    );
    assert!(
        request.body_contains_text(ROLE_DEVELOPER_MARKER),
        "request body did not contain the role developer marker: {}",
        request.body_json()
    );
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("approval: never"),
        "unexpected stderr: {stderr}"
    );
    assert!(
        stderr.contains("sandbox: danger-full-access"),
        "unexpected stderr: {stderr}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_agent_role_preserves_config_model_override_and_role_effort() -> anyhow::Result<()> {
    let test = test_codex_exec();
    install_adversary_role(test.home_path())?;
    let server = responses::start_mock_server().await;
    let body = responses::sse(vec![
        responses::ev_response_created("response_1"),
        responses::ev_assistant_message("message_1", "done"),
        responses::ev_completed("response_1"),
    ]);
    let response_mock = responses::mount_sse_once(&server, body).await;

    let output = test
        .cmd_with_server(&server)
        .arg("--agent")
        .arg("adversary")
        .arg("-c")
        .arg(format!("model=\"{CLI_MODEL}\""))
        .arg("--skip-git-repo-check")
        .arg("Review this claim")
        .output()?;

    assert!(output.status.success(), "exec run failed: {output:?}");
    let request = response_mock.single_request();
    assert_eq!(request.body_json()["model"], json!(CLI_MODEL));
    assert_eq!(
        request.body_json()["reasoning"]["effort"],
        json!(ROLE_EFFORT)
    );
    assert!(request.body_contains_text(ROLE_DEVELOPER_MARKER));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_unknown_agent_role_fails_before_responses_request() -> anyhow::Result<()> {
    let test = test_codex_exec();
    let server = responses::start_mock_server().await;

    let output = test
        .cmd_with_server(&server)
        .arg("--agent")
        .arg("missing-role")
        .arg("--skip-git-repo-check")
        .arg("Review this claim")
        .output()?;

    assert!(
        !output.status.success(),
        "unknown role unexpectedly succeeded"
    );
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("unknown agent_type 'missing-role'"),
        "unexpected stderr: {stderr}"
    );
    let requests = server.received_requests().await.unwrap_or_default();
    assert!(requests.is_empty(), "unexpected requests: {requests:?}");

    Ok(())
}
