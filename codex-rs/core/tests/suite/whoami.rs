use anyhow::Result;
use codex_protocol::config_types::ModeKind;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::Op;
use codex_protocol::user_input::UserInput;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_response_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::sse_response;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::local_selections;
use core_test_support::test_codex::test_codex;
use core_test_support::test_codex::turn_permission_fields;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use serde_json::Value;

const REQUESTED_MODEL: &str = "gpt-5.4";
const SERVER_MODEL: &str = "gpt-5.2";

fn user_turn(test: &TestCodex) -> Op {
    let (sandbox_policy, permission_profile) =
        turn_permission_fields(PermissionProfile::Disabled, test.cwd_path());
    Op::UserInput {
        items: vec![UserInput::Text {
            text: "identify the model serving this response".to_string(),
            text_elements: Vec::new(),
        }],
        final_output_json_schema: None,
        responsesapi_client_metadata: None,
        additional_context: Default::default(),
        thread_settings: codex_protocol::protocol::ThreadSettingsOverrides {
            environments: Some(local_selections(test.config.cwd.clone())),
            approval_policy: Some(AskForApproval::Never),
            sandbox_policy: Some(sandbox_policy),
            permission_profile,
            collaboration_mode: Some(codex_protocol::config_types::CollaborationMode {
                mode: ModeKind::Default,
                settings: codex_protocol::config_types::Settings {
                    model: test.session_configured.model.clone(),
                    reasoning_effort: test.config.model_reasoning_effort.clone(),
                    developer_instructions: None,
                },
            }),
            ..Default::default()
        },
    }
}

fn whoami_response(response_id: &str, call_id: &str) -> wiremock::ResponseTemplate {
    sse_response(sse(vec![
        ev_response_created(response_id),
        ev_function_call(call_id, "whoami", "{}"),
        ev_completed(response_id),
    ]))
}

async fn submit_and_read_call(
    test: &TestCodex,
    response_mock: &core_test_support::responses::ResponseMock,
    call_id: &str,
) -> Result<Value> {
    test.codex.submit(user_turn(test)).await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, codex_protocol::protocol::EventMsg::TurnComplete(_))
    })
    .await;

    let output = response_mock
        .requests()
        .iter()
        .find_map(|request| request.function_call_output_text(call_id))
        .expect("whoami function-call output should be sent to the model");
    Ok(serde_json::from_str(&output)?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn whoami_reports_server_routed_model_and_request_provenance() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let first = whoami_response("response-1", "whoami-call-1")
        .insert_header("OpenAI-Model", SERVER_MODEL);
    let second = sse_response(sse(vec![
        ev_assistant_message("message-1", "identity recorded"),
        ev_completed("response-2"),
    ]));
    let response_mock = mount_response_sequence(&server, vec![first, second]).await;

    let mut builder = test_codex().with_model(REQUESTED_MODEL);
    let test = builder.build(&server).await?;
    let output = submit_and_read_call(&test, &response_mock, "whoami-call-1").await?;
    let request = response_mock
        .requests()
        .first()
        .expect("initial sampling request should be captured");
    let request_effort = request.body_json()["reasoning"]["effort"].clone();

    assert_eq!(output["slug"], SERVER_MODEL);
    assert_eq!(output["requested_model"], REQUESTED_MODEL);
    assert_eq!(output["server_reported_model"], SERVER_MODEL);
    assert_eq!(output["model_identity_provenance"], "server_reported");
    assert_eq!(output["model_identity_verified"], true);
    assert_eq!(output["display_name_provenance"], "model_catalog_configuration");
    assert_ne!(output["slug"], output["requested_model"]);
    assert_eq!(output["display_name"], "GPT-5.2");
    assert_ne!(output["display_name"], output["requested_display_name"]);
    assert!(
        request_effort.is_string(),
        "request should apply the model default effort"
    );
    assert_eq!(output["reasoning_effort"], request_effort);
    assert_eq!(output["reasoning_effort_provenance"], "request_metadata");
    assert!(output["context_window"].is_i64());
    assert_eq!(output["context_window_provenance"], "model_catalog_configuration");
    assert_eq!(request.body_json()["model"], REQUESTED_MODEL);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn whoami_does_not_reuse_server_identity_across_sampling_steps() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let first = whoami_response("response-1", "whoami-call-1")
        .insert_header("OpenAI-Model", SERVER_MODEL);
    let second = whoami_response("response-2", "whoami-call-2");
    let third = sse_response(sse(vec![
        ev_assistant_message("message-1", "identity recorded"),
        ev_completed("response-3"),
    ]));
    let response_mock = mount_response_sequence(&server, vec![first, second, third]).await;

    let mut builder = test_codex().with_model(REQUESTED_MODEL);
    let test = builder.build(&server).await?;
    let output = submit_and_read_call(&test, &response_mock, "whoami-call-2").await?;

    assert_eq!(output["slug"], REQUESTED_MODEL);
    assert_eq!(output["requested_model"], REQUESTED_MODEL);
    assert_eq!(output["server_reported_model"], Value::Null);
    assert_eq!(output["model_identity_provenance"], "request_metadata_unverified");
    assert_eq!(output["model_identity_verified"], false);

    Ok(())
}
