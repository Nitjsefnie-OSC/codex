use super::*;

#[test_case(
    Some(REQUESTED_MODEL),
    Some(REQUESTED_REASONING_EFFORT),
    REQUESTED_MODEL,
    Some(REQUESTED_REASONING_EFFORT);
    "both explicit"
)]
#[test_case(
    Some(REQUESTED_MODEL),
    None,
    REQUESTED_MODEL,
    Some(ROLE_REASONING_EFFORT);
    "model only"
)]
#[test_case(
    None,
    Some(REQUESTED_REASONING_EFFORT),
    OVERRIDABLE_ROLE_MODEL,
    Some(REQUESTED_REASONING_EFFORT);
    "reasoning effort only"
)]
#[test_case(
    None,
    None,
    OVERRIDABLE_ROLE_MODEL,
    Some(ROLE_REASONING_EFFORT);
    "role replaces configured defaults"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_agent_explicit_identity_fields_override_role_defaults(
    requested_model: Option<&str>,
    requested_reasoning_effort: Option<ReasoningEffort>,
    expected_model: &str,
    expected_reasoning_effort: Option<ReasoningEffort>,
) -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mut spawn_args = json!({
        "message": CHILD_PROMPT,
        "agent_type": "custom",
    });
    if let Some(requested_model) = requested_model {
        spawn_args["model"] = json!(requested_model);
    }
    if let Some(requested_reasoning_effort) = requested_reasoning_effort {
        spawn_args["reasoning_effort"] = json!(requested_reasoning_effort);
    }
    let child_snapshot = spawn_child_and_capture_snapshot(
        &server,
        spawn_args,
        |builder| {
            builder.with_config(|config| {
                let role_path = config.codex_home.join("custom-role.toml");
                std::fs::write(
                    &role_path,
                    format!(
                        "model = \"{OVERRIDABLE_ROLE_MODEL}\"\nmodel_reasoning_effort = \"{ROLE_REASONING_EFFORT}\"\n",
                    ),
                )
                .expect("write role config");
                config.agent_roles.insert(
                    "custom".to_string(),
                    AgentRoleConfig {
                        description: Some("Custom role".to_string()),
                        config_file: Some(role_path.to_path_buf()),
                        nickname_candidates: None,
                    },
                );
                config.agent_default_subagent_model =
                    Some("missing-configured-model".to_string());
                config.agent_default_subagent_reasoning_effort = Some(ReasoningEffort::Minimal);
            })
        },
    )
    .await?;

    assert_eq!(
        (child_snapshot.model, child_snapshot.reasoning_effort),
        (expected_model.to_string(), expected_reasoning_effort)
    );

    Ok(())
}

#[test_case(
    Some(V2_REQUESTED_MODEL),
    Some(V2_REQUESTED_REASONING_EFFORT),
    V2_REQUESTED_MODEL,
    Some(V2_REQUESTED_REASONING_EFFORT);
    "both explicit"
)]
#[test_case(
    Some(V2_REQUESTED_MODEL),
    None,
    V2_REQUESTED_MODEL,
    Some(ROLE_REASONING_EFFORT);
    "model only"
)]
#[test_case(
    None,
    Some(V2_REQUESTED_REASONING_EFFORT),
    V2_ROLE_MODEL,
    Some(V2_REQUESTED_REASONING_EFFORT);
    "reasoning effort only"
)]
#[test_case(
    None,
    None,
    V2_ROLE_MODEL,
    Some(ROLE_REASONING_EFFORT);
    "role replaces configured defaults"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_agent_v2_spawn_agent_explicit_identity_fields_override_role_defaults(
    requested_model: Option<&str>,
    requested_reasoning_effort: Option<ReasoningEffort>,
    expected_model: &str,
    expected_reasoning_effort: Option<ReasoningEffort>,
) -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let mut spawn_args = json!({
        "message": CHILD_PROMPT,
        "agent_type": "custom",
        "task_name": "v2_role_override",
        "fork_turns": "none",
    });
    if let Some(requested_model) = requested_model {
        spawn_args["model"] = json!(requested_model);
    }
    if let Some(requested_reasoning_effort) = requested_reasoning_effort {
        spawn_args["reasoning_effort"] = json!(requested_reasoning_effort);
    }
    let child_snapshot = spawn_child_and_capture_snapshot_with_version(
        &server,
        MultiAgentVersion::V2,
        spawn_args,
        Some(expected_model),
        expected_reasoning_effort.clone(),
        |builder| {
            builder.with_config(|config| {
                let role_path = config.codex_home.join("custom-v2-role.toml");
                std::fs::write(
                    &role_path,
                    format!(
                        "model = \"{V2_ROLE_MODEL}\"\nmodel_reasoning_effort = \"{ROLE_REASONING_EFFORT}\"\n",
                    ),
                )
                .expect("write V2 role config");
                config.agent_roles.insert(
                    "custom".to_string(),
                    AgentRoleConfig {
                        description: Some("Custom V2 role".to_string()),
                        config_file: Some(role_path.to_path_buf()),
                        nickname_candidates: None,
                    },
                );
                config.agent_default_subagent_model =
                    Some("missing-configured-v2-model".to_string());
                config.agent_default_subagent_reasoning_effort = Some(ReasoningEffort::Minimal);
            })
        },
    )
    .await?;

    assert_eq!(
        (child_snapshot.model, child_snapshot.reasoning_effort),
        (expected_model.to_string(), expected_reasoning_effort)
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_agent_v2_spawn_agent_rejects_invalid_final_model_effort_pair() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let spawn_args = serde_json::to_string(&json!({
        "message": CHILD_PROMPT,
        "agent_type": "custom",
        "task_name": "v2-invalid-pair",
        "fork_turns": "none",
        "reasoning_effort": ReasoningEffort::Minimal,
    }))?;
    mount_sse_once_match(
        &server,
        |request: &wiremock::Request| body_contains(request, TURN_1_PROMPT),
        sse(vec![
            ev_response_created("resp-v2-turn1-1"),
            ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                MULTI_AGENT_V2_NAMESPACE,
                "spawn_agent",
                &spawn_args,
            ),
            ev_completed("resp-v2-turn1-1"),
        ]),
    )
    .await;
    let tool_output = mount_sse_once_match(
        &server,
        |request: &wiremock::Request| body_contains(request, SPAWN_CALL_ID),
        sse(vec![
            ev_response_created("resp-v2-turn1-2"),
            ev_completed("resp-v2-turn1-2"),
        ]),
    )
    .await;

    let test = test_codex()
        .with_config(move |config| {
            config
                .features
                .enable(Feature::Collab)
                .expect("test config should allow feature update");
            config
                .features
                .enable(Feature::MultiAgentV2)
                .expect("test config should allow feature update");
            config.model = Some(V2_DEFAULT_MODEL.to_string());
            config.model_reasoning_effort = Some(V2_DEFAULT_REASONING_EFFORT);
            let role_path = config.codex_home.join("invalid-v2-role.toml");
            std::fs::write(
                &role_path,
                format!(
                    "model = \"{V2_ROLE_MODEL}\"\nmodel_reasoning_effort = \"{ROLE_REASONING_EFFORT}\"\n",
                ),
            )
            .expect("write invalid V2 role config");
            config.agent_roles.insert(
                "custom".to_string(),
                AgentRoleConfig {
                    description: Some("Custom V2 role".to_string()),
                    config_file: Some(role_path.to_path_buf()),
                    nickname_candidates: None,
                },
            );
        })
        .build_with_auto_env(&server)
        .await?;

    test.submit_turn(TURN_1_PROMPT).await?;

    let (output, _) = tool_output
        .single_request()
        .function_call_output_content_and_success(SPAWN_CALL_ID)
        .expect("spawn_agent output");
    assert!(output.as_deref().is_some_and(|output| {
        output.contains("Reasoning effort `minimal` is not supported for model `gpt-5.6-luna`")
    }));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_agent_tool_description_mentions_overridable_role_defaults() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let call_id = "tool-search-spawn-agent";
    let resp_mock = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-turn1-1"),
                ev_tool_search_call(
                    call_id,
                    &json!({
                        "query": "spawn agent custom role",
                        "limit": 1,
                    }),
                ),
                ev_completed("resp-turn1-1"),
            ]),
            sse(vec![
                ev_response_created("resp-turn1-2"),
                ev_assistant_message("msg-turn1-2", "done"),
                ev_completed("resp-turn1-2"),
            ]),
        ],
    )
    .await;

    let builder = test_codex().with_config(|config| {
        config
            .features
            .enable(Feature::Collab)
            .expect("test config should allow feature update");
        config.multi_agent_v2.hide_spawn_agent_metadata = false;
        let role_path = config.codex_home.join("custom-role.toml");
        std::fs::write(
            &role_path,
            format!(
                "developer_instructions = \"Stay focused\"\nmodel = \"{ROLE_MODEL}\"\nmodel_reasoning_effort = \"{ROLE_REASONING_EFFORT}\"\n",
            ),
        )
        .expect("write role config");
        config.agent_roles.insert(
            "custom".to_string(),
            AgentRoleConfig {
                description: Some("Custom role".to_string()),
                config_file: Some(role_path.to_path_buf()),
                nickname_candidates: None,
            },
        );
    });
    let test = builder.build(&server).await?;

    test.submit_turn(TURN_1_PROMPT).await?;

    let requests = resp_mock.requests();
    assert_eq!(requests.len(), 2);
    let output = requests[1].tool_search_output(call_id);
    let spawn_agent = namespace_child_tool(&output, "multi_agent_v1", "spawn_agent")
        .expect("tool_search should return multi_agent_v1.spawn_agent");
    let agent_type_description = tool_parameter_description(spawn_agent, "agent_type")
        .expect("spawn_agent agent_type description");
    let custom_role_description =
        role_block(&agent_type_description, "custom").expect("custom role description");
    assert_eq!(
        custom_role_description,
        "custom: {\nCustom role\n- This role's model defaults to `gpt-5.4` and its reasoning effort defaults to `high`. Explicit `model` and `reasoning_effort` spawn arguments override these defaults.\n}"
    );
    assert!(
        tool_parameter_description(spawn_agent, "model")
            .expect("spawn_agent model description")
            .contains("selected role")
    );
    assert!(
        tool_parameter_description(spawn_agent, "reasoning_effort")
            .expect("spawn_agent reasoning effort description")
            .contains("selected model default")
    );

    Ok(())
}

async fn direct_v2_spawn_agent_tool(expose_model_overrides: bool) -> Result<Value> {
    let server = start_mock_server().await;
    let response = mount_sse_once(
        &server,
        sse(vec![
            ev_response_created("resp-hidden"),
            ev_completed("resp-hidden"),
        ]),
    )
    .await;
    let test = test_codex()
        .with_config(move |config| {
            config
                .features
                .enable(Feature::Collab)
                .expect("test config should allow feature update");
            config
                .features
                .enable(Feature::MultiAgentV2)
                .expect("test config should allow feature update");
            config.multi_agent_v2.expose_spawn_agent_model_overrides = expose_model_overrides;
            let role_path = config.codex_home.join("hidden-role.toml");
            std::fs::write(
                &role_path,
                "model = \"gpt-5.4\"\nmodel_reasoning_effort = \"high\"\n",
            )
            .expect("write hidden role config");
            config.agent_roles.insert(
                "custom".to_string(),
                AgentRoleConfig {
                    description: Some("Custom role".to_string()),
                    config_file: Some(role_path),
                    nickname_candidates: None,
                },
            );
        })
        .build_with_auto_env(&server)
        .await?;

    test.submit_turn("show the collaboration tools").await?;
    Ok(response.single_request().body_json())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_agent_v2_model_controls_describe_role_override_precedence() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let body = direct_v2_spawn_agent_tool(true).await?;
    let tool = namespace_child_tool(&body, MULTI_AGENT_V2_NAMESPACE, "spawn_agent")
        .expect("direct V2 spawn_agent tool should be present");
    let description = tool_parameter_description(tool, "agent_type")
        .expect("V2 role description should be present");
    let custom_role_description =
        role_block(&description, "custom").expect("custom role description should be present");
    assert!(custom_role_description.contains("Explicit `model` and `reasoning_effort`"));
    assert!(
        tool_parameter_description(tool, "model")
            .expect("V2 model description should be present")
            .contains("selected role")
    );
    assert!(
        tool_parameter_description(tool, "reasoning_effort")
            .expect("V2 reasoning effort description should be present")
            .contains("selected model default")
    );
    assert!(tool.pointer("/parameters/properties/model").is_some());
    assert!(
        tool.pointer("/parameters/properties/reasoning_effort")
            .is_some()
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_agent_v2_hidden_model_controls_omit_role_override_guidance() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let body = direct_v2_spawn_agent_tool(false).await?;
    let tool = namespace_child_tool(&body, MULTI_AGENT_V2_NAMESPACE, "spawn_agent")
        .expect("direct V2 spawn_agent tool should be present");
    let description = tool_parameter_description(tool, "agent_type")
        .expect("hidden V2 role description should be present");
    let custom_role_description =
        role_block(&description, "custom").expect("custom role description should be present");
    assert!(custom_role_description.contains(
        "custom: {\nCustom role\n- This role's model defaults to `gpt-5.4` and its reasoning effort defaults to `high`.\n}"
    ));
    assert!(!custom_role_description.contains("Explicit `model`"));
    assert!(tool.pointer("/parameters/properties/model").is_none());
    assert!(
        tool.pointer("/parameters/properties/reasoning_effort")
            .is_none()
    );

    Ok(())
}
