use super::*;
use crate::session::tests::make_session_and_context_with_rx;
use codex_protocol::protocol::MultiAgentVersion;

async fn install_uncatalogued_model_only_role(turn: &mut TurnContext) -> String {
    let role_name = "uncatalogued-model-only-role".to_string();
    let role_path = turn
        .config
        .codex_home
        .as_path()
        .join("uncatalogued-model-only-role.toml");
    tokio::fs::write(
        &role_path,
        "model = \"custom-provider/uncatalogued-role-model\"\nmodel_provider = \"ollama\"\n",
    )
    .await
    .expect("role config should be written");
    let mut config = (*turn.config).clone();
    config.agent_roles.insert(
        role_name.clone(),
        AgentRoleConfig {
            description: Some("Uncatalogued model-only role".to_string()),
            config_file: Some(role_path),
            nickname_candidates: None,
        },
    );
    set_turn_config(turn, config);
    role_name
}

async fn spawn_v1_agent_with_role_identity(
    model: Option<&str>,
    reasoning_effort: Option<ReasoningEffort>,
) -> Result<crate::codex_thread::ThreadConfigSnapshot, FunctionCallError> {
    spawn_v1_agent_with_role_identity_and_configured_defaults(
        model,
        reasoning_effort,
        /*configured_model*/ None,
        /*configured_reasoning_effort*/ None,
    )
    .await
}

async fn spawn_v1_agent_with_role_identity_and_configured_defaults(
    model: Option<&str>,
    reasoning_effort: Option<ReasoningEffort>,
    configured_model: Option<&str>,
    configured_reasoning_effort: Option<ReasoningEffort>,
) -> Result<crate::codex_thread::ThreadConfigSnapshot, FunctionCallError> {
    #[derive(Debug, Deserialize)]
    struct SpawnAgentResult {
        agent_id: String,
    }

    let (mut session, mut turn) = make_session_and_context().await;
    let mut config = (*turn.config).clone();
    config.agent_default_subagent_model = configured_model.map(str::to_string);
    config.agent_default_subagent_reasoning_effort = configured_reasoning_effort;
    set_turn_config(&mut turn, config);
    let role_name = install_role_with_model_override(&mut turn).await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();
    let mut args = json!({
        "message": "inspect this repo",
        "agent_type": role_name,
    });
    if let Some(model) = model {
        args["model"] = json!(model);
    }
    if let Some(reasoning_effort) = reasoning_effort {
        args["reasoning_effort"] = json!(reasoning_effort);
    }

    let output = SpawnAgentHandler::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(args),
        ))
        .await?;
    let (content, _) = expect_text_output(output);
    let result: SpawnAgentResult =
        serde_json::from_str(&content).expect("spawn_agent result should be json");
    Ok(manager
        .get_thread(parse_agent_id(&result.agent_id))
        .await
        .expect("spawned agent thread should exist")
        .config_snapshot()
        .await)
}

#[tokio::test]
async fn spawn_agent_explicit_model_and_effort_override_role_defaults() {
    let snapshot = spawn_v1_agent_with_role_identity(Some("gpt-5.4"), Some(ReasoningEffort::High))
        .await
        .expect("spawn_agent should honor explicit identity overrides");

    pretty_assertions::assert_eq!(
        (snapshot.model.as_str(), snapshot.reasoning_effort),
        ("gpt-5.4", Some(ReasoningEffort::High))
    );
}

#[tokio::test]
async fn spawn_agent_explicit_model_preserves_role_effort_default() {
    let snapshot = spawn_v1_agent_with_role_identity(Some("gpt-5.4"), None)
        .await
        .expect("spawn_agent should honor an explicit model");

    pretty_assertions::assert_eq!(
        (snapshot.model.as_str(), snapshot.reasoning_effort),
        ("gpt-5.4", Some(ReasoningEffort::Low))
    );
}

#[tokio::test]
async fn spawn_agent_explicit_effort_preserves_role_model_default() {
    let snapshot = spawn_v1_agent_with_role_identity(None, Some(ReasoningEffort::High))
        .await
        .expect("spawn_agent should honor an explicit reasoning effort");

    pretty_assertions::assert_eq!(
        (snapshot.model.as_str(), snapshot.reasoning_effort),
        ("gpt-5-role-override", Some(ReasoningEffort::High))
    );
}

#[tokio::test]
async fn spawn_agent_role_overrides_invalid_configured_identity_defaults() {
    let snapshot = spawn_v1_agent_with_role_identity_and_configured_defaults(
        /*model*/ None,
        /*reasoning_effort*/ None,
        Some("missing-configured-model"),
        Some(ReasoningEffort::Minimal),
    )
    .await
    .expect("role defaults should replace lower configured defaults before validation");

    pretty_assertions::assert_eq!(
        (snapshot.model.as_str(), snapshot.reasoning_effort),
        ("gpt-5-role-override", Some(ReasoningEffort::Low))
    );
}

#[tokio::test]
async fn spawn_agent_role_model_uses_selected_model_default_effort() {
    let (mut session, mut turn) = make_session_and_context().await;
    turn.reasoning_effort = Some(ReasoningEffort::High);
    let role_name = "model-only-role".to_string();
    let role_path = turn
        .config
        .codex_home
        .as_path()
        .join("model-only-role.toml");
    tokio::fs::write(&role_path, "model = \"gpt-5.6-sol\"\n")
        .await
        .expect("role config should be written");
    let mut config = (*turn.config).clone();
    config.agent_roles.insert(
        role_name.clone(),
        AgentRoleConfig {
            description: Some("Model-only role".to_string()),
            config_file: Some(role_path),
            nickname_candidates: None,
        },
    );
    set_turn_config(&mut turn, config);
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();

    let output = SpawnAgentHandler::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "agent_type": role_name,
            })),
        ))
        .await
        .expect("spawn should resolve the selected model default effort");
    let (content, _) = expect_text_output(output);
    let result: serde_json::Value = serde_json::from_str(&content).expect("spawn result json");
    let snapshot = manager
        .get_thread(parse_agent_id(
            result["agent_id"].as_str().expect("agent id"),
        ))
        .await
        .expect("spawned agent thread should exist")
        .config_snapshot()
        .await;

    pretty_assertions::assert_eq!(
        (snapshot.model, snapshot.reasoning_effort),
        ("gpt-5.6-sol".to_string(), Some(ReasoningEffort::Low))
    );
}

#[tokio::test]
async fn spawn_agent_uncatalogued_role_model_preserves_parent_effort() {
    let (mut session, mut turn) = make_session_and_context().await;
    turn.reasoning_effort = Some(ReasoningEffort::High);
    let role_name = install_uncatalogued_model_only_role(&mut turn).await;
    let manager = thread_manager();
    session.services.agent_control = manager.agent_control();

    let output = SpawnAgentHandler::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "agent_type": role_name,
            })),
        ))
        .await
        .expect("uncatalogued role models should use fallback metadata");
    let (content, _) = expect_text_output(output);
    let result: serde_json::Value = serde_json::from_str(&content).expect("spawn result json");
    let snapshot = manager
        .get_thread(parse_agent_id(
            result["agent_id"].as_str().expect("agent id"),
        ))
        .await
        .expect("spawned agent thread should exist")
        .config_snapshot()
        .await;

    pretty_assertions::assert_eq!(
        (snapshot.model, snapshot.reasoning_effort),
        (
            "custom-provider/uncatalogued-role-model".to_string(),
            Some(ReasoningEffort::High),
        )
    );
}

#[tokio::test]
async fn multi_agent_v2_role_model_uses_selected_model_default_effort() {
    let (mut session, mut turn) = make_session_and_context().await;
    turn.reasoning_effort = Some(ReasoningEffort::High);
    let role_name = "v2-model-only-role".to_string();
    let role_path = turn
        .config
        .codex_home
        .as_path()
        .join("v2-model-only-role.toml");
    tokio::fs::write(&role_path, "model = \"gpt-5.6-sol\"\n")
        .await
        .expect("role config should be written");
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    config.agent_roles.insert(
        role_name.clone(),
        AgentRoleConfig {
            description: Some("V2 model-only role".to_string()),
            config_file: Some(role_path),
            nickname_candidates: None,
        },
    );
    set_turn_config(&mut turn, config);
    let manager = thread_manager();
    let root = manager
        .start_thread(StartThreadOptions::new((*turn.config).clone()))
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.thread_id = root.thread_id;

    SpawnAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "v2_model_only",
                "agent_type": role_name,
                "fork_turns": "none",
            })),
        ))
        .await
        .expect("spawn should resolve the selected model default effort");
    let child_id = manager
        .captured_ops()
        .into_iter()
        .map(|(thread_id, _)| thread_id)
        .find(|thread_id| *thread_id != root.thread_id)
        .expect("spawned agent should receive an op");
    let snapshot = manager
        .get_thread(child_id)
        .await
        .expect("spawned agent thread should exist")
        .config_snapshot()
        .await;

    pretty_assertions::assert_eq!(
        (snapshot.model, snapshot.reasoning_effort),
        ("gpt-5.6-sol".to_string(), Some(ReasoningEffort::Low))
    );
}

#[tokio::test]
async fn multi_agent_v2_uncatalogued_role_model_preserves_parent_effort() {
    let (mut session, mut turn) = make_session_and_context().await;
    turn.reasoning_effort = Some(ReasoningEffort::High);
    let role_name = install_uncatalogued_model_only_role(&mut turn).await;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    set_turn_config(&mut turn, config);
    let manager = thread_manager();
    let root = manager
        .start_thread(StartThreadOptions::new((*turn.config).clone()))
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.thread_id = root.thread_id;

    SpawnAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "uncatalogued_role_model",
                "agent_type": role_name,
                "fork_turns": "none",
            })),
        ))
        .await
        .expect("uncatalogued role models should use fallback metadata");
    let child_id = manager
        .captured_ops()
        .into_iter()
        .map(|(thread_id, _)| thread_id)
        .find(|thread_id| *thread_id != root.thread_id)
        .expect("spawned agent should receive an op");
    let snapshot = manager
        .get_thread(child_id)
        .await
        .expect("spawned agent thread should exist")
        .config_snapshot()
        .await;

    pretty_assertions::assert_eq!(
        (snapshot.model, snapshot.reasoning_effort),
        (
            "custom-provider/uncatalogued-role-model".to_string(),
            Some(ReasoningEffort::High),
        )
    );
}

#[tokio::test]
async fn spawn_agent_rejects_invalid_final_model_effort_pair() {
    let error = spawn_v1_agent_with_role_identity(Some("gpt-5.4"), Some(ReasoningEffort::Minimal))
        .await
        .expect_err("the final model and effort should be validated together");

    pretty_assertions::assert_eq!(
        error,
        FunctionCallError::RespondToModel(
            "Reasoning effort `minimal` is not supported for model `gpt-5.4`. Supported reasoning efforts: low, medium, high, xhigh".to_string()
        )
    );
}

async fn spawn_v2_agent_with_role_identity(
    model: Option<&str>,
    reasoning_effort: Option<ReasoningEffort>,
) -> Result<crate::codex_thread::ThreadConfigSnapshot, FunctionCallError> {
    spawn_v2_agent_with_role_identity_and_configured_defaults(
        model,
        reasoning_effort,
        /*configured_model*/ None,
        /*configured_reasoning_effort*/ None,
    )
    .await
}

async fn spawn_v2_agent_with_role_identity_and_configured_defaults(
    model: Option<&str>,
    reasoning_effort: Option<ReasoningEffort>,
    configured_model: Option<&str>,
    configured_reasoning_effort: Option<ReasoningEffort>,
) -> Result<crate::codex_thread::ThreadConfigSnapshot, FunctionCallError> {
    let (mut session, mut turn) = make_session_and_context().await;
    let mut configured = (*turn.config).clone();
    configured.agent_default_subagent_model = configured_model.map(str::to_string);
    configured.agent_default_subagent_reasoning_effort = configured_reasoning_effort;
    set_turn_config(&mut turn, configured);
    let role_name = install_role_with_model_override(&mut turn).await;
    let manager = thread_manager();
    let root = manager
        .start_thread(StartThreadOptions::new((*turn.config).clone()))
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.thread_id = root.thread_id;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    let turn = TurnContext {
        config: Arc::new(config),
        multi_agent_version: MultiAgentVersion::V2,
        ..turn
    };
    let mut args = json!({
        "message": "inspect this repo",
        "task_name": "explicit_identity",
        "agent_type": role_name,
        "fork_turns": "none",
    });
    if let Some(model) = model {
        args["model"] = json!(model);
    }
    if let Some(reasoning_effort) = reasoning_effort {
        args["reasoning_effort"] = json!(reasoning_effort);
    }

    SpawnAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(args),
        ))
        .await?;
    let agent_id = manager
        .captured_ops()
        .into_iter()
        .map(|(thread_id, _)| thread_id)
        .find(|thread_id| *thread_id != root.thread_id)
        .expect("spawned agent should receive an op");
    Ok(manager
        .get_thread(agent_id)
        .await
        .expect("spawned agent thread should exist")
        .config_snapshot()
        .await)
}

#[tokio::test]
async fn multi_agent_v2_spawn_explicit_model_and_effort_override_role_defaults() {
    let snapshot = spawn_v2_agent_with_role_identity(Some("gpt-5.4"), Some(ReasoningEffort::High))
        .await
        .expect("spawn_agent should honor explicit identity overrides");

    pretty_assertions::assert_eq!(
        (snapshot.model.as_str(), snapshot.reasoning_effort),
        ("gpt-5.4", Some(ReasoningEffort::High))
    );
}

#[tokio::test]
async fn multi_agent_v2_spawn_explicit_model_preserves_role_effort_default() {
    let snapshot = spawn_v2_agent_with_role_identity(Some("gpt-5.4"), None)
        .await
        .expect("spawn_agent should honor an explicit model");

    pretty_assertions::assert_eq!(
        (snapshot.model.as_str(), snapshot.reasoning_effort),
        ("gpt-5.4", Some(ReasoningEffort::Low))
    );
}

#[tokio::test]
async fn multi_agent_v2_spawn_explicit_effort_preserves_role_model_default() {
    let snapshot = spawn_v2_agent_with_role_identity(None, Some(ReasoningEffort::High))
        .await
        .expect("spawn_agent should honor an explicit reasoning effort");

    pretty_assertions::assert_eq!(
        (snapshot.model.as_str(), snapshot.reasoning_effort),
        ("gpt-5-role-override", Some(ReasoningEffort::High))
    );
}

#[tokio::test]
async fn multi_agent_v2_role_overrides_invalid_configured_identity_defaults() {
    let snapshot = spawn_v2_agent_with_role_identity_and_configured_defaults(
        /*model*/ None,
        /*reasoning_effort*/ None,
        Some("missing-configured-model"),
        Some(ReasoningEffort::Minimal),
    )
    .await
    .expect("role defaults should replace lower configured defaults before validation");

    pretty_assertions::assert_eq!(
        (snapshot.model.as_str(), snapshot.reasoning_effort),
        ("gpt-5-role-override", Some(ReasoningEffort::Low))
    );
}

#[tokio::test]
async fn multi_agent_v2_rejects_invalid_final_model_effort_pair() {
    let error = spawn_v2_agent_with_role_identity(Some("gpt-5.4"), Some(ReasoningEffort::Minimal))
        .await
        .expect_err("the final model and effort should be validated together");

    pretty_assertions::assert_eq!(
        error,
        FunctionCallError::RespondToModel(
            "Reasoning effort `minimal` is not supported for model `gpt-5.4`. Supported reasoning efforts: low, medium, high, xhigh".to_string()
        )
    );
}

async fn reject_v1_full_history_identity_overrides(args: serde_json::Value) -> FunctionCallError {
    let (session, turn) = make_session_and_context().await;
    match SpawnAgentHandler::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(args),
        ))
        .await
    {
        Ok(_) => panic!("full-history identity overrides should be rejected"),
        Err(error) => error,
    }
}

#[tokio::test]
async fn spawn_agent_full_history_rejects_identity_overrides() {
    let overrides = [
        json!({ "agent_type": "custom" }),
        json!({ "model": "gpt-5.4" }),
        json!({ "reasoning_effort": "high" }),
        json!({ "agent_type": "custom", "model": "gpt-5.4", "reasoning_effort": "high" }),
    ];
    for mut override_args in overrides {
        override_args["message"] = json!("inspect this repo");
        override_args["fork_context"] = json!(true);
        let error = reject_v1_full_history_identity_overrides(override_args).await;
        assert!(
            matches!(error, FunctionCallError::RespondToModel(message) if message.contains("Full-history forked agents inherit"))
        );
    }
}

#[tokio::test]
async fn spawn_agent_rejection_does_not_emit_in_progress_item() {
    let (session, turn, events) = make_session_and_context_with_rx().await;
    let error = match SpawnAgentHandler::default()
        .handle(invocation(
            session,
            turn,
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "model": "gpt-5.4",
                "fork_context": true,
            })),
        ))
        .await
    {
        Ok(_) => panic!("full-history identity overrides should be rejected"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        FunctionCallError::RespondToModel(message)
            if message.contains("Full-history forked agents inherit")
    ));
    assert!(events.try_recv().is_err());
}

async fn reject_v2_full_history_identity_overrides(args: serde_json::Value) -> FunctionCallError {
    let (session, mut turn) = make_session_and_context().await;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    set_turn_config(&mut turn, config);
    match SpawnAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(args),
        ))
        .await
    {
        Ok(_) => panic!("full-history identity overrides should be rejected"),
        Err(error) => error,
    }
}

#[tokio::test]
async fn multi_agent_v2_full_history_rejects_identity_overrides() {
    let overrides = [
        json!({ "agent_type": "custom" }),
        json!({ "model": "gpt-5.4" }),
        json!({ "reasoning_effort": "high" }),
        json!({ "agent_type": "custom", "model": "gpt-5.4", "reasoning_effort": "high" }),
    ];
    for mut override_args in overrides {
        override_args["message"] = json!("inspect this repo");
        override_args["task_name"] = json!("full_history_identity");
        override_args["fork_turns"] = json!("all");
        let error = reject_v2_full_history_identity_overrides(override_args).await;
        assert!(
            matches!(error, FunctionCallError::RespondToModel(message) if message.contains("Full-history forked agents inherit"))
        );
    }
}

#[tokio::test]
async fn spawn_agent_full_history_inherits_parent_identity() {
    #[derive(Debug, Deserialize)]
    struct SpawnAgentResult {
        agent_id: String,
    }

    let (mut session, mut turn) = make_session_and_context().await;
    let mut config = (*turn.config).clone();
    config.agent_default_subagent_model = Some("gpt-5.6-luna".to_string());
    config.agent_default_subagent_reasoning_effort = Some(ReasoningEffort::Low);
    set_turn_config(&mut turn, config);
    let expected_identity = (
        turn.model_info.slug.clone(),
        turn.reasoning_effort
            .clone()
            .or_else(|| turn.model_info.default_reasoning_level.clone()),
    );
    let manager = thread_manager();
    let root = manager
        .start_thread(StartThreadOptions::new((*turn.config).clone()))
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.thread_id = root.thread_id;
    let output = SpawnAgentHandler::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "fork_context": true,
            })),
        ))
        .await
        .expect("full-history spawn should inherit parent identity");
    let (content, _) = expect_text_output(output);
    let result: SpawnAgentResult =
        serde_json::from_str(&content).expect("spawn result should be json");
    let snapshot = manager
        .get_thread(parse_agent_id(&result.agent_id))
        .await
        .expect("spawned agent thread should exist")
        .config_snapshot()
        .await;
    pretty_assertions::assert_eq!(
        (snapshot.model, snapshot.reasoning_effort),
        expected_identity
    );
}

#[tokio::test]
async fn spawn_agent_full_history_inherits_parent_role_metadata() {
    let (mut session, mut turn) = make_session_and_context().await;
    let role_name = install_role_with_model_override(&mut turn).await;
    turn.developer_instructions = Some("parent role instructions".to_string());
    let expected_identity = (
        turn.model_info.slug.clone(),
        turn.reasoning_effort
            .clone()
            .or_else(|| turn.model_info.default_reasoning_level.clone()),
    );
    let manager = thread_manager();
    let root = manager
        .start_thread(StartThreadOptions::new((*turn.config).clone()))
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.thread_id = root.thread_id;
    turn.session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: root.thread_id,
        depth: 0,
        agent_path: Some(AgentPath::root()),
        agent_nickname: None,
        agent_role: Some(role_name.clone()),
    });

    let output = SpawnAgentHandler::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "fork_context": true,
            })),
        ))
        .await
        .expect("spawn should inherit the parent role metadata");
    let (content, _) = expect_text_output(output);
    let result: serde_json::Value = serde_json::from_str(&content).expect("spawn result json");
    let snapshot = manager
        .get_thread(parse_agent_id(
            result["agent_id"].as_str().expect("agent id"),
        ))
        .await
        .expect("spawned agent thread should exist")
        .config_snapshot()
        .await;

    pretty_assertions::assert_eq!(
        (
            snapshot.session_source.get_agent_role(),
            snapshot.model,
            snapshot.reasoning_effort,
        ),
        (Some(role_name), expected_identity.0, expected_identity.1)
    );
    pretty_assertions::assert_eq!(
        manager
            .get_thread(parse_agent_id(
                result["agent_id"].as_str().expect("agent id"),
            ))
            .await
            .expect("spawned agent thread should exist")
            .session
            .new_default_turn()
            .await
            .developer_instructions
            .as_deref(),
        Some("parent role instructions")
    );
}

#[tokio::test]
async fn spawn_agent_fresh_does_not_inherit_parent_role_metadata() {
    let (mut session, mut turn) = make_session_and_context().await;
    let role_name = install_role_with_model_override(&mut turn).await;
    let manager = thread_manager();
    let root = manager
        .start_thread(StartThreadOptions::new((*turn.config).clone()))
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.thread_id = root.thread_id;
    turn.session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: root.thread_id,
        depth: 0,
        agent_path: Some(AgentPath::root()),
        agent_nickname: None,
        agent_role: Some(role_name),
    });

    let output = SpawnAgentHandler::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
            })),
        ))
        .await
        .expect("fresh spawn should succeed");
    let (content, _) = expect_text_output(output);
    let result: serde_json::Value = serde_json::from_str(&content).expect("spawn result json");
    let snapshot = manager
        .get_thread(parse_agent_id(
            result["agent_id"].as_str().expect("agent id"),
        ))
        .await
        .expect("spawned agent thread should exist")
        .config_snapshot()
        .await;

    pretty_assertions::assert_eq!(snapshot.session_source.get_agent_role(), None);
}

#[tokio::test]
async fn multi_agent_v2_full_history_inherits_parent_role_metadata() {
    let (mut session, mut turn) = make_session_and_context().await;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    config
        .features
        .enable(Feature::Sqlite)
        .expect("test config should allow sqlite");
    let role_name = "full-history-role".to_string();
    let role_path = config.codex_home.as_path().join("full-history-role.toml");
    tokio::fs::write(
        &role_path,
        "developer_instructions = \"Full-history role instructions\"\nmodel = \"gpt-5.6-terra\"\nmodel_reasoning_effort = \"xhigh\"\n",
    )
    .await
    .expect("role config should be written");
    config.agent_roles.insert(
        role_name.clone(),
        AgentRoleConfig {
            description: Some("Full-history role".to_string()),
            config_file: Some(role_path),
            nickname_candidates: None,
        },
    );
    set_turn_config(&mut turn, config.clone());
    let expected_parent_identity = (
        turn.model_info.slug.clone(),
        turn.reasoning_effort
            .clone()
            .or_else(|| turn.model_info.default_reasoning_level.clone()),
    );
    let state_db = init_state_db(&config)
        .await
        .expect("sqlite state db should initialize");
    let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
        CodexAuth::from_api_key("dummy"),
        config.model_provider.clone(),
        config.codex_home.to_path_buf(),
        Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
        Some(state_db),
    );
    let root = manager
        .start_thread(StartThreadOptions::new(config.clone()))
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.thread_id = root.thread_id;
    turn.session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: root.thread_id,
        depth: 0,
        agent_path: Some(AgentPath::root()),
        agent_nickname: None,
        agent_role: Some(role_name.clone()),
    });

    SpawnAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "inherited_role",
                "fork_turns": "all",
            })),
        ))
        .await
        .expect("full-history spawn should inherit the parent role metadata");
    let child_id = manager
        .captured_ops()
        .into_iter()
        .map(|(thread_id, _)| thread_id)
        .find(|thread_id| *thread_id != root.thread_id)
        .expect("spawned agent should receive an op");
    let child_thread = manager
        .get_thread(child_id)
        .await
        .expect("spawned agent thread should exist");
    let snapshot = child_thread.config_snapshot().await;
    pretty_assertions::assert_eq!(
        (
            snapshot.session_source.get_agent_role(),
            snapshot.model.clone(),
            snapshot.reasoning_effort.clone(),
        ),
        (
            Some(role_name.clone()),
            expected_parent_identity.0.clone(),
            expected_parent_identity.1.clone(),
        )
    );

    child_thread
        .inject_response_items(vec![ResponseItem::Message {
            id: None,
            role: "assistant".to_string(),
            content: vec![ContentItem::OutputText {
                text: "child persisted".to_string(),
            }],
            phase: Some(codex_protocol::models::MessagePhase::FinalAnswer),
            internal_chat_message_metadata_passthrough: None,
        }])
        .await
        .expect("child rollout should persist");
    child_thread
        .shutdown_and_wait()
        .await
        .expect("child thread should shut down");
    let stored_child = child_thread
        .read_thread(
            /*include_archived*/ true, /*include_history*/ false,
        )
        .await
        .expect("child metadata should be readable");
    pretty_assertions::assert_eq!(stored_child.agent_role.as_deref(), Some(role_name.as_str()));
    pretty_assertions::assert_eq!(stored_child.model, Some(expected_parent_identity.0.clone()));
    pretty_assertions::assert_eq!(
        stored_child.reasoning_effort,
        expected_parent_identity.1.clone()
    );
    assert!(manager.remove_thread(&child_id).await.is_some());

    let mut sender_config = config;
    sender_config.model = Some("gpt-5.6-luna".to_string());
    sender_config.model_reasoning_effort = Some(ReasoningEffort::Minimal);
    manager
        .agent_control()
        .ensure_v2_agent_loaded(sender_config, child_id)
        .await
        .expect("full-history child should reload");
    let reloaded_child = manager
        .get_thread(child_id)
        .await
        .expect("reloaded child thread should exist");
    let reloaded_snapshot = reloaded_child.config_snapshot().await;
    pretty_assertions::assert_eq!(
        (
            reloaded_snapshot.session_source.get_agent_role(),
            reloaded_snapshot.model,
            reloaded_snapshot.reasoning_effort,
        ),
        (
            Some(role_name),
            expected_parent_identity.0,
            expected_parent_identity.1,
        )
    );
    pretty_assertions::assert_eq!(
        reloaded_child
            .session
            .new_default_turn()
            .await
            .developer_instructions
            .as_deref(),
        Some("Full-history role instructions")
    );
}

#[tokio::test]
async fn multi_agent_v2_fresh_does_not_inherit_parent_role_metadata() {
    let (mut session, mut turn) = make_session_and_context().await;
    let role_name = install_role_with_model_override(&mut turn).await;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    set_turn_config(&mut turn, config);
    let manager = thread_manager();
    let root = manager
        .start_thread(StartThreadOptions::new((*turn.config).clone()))
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.thread_id = root.thread_id;
    turn.session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: root.thread_id,
        depth: 0,
        agent_path: Some(AgentPath::root()),
        agent_nickname: None,
        agent_role: Some(role_name),
    });

    SpawnAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "fresh_child",
                "fork_turns": "none",
            })),
        ))
        .await
        .expect("fresh spawn should succeed");
    let child_id = manager
        .captured_ops()
        .into_iter()
        .map(|(thread_id, _)| thread_id)
        .find(|thread_id| *thread_id != root.thread_id)
        .expect("spawned agent should receive an op");
    let snapshot = manager
        .get_thread(child_id)
        .await
        .expect("spawned agent thread should exist")
        .config_snapshot()
        .await;

    pretty_assertions::assert_eq!(snapshot.session_source.get_agent_role(), None);
}

#[tokio::test]
async fn multi_agent_v2_full_history_inherits_parent_identity() {
    let (mut session, mut turn) = make_session_and_context().await;
    let mut config = (*turn.config).clone();
    config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");
    config.agent_default_subagent_model = Some("gpt-5.6-luna".to_string());
    config.agent_default_subagent_reasoning_effort = Some(ReasoningEffort::Low);
    set_turn_config(&mut turn, config);
    let expected_identity = (
        turn.model_info.slug.clone(),
        turn.reasoning_effort
            .clone()
            .or_else(|| turn.model_info.default_reasoning_level.clone()),
    );
    let manager = thread_manager();
    let root = manager
        .start_thread(StartThreadOptions::new((*turn.config).clone()))
        .await
        .expect("root thread should start");
    session.services.agent_control = manager.agent_control();
    session.thread_id = root.thread_id;
    let output = SpawnAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(json!({
                "message": "inspect this repo",
                "task_name": "full_history_identity",
                "fork_turns": "all",
            })),
        ))
        .await
        .expect("full-history spawn should inherit parent identity");
    let (content, _) = expect_text_output(output);
    let result: serde_json::Value =
        serde_json::from_str(&content).expect("spawn result should be json");
    let child_id = manager
        .captured_ops()
        .into_iter()
        .map(|(thread_id, _)| thread_id)
        .find(|thread_id| *thread_id != root.thread_id)
        .expect("spawned agent should receive an op");
    pretty_assertions::assert_eq!(result["task_name"], "/root/full_history_identity");
    let snapshot = manager
        .get_thread(child_id)
        .await
        .expect("spawned agent thread should exist")
        .config_snapshot()
        .await;
    pretty_assertions::assert_eq!(
        (snapshot.model, snapshot.reasoning_effort),
        expected_identity
    );
}
