use super::*;
use crate::session::tests::make_session_and_context_with_rx;
use codex_protocol::protocol::MultiAgentVersion;
use pretty_assertions::assert_eq;

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

    assert_eq!(
        (snapshot.model.as_str(), snapshot.reasoning_effort),
        ("gpt-5.4", Some(ReasoningEffort::High))
    );
}

#[tokio::test]
async fn spawn_agent_explicit_model_preserves_role_effort_default() {
    let snapshot = spawn_v1_agent_with_role_identity(Some("gpt-5.4"), None)
        .await
        .expect("spawn_agent should honor an explicit model");

    assert_eq!(
        (snapshot.model.as_str(), snapshot.reasoning_effort),
        ("gpt-5.4", Some(ReasoningEffort::Low))
    );
}

#[tokio::test]
async fn spawn_agent_explicit_effort_preserves_role_model_default() {
    let snapshot = spawn_v1_agent_with_role_identity(None, Some(ReasoningEffort::High))
        .await
        .expect("spawn_agent should honor an explicit reasoning effort");

    assert_eq!(
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

    assert_eq!(
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

    assert_eq!(
        (snapshot.model, snapshot.reasoning_effort),
        ("gpt-5.6-sol".to_string(), Some(ReasoningEffort::Low))
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

    assert_eq!(
        (snapshot.model, snapshot.reasoning_effort),
        ("gpt-5.6-sol".to_string(), Some(ReasoningEffort::Low))
    );
}

#[tokio::test]
async fn spawn_agent_rejects_invalid_final_model_effort_pair() {
    let error = spawn_v1_agent_with_role_identity(Some("gpt-5.4"), Some(ReasoningEffort::Minimal))
        .await
        .expect_err("the final model and effort should be validated together");

    assert_eq!(
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

    assert_eq!(
        (snapshot.model.as_str(), snapshot.reasoning_effort),
        ("gpt-5.4", Some(ReasoningEffort::High))
    );
}

#[tokio::test]
async fn multi_agent_v2_spawn_explicit_model_preserves_role_effort_default() {
    let snapshot = spawn_v2_agent_with_role_identity(Some("gpt-5.4"), None)
        .await
        .expect("spawn_agent should honor an explicit model");

    assert_eq!(
        (snapshot.model.as_str(), snapshot.reasoning_effort),
        ("gpt-5.4", Some(ReasoningEffort::Low))
    );
}

#[tokio::test]
async fn multi_agent_v2_spawn_explicit_effort_preserves_role_model_default() {
    let snapshot = spawn_v2_agent_with_role_identity(None, Some(ReasoningEffort::High))
        .await
        .expect("spawn_agent should honor an explicit reasoning effort");

    assert_eq!(
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

    assert_eq!(
        (snapshot.model.as_str(), snapshot.reasoning_effort),
        ("gpt-5-role-override", Some(ReasoningEffort::Low))
    );
}

#[tokio::test]
async fn multi_agent_v2_rejects_invalid_final_model_effort_pair() {
    let error = spawn_v2_agent_with_role_identity(Some("gpt-5.4"), Some(ReasoningEffort::Minimal))
        .await
        .expect_err("the final model and effort should be validated together");

    assert_eq!(
        error,
        FunctionCallError::RespondToModel(
            "Reasoning effort `minimal` is not supported for model `gpt-5.4`. Supported reasoning efforts: low, medium, high, xhigh".to_string()
        )
    );
}

async fn reject_v1_full_history_identity_overrides(args: serde_json::Value) -> FunctionCallError {
    let (session, turn) = make_session_and_context().await;
    SpawnAgentHandler::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(args),
        ))
        .await
        .expect_err("full-history identity overrides should be rejected")
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
    let error = SpawnAgentHandler::default()
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
        .expect_err("full-history identity overrides should be rejected");

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
    SpawnAgentHandlerV2::default()
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "spawn_agent",
            function_payload(args),
        ))
        .await
        .expect_err("full-history identity overrides should be rejected")
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
    assert_eq!(
        (snapshot.model, snapshot.reasoning_effort),
        expected_identity
    );
}

#[tokio::test]
async fn spawn_agent_inherits_parent_role_metadata() {
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
        agent_role: Some(role_name.clone()),
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

    assert_eq!(snapshot.session_source.get_agent_role(), Some(role_name));
}

#[tokio::test]
async fn multi_agent_v2_spawn_inherits_parent_role_metadata() {
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
                "fork_turns": "none",
            })),
        ))
        .await
        .expect("spawn should inherit the parent role metadata");
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

    assert_eq!(snapshot.session_source.get_agent_role(), Some(role_name));
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
    assert_eq!(result["task_name"], "/root/full_history_identity");
    let snapshot = manager
        .get_thread(child_id)
        .await
        .expect("spawned agent thread should exist")
        .config_snapshot()
        .await;
    assert_eq!(
        (snapshot.model, snapshot.reasoning_effort),
        expected_identity
    );
}
