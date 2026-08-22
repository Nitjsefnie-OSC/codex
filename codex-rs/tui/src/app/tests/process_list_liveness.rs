//! `/ps` liveness regression coverage for native agents.
//!
//! The authoritative source for whether a native agent is running is the real
//! AgentControl/multi-agent-v2 boundary, never the TUI's own historical caches
//! (`AgentNavigationState` or a `ThreadEventChannel`). This module spawns a genuine native child
//! through the real `spawn_agent` tool dispatch, attaches app-server watch state to it through
//! the production `thread/resume` rejoin RPC (never a TUI channel), lets its first turn complete
//! for real, then resumes it through the real `followup_task` tool dispatch while the TUI's own
//! caches stay stale, proving `/ps` cannot be satisfied without a fresh authoritative query.

use super::*;
use codex_app_server_protocol::SubAgentActivityKind;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call_with_namespace;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_response_once_match;
use core_test_support::responses::mount_sse_once_match;
use core_test_support::responses::sse;
use core_test_support::responses::sse_response;
use core_test_support::responses::start_mock_server;
use pretty_assertions::assert_eq;
use std::time::Duration;
use wiremock::Request;

const MODEL: &str = "gpt-5.4";
const MODEL_PROVIDER_ID: &str = "ps-liveness-test";
const COLLABORATION_NAMESPACE: &str = "collaboration";

// Distinctive, never-otherwise-occurring markers used to route each mocked model exchange to the
// exact real HTTP request it is meant to answer. Because a request's body includes the full prior
// conversation, later requests remain a superset of earlier markers; correctness here comes from
// staging each mock only once its predecessor is consumed, plus the `agent_message` input-type
// check that always separates the parent's own conversation from the child's.
const SPAWN_PROMPT: &str = "codex-ps-liveness: spawn the worker";
const SPAWN_CALL_ID: &str = "codex-ps-liveness-spawn-call";
const CHILD_TASK: &str = "codex-ps-liveness: do the first task";
const RESUME_PROMPT: &str = "codex-ps-liveness: resume the worker";
const FOLLOWUP_CALL_ID: &str = "codex-ps-liveness-followup-call";

/// Reads a mocked request body as raw bytes. Loopback test traffic here is never
/// content-encoded, so no decompression is needed.
fn request_body(request: &Request) -> Option<Vec<u8>> {
    Some(request.body.clone())
}

fn body_contains(request: &Request, text: &str) -> bool {
    request_body(request)
        .and_then(|body| String::from_utf8(body).ok())
        .is_some_and(|body| body.contains(text))
}

/// True only for a request that carries an `agent_message`-typed input item — the wire marker a
/// spawned child's own conversation carries that the parent's own conversation never does.
fn request_has_input_type(request: &Request, input_type: &str) -> bool {
    request_body(request)
        .and_then(|body| serde_json::from_slice::<serde_json::Value>(&body).ok())
        .and_then(|body| {
            body.get("input")
                .and_then(serde_json::Value::as_array)
                .cloned()
        })
        .is_some_and(|items| {
            items.iter().any(|item| {
                item.get("type").and_then(serde_json::Value::as_str) == Some(input_type)
            })
        })
}

fn is_parent_request(request: &Request) -> bool {
    !request_has_input_type(request, "agent_message")
}

fn immediate_child_turn_body(response_id: &str) -> String {
    sse(vec![
        ev_response_created(response_id),
        ev_assistant_message(&format!("msg-{response_id}"), "first task done"),
        ev_completed(response_id),
    ])
}

/// Extracts the spawned child's real thread id and agent path from a `SubAgentActivity::Started`
/// item, wherever it appears in a parent-thread notification.
fn started_child_from_item(item: &ThreadItem) -> Option<(ThreadId, String)> {
    let ThreadItem::SubAgentActivity {
        kind: SubAgentActivityKind::Started,
        agent_thread_id,
        agent_path,
        ..
    } = item
    else {
        return None;
    };
    Some((
        ThreadId::from_string(agent_thread_id).ok()?,
        agent_path.clone(),
    ))
}

fn notification_item_for_thread<'a>(
    notification: &'a ServerNotification,
    parent_thread_id: ThreadId,
) -> Option<&'a ThreadItem> {
    match notification {
        ServerNotification::ItemStarted(n) if n.thread_id == parent_thread_id.to_string() => {
            Some(&n.item)
        }
        ServerNotification::ItemCompleted(n) if n.thread_id == parent_thread_id.to_string() => {
            Some(&n.item)
        }
        _ => None,
    }
}

/// Waits for the real app server to report a `SubAgentActivity::Started` item on the parent's own
/// transcript, returning the spawned child's real thread id and agent path.
async fn wait_for_spawned_child(
    app_server: &mut AppServerSession,
    parent_thread_id: ThreadId,
) -> (ThreadId, String) {
    for _ in 0..40 {
        let event = tokio::time::timeout(
            std::time::Duration::from_secs(/*secs*/ 10),
            app_server.next_event(),
        )
        .await
        .expect("app-server should emit an event while spawning the child")
        .expect("app-server event stream should remain open");
        if let codex_app_server_client::AppServerEvent::ServerNotification(notification) = event
            && let Some(item) = notification_item_for_thread(&notification, parent_thread_id)
            && let Some(found) = started_child_from_item(item)
        {
            return found;
        }
    }
    panic!("expected a SubAgentActivity::Started item for the spawned child");
}

/// Polls the child's real, authoritative `thread_read` status until `is_expected_status` matches.
///
/// Observing liveness through a direct `thread_read` call — the same authoritative source
/// `/subagents` already refreshes from — queries the resource itself instead of depending on
/// notification-delivery timing, so this can never itself become a false-RED wait: it either
/// observes the real status or the deadline panic fires as an explicit fixture-setup failure,
/// distinct from the intended `/ps` assertion failure. The caller must first attach app-server
/// watch state for the child (the `thread/resume` rejoin in the test body); a child that was only
/// ever created by `spawn_agent`'s internal machinery has no `ThreadWatchManager` entry of its
/// own, and an unwatched idle thread reads back as `NotLoaded`, never `Idle`.
async fn wait_for_child_thread_status(
    app_server: &mut AppServerSession,
    thread_id: ThreadId,
    what: &'static str,
    mut is_expected_status: impl FnMut(&codex_app_server_protocol::ThreadStatus) -> bool,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let thread = app_server
            .thread_read(thread_id, /*include_turns*/ false)
            .await
            .unwrap_or_else(|err| panic!("thread_read failed while waiting for {what}: {err}"));
        if is_expected_status(&thread.status) {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for {what} (thread {thread_id}); last status: {:?}",
                thread.status
            );
        }
        tokio::time::sleep(Duration::from_millis(/*millis*/ 25)).await;
    }
}

/// `/ps` must list the agents that are running right now, per the authoritative agent controller,
/// not the last liveness verdict the TUI's own navigation cache or event channels happened to
/// record.
///
/// This drives the real lifecycle end to end: the parent's own turn calls the real `spawn_agent`
/// tool, which creates a genuine native child through the real multi-agent-v2 path; app-server
/// watch state is attached to the child through the production `thread/resume` rejoin RPC, so
/// authoritative `thread_read` reports the child's real status instead of `NotLoaded`; the child's
/// first turn genuinely completes; the TUI's cache is populated and left stale by a single real
/// `thread_read`-backed refresh (exactly the same call `/subagents` already uses, and exactly the
/// gap a `followup_task` resume leaves behind, since the only signal the TUI would otherwise see is
/// a `SubAgentActivity::Interacted` item, which `sub_agent_activity_display` maps to `None`); the
/// parent's second turn calls the real `followup_task` tool, which starts the child's second turn
/// for real, held open by a delayed mock response. No `ThreadEventChannel` is ever created for the
/// child. A fix that reads only `agent_navigation` or only a `ThreadEventChannel` cannot pass; only
/// a fresh query against the real app server can.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_list_includes_resumed_agent_when_navigation_cache_is_stale() -> Result<()> {
    let server = start_mock_server().await;

    let (mut app, mut app_event_rx, _op_rx) = make_test_app_with_channels().await;
    let mut tui = crate::tui::test_support::make_test_tui()?;
    let codex_home = tempdir()?;
    // The embedded app server builds every thread's config through its `ConfigManager`, which
    // loads from this on-disk `config.toml` (plus the request's own overrides) — never from the
    // in-memory `app.config` mutated below (`thread_start_task` calls
    // `config_manager.load_with_overrides(..)` in
    // `app-server/src/request_processors/thread_processor.rs`). `multi_agent_v2` is default-off
    // and must therefore be enabled HERE: without it the parent's turn runs multi-agent V1, whose
    // registry has no namespaced `collaboration.spawn_agent`/`followup_task`, so the mocked
    // function calls never reach the real AgentControl spawn path and no `SubAgentActivity` item
    // ever appears on the parent thread. This is the same incantation the CI-proven app-server
    // integration test `app-server/tests/suite/v2/multi_agent_v2_developer_instructions.rs` uses
    // for the identical spawn/followup flow.
    std::fs::write(
        codex_home.path().join("config.toml"),
        format!(
            r#"
model = "{MODEL}"
model_provider = "{MODEL_PROVIDER_ID}"

[model_providers.{MODEL_PROVIDER_ID}]
name = "ps liveness test"
base_url = "{}/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0

[features.multi_agent_v2]
enabled = true
"#,
            server.uri()
        ),
    )?;
    app.config.codex_home = codex_home.path().to_path_buf().abs();
    app.config.sqlite = codex_state::SqliteConfig::new_for_testing(codex_home.path().abs());
    app.config.model = Some(MODEL.to_string());
    app.config.model_provider_id = MODEL_PROVIDER_ID.to_string();
    app.config.model_provider = ModelProviderInfo {
        name: "ps liveness test".to_string(),
        base_url: Some(format!("{}/v1", server.uri())),
        request_max_retries: Some(0),
        stream_max_retries: Some(0),
        ..ModelProviderInfo::default()
    };
    // These in-memory flags shape only the TUI-side `App`'s own view of the world (e.g. the
    // `Feature::Collab` gate in `session_lifecycle.rs`); the app-server side reads the on-disk
    // `config.toml` written above. Keep both sides consistent.
    app.config
        .features
        .enable(Feature::Collab)
        .expect("test config should allow feature update");
    app.config
        .features
        .enable(Feature::MultiAgentV2)
        .expect("test config should allow feature update");

    let mut app_server = Box::pin(crate::start_embedded_app_server_for_picker(&app.config)).await?;

    // Parent thread. Never attached to `App` (no `thread_event_channels` entry) — it exists purely
    // to drive real `spawn_agent`/`followup_task` tool calls.
    let parent_thread_id = app_server
        .start_thread(&app.config)
        .await?
        .session
        .thread_id;
    let workspace_roots = app
        .config
        .permissions
        .user_visible_workspace_roots()
        .to_vec();

    // Stage 1: the parent's own turn spawns a real native child through the real
    // `spawn_agent` tool, and the child's own first turn completes normally.
    let spawn_args = serde_json::to_string(&serde_json::json!({
        "message": CHILD_TASK,
        "task_name": "resumed_worker",
        "fork_turns": "none",
    }))?;
    mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, SPAWN_PROMPT) && is_parent_request(request),
        sse(vec![
            ev_response_created("resp-spawn-1"),
            ev_function_call_with_namespace(
                SPAWN_CALL_ID,
                COLLABORATION_NAMESPACE,
                "spawn_agent",
                &spawn_args,
            ),
            ev_completed("resp-spawn-1"),
        ]),
    )
    .await;
    mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, SPAWN_CALL_ID) && is_parent_request(request),
        sse(vec![
            ev_response_created("resp-spawn-2"),
            ev_assistant_message("msg-spawn-2", "worker spawned"),
            ev_completed("resp-spawn-2"),
        ]),
    )
    .await;
    mount_sse_once_match(
        &server,
        |request: &Request| {
            request_has_input_type(request, "agent_message") && body_contains(request, CHILD_TASK)
        },
        immediate_child_turn_body("resp-child-turn-1"),
    )
    .await;

    app_server
        .turn_start(
            parent_thread_id,
            vec![codex_app_server_protocol::UserInput::Text {
                text: SPAWN_PROMPT.to_string(),
                text_elements: Vec::new(),
            }],
            app.config.cwd.to_path_buf(),
            AskForApproval::Never,
            ApprovalsReviewer::User,
            TurnPermissionsOverride::Preserve,
            &workspace_roots,
            MODEL.to_string(),
            /*effort*/ None,
            /*summary*/ None,
            /*service_tier*/ None,
            /*collaboration_mode*/ None,
            /*personality*/ None,
            /*output_schema*/ None,
        )
        .await?;

    let (child_thread_id, discovered_agent_path) =
        wait_for_spawned_child(&mut app_server, parent_thread_id).await;

    // The child was created by `spawn_agent`'s internal machinery, never by this client's own
    // `thread/start`, so the embedded app server may hold no listener or `ThreadWatchManager`
    // entry for it. Without one, authoritative `thread_read` reports `NotLoaded` — which the
    // production refresh below misclassifies as closed, and which the strict `Idle` poll would
    // turn into a false-RED timeout. Attach app-server-side watch state through the same
    // production rejoin RPC the TUI already uses for loaded threads it did not start itself
    // (`thread_routing::refresh_snapshot_session_if_needed`): `resume_thread` with
    // `PreserveExistingThread` sends a bare `thread/resume` carrying only the thread id, which
    // the server answers via its running-thread rejoin path — ensuring the child's listener task
    // is running, which drains the child's buffered turn lifecycle events into the watch state so
    // `thread_read` reports the child's real `Idle`/`Active` status from here on. The call is
    // app-server-only: it never touches `App`, creates no `ThreadEventChannel`, and writes
    // nothing into any TUI cache. The bounded retry absorbs thread-store visibility lag right
    // after the spawn; exhausting it panics as an explicit fixture-setup failure, distinct from
    // the intended `/ps` assertion failure.
    let attach_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match app_server
            .resume_thread(
                app.config.clone(),
                child_thread_id,
                crate::app_server_session::ResumeModelSettings::PreserveExistingThread,
            )
            .await
        {
            Ok(_) => break,
            Err(err) => {
                if tokio::time::Instant::now() >= attach_deadline {
                    panic!(
                        "failed to attach app-server watch state to the spawned child \
                         {child_thread_id}: {err}"
                    );
                }
                tokio::time::sleep(Duration::from_millis(/*millis*/ 25)).await;
            }
        }
    }

    wait_for_child_thread_status(
        &mut app_server,
        child_thread_id,
        "the child's first turn to finish",
        |status| matches!(status, codex_app_server_protocol::ThreadStatus::Idle),
    )
    .await;

    // Stage 2: populate `agent_navigation` the same way `/subagents` already does — a single real
    // `thread_read`-backed refresh, taken while the child is genuinely idle. This is the only
    // identity/liveness source; nothing here is hand-labeled.
    app.refresh_agent_picker_thread_liveness(&mut app_server, child_thread_id)
        .await;
    let stale_entry = app
        .agent_navigation
        .get(&child_thread_id)
        .cloned()
        .expect("spawned child should be cached after a real liveness refresh");
    assert!(
        !stale_entry.is_running,
        "child must be genuinely idle at this snapshot"
    );
    assert!(!stale_entry.is_closed);
    // No fallback: if the authoritative refresh failed to populate a real `agent_path`, that is a
    // fixture-setup failure and must be reported as one here, not masked into a later, misleading
    // `/ps` assertion failure.
    let agent_path = stale_entry
        .agent_path
        .clone()
        .filter(|path| !path.trim().is_empty())
        .expect("a real thread_read-backed refresh should populate the child's real agent_path");
    assert_eq!(
        agent_path, discovered_agent_path,
        "the refreshed cached agent_path must match the real path discovered from \
         SubAgentActivity::Started"
    );
    let expected_label = format_agent_picker_item_name(
        stale_entry.agent_nickname.as_deref(),
        stale_entry.agent_role.as_deref(),
        /*is_primary*/ false,
    );
    assert!(
        !app.thread_event_channels.contains_key(&child_thread_id),
        "the child must never be attached to a ThreadEventChannel"
    );
    while app_event_rx.try_recv().is_ok() {}

    // Snapshot A: the child is genuinely completed (idle) right now, so it must be absent.
    app.handle_event(&mut tui, &mut app_server, AppEvent::OpenProcessList)
        .await?;
    let rendered_idle = std::iter::from_fn(|| app_event_rx.try_recv().ok())
        .filter_map(|event| match event {
            AppEvent::InsertHistoryCell(cell) => {
                Some(lines_to_single_string(&cell.display_lines(/*width*/ 200)))
            }
            _ => None,
        })
        .find(|rendered| rendered.contains("Native agents"))
        .expect("process list history cell");
    assert!(
        !rendered_idle.contains(&agent_path),
        "a genuinely completed child must stay out of the process list:\n{rendered_idle}"
    );

    // Stage 3: the parent's second turn resumes the same child through the real `followup_task`
    // tool, targeting it by its real thread id. The child's second turn is held open by a delayed
    // response, so it is genuinely `InProgress` on the app server for the rest of this test.
    let followup_args = serde_json::to_string(&serde_json::json!({
        "target": child_thread_id.to_string(),
        "message": "keep going",
    }))?;
    mount_sse_once_match(
        &server,
        |request: &Request| body_contains(request, RESUME_PROMPT) && is_parent_request(request),
        sse(vec![
            ev_response_created("resp-followup-1"),
            ev_function_call_with_namespace(
                FOLLOWUP_CALL_ID,
                COLLABORATION_NAMESPACE,
                "followup_task",
                &followup_args,
            ),
            ev_completed("resp-followup-1"),
        ]),
    )
    .await;
    mount_response_once_match(
        &server,
        |request: &Request| {
            request_has_input_type(request, "agent_message") && body_contains(request, CHILD_TASK)
        },
        sse_response(immediate_child_turn_body("resp-child-turn-2"))
            .set_delay(Duration::from_secs(60)),
    )
    .await;

    app_server
        .turn_start(
            parent_thread_id,
            vec![codex_app_server_protocol::UserInput::Text {
                text: RESUME_PROMPT.to_string(),
                text_elements: Vec::new(),
            }],
            app.config.cwd.to_path_buf(),
            AskForApproval::Never,
            ApprovalsReviewer::User,
            TurnPermissionsOverride::Preserve,
            &workspace_roots,
            MODEL.to_string(),
            /*effort*/ None,
            /*summary*/ None,
            /*service_tier*/ None,
            /*collaboration_mode*/ None,
            /*personality*/ None,
            /*output_schema*/ None,
        )
        .await?;
    wait_for_child_thread_status(
        &mut app_server,
        child_thread_id,
        "the child's resumed turn to start",
        |status| {
            matches!(
                status,
                codex_app_server_protocol::ThreadStatus::Active { .. }
            )
        },
    )
    .await;

    // Historical TUI state is unchanged since stage 2: `agent_navigation` still says not-running,
    // and the child still has no `ThreadEventChannel`.
    assert!(
        !app.agent_navigation
            .get(&child_thread_id)
            .expect("child stays cached")
            .is_running,
        "the cache must remain stale across the resume"
    );
    assert!(!app.thread_event_channels.contains_key(&child_thread_id));
    while app_event_rx.try_recv().is_ok() {}

    // Snapshot B: the child's resumed turn is genuinely running on the app server right now.
    app.handle_event(&mut tui, &mut app_server, AppEvent::OpenProcessList)
        .await?;
    let rendered_running = std::iter::from_fn(|| app_event_rx.try_recv().ok())
        .filter_map(|event| match event {
            AppEvent::InsertHistoryCell(cell) => {
                Some(lines_to_single_string(&cell.display_lines(/*width*/ 200)))
            }
            _ => None,
        })
        .find(|rendered| rendered.contains("Native agents"))
        .expect("process list history cell");
    assert!(
        rendered_running.contains(&format!("{agent_path} — {expected_label} — running")),
        "resumed native agent should be listed while its turn is genuinely in progress on the \
         app server, even though the TUI's own cache still says stopped:\n{rendered_running}"
    );

    app_server.shutdown().await?;
    Ok(())
}
