//! `/ps` liveness regression coverage for native agents.
//!
//! The authoritative source for whether a native agent is running is the real
//! AgentControl/app-server boundary (a fresh `thread_read`-equivalent snapshot), never the TUI's
//! own historical caches (`AgentNavigationState` or a `ThreadEventChannel`). This module drives two
//! genuinely independent app-server threads through the real embedded app server, with neither
//! thread ever attached to the TUI's own live event machinery, so the only way `/ps` can learn
//! their true liveness is a fresh query against the app server itself.

use super::*;
use codex_app_server_protocol::UserInput;
use core_test_support::responses;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use tokio::sync::oneshot;

const MODEL: &str = "gpt-5.4";
const MODEL_PROVIDER_ID: &str = "ps-liveness-test";

fn immediate_response_chunks(response_id: &str) -> Vec<StreamingSseChunk> {
    [
        ev_response_created(response_id),
        ev_assistant_message(&format!("message-{response_id}"), "done"),
        ev_completed(response_id),
    ]
    .into_iter()
    .map(|event| StreamingSseChunk {
        gate: None,
        body: responses::sse(vec![event]),
    })
    .collect()
}

/// Splits a response into an ungated `response_created` chunk followed by a gated `completed`
/// chunk, so the caller can hold the turn genuinely `InProgress` on the app server until it
/// chooses to release it.
fn gated_response_chunks(response_id: &str) -> (Vec<StreamingSseChunk>, oneshot::Sender<()>) {
    let (release_tx, release_rx) = oneshot::channel();
    (
        vec![
            StreamingSseChunk {
                gate: None,
                body: responses::sse(vec![ev_response_created(response_id)]),
            },
            StreamingSseChunk {
                gate: Some(release_rx),
                body: responses::sse(vec![ev_completed(response_id)]),
            },
        ],
        release_tx,
    )
}

/// Waits for the real app server to report that `thread_id`'s turn has started, without routing
/// the notification through `App` — this deliberately never touches `agent_navigation` or any
/// `ThreadEventChannel`, so the resumed thread's liveness stays known only to the app server.
async fn wait_for_turn_started(app_server: &mut AppServerSession, thread_id: ThreadId) {
    for _ in 0..20 {
        let event = tokio::time::timeout(
            std::time::Duration::from_secs(/*secs*/ 5),
            app_server.next_event(),
        )
        .await
        .expect("app-server should emit a turn/start event")
        .expect("app-server event stream should remain open");
        if let codex_app_server_client::AppServerEvent::ServerNotification(notification) = event
            && let ServerNotification::TurnStarted(notification) = *notification
            && notification.thread_id == thread_id.to_string()
        {
            return;
        }
    }
    panic!("expected TurnStarted for thread {thread_id}");
}

/// Waits for the real app server to report that `thread_id`'s turn has completed, without routing
/// the notification through `App` (see `wait_for_turn_started`).
async fn wait_for_turn_completed(app_server: &mut AppServerSession, thread_id: ThreadId) {
    for _ in 0..20 {
        let event = tokio::time::timeout(
            std::time::Duration::from_secs(/*secs*/ 5),
            app_server.next_event(),
        )
        .await
        .expect("app-server should emit a turn/completed event")
        .expect("app-server event stream should remain open");
        if let codex_app_server_client::AppServerEvent::ServerNotification(notification) = event
            && let ServerNotification::TurnCompleted(notification) = *notification
            && notification.thread_id == thread_id.to_string()
        {
            return;
        }
    }
    panic!("expected TurnCompleted for thread {thread_id}");
}

/// `/ps` must list the agents that are running right now, per the authoritative agent controller,
/// not the last liveness verdict the TUI's own navigation cache or event channels happened to
/// record.
///
/// Both threads here are started directly against the real embedded app server and never attached
/// to `App` (no `thread_event_channels` entry, no `handle_app_server_event` routing), so their only
/// representation inside `App` is a stale, explicitly-not-running `agent_navigation` entry — the
/// same shape a `followup_task` resume leaves behind, since the only signal the TUI observes for
/// that case is a `SubAgentActivity::Interacted` item, which `sub_agent_activity_display` maps to
/// `None` and therefore never revives cached liveness. Meanwhile the resumed thread's turn is
/// genuinely `InProgress` on the app server (held open by a gated SSE response) at the moment `/ps`
/// is invoked, and the completed thread's turn has genuinely finished. A fix that reads only
/// `agent_navigation` or only a `ThreadEventChannel` has no way to see either fact; a fix that
/// queries the app server's authoritative current state for each candidate thread does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_list_includes_resumed_agent_when_navigation_cache_is_stale() -> Result<()> {
    let (running_chunks, release_running_response) =
        gated_response_chunks("resumed-agent-response");
    let finished_chunks = immediate_response_chunks("finished-agent-response");
    let (server, _completions) =
        start_streaming_sse_server(vec![running_chunks, finished_chunks]).await;

    let (mut app, mut app_event_rx, _op_rx) = make_test_app_with_channels().await;
    let mut tui = crate::tui::test_support::make_test_tui()?;
    let codex_home = tempdir()?;
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

    let mut app_server = Box::pin(crate::start_embedded_app_server_for_picker(&app.config)).await?;

    // Two genuinely independent app-server threads. Neither is ever attached to `App`, so nothing
    // here can populate `agent_navigation`'s liveness from real events, matching the resumed-child
    // scenario where the TUI never watches the child thread directly.
    let resumed_thread_id = app_server
        .start_thread(&app.config)
        .await?
        .session
        .thread_id;
    let completed_thread_id = app_server
        .start_thread(&app.config)
        .await?
        .session
        .thread_id;

    let workspace_roots = app
        .config
        .permissions
        .user_visible_workspace_roots()
        .to_vec();
    app_server
        .turn_start(
            resumed_thread_id,
            vec![UserInput::Text {
                text: "keep this resumed turn running".to_string(),
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
    server.wait_for_request_count(1).await;
    wait_for_turn_started(&mut app_server, resumed_thread_id).await;

    app_server
        .turn_start(
            completed_thread_id,
            vec![UserInput::Text {
                text: "finish this turn immediately".to_string(),
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
    server.wait_for_request_count(2).await;
    wait_for_turn_completed(&mut app_server, completed_thread_id).await;

    // Historical TUI state: both children are remembered as stale and explicitly not running,
    // exactly what a `followup_task` resume leaves behind.
    for (thread_id, agent_path, agent_nickname) in [
        (resumed_thread_id, "/root/resumed", "Ada"),
        (completed_thread_id, "/root/finished", "Grace"),
    ] {
        app.agent_navigation.upsert(
            thread_id,
            Some(agent_nickname.to_string()),
            Some("explorer".to_string()),
            /*is_closed*/ false,
        );
        app.agent_navigation
            .record_sub_agent_activity(SubAgentActivityDisplay {
                thread_id,
                agent_path: agent_path.to_string(),
                is_running_hint: false,
            });
    }
    assert!(!app.thread_event_channels.contains_key(&resumed_thread_id));
    assert!(!app.thread_event_channels.contains_key(&completed_thread_id));
    while app_event_rx.try_recv().is_ok() {}

    app.handle_event(&mut tui, &mut app_server, AppEvent::OpenProcessList)
        .await?;

    let rendered = std::iter::from_fn(|| app_event_rx.try_recv().ok())
        .filter_map(|event| match event {
            AppEvent::InsertHistoryCell(cell) => {
                Some(lines_to_single_string(&cell.display_lines(/*width*/ 200)))
            }
            _ => None,
        })
        .find(|rendered| rendered.contains("Native agents"))
        .expect("process list history cell");

    assert!(
        rendered.contains("/root/resumed — Ada [explorer] — running"),
        "resumed agent should be listed while its turn is genuinely in progress on the app \
         server, even though the TUI's own cache still says stopped:\n{rendered}"
    );
    assert!(
        !rendered.contains("/root/finished"),
        "an agent whose turn has genuinely completed on the app server must stay out of the \
         process list:\n{rendered}"
    );

    let _ = release_running_response.send(());
    app_server.shutdown().await?;
    server.shutdown().await;
    Ok(())
}
