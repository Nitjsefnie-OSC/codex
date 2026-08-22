//! `/ps` liveness regression coverage for native agents.

use super::*;

/// `/ps` must list the agents that are running right now, not the last liveness verdict the
/// navigation cache happened to record.
///
/// When a parent resumes a child through `followup_task`, the only history the TUI observes is a
/// `SubAgentActivity::Interacted` item, which never revives the cached `is_running` flag after the
/// child's previous turn completed. The authoritative view of that child is its app-server thread
/// snapshot, where the resumed turn is still `InProgress`. A child that genuinely finished has no
/// in-progress turn in the same snapshot and must stay out of the list.
#[tokio::test]
async fn process_list_includes_resumed_agent_when_navigation_cache_is_stale() -> Result<()> {
    let (mut app, mut app_event_rx, _op_rx) = make_test_app_with_channels().await;
    let mut tui = crate::tui::test_support::make_test_tui()?;
    let mut app_server = Box::pin(crate::start_embedded_app_server_for_picker(&app.config)).await?;

    let resumed_thread_id = ThreadId::new();
    let completed_thread_id = ThreadId::new();

    // Authoritative current state: the resumed child is mid-turn, the other child is finished.
    app.thread_event_channels.insert(
        resumed_thread_id,
        ThreadEventChannel::new_with_session(
            THREAD_EVENT_CHANNEL_CAPACITY,
            test_thread_session(resumed_thread_id, test_path_buf("/tmp/project")),
            vec![
                test_turn("turn-1", TurnStatus::Completed, Vec::new()),
                test_turn("turn-2", TurnStatus::InProgress, Vec::new()),
            ],
        ),
    );
    app.thread_event_channels.insert(
        completed_thread_id,
        ThreadEventChannel::new_with_session(
            THREAD_EVENT_CHANNEL_CAPACITY,
            test_thread_session(completed_thread_id, test_path_buf("/tmp/project")),
            vec![test_turn("turn-1", TurnStatus::Completed, Vec::new())],
        ),
    );

    // Historical TUI state: both children are remembered as stopped, because the resume only
    // produced an `Interacted` activity item that leaves cached liveness untouched.
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
        "resumed agent should be listed while its turn is in progress:\n{rendered}"
    );
    assert!(
        !rendered.contains("/root/finished"),
        "completed agent should stay out of the process list:\n{rendered}"
    );
    Ok(())
}
