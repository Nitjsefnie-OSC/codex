use codex_core::NotSubmittedReason;
use codex_core::StartIfIdleSubmission;
use codex_core::TurnInputRequest;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::TurnLifecycleContributor;
use codex_extension_api::TurnStopInput;
use codex_history::RolloutItem;
use codex_protocol::AgentPath;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::ThreadSettingsOverrides;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::sse;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::StreamingSseServer;
use core_test_support::streaming_sse::start_streaming_sse_server;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;
use tokio::time::timeout;

struct FinishingTurnGate {
    entered: Arc<Notify>,
    release: Arc<Semaphore>,
}

impl TurnLifecycleContributor for FinishingTurnGate {
    fn on_turn_stop<'a>(&'a self, _input: TurnStopInput<'a>) -> ExtensionFuture<'a, ()> {
        Box::pin(async move {
            self.entered.notify_one();
            let _permit = self
                .release
                .acquire()
                .await
                .expect("finishing-turn gate should remain open");
        })
    }
}

fn completion(content: &str) -> InterAgentCommunication {
    InterAgentCommunication::new(
        AgentPath::try_from("/root/worker").expect("worker path should parse"),
        AgentPath::root(),
        Vec::new(),
        content.to_string(),
        /*trigger_turn*/ false,
    )
}

async fn wait_for_post_boundary_request(
    server: &StreamingSseServer,
    expected_content: &str,
) -> anyhow::Result<()> {
    wait_for_request_count(server, /*count*/ 2).await?;
    let requests = server.requests().await;
    assert_eq!(
        requests.len(),
        2,
        "interrupt must not start an extra request"
    );
    let post_boundary_request = String::from_utf8_lossy(&requests[1]);
    assert!(
        post_boundary_request.contains(expected_content),
        "post-boundary completion must trigger the next model request",
    );
    Ok(())
}

async fn assert_persisted_completion_once(
    test: &TestCodex,
    expected_content: &str,
) -> anyhow::Result<()> {
    test.codex.ensure_rollout_materialized().await;
    test.codex.flush_rollout().await?;
    let history = test.codex.load_history(/*include_archived*/ true).await?;
    let count = history
        .items
        .iter()
        .filter(|item| {
            matches!(
                item,
                RolloutItem::ResponseItem(envelope)
                    if matches!(
                        &envelope.item,
                        codex_protocol::models::ResponseItem::AgentMessage { content, .. }
                            if content.iter().any(|content| matches!(
                                content,
                                codex_protocol::models::AgentMessageInputContent::InputText { text }
                                    if text == expected_content
                            ))
                    )
            )
        })
        .count();
    assert_eq!(
        count, 1,
        "completion should appear exactly once in durable history",
    );
    Ok(())
}

async fn wait_for_interrupt_boundary(test: &TestCodex) -> anyhow::Result<()> {
    test.codex
        .submit(Op::ThreadSettings {
            thread_settings: ThreadSettingsOverrides::default(),
        })
        .await?;
    timeout(
        Duration::from_secs(5),
        wait_for_event(&test.codex, |event| {
            matches!(event, EventMsg::ThreadSettingsApplied(_))
        }),
    )
    .await?;
    Ok(())
}

async fn assert_no_follow_up_request(server: &StreamingSseServer) {
    assert!(
        timeout(
            Duration::from_secs(2),
            server.wait_for_request_count(/*count*/ 2),
        )
        .await
        .is_err(),
        "interrupt must not start an automatic follow-up model request",
    );
    assert_eq!(server.requests().await.len(), 1);
}

async fn wait_for_request_count(server: &StreamingSseServer, count: usize) -> anyhow::Result<()> {
    timeout(Duration::from_secs(5), server.wait_for_request_count(count)).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_does_not_resume_after_outstanding_completion_wake() -> anyhow::Result<()> {
    const INTERRUPTED_COMPLETION: &str = "completion with outstanding wake";
    const POST_BOUNDARY_COMPLETION: &str = "completion after outstanding-wake interrupt";
    let (gate_tx, gate_rx) = oneshot::channel();
    let (server, _completions) = start_streaming_sse_server(vec![
        vec![StreamingSseChunk {
            gate: Some(gate_rx),
            body: sse(vec![ev_response_created("interrupt-wake")]),
        }],
        vec![StreamingSseChunk {
            gate: None,
            body: sse(vec![
                ev_response_created("post-interrupt-wake"),
                ev_completed("post-interrupt-wake"),
            ]),
        }],
    ])
    .await;
    let mut builder = test_codex().with_model("gpt-5.4");
    let test = builder.build_with_streaming_server(&server).await?;

    test.codex
        .submit(Op::InterAgentCompletion {
            communication: completion(INTERRUPTED_COMPLETION),
        })
        .await?;
    wait_for_request_count(&server, /*count*/ 1).await?;

    test.codex.submit(Op::Interrupt).await?;
    timeout(
        Duration::from_secs(5),
        wait_for_event(&test.codex, |event| {
            matches!(event, EventMsg::TurnAborted(_))
        }),
    )
    .await?;
    let _ = gate_tx.send(());
    wait_for_interrupt_boundary(&test).await?;
    assert_no_follow_up_request(&server).await;
    test.codex
        .submit(Op::InterAgentCompletion {
            communication: completion(POST_BOUNDARY_COMPLETION),
        })
        .await?;

    wait_for_post_boundary_request(&server, POST_BOUNDARY_COMPLETION).await?;
    timeout(
        Duration::from_secs(5),
        wait_for_event(&test.codex, |event| {
            matches!(event, EventMsg::TurnComplete(_))
        }),
    )
    .await?;
    assert_persisted_completion_once(&test, INTERRUPTED_COMPLETION).await?;
    assert_persisted_completion_once(&test, POST_BOUNDARY_COMPLETION).await?;
    assert_eq!(server.requests().await.len(), 2);
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_does_not_resume_after_completion_buffered_on_finishing_turn()
-> anyhow::Result<()> {
    const INTERRUPTED_COMPLETION: &str = "completion buffered while finishing";
    const POST_BOUNDARY_COMPLETION: &str = "completion after finishing-turn interrupt";
    let (server, _completions) = start_streaming_sse_server(vec![
        vec![StreamingSseChunk {
            gate: None,
            body: sse(vec![
                ev_response_created("interrupt-finishing"),
                ev_completed("interrupt-finishing"),
            ]),
        }],
        vec![StreamingSseChunk {
            gate: None,
            body: sse(vec![
                ev_response_created("post-finishing-interrupt"),
                ev_completed("post-finishing-interrupt"),
            ]),
        }],
    ])
    .await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let mut extensions =
        codex_extension_api::ExtensionRegistryBuilder::<codex_core::config::Config>::new();
    extensions.turn_lifecycle_contributor(Arc::new(FinishingTurnGate {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    }));
    let mut builder = test_codex()
        .with_model("gpt-5.4")
        .with_extensions(Arc::new(extensions.build()));
    let test = builder.build_with_streaming_server(&server).await?;

    test.codex
        .start_or_steer_turn(TurnInputRequest::user_input(Vec::new()))
        .await?;
    wait_for_request_count(&server, /*count*/ 1).await?;
    timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("turn should reach the finishing lifecycle boundary");

    test.codex
        .submit(Op::InterAgentCompletion {
            communication: completion(INTERRUPTED_COMPLETION),
        })
        .await?;
    let submission = timeout(
        Duration::from_secs(5),
        test.codex
            .start_turn_if_idle(TurnInputRequest::user_input(Vec::new())),
    )
    .await??;
    assert_eq!(
        submission,
        StartIfIdleSubmission::NotSubmitted {
            reason: NotSubmittedReason::NotIdle,
        }
    );

    test.codex.submit(Op::Interrupt).await?;
    release.add_permits(1);
    timeout(
        Duration::from_secs(5),
        wait_for_event(&test.codex, |event| {
            matches!(event, EventMsg::TurnComplete(_))
        }),
    )
    .await?;

    wait_for_interrupt_boundary(&test).await?;
    assert_no_follow_up_request(&server).await;
    test.codex
        .submit(Op::InterAgentCompletion {
            communication: completion(POST_BOUNDARY_COMPLETION),
        })
        .await?;
    wait_for_post_boundary_request(&server, POST_BOUNDARY_COMPLETION).await?;
    release.add_permits(1);
    timeout(
        Duration::from_secs(5),
        wait_for_event(&test.codex, |event| {
            matches!(event, EventMsg::TurnComplete(_))
        }),
    )
    .await?;
    assert_persisted_completion_once(&test, INTERRUPTED_COMPLETION).await?;
    assert_persisted_completion_once(&test, POST_BOUNDARY_COMPLETION).await?;
    assert_eq!(server.requests().await.len(), 2);
    server.shutdown().await;
    Ok(())
}
