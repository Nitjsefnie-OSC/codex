use std::sync::Arc;
use std::time::Duration;

use codex_code_mode_protocol::CellId;
use codex_code_mode_protocol::RuntimeResponse;
use codex_code_mode_protocol::StartedCell;
use codex_code_mode_protocol::host::DelegateRequest;
use codex_code_mode_protocol::host::DelegateRequestId;
use codex_code_mode_protocol::host::DelegateResponse;
use codex_code_mode_protocol::host::EncodedFrame;
use codex_code_mode_protocol::host::HostToClient;
use codex_code_mode_protocol::host::RequestId;
use codex_code_mode_protocol::host::SessionId;
use pretty_assertions::assert_eq;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio_util::sync::CancellationToken;

use super::CellMessage;
use super::CellRoute;
use super::HostPeer;
use super::MAX_PENDING_DELEGATE_CALLS;

fn session_id(value: &str) -> SessionId {
    SessionId::new(value).expect("session ID")
}

#[tokio::test]
async fn start_cell_reports_when_initial_response_is_enqueued() {
    let (outgoing_tx, mut outgoing_rx) = mpsc::channel(/*max_capacity*/ 4);
    let peer = Arc::new(HostPeer::new(outgoing_tx));
    let cell_id = CellId::new("cell-1".to_string());
    let (response_tx, response_rx) = oneshot::channel();
    let started = StartedCell::new(cell_id.clone(), response_rx);
    let active_cell_permits = Arc::new(Semaphore::new(/*permits*/ 1));
    let active_cell_permit = Arc::clone(&active_cell_permits)
        .try_acquire_owned()
        .expect("active cell permit");

    let mut initial_response_sent = peer.start_cell(
        session_id("session-1"),
        RequestId::new(/*value*/ 1),
        started,
        active_cell_permit,
    );
    assert_eq!(initial_response_sent.try_recv(), Err(TryRecvError::Empty));

    response_tx
        .send(RuntimeResponse::Result {
            cell_id: cell_id.clone(),
            content_items: Vec::new(),
            error_text: None,
        })
        .expect("initial response receiver");
    initial_response_sent
        .await
        .expect("initial response completion");
    outgoing_rx.recv().await.expect("initial response frame");
    assert_eq!(active_cell_permits.available_permits(), 0);

    peer.close_cell(session_id("session-1"), cell_id);
    let permit = tokio::time::timeout(
        Duration::from_secs(1),
        Arc::clone(&active_cell_permits).acquire_owned(),
    )
    .await
    .expect("cell permit should be released")
    .expect("cell permit semaphore should remain open");
    drop(permit);
}

#[tokio::test]
async fn pending_delegate_limit_rejects_call_without_disconnecting() {
    let (outgoing_tx, _outgoing_rx) = mpsc::channel(/*max_capacity*/ 1);
    let peer = Arc::new(HostPeer::new(outgoing_tx));
    let permits = Arc::clone(&peer.delegate_permits)
        .acquire_many_owned(MAX_PENDING_DELEGATE_CALLS as u32)
        .await
        .expect("delegate permits");

    let result = peer
        .call(
            session_id("session-1"),
            DelegateRequest::Notify {
                call_id: "call-1".to_string(),
                cell_id: CellId::new("cell-1".to_string()).into(),
                text: "hello".to_string(),
            },
            CancellationToken::new(),
        )
        .await;

    assert_eq!(
        result,
        Err("code-mode host has too many pending delegate calls".to_string())
    );
    assert!(!peer.is_disconnected());
    drop(permits);
}

#[tokio::test]
async fn tool_result_delivery_uses_its_dedicated_delegate_lane() {
    let (outgoing_tx, mut outgoing_rx) = mpsc::channel(/*max_capacity*/ 1);
    let peer = Arc::new(HostPeer::new(outgoing_tx));
    let session_id = session_id("session-1");
    let cell_id = CellId::new("cell-1".to_string());
    let (_response_tx, response_rx) = oneshot::channel();
    let active_cell_permits = Arc::new(Semaphore::new(/*permits*/ 1));
    let active_cell_permit = Arc::clone(&active_cell_permits)
        .try_acquire_owned()
        .expect("active cell permit");
    let _initial_response = peer.start_cell(
        session_id.clone(),
        RequestId::new(/*value*/ 1),
        StartedCell::new(cell_id.clone(), response_rx),
        active_cell_permit,
    );
    let ordinary_permits = Arc::clone(&peer.delegate_permits)
        .acquire_many_owned(MAX_PENDING_DELEGATE_CALLS as u32)
        .await
        .expect("ordinary delegate permits");
    peer.outgoing_tx
        .try_send(
            EncodedFrame::encode(&HostToClient::CellClosed {
                session_id: session_id.clone(),
                cell_id: CellId::new("unrelated-cell".to_string()).into(),
            })
            .expect("encode ordinary control frame"),
        )
        .expect("fill ordinary control lane");

    let receipt_peer = Arc::clone(&peer);
    let receipt = tokio::spawn(async move {
        receipt_peer
            .call_tool_result_delivered(session_id, cell_id, "runtime-call-1".to_string(), true)
            .await
    });
    tokio::task::yield_now().await;
    assert!(!peer.is_disconnected());
    let blocked_frame = outgoing_rx.recv().await.expect("blocked control frame");
    assert!(matches!(
        EncodedFrame::decode_framed::<HostToClient>(&blocked_frame.into_framed_bytes())
            .expect("decode blocked control frame"),
        HostToClient::CellClosed { .. }
    ));
    let frame = outgoing_rx.recv().await.expect("delivery receipt frame");
    let message = EncodedFrame::decode_framed::<HostToClient>(&frame.into_framed_bytes())
        .expect("decode delivery receipt frame");
    let HostToClient::DelegateRequest {
        id,
        request:
            DelegateRequest::ToolResultDelivered {
                runtime_tool_call_id,
                delivered,
                ..
            },
        ..
    } = message
    else {
        panic!("expected typed tool-result delivery request");
    };
    assert_eq!(runtime_tool_call_id, "runtime-call-1");
    assert!(delivered);
    peer.complete(id, Ok(DelegateResponse::ToolResultDeliveryRecorded))
        .await;
    assert_eq!(
        receipt.await.expect("delivery receipt task"),
        Ok(DelegateResponse::ToolResultDeliveryRecorded)
    );
    assert_eq!(peer.delegate_permits.available_permits(), 0);
    drop(ordinary_permits);
}

#[tokio::test]
async fn full_ordinary_cell_queue_preserves_the_delivery_receipt_slot() {
    let (outgoing_tx, _outgoing_rx) = mpsc::channel(/*max_capacity*/ 1);
    let peer = Arc::new(HostPeer::new(outgoing_tx));
    let session_id = session_id("session-1");
    let cell_id = CellId::new("cell-1".to_string());
    let key = (session_id.clone(), cell_id.clone());
    for value in 1..=MAX_PENDING_DELEGATE_CALLS {
        let (dispatched_tx, _dispatched_rx) = oneshot::channel();
        peer.route_cell_message(
            key.clone(),
            CellMessage::Delegate {
                id: DelegateRequestId::new(value as i64),
                request: DelegateRequest::Notify {
                    call_id: format!("call-{value}"),
                    cell_id: cell_id.clone().into(),
                    text: "pending".to_string(),
                },
                dispatched_tx,
            },
        )
        .expect("queue ordinary delegate");
    }

    let receipt_peer = Arc::clone(&peer);
    let receipt = tokio::spawn(async move {
        receipt_peer
            .call_tool_result_delivered(session_id, cell_id, "runtime-call-1".to_string(), true)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let has_receipt = {
                let routes = peer.cell_routes.lock().expect("cell routes lock");
                matches!(
                    routes.get(&key),
                    Some(CellRoute::Pending(messages))
                        if messages.len() == MAX_PENDING_DELEGATE_CALLS + 1
                            && matches!(
                                messages.back(),
                                Some(CellMessage::Delegate {
                                    request: DelegateRequest::ToolResultDelivered { .. },
                                    ..
                                })
                            )
                )
            };
            if has_receipt {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("delivery receipt should use the reserved cell queue slot");
    assert!(!peer.is_disconnected());
    peer.disconnect();
    assert_eq!(
        receipt.await.expect("delivery receipt task"),
        Err("code-mode client connection closed".to_string())
    );
}

#[tokio::test]
async fn ordinary_delegate_backpressure_cannot_disconnect_before_delivery_receipt() {
    const ORDINARY_REQUESTS: usize = 130;

    let (outgoing_tx, mut outgoing_rx) = mpsc::channel(/*max_capacity*/ 1);
    let peer = Arc::new(HostPeer::new(outgoing_tx));
    let session_id = session_id("session-1");
    let cell_id = CellId::new("cell-1".to_string());
    let (_response_tx, response_rx) = oneshot::channel();
    let active_cell_permits = Arc::new(Semaphore::new(/*permits*/ 1));
    let active_cell_permit = Arc::clone(&active_cell_permits)
        .try_acquire_owned()
        .expect("active cell permit");
    let _initial_response = peer.start_cell(
        session_id.clone(),
        RequestId::new(/*value*/ 1),
        StartedCell::new(cell_id.clone(), response_rx),
        active_cell_permit,
    );

    let mut ordinary = Vec::new();
    for value in 0..ORDINARY_REQUESTS {
        let call_peer = Arc::clone(&peer);
        let call_session = session_id.clone();
        let call_cell = cell_id.clone();
        ordinary.push(tokio::spawn(async move {
            call_peer
                .call(
                    call_session,
                    DelegateRequest::Notify {
                        call_id: format!("call-{value}"),
                        cell_id: call_cell.into(),
                        text: "pending".to_string(),
                    },
                    CancellationToken::new(),
                )
                .await
        }));
        tokio::time::timeout(Duration::from_secs(1), async {
            while peer.pending.lock().await.len() <= value {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ordinary delegate admission");
    }
    let receipt_peer = Arc::clone(&peer);
    let receipt = tokio::spawn(async move {
        receipt_peer
            .call_tool_result_delivered(session_id, cell_id, "runtime-call-1".to_string(), true)
            .await
    });

    let mut ordinary_seen = 0;
    let receipt_id = loop {
        let frame = outgoing_rx.recv().await.expect("delegate request frame");
        let HostToClient::DelegateRequest { id, request, .. } =
            EncodedFrame::decode_framed::<HostToClient>(&frame.into_framed_bytes())
                .expect("decode delegate request")
        else {
            panic!("expected delegate request");
        };
        match request {
            DelegateRequest::Notify { .. } => {
                ordinary_seen += 1;
                peer.complete(id, Ok(DelegateResponse::NotificationDelivered))
                    .await;
            }
            DelegateRequest::ToolResultDelivered { .. } => break id,
            DelegateRequest::InvokeTool { .. } => panic!("unexpected tool invocation"),
        }
    };
    assert_eq!(ordinary_seen, ORDINARY_REQUESTS);
    assert!(!peer.is_disconnected());
    peer.complete(receipt_id, Ok(DelegateResponse::ToolResultDeliveryRecorded))
        .await;
    assert_eq!(
        receipt.await.expect("delivery receipt task"),
        Ok(DelegateResponse::ToolResultDeliveryRecorded)
    );
    for call in ordinary {
        assert_eq!(
            call.await.expect("ordinary delegate task"),
            Ok(DelegateResponse::NotificationDelivered)
        );
    }
}
