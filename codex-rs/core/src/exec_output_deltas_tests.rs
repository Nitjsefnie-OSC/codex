use super::*;
use codex_protocol::protocol::Event;
use pretty_assertions::assert_eq;

fn emitter(
    chunking: ExecDeltaChunking,
    max_deltas: usize,
) -> (ExecDeltaEmitter, async_channel::Receiver<Event>) {
    let (tx_event, rx_event) = async_channel::unbounded();
    let stream = StdoutStream {
        sub_id: "sub".to_string(),
        call_id: "call".to_string(),
        tx_event,
        chunking,
    };
    (
        ExecDeltaEmitter::new(stream, /*is_stderr*/ false, max_deltas),
        rx_event,
    )
}

fn drain(rx: &async_channel::Receiver<Event>) -> Vec<Vec<u8>> {
    let mut chunks = Vec::new();
    while let Ok(event) = rx.try_recv() {
        match event.msg {
            EventMsg::ExecCommandOutputDelta(delta) => chunks.push(delta.chunk),
            other => panic!("unexpected event: {other:?}"),
        }
    }
    chunks
}

#[tokio::test]
async fn byte_mode_forwards_each_read_verbatim() {
    let (mut emitter, rx) = emitter(ExecDeltaChunking::Bytes, 100);
    emitter.push(b"par").await;
    emitter.push(b"tial\nline\n").await;
    emitter.flush().await;

    assert_eq!(drain(&rx), vec![b"par".to_vec(), b"tial\nline\n".to_vec()]);
}

#[tokio::test]
async fn line_mode_delivers_each_line_as_its_own_event() {
    let (mut emitter, rx) = emitter(ExecDeltaChunking::Lines, 100);
    emitter.push(b"first\n").await;
    emitter.push(b"second\n").await;
    emitter.push(b"third\n").await;

    assert_eq!(
        drain(&rx),
        vec![
            b"first\n".to_vec(),
            b"second\n".to_vec(),
            b"third\n".to_vec(),
        ]
    );
}

#[tokio::test]
async fn line_mode_holds_back_a_partial_line_until_it_completes() {
    let (mut emitter, rx) = emitter(ExecDeltaChunking::Lines, 100);
    emitter.push(b"half").await;
    assert_eq!(drain(&rx), Vec::<Vec<u8>>::new());

    emitter.push(b"-a-line\n").await;
    assert_eq!(drain(&rx), vec![b"half-a-line\n".to_vec()]);
}

#[tokio::test]
async fn line_mode_batches_lines_that_arrive_together() {
    let (mut emitter, rx) = emitter(ExecDeltaChunking::Lines, 100);
    emitter.push(b"a\nb\nc\nd\n").await;

    // One read carrying four lines is one event, not four.
    assert_eq!(drain(&rx), vec![b"a\nb\nc\nd\n".to_vec()]);
}

#[tokio::test]
async fn line_mode_splits_a_batch_at_the_last_newline() {
    let (mut emitter, rx) = emitter(ExecDeltaChunking::Lines, 100);
    emitter.push(b"a\nb\nc").await;
    assert_eq!(drain(&rx), vec![b"a\nb\n".to_vec()]);

    emitter.push(b"\n").await;
    assert_eq!(drain(&rx), vec![b"c\n".to_vec()]);
}

#[tokio::test]
async fn line_mode_flushes_a_trailing_line_without_a_newline() {
    let (mut emitter, rx) = emitter(ExecDeltaChunking::Lines, 100);
    emitter.push(b"done\nno-newline").await;
    assert_eq!(drain(&rx), vec![b"done\n".to_vec()]);

    emitter.flush().await;
    assert_eq!(drain(&rx), vec![b"no-newline".to_vec()]);
}

#[tokio::test]
async fn line_mode_flush_is_a_no_op_when_nothing_is_held_back() {
    let (mut emitter, rx) = emitter(ExecDeltaChunking::Lines, 100);
    emitter.push(b"whole\n").await;
    assert_eq!(drain(&rx), vec![b"whole\n".to_vec()]);

    emitter.flush().await;
    assert_eq!(drain(&rx), Vec::<Vec<u8>>::new());
    assert_eq!(emitter.emitted(), 1);
}

#[tokio::test]
async fn line_mode_emits_unaligned_output_that_never_contains_a_newline() {
    let (mut emitter, rx) = emitter(ExecDeltaChunking::Lines, 100);
    let newline_free = vec![b'x'; MAX_LINE_DELTA_BYTES + 5];
    emitter.push(&newline_free).await;

    // The bounded payload goes out; only the remainder is held back.
    assert_eq!(drain(&rx), vec![vec![b'x'; MAX_LINE_DELTA_BYTES]]);
    emitter.flush().await;
    assert_eq!(drain(&rx), vec![vec![b'x'; 5]]);
}

#[tokio::test]
async fn the_volume_cap_stops_emission_and_never_reopens() {
    let (mut emitter, rx) = emitter(ExecDeltaChunking::Lines, 3);
    for index in 0..50 {
        emitter.push(format!("line {index}\n").as_bytes()).await;
    }
    emitter.flush().await;

    assert_eq!(emitter.emitted(), 3);
    assert_eq!(
        drain(&rx),
        vec![
            b"line 0\n".to_vec(),
            b"line 1\n".to_vec(),
            b"line 2\n".to_vec(),
        ]
    );
}

#[tokio::test]
async fn the_volume_cap_applies_to_byte_mode_too() {
    let (mut emitter, rx) = emitter(ExecDeltaChunking::Bytes, 2);
    for index in 0..10 {
        emitter.push(format!("{index}").as_bytes()).await;
    }

    assert_eq!(emitter.emitted(), 2);
    assert_eq!(drain(&rx), vec![b"0".to_vec(), b"1".to_vec()]);
}

#[tokio::test]
async fn capped_line_mode_does_not_retain_dropped_output() {
    let (mut emitter, _rx) = emitter(ExecDeltaChunking::Lines, 1);
    emitter.push(b"kept\n").await;
    for _ in 0..1000 {
        emitter.push(b"dropped\n").await;
    }

    assert_eq!(emitter.emitted(), 1);
    assert!(
        emitter.pending.is_empty(),
        "capped output must not accumulate in the pending buffer"
    );
}

#[tokio::test]
async fn stderr_deltas_are_tagged_as_stderr() {
    let (tx_event, rx_event) = async_channel::unbounded();
    let stream = StdoutStream {
        sub_id: "sub".to_string(),
        call_id: "call".to_string(),
        tx_event,
        chunking: ExecDeltaChunking::Lines,
    };
    let mut emitter = ExecDeltaEmitter::new(stream, /*is_stderr*/ true, 10);
    emitter.push(b"boom\n").await;

    let event = rx_event.try_recv().expect("a delta event");
    match event.msg {
        EventMsg::ExecCommandOutputDelta(delta) => {
            assert_eq!(delta.stream, ExecOutputStream::Stderr);
            assert_eq!(delta.call_id, "call");
        }
        other => panic!("unexpected event: {other:?}"),
    }
}
