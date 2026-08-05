//! Delivery of live `ExecCommandOutputDelta` events for a running child process.
//!
//! Two shapes are supported. [`ExecDeltaChunking::Bytes`] forwards each read as
//! it arrives, which is what the shell tools have always done. `Lines` holds
//! back a trailing partial line so every emitted delta ends on a line boundary,
//! which lets a consumer render whole lines while a long-lived command is still
//! running. Batching is implicit in both: one read of the pipe becomes at most
//! one event, so a firehose produces large line-aligned events rather than one
//! event per line.

use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ExecCommandOutputDeltaEvent;
use codex_protocol::protocol::ExecOutputStream;

use crate::exec::StdoutStream;

/// Upper bound for a single line-mode delta payload.
///
/// Output without any newline (a progress bar, a binary dump) must still reach
/// the consumer instead of accumulating forever, so once the held-back bytes
/// reach this size they are emitted unaligned.
const MAX_LINE_DELTA_BYTES: usize = 8192;

/// How raw child output is cut into `ExecCommandOutputDelta` payloads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExecDeltaChunking {
    /// Emit each read verbatim. Payload boundaries follow read boundaries.
    #[default]
    Bytes,
    /// Emit only complete lines; hold back a trailing partial line until the
    /// rest of it arrives or the stream ends.
    Lines,
}

/// Emits `ExecCommandOutputDelta` events for one stream of one child process,
/// enforcing the per-call volume cap.
pub(crate) struct ExecDeltaEmitter {
    stream: StdoutStream,
    output_stream: ExecOutputStream,
    chunking: ExecDeltaChunking,
    max_deltas: usize,
    emitted: usize,
    pending: Vec<u8>,
}

impl ExecDeltaEmitter {
    pub(crate) fn new(stream: StdoutStream, is_stderr: bool, max_deltas: usize) -> Self {
        let chunking = stream.chunking;
        Self {
            stream,
            output_stream: if is_stderr {
                ExecOutputStream::Stderr
            } else {
                ExecOutputStream::Stdout
            },
            chunking,
            max_deltas,
            emitted: 0,
            pending: Vec::new(),
        }
    }

    /// Number of deltas emitted so far. The cap silently drops everything past
    /// `max_deltas`; aggregated output is unaffected.
    #[cfg(test)]
    pub(crate) fn emitted(&self) -> usize {
        self.emitted
    }

    /// Feed the bytes from one read of the child's pipe.
    pub(crate) async fn push(&mut self, bytes: &[u8]) {
        match self.chunking {
            ExecDeltaChunking::Bytes => self.send(bytes.to_vec()).await,
            ExecDeltaChunking::Lines => {
                self.pending.extend_from_slice(bytes);
                while let Some(chunk) = self.take_ready() {
                    self.send(chunk).await;
                }
            }
        }
    }

    /// Emit any bytes still held back. Call once the stream has reached EOF so a
    /// final line without a trailing newline is not lost.
    pub(crate) async fn flush(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let chunk = std::mem::take(&mut self.pending);
        self.send(chunk).await;
    }

    /// Split off the next line-aligned payload, if one is ready.
    fn take_ready(&mut self) -> Option<Vec<u8>> {
        let limit = self.pending.len().min(MAX_LINE_DELTA_BYTES);
        if let Some(index) = self.pending[..limit]
            .iter()
            .rposition(|byte| *byte == b'\n')
        {
            return Some(self.pending.drain(..=index).collect());
        }
        if self.pending.len() >= MAX_LINE_DELTA_BYTES {
            return Some(self.pending.drain(..MAX_LINE_DELTA_BYTES).collect());
        }
        None
    }

    async fn send(&mut self, chunk: Vec<u8>) {
        if chunk.is_empty() || self.emitted >= self.max_deltas {
            return;
        }
        let event = Event {
            id: self.stream.sub_id.clone(),
            msg: EventMsg::ExecCommandOutputDelta(ExecCommandOutputDeltaEvent {
                call_id: self.stream.call_id.clone(),
                stream: self.output_stream.clone(),
                chunk,
            }),
        };
        #[allow(clippy::let_unit_value)]
        let _ = self.stream.tx_event.send(event).await;
        self.emitted += 1;
    }
}

#[cfg(test)]
#[path = "exec_output_deltas_tests.rs"]
mod tests;
