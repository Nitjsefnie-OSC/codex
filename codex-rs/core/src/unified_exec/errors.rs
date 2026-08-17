use super::head_tail_buffer::HeadTailBuffer;
use codex_protocol::exec_output::ExecToolCallOutput;
use codex_utils_path_uri::PathUri;
use std::num::NonZeroUsize;
use thiserror::Error;

const PROCESS_FAILED_OUTPUT_MAX_BYTES: usize = 4 * 1024;

#[derive(Debug, Error)]
pub(crate) enum UnifiedExecError {
    #[error("Failed to create unified exec process: {message}")]
    CreateProcess { message: String },
    #[error("Unified exec process failed: {message}")]
    ProcessFailed { message: String },
    // The model is trained on `session_id`, but internally we track a `process_id`.
    #[error("Unknown process id {process_id}")]
    UnknownProcessId { process_id: i32 },
    #[error("failed to write to stdin")]
    WriteToStdin,
    #[error(
        "stdin is closed for this session; rerun exec_command with tty=true to keep stdin open"
    )]
    StdinClosed,
    #[error("missing command line for unified exec request")]
    MissingCommandLine,
    #[error("Command denied by sandbox: {message}")]
    SandboxDenied {
        message: String,
        output: ExecToolCallOutput,
        original_token_count: Option<usize>,
        output_omitted_bytes: Option<NonZeroUsize>,
    },
    #[error("{path} is not valid on {}", std::env::consts::OS)]
    ForeignPath { path: PathUri },
}

impl UnifiedExecError {
    pub(crate) fn create_process(message: String) -> Self {
        Self::CreateProcess { message }
    }

    pub(crate) fn process_failed(message: String) -> Self {
        Self::ProcessFailed { message }
    }

    /// Preserve output drained by `write_stdin` when a stored process reports
    /// a deferred failure. This error bypasses normal tool-output truncation,
    /// so cap the model-visible output here.
    pub(crate) fn with_collected_process_output(self, output: &[u8]) -> Self {
        match self {
            Self::ProcessFailed { message } if !output.is_empty() => {
                let mut bounded_output =
                    HeadTailBuffer::<PROCESS_FAILED_OUTPUT_MAX_BYTES>::default();
                bounded_output.push_chunk(output);
                Self::ProcessFailed {
                    message: format!(
                        "{message}\n\nFinal output:\n{}",
                        String::from_utf8_lossy(&bounded_output.to_bytes_with_omission_marker())
                    ),
                }
            }
            other => other,
        }
    }

    pub(crate) fn sandbox_denied(message: String, output: ExecToolCallOutput) -> Self {
        Self::SandboxDenied {
            message,
            output,
            original_token_count: None,
            output_omitted_bytes: None,
        }
    }

    pub(crate) fn with_output_collection_metadata(
        self,
        original_token_count: usize,
        output_omitted_bytes: Option<NonZeroUsize>,
    ) -> Self {
        match self {
            Self::SandboxDenied {
                message, output, ..
            } => Self::SandboxDenied {
                message,
                output,
                original_token_count: Some(original_token_count),
                output_omitted_bytes,
            },
            other => other,
        }
    }
}

#[cfg(test)]
#[path = "errors_tests.rs"]
mod tests;
