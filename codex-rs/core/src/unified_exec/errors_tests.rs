use super::PROCESS_FAILED_OUTPUT_MAX_BYTES;
use super::UnifiedExecError;
use pretty_assertions::assert_eq;

#[test]
fn process_failure_bounds_collected_output() {
    let output = [
        vec![b'a'; PROCESS_FAILED_OUTPUT_MAX_BYTES],
        vec![b'b'; PROCESS_FAILED_OUTPUT_MAX_BYTES],
    ]
    .concat();

    let error = UnifiedExecError::process_failed("failure".to_string())
        .with_collected_process_output(&output)
        .to_string();
    let expected = format!(
        "Unified exec process failed: failure\n\nFinal output:\n{}\n... 4096 bytes omitted ...\n{}",
        "a".repeat(PROCESS_FAILED_OUTPUT_MAX_BYTES / 2),
        "b".repeat(PROCESS_FAILED_OUTPUT_MAX_BYTES / 2),
    );

    assert_eq!(error, expected);
}
