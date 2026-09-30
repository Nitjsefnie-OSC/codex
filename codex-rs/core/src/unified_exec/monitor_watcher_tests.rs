use super::*;
use pretty_assertions::assert_eq;

fn split(input: &[u8]) -> (Vec<String>, Vec<u8>) {
    let mut pending = input.to_vec();
    let mut batch = Vec::new();
    take_complete_lines(&mut pending, &mut batch);
    (batch, pending)
}

#[test]
fn only_complete_lines_are_batched() {
    let (batch, pending) = split(b"first\nsecond\nthi");

    assert_eq!(batch, vec!["first".to_string(), "second".to_string()]);
    assert_eq!(pending, b"thi".to_vec());
}

#[test]
fn a_partial_line_is_held_back_until_it_terminates() {
    let mut pending = b"half".to_vec();
    let mut batch = Vec::new();

    take_complete_lines(&mut pending, &mut batch);
    assert!(batch.is_empty(), "an unterminated line is not a line yet");

    pending.extend_from_slice(b"-line\n");
    take_complete_lines(&mut pending, &mut batch);

    assert_eq!(batch, vec!["half-line".to_string()]);
    assert!(pending.is_empty());
}

#[test]
fn carriage_returns_are_stripped_from_line_ends() {
    let (batch, _) = split(b"windows\r\nunix\n");

    assert_eq!(batch, vec!["windows".to_string(), "unix".to_string()]);
}

#[test]
fn a_line_that_never_terminates_is_flushed_rather_than_buffered_forever() {
    let progress_bar = vec![b'#'; MAX_PARTIAL_LINE_BYTES + 16];

    let (batch, pending) = split(&progress_bar);

    assert_eq!(batch.len(), 1, "the runaway line is emitted unaligned");
    assert_eq!(batch[0].len(), progress_bar.len());
    assert!(pending.is_empty());
}

#[test]
fn a_notification_carries_at_most_its_line_cap() {
    let lines: Vec<String> = (0..MAX_LINES_PER_NOTIFICATION + 7)
        .map(|index| format!("line-{index}"))
        .collect();

    let (kept, omitted) = cap_lines(lines);

    assert_eq!(kept.len(), MAX_LINES_PER_NOTIFICATION);
    assert_eq!(omitted, 7);
}

#[test]
fn a_notification_stays_within_its_byte_budget() {
    let long = "x".repeat(MAX_NOTIFICATION_BYTES / 2 + 1);
    let lines = vec![long.clone(), long.clone(), long];

    let (kept, omitted) = cap_lines(lines);

    let total: usize = kept.iter().map(String::len).sum();
    assert!(
        total <= MAX_NOTIFICATION_BYTES,
        "kept {total} bytes, budget is {MAX_NOTIFICATION_BYTES}"
    );
    assert_eq!(omitted, 1);
}

#[test]
fn one_over_long_line_is_truncated_instead_of_producing_an_empty_batch() {
    let lines = vec!["y".repeat(MAX_NOTIFICATION_BYTES * 2)];

    let (kept, omitted) = cap_lines(lines);

    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].len(), MAX_NOTIFICATION_BYTES);
    assert_eq!(omitted, 0);
}

#[test]
fn truncation_does_not_split_a_multibyte_character() {
    let line = "é".repeat(10);

    // 15 bytes lands mid-character: `é` is two bytes, so the cut must fall back.
    let truncated = truncate_on_char_boundary(&line, 15);

    assert_eq!(truncated, "é".repeat(7));
}
