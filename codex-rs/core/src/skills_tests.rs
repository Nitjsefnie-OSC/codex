use std::sync::Arc;

use codex_extension_api::ExtensionData;
use codex_hooks::SkillActivation;
use codex_hooks::SkillActivationKind;
use codex_hooks::SkillActivationScope;
use pretty_assertions::assert_eq;

use super::discard_pending_skill_activation;
use super::promote_pending_skill_activation;
use super::record_skill_activation;
use super::retain_pending_skill_activation;
use super::skill_activation_snapshot;
use crate::session::tests::make_session_and_context;

fn activation(name: &str, path: &str, digest_digit: char) -> SkillActivation {
    SkillActivation::new(
        name.to_string(),
        path.to_string(),
        SkillActivationScope::Repo,
        SkillActivationKind::Explicit,
        "turn-1".to_string(),
        digest_digit.to_string().repeat(64),
    )
    .expect("valid activation")
}

#[tokio::test]
async fn skill_activation_store_collapses_only_exact_duplicate_records_in_stable_order() {
    let (_session, turn) = make_session_and_context().await;
    let first_hash = activation("review", "/repo/b/SKILL.md", 'a');
    let second_hash = activation("review", "/repo/b/SKILL.md", 'b');
    let earlier = activation("format", "/repo/a/SKILL.md", 'c');

    record_skill_activation(&turn, second_hash.clone());
    record_skill_activation(&turn, first_hash.clone());
    record_skill_activation(&turn, first_hash.clone());
    record_skill_activation(&turn, earlier.clone());

    assert_eq!(
        skill_activation_snapshot(&turn),
        vec![earlier, first_hash, second_hash]
    );
}

#[tokio::test]
async fn skill_activation_store_is_turn_local() {
    let (_session_a, mut turn_a) = make_session_and_context().await;
    let (_session_b, mut turn_b) = make_session_and_context().await;
    turn_a.extension_data = Arc::new(ExtensionData::new("turn-a"));
    turn_b.extension_data = Arc::new(ExtensionData::new("turn-b"));
    let activation_a = activation("first", "/repo/first/SKILL.md", 'a');
    let activation_b = activation("second", "/repo/second/SKILL.md", 'b');

    record_skill_activation(&turn_a, activation_a.clone());
    record_skill_activation(&turn_b, activation_b.clone());

    assert_eq!(skill_activation_snapshot(&turn_a), vec![activation_a]);
    assert_eq!(skill_activation_snapshot(&turn_b), vec![activation_b]);
}

#[tokio::test]
async fn pending_skill_activation_is_hidden_until_promoted_and_can_be_discarded() {
    let (_session, turn) = make_session_and_context().await;
    let promoted = activation("promoted", "/repo/promoted/SKILL.md", 'a');
    let discarded = activation("discarded", "/repo/discarded/SKILL.md", 'b');

    retain_pending_skill_activation(&turn, 7, promoted.clone());
    retain_pending_skill_activation(&turn, 8, discarded);
    assert_eq!(skill_activation_snapshot(&turn), Vec::new());

    assert_eq!(promote_pending_skill_activation(&turn, 7), true);
    assert_eq!(promote_pending_skill_activation(&turn, 7), false);
    assert_eq!(discard_pending_skill_activation(&turn, 8), true);
    assert_eq!(discard_pending_skill_activation(&turn, 8), false);
    assert_eq!(skill_activation_snapshot(&turn), vec![promoted]);
}
