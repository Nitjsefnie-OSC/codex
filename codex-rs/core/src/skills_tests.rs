use std::path::Path;
use std::sync::Arc;

use codex_config::ConfigLayerEntry;
use codex_config::ConfigLayerSource;
use codex_config::ConfigLayerStack;
use codex_config::ConfigRequirementsToml;
use codex_exec_server::LOCAL_ENVIRONMENT_ID;
use codex_exec_server::LOCAL_FS;
use codex_extension_api::ExtensionData;
use codex_hooks::SkillActivation;
use codex_hooks::SkillActivationKind;
use codex_hooks::SkillActivationScope;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::SkillScope;
use codex_skills::system_cache_root_dir;
use codex_skills_extension::HostSkillsLoadInput;
use codex_skills_extension::HostSkillsService;
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_absolute_path::test_support::PathBufExt;
use codex_utils_path_uri::PathUri;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::discard_pending_skill_activation;
use super::prepare_implicit_skill_activation;
use super::promote_pending_skill_activation;
use super::record_skill_activation;
use super::retain_pending_skill_activation;
use super::skill_activation_snapshot;
use crate::config::PermissionProfileSnapshot;
use crate::environment_selection::TurnEnvironmentState;
use crate::session::session::Session;
use crate::session::tests::make_session_and_context;
use crate::session::turn_context::TurnContext;

pub(crate) const ORIGINAL_SKILL_SOURCE: &str =
    "---\nname: audit-skill\ndescription: Audit reads.\n---\noriginal body\n";
pub(crate) const EDITED_SKILL_SOURCE: &str =
    "---\nname: audit-skill\ndescription: Audit reads.\n---\nedited body\n";

pub(crate) struct ImplicitSkillFixture {
    pub(crate) session: Session,
    pub(crate) turn: TurnContext,
    pub(crate) _temp_dir: TempDir,
    pub(crate) skill_path: AbsolutePathBuf,
    pub(crate) workdir: AbsolutePathBuf,
}

pub(crate) async fn implicit_skill_fixture(scope: SkillScope) -> ImplicitSkillFixture {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let codex_home = temp_dir.path().to_path_buf().abs();
    let (skills_root, layer_source, bundled_skills_enabled) = match scope {
        SkillScope::User => (
            codex_home.join("skills"),
            ConfigLayerSource::User {
                file: codex_home.join("config.toml"),
                profile: None,
            },
            false,
        ),
        SkillScope::Repo => {
            let dot_codex_folder = codex_home.join(".codex");
            (
                dot_codex_folder.join("skills"),
                ConfigLayerSource::Project { dot_codex_folder },
                false,
            )
        }
        SkillScope::System => (
            system_cache_root_dir(&codex_home),
            ConfigLayerSource::User {
                file: codex_home.join("config.toml"),
                profile: None,
            },
            true,
        ),
        SkillScope::Admin => (
            codex_home.join("skills"),
            ConfigLayerSource::System {
                file: codex_home.join("config.toml"),
            },
            false,
        ),
    };
    let skills_service = HostSkillsService::new(codex_home.clone(), bundled_skills_enabled);
    let skill_dir = skills_root.join("audit-skill");
    std::fs::create_dir_all(skill_dir.as_path()).expect("create skill dir");
    let skill_path = skill_dir.join("SKILL.md");
    std::fs::write(skill_path.as_path(), ORIGINAL_SKILL_SOURCE).expect("write skill source");
    let config_layer_stack = ConfigLayerStack::new(
        vec![ConfigLayerEntry::new(
            layer_source,
            toml::Value::Table(Default::default()),
        )],
        Default::default(),
        ConfigRequirementsToml::default(),
    )
    .expect("fixture config stack should be valid");
    let skills_input = HostSkillsLoadInput::new(codex_home.clone(), Vec::new(), config_layer_stack);
    let (session, turn) = make_session_and_context().await;
    let snapshot = skills_service
        .snapshot_for_config(&skills_input, Some(Arc::clone(&LOCAL_FS)))
        .await;
    assert_eq!(snapshot.outcome().errors, Vec::new());
    assert_eq!(
        snapshot
            .outcome()
            .skills
            .iter()
            .find(|skill| skill.path_to_skills_md == skill_path)
            .map(|skill| (skill.name.as_str(), skill.scope)),
        Some(("audit-skill", scope))
    );
    turn.extension_data.insert(snapshot);
    ImplicitSkillFixture {
        session,
        turn,
        _temp_dir: temp_dir,
        skill_path,
        workdir: skill_dir,
    }
}

pub(crate) fn configure_implicit_skill_fixture_for_exec(
    fixture: &mut ImplicitSkillFixture,
    permission_profile: PermissionProfile,
) {
    let mut config = (*fixture.turn.config).clone();
    config
        .permissions
        .set_permission_profile(permission_profile.clone())
        .expect("test setup should allow updating the permission profile");
    config.permissions.approval_policy =
        codex_config::Constrained::allow_any(AskForApproval::Never);
    fixture.turn.config = Arc::new(config);
    let TurnEnvironmentState::Ready(environment) = fixture
        .turn
        .environments
        .environments
        .first_mut()
        .expect("test session should have a primary environment")
    else {
        panic!("test session primary environment should be ready");
    };
    environment.config.permission_profile = PermissionProfileSnapshot::legacy(permission_profile);
}

pub(crate) fn quote_skill_test_path(path: &Path) -> String {
    shlex::try_quote(path.to_string_lossy().as_ref())
        .expect("skill test path should be shell-quotable")
        .into_owned()
}

async fn prepare_test_implicit_skill_activation(
    session: &Session,
    turn: &TurnContext,
    command: &str,
    workdir: &AbsolutePathBuf,
) -> Option<SkillActivation> {
    prepare_implicit_skill_activation(
        session,
        turn,
        command,
        &PathUri::from_abs_path(workdir),
        Some(workdir),
        LOCAL_ENVIRONMENT_ID,
    )
    .await
}

pub(crate) async fn assert_implicit_skill_candidate(fixture: &ImplicitSkillFixture, command: &str) {
    let cwd = fixture
        .turn
        .environments
        .primary()
        .expect("test session should have a primary environment")
        .cwd()
        .to_abs_path()
        .expect("test session should use a native cwd");
    assert!(
        prepare_test_implicit_skill_activation(&fixture.session, &fixture.turn, command, &cwd)
            .await
            .is_some(),
        "exact handler command should prepare an implicit skill candidate: {command}"
    );
}

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

#[tokio::test]
async fn implicit_skill_activation_candidate_reads_current_source_and_keeps_distinct_digests() {
    let fixture = implicit_skill_fixture(SkillScope::Repo).await;
    let command = format!("cat {}", quote_skill_test_path(&fixture.skill_path));

    let original = prepare_test_implicit_skill_activation(
        &fixture.session,
        &fixture.turn,
        &command,
        &fixture.workdir,
    )
    .await
    .expect("recognized readable skill");
    assert_eq!(original.name(), "audit-skill");
    assert_eq!(original.path(), fixture.skill_path.to_string_lossy());
    assert_eq!(original.scope(), SkillActivationScope::Repo);
    assert_eq!(original.invocation(), SkillActivationKind::Implicit);
    assert_eq!(original.turn_id(), fixture.turn.sub_id);
    assert_eq!(
        original.content_sha256(),
        "d64fe583cfaa5c1380c94f44fd031f7d9132bff42881165ef5f8cc61f66751c0"
    );
    record_skill_activation(&fixture.turn, original.clone());

    std::fs::write(fixture.skill_path.as_path(), EDITED_SKILL_SOURCE).expect("edit skill source");
    let edited = prepare_test_implicit_skill_activation(
        &fixture.session,
        &fixture.turn,
        &command,
        &fixture.workdir,
    )
    .await
    .expect("recognized edited skill");
    assert_eq!(
        edited.content_sha256(),
        "c5319c3b098c62f7590ab7a2334e8fa34bfe4e81f39e83b671e2b717ad812b9e"
    );
    record_skill_activation(&fixture.turn, edited.clone());

    assert_eq!(
        skill_activation_snapshot(&fixture.turn),
        vec![edited, original]
    );
}

#[tokio::test]
async fn implicit_skill_activation_candidate_repeated_unchanged_read_collapses_exact_record() {
    let fixture = implicit_skill_fixture(SkillScope::User).await;
    let command = format!("cat {}", quote_skill_test_path(&fixture.skill_path));

    let first = prepare_test_implicit_skill_activation(
        &fixture.session,
        &fixture.turn,
        &command,
        &fixture.workdir,
    )
    .await
    .expect("first recognized read");
    let second = prepare_test_implicit_skill_activation(
        &fixture.session,
        &fixture.turn,
        &command,
        &fixture.workdir,
    )
    .await
    .expect("analytics dedup must not suppress candidate creation");
    assert_eq!(first, second);

    record_skill_activation(&fixture.turn, first.clone());
    record_skill_activation(&fixture.turn, second);
    assert_eq!(skill_activation_snapshot(&fixture.turn), vec![first]);
}

#[tokio::test]
async fn implicit_skill_activation_candidate_skips_unrelated_or_unreadable_source() {
    let fixture = implicit_skill_fixture(SkillScope::System).await;
    assert_eq!(
        prepare_test_implicit_skill_activation(
            &fixture.session,
            &fixture.turn,
            "cat unrelated.txt",
            &fixture.workdir,
        )
        .await,
        None
    );

    std::fs::remove_file(fixture.skill_path.as_path()).expect("remove skill source");
    let recognized_command = format!("cat {}", quote_skill_test_path(&fixture.skill_path));
    assert_eq!(
        prepare_test_implicit_skill_activation(
            &fixture.session,
            &fixture.turn,
            &recognized_command,
            &fixture.workdir,
        )
        .await,
        None
    );
    assert_eq!(skill_activation_snapshot(&fixture.turn), Vec::new());
}
