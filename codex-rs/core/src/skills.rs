use crate::config::Config;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use codex_analytics::InvocationType;
use codex_analytics::SkillInvocation;
use codex_analytics::TrackEventsContext;
use codex_analytics::build_track_events_context;
use codex_extension_api::SkillInvocationInput;
use codex_extension_api::SkillInvocationKind;
use codex_hooks::SkillActivation;
use codex_hooks::SkillActivationKind;
use codex_hooks::SkillActivationScope;
use codex_otel::sanitize_metric_tag_value;
use codex_protocol::protocol::SkillScope;
use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_plugins::PluginSkillRoot;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Mutex as StdMutex;
use std::sync::PoisonError;
use tokio::sync::Mutex;

pub use codex_skills::SkillError;
pub use codex_skills::SkillMetadata;
pub use codex_skills::SkillPolicy;
pub use codex_skills::build_skill_name_counts;
pub use codex_skills::collect_explicit_skill_mentions;
pub use codex_skills::detect_implicit_skill_invocation_for_command;
pub use codex_skills_extension::HostSkillsLoadInput;
pub use codex_skills_extension::HostSkillsService;
use codex_skills_extension::InjectedHostSkill;
pub use codex_skills_extension::SkillLoadOutcome;
pub use codex_skills_extension::bundled_skills_enabled_from_stack;

#[derive(Debug, Default)]
struct ImplicitSkillInvocations(Mutex<HashSet<String>>);

#[derive(Debug, Default)]
struct SkillActivations(StdMutex<SkillActivationState>);

#[derive(Debug, Default)]
struct SkillActivationState {
    active: BTreeSet<SkillActivation>,
    pending_by_process_id: HashMap<i32, SkillActivation>,
}

pub(crate) fn record_skill_activation(turn_context: &TurnContext, activation: SkillActivation) {
    skill_activation_state(turn_context)
        .lock()
        .active
        .insert(activation);
}

pub(crate) fn skill_activation_snapshot(turn_context: &TurnContext) -> Vec<SkillActivation> {
    skill_activation_state(turn_context)
        .lock()
        .active
        .iter()
        .cloned()
        .collect()
}

pub(crate) fn retain_pending_skill_activation(
    turn_context: &TurnContext,
    process_id: i32,
    activation: SkillActivation,
) {
    skill_activation_state(turn_context)
        .lock()
        .pending_by_process_id
        .insert(process_id, activation);
}

pub(crate) fn promote_pending_skill_activation(
    turn_context: &TurnContext,
    process_id: i32,
) -> bool {
    let state = skill_activation_state(turn_context);
    let mut state = state.lock();
    let Some(activation) = state.pending_by_process_id.remove(&process_id) else {
        return false;
    };
    state.active.insert(activation);
    true
}

pub(crate) fn discard_pending_skill_activation(
    turn_context: &TurnContext,
    process_id: i32,
) -> bool {
    skill_activation_state(turn_context)
        .lock()
        .pending_by_process_id
        .remove(&process_id)
        .is_some()
}

#[cfg(test)]
pub(crate) fn has_pending_skill_activation(turn_context: &TurnContext, process_id: i32) -> bool {
    skill_activation_state(turn_context)
        .lock()
        .pending_by_process_id
        .contains_key(&process_id)
}

fn skill_activation_state(turn_context: &TurnContext) -> std::sync::Arc<SkillActivations> {
    turn_context
        .extension_data
        .get_or_init(SkillActivations::default)
}

impl SkillActivations {
    fn lock(&self) -> std::sync::MutexGuard<'_, SkillActivationState> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

pub(crate) fn skills_load_input_from_config(
    config: &Config,
    effective_skill_roots: Vec<PluginSkillRoot>,
) -> HostSkillsLoadInput {
    HostSkillsLoadInput::new(
        config.cwd.clone(),
        effective_skill_roots,
        config.config_layer_stack.clone(),
        config.bundled_skills_enabled(),
    )
}

pub(crate) fn emit_explicit_skill_invocations(
    sess: &Session,
    turn_context: &TurnContext,
    mentioned_skills: &[SkillMetadata],
    injected_skills: &[InjectedHostSkill],
    tracking: TrackEventsContext,
) {
    let injected_skill_paths = injected_skills
        .iter()
        .map(|injected| &injected.skill.path_to_skills_md)
        .collect::<HashSet<_>>();
    for skill in mentioned_skills {
        let skill_name_tag = sanitize_metric_tag_value(skill.name.as_str());
        let status = if injected_skill_paths.contains(&skill.path_to_skills_md) {
            "ok"
        } else {
            "error"
        };
        turn_context.session_telemetry.counter(
            "codex.skill.injected",
            /*inc*/ 1,
            &[
                ("status", status),
                ("skill", skill_name_tag.as_str()),
                ("invoke_type", "explicit"),
            ],
        );
    }

    let invocations = injected_skills
        .iter()
        .map(|injected| SkillInvocation {
            skill_name: injected.skill.name.clone(),
            skill_scope: injected.skill.scope,
            skill_path: injected.skill.path_to_skills_md.to_path_buf(),
            plugin_id: injected.skill.plugin_id.clone(),
            remote_plugin_id: injected.skill.remote_plugin_id.clone(),
            invocation_type: InvocationType::Explicit,
        })
        .collect();
    sess.services
        .analytics_events_client
        .track_skill_invocations(tracking, invocations);
}

pub(crate) async fn prepare_implicit_skill_activation(
    sess: &Session,
    turn_context: &TurnContext,
    command: &str,
    workdir: &AbsolutePathBuf,
) -> Option<SkillActivation> {
    let Some(candidate) = detect_implicit_skill_invocation_for_command(
        turn_context.skills_snapshot().outcome(),
        command,
        workdir,
    ) else {
        return None;
    };

    // Candidate creation is intentionally independent of analytics deduplication. A later read of
    // an edited SKILL.md must produce its new digest even when this skill's invocation telemetry
    // was already emitted for the turn.
    let activation = build_implicit_skill_activation(turn_context, &candidate).await;
    emit_implicit_skill_invocation(sess, turn_context, candidate).await;
    activation
}

pub(crate) async fn maybe_emit_implicit_skill_invocation(
    sess: &Session,
    turn_context: &TurnContext,
    command: &str,
    workdir: &AbsolutePathBuf,
) -> Option<SkillActivation> {
    prepare_implicit_skill_activation(sess, turn_context, command, workdir).await
}

async fn build_implicit_skill_activation(
    turn_context: &TurnContext,
    candidate: &SkillMetadata,
) -> Option<SkillActivation> {
    let contents = turn_context
        .skills_snapshot()
        .outcome()
        .read_skill_text(candidate)
        .await
        .ok()?;
    let scope = match candidate.scope {
        SkillScope::User => SkillActivationScope::User,
        SkillScope::Repo => SkillActivationScope::Repo,
        SkillScope::System => SkillActivationScope::System,
        SkillScope::Admin => SkillActivationScope::Admin,
    };
    SkillActivation::new(
        candidate.name.clone(),
        candidate.path_to_skills_md.to_string_lossy().into_owned(),
        scope,
        SkillActivationKind::Implicit,
        turn_context.sub_id.clone(),
        codex_skills_extension::sha256_hex(&contents),
    )
    .ok()
}

async fn emit_implicit_skill_invocation(
    sess: &Session,
    turn_context: &TurnContext,
    candidate: SkillMetadata,
) {
    let invocation = SkillInvocation {
        skill_name: candidate.name,
        skill_scope: candidate.scope,
        skill_path: candidate.path_to_skills_md.to_path_buf(),
        plugin_id: candidate.plugin_id,
        remote_plugin_id: candidate.remote_plugin_id,
        invocation_type: InvocationType::Implicit,
    };
    let skill_scope = match invocation.skill_scope {
        SkillScope::User => "user",
        SkillScope::Repo => "repo",
        SkillScope::System => "system",
        SkillScope::Admin => "admin",
    };
    let skill_path = invocation.skill_path.to_string_lossy();
    let skill_name = invocation.skill_name.clone();
    let seen_key = format!("{skill_scope}:{skill_path}:{skill_name}");
    let inserted = {
        let skill_invocations = turn_context
            .extension_data
            .get_or_init(ImplicitSkillInvocations::default);
        let mut seen_skills = skill_invocations.0.lock().await;
        seen_skills.insert(seen_key)
    };
    if !inserted {
        return;
    }
    let skill_name_tag = sanitize_metric_tag_value(skill_name.as_str());

    for contributor in sess.services.extensions.skill_invocation_contributors() {
        contributor
            .on_skill_invocation(SkillInvocationInput {
                session_store: &sess.services.session_extension_data,
                thread_store: &sess.services.thread_extension_data,
                turn_store: turn_context.extension_data.as_ref(),
                turn_id: turn_context.sub_id.as_str(),
                skill_resource: skill_path.as_ref(),
                kind: SkillInvocationKind::Implicit,
            })
            .await;
    }

    turn_context.session_telemetry.counter(
        "codex.skill.injected",
        /*inc*/ 1,
        &[
            ("status", "ok"),
            ("skill", skill_name_tag.as_str()),
            ("invoke_type", "implicit"),
        ],
    );
    sess.services
        .analytics_events_client
        .track_skill_invocations(
            build_track_events_context(
                turn_context.model_info.slug.clone(),
                sess.thread_id.to_string(),
                turn_context.sub_id.clone(),
                turn_context.originator.clone(),
            ),
            vec![invocation],
        );
}

#[cfg(test)]
#[path = "skills_tests.rs"]
pub(crate) mod tests;
