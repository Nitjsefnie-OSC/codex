use std::collections::HashSet;

use codex_extension_api::ContextualUserFragment;
use codex_skills::SkillMetadata;
use codex_skills::normalize_skill_path;
use sha2::Digest;
use sha2::Sha256;

use crate::HostSkillsSnapshot;
use crate::fragments::SkillInstructions;
use crate::render::truncate_main_prompt_contents;

/// Host skill prompts already supplied or superseded by an extension.
///
/// Core preserves its host skill invocation lifecycle while avoiding duplicate
/// prompts and retaining executor/orchestrator skill precedence.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InjectedHostSkillPrompts {
    paths: HashSet<String>,
}

impl InjectedHostSkillPrompts {
    pub fn insert_path(&mut self, path: impl Into<String>) {
        let path = path.into();
        self.paths.insert(normalize_host_skill_path(&path));
        self.paths.insert(path);
    }

    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    pub fn contains_path(&self, path: &str) -> bool {
        self.paths.contains(path) || self.paths.contains(&normalize_host_skill_path(path))
    }
}

/// Prompt fragments and read outcomes for a set of selected host skills.
#[derive(Debug, Clone)]
pub struct InjectedHostSkill {
    pub skill: SkillMetadata,
    /// Digest of the complete source, before any prompt-size truncation.
    pub content_sha256: String,
}

pub struct HostSkillPrompts {
    pub fragments: Vec<Box<dyn ContextualUserFragment + Send>>,
    pub injected: Vec<InjectedHostSkill>,
    pub warnings: Vec<String>,
}

fn normalize_host_skill_path(path: &str) -> String {
    normalize_skill_path(path).replace('\\', "/")
}

impl HostSkillsSnapshot {
    /// Reads selected host skills and builds their model-visible prompt fragments.
    ///
    /// Core calls this directly, including for hosts without an installed skills extension.
    #[tracing::instrument(
        level = "trace",
        skip_all,
        fields(selected_skill_count = selected_skills.len())
    )]
    pub async fn load_skill_prompts(&self, selected_skills: &[SkillMetadata]) -> HostSkillPrompts {
        let mut prompts = HostSkillPrompts {
            fragments: Vec::with_capacity(selected_skills.len()),
            injected: Vec::with_capacity(selected_skills.len()),
            warnings: Vec::new(),
        };

        for skill in selected_skills {
            match self.read_skill_text(skill).await {
                Ok(contents) => {
                    let content_sha256 = sha256_hex(&contents);
                    let (contents, truncated) = if self.outcome().is_agent_plugin_skill(skill) {
                        truncate_main_prompt_contents(&contents)
                    } else {
                        (contents, false)
                    };
                    if truncated {
                        prompts.warnings.push(format!(
                            "Skill `{}` exceeded the main prompt context limit and was truncated.",
                            skill.name
                        ));
                    }
                    prompts.fragments.push(Box::new(SkillInstructions {
                        name: skill.name.clone(),
                        path: skill.path_to_skills_md.to_string_lossy().into_owned(),
                        contents,
                        resource_access: None,
                    }));
                    prompts.injected.push(InjectedHostSkill {
                        skill: skill.clone(),
                        content_sha256,
                    });
                }
                Err(err) => {
                    prompts.warnings.push(format!(
                        "Failed to load skill {} at {}: {err:#}",
                        skill.name,
                        skill.path_to_skills_md.display()
                    ));
                }
            }
        }

        prompts
    }
}

pub fn sha256_hex(contents: &str) -> String {
    format!("{:x}", Sha256::digest(contents.as_bytes()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::Arc;

    use codex_exec_server::LOCAL_FS;
    use codex_protocol::protocol::SkillScope;
    use codex_skills::SkillMetadata;
    use codex_utils_absolute_path::AbsolutePathBuf;
    use tempfile::tempdir;

    use super::HostSkillsSnapshot;
    use super::sha256_hex;
    use crate::SkillLoadOutcome;

    #[tokio::test]
    async fn host_skill_prompt_digest_uses_full_source_before_truncation() {
        let temp_dir = tempdir().expect("create temporary skill directory");
        let path = temp_dir.path().join("SKILL.md");
        let source = "a".repeat(8_001);
        std::fs::write(&path, &source).expect("write skill source");
        let path = AbsolutePathBuf::from_absolute_path(
            std::fs::canonicalize(path).expect("canonicalize skill source"),
        )
        .expect("skill source should be absolute");
        let skill = SkillMetadata {
            name: "large-skill".to_string(),
            description: "A large skill".to_string(),
            short_description: None,
            interface: None,
            dependencies: None,
            policy: None,
            path_to_skills_md: path.clone(),
            scope: SkillScope::User,
            plugin_id: Some("fixture-plugin".to_string()),
            remote_plugin_id: None,
        };
        let outcome = SkillLoadOutcome::from_parts(
            vec![skill.clone()],
            Vec::new(),
            Vec::new(),
            HashMap::new(),
            HashMap::new(),
            HashSet::from([path.clone()]),
            HashMap::from([(path, Arc::clone(&LOCAL_FS))]),
        );

        let prompts = HostSkillsSnapshot::new(Arc::new(outcome))
            .load_skill_prompts(std::slice::from_ref(&skill))
            .await;
        let body = prompts.fragments[0].body();

        assert!(body.contains(&source[..8_000]));
        assert!(!body.contains(&source));
        assert_eq!(prompts.warnings.len(), 1);
        assert_eq!(prompts.injected[0].skill, skill);
        assert_eq!(prompts.injected[0].content_sha256, sha256_hex(&source));
    }
}
