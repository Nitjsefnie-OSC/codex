use schemars::JsonSchema;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SkillActivationKind {
    Explicit,
    Implicit,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SkillActivationScope {
    User,
    Repo,
    System,
    Admin,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct SkillActivation {
    #[schemars(length(min = 1), regex(pattern = r".*\S.*"))]
    name: String,
    #[schemars(length(min = 1), regex(pattern = r".*\S.*"))]
    path: String,
    scope: SkillActivationScope,
    invocation: SkillActivationKind,
    #[schemars(length(min = 1), regex(pattern = r".*\S.*"))]
    turn_id: String,
    #[schemars(length(min = 64, max = 64), regex(pattern = r"^[0-9a-f]{64}$"))]
    content_sha256: String,
}

impl SkillActivation {
    pub fn new(
        name: String,
        path: String,
        scope: SkillActivationScope,
        invocation: SkillActivationKind,
        turn_id: String,
        content_sha256: String,
    ) -> Result<Self, &'static str> {
        if name.trim().is_empty() {
            return Err("skill activation name must not be blank");
        }
        if path.trim().is_empty() {
            return Err("skill activation path must not be blank");
        }
        if turn_id.trim().is_empty() {
            return Err("skill activation turn_id must not be blank");
        }
        if content_sha256.len() != 64
            || !content_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("skill activation content_sha256 must be 64 lowercase hexadecimal digits");
        }

        Ok(Self {
            name,
            path,
            scope,
            invocation,
            turn_id,
            content_sha256,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn scope(&self) -> SkillActivationScope {
        self.scope
    }

    pub fn invocation(&self) -> SkillActivationKind {
        self.invocation
    }

    pub fn turn_id(&self) -> &str {
        &self.turn_id
    }

    pub fn content_sha256(&self) -> &str {
        &self.content_sha256
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillActivationWire {
    name: String,
    path: String,
    scope: SkillActivationScope,
    invocation: SkillActivationKind,
    turn_id: String,
    content_sha256: String,
}

impl<'de> Deserialize<'de> for SkillActivation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SkillActivationWire::deserialize(deserializer)?;
        Self::new(
            wire.name,
            wire.path,
            wire.scope,
            wire.invocation,
            wire.turn_id,
            wire.content_sha256,
        )
        .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::SkillActivation;
    use super::SkillActivationKind;
    use super::SkillActivationScope;

    #[test]
    fn constructor_serializes_the_stable_wire_contract() {
        let activation = SkillActivation::new(
            "review".to_string(),
            "/repo/.codex/skills/review/SKILL.md".to_string(),
            SkillActivationScope::Repo,
            SkillActivationKind::Explicit,
            "turn-7".to_string(),
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        )
        .expect("valid activation");

        assert_eq!(
            serde_json::to_value(activation).expect("serialize activation"),
            json!({
                "name": "review",
                "path": "/repo/.codex/skills/review/SKILL.md",
                "scope": "repo",
                "invocation": "explicit",
                "turn_id": "turn-7",
                "content_sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            })
        );
    }

    #[test]
    fn constructor_rejects_blank_identities_and_malformed_digests() {
        let valid_digest = "a".repeat(64);
        let invalid = [
            (
                "",
                "/skills/review/SKILL.md",
                "turn-7",
                valid_digest.as_str(),
            ),
            ("review", " \t", "turn-7", valid_digest.as_str()),
            (
                "review",
                "/skills/review/SKILL.md",
                "\n",
                valid_digest.as_str(),
            ),
            ("review", "/skills/review/SKILL.md", "turn-7", "abcdef"),
            (
                "review",
                "/skills/review/SKILL.md",
                "turn-7",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            ),
            (
                "review",
                "/skills/review/SKILL.md",
                "turn-7",
                "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
            ),
        ];

        for (name, path, turn_id, digest) in invalid {
            assert!(
                SkillActivation::new(
                    name.to_string(),
                    path.to_string(),
                    SkillActivationScope::Repo,
                    SkillActivationKind::Implicit,
                    turn_id.to_string(),
                    digest.to_string(),
                )
                .is_err(),
                "accepted invalid activation: name={name:?} path={path:?} turn_id={turn_id:?} digest={digest:?}"
            );
        }
    }

    #[test]
    fn deserialization_cannot_bypass_activation_validation() {
        let invalid = json!({
            "name": "review",
            "path": "/skills/review/SKILL.md",
            "scope": "repo",
            "invocation": "implicit",
            "turn_id": "turn-7",
            "content_sha256": "ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD",
        });

        assert!(serde_json::from_value::<SkillActivation>(invalid).is_err());
    }
}
