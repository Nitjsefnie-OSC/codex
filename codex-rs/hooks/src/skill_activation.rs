use schemars::JsonSchema;
use serde::Deserialize;
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

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct SkillActivation {
    pub name: String,
    pub path: String,
    pub scope: SkillActivationScope,
    pub invocation: SkillActivationKind,
    pub turn_id: String,
    pub content_sha256: String,
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
