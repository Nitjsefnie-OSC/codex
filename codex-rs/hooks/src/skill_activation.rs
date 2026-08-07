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
