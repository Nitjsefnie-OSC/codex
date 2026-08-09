//! Machine-readable identity, capability, and release-channel manifest for this
//! fork of `openai/codex`.
//!
//! A fork build is indistinguishable from an upstream build at runtime unless it
//! says so: the version number is the upstream one, the binary is called
//! `codex`, and the tools it adds are only visible from inside a model turn.
//! Anything that has to make a decision about *which* build it is talking to —
//! an installer deciding whether the fork's tools are present, an updater
//! deciding which release channel is safe to pull from, an operator reading a
//! bug report — needs an authoritative answer instead of an inference.
//!
//! The declarative half lives in `fork-manifest.json`, checked in beside this
//! file and embedded at compile time. The build half ([`BuildInfo`]) is stamped
//! in by `build.rs`, so the manifest a binary reports is the manifest of the
//! tree that binary was actually built from.

use std::sync::OnceLock;

use serde::Deserialize;
use serde::Serialize;

/// The declarative manifest, embedded so a binary carries it without needing
/// its source tree.
const MANIFEST_JSON: &str = include_str!("../fork-manifest.json");

/// Commit this binary was built from, or `unknown`. Stamped by `build.rs`.
const BUILD_COMMIT: &str = env!("CODEX_FORK_BUILD_COMMIT");

/// `clean`, `dirty`, or `unknown`. Stamped by `build.rs`.
const BUILD_COMMIT_STATE: &str = env!("CODEX_FORK_BUILD_COMMIT_STATE");

/// Schema version this crate knows how to read.
pub const SCHEMA_VERSION: u32 = 1;

/// Reported for a commit or state that could not be determined at build time.
pub const UNKNOWN: &str = "unknown";

/// Everything a caller can learn about the build it is talking to.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ForkManifest {
    pub schema_version: u32,
    pub fork: ForkIdentity,
    pub upstream: UpstreamIdentity,
    pub release_channel: ReleaseChannel,
    pub capabilities: Vec<Capability>,
    /// Stamped in at load time rather than read from the checked-in file — a
    /// commit cannot describe the build that contains it.
    #[serde(default)]
    pub build: BuildInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ForkIdentity {
    /// `owner/repo` on GitHub.
    pub slug: String,
    pub repository: String,
    pub default_branch: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct UpstreamIdentity {
    /// `owner/repo` on GitHub.
    pub slug: String,
    pub repository: String,
}

/// Where a fork build may pull an upgrade from.
///
/// Upstream's own channels are deliberately *not* listed here: pulling from
/// them replaces a fork binary with one that has none of the capabilities
/// below, silently.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ReleaseChannel {
    /// Currently always `github`. Present so a future channel can be told apart
    /// without sniffing the URL.
    pub provider: String,
    pub latest_release_api_url: String,
    pub releases_page_url: String,
    /// Prefix a fork release tag carries, e.g. `fork-v` in `fork-v0.1.0`.
    pub tag_prefix: String,
    /// Name of the checksum asset published alongside every release artifact.
    pub checksums_asset: String,
    /// Whether the fork publishes to npm/Homebrew. It does not, so an install
    /// managed by one of those is not upgradable in place.
    pub package_manager_releases: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityKind {
    /// A tool the model can call.
    Tool,
    /// A field added to a hook payload.
    HookField,
    /// A CLI subcommand or flag.
    Cli,
}

/// One thing this fork adds that upstream does not have.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Capability {
    /// Stable identifier, e.g. `tool.monitor`. This is what a caller matches
    /// on; `name` is for humans and may collide across kinds.
    pub id: String,
    pub kind: CapabilityKind,
    pub name: String,
    /// Config key that turns the capability off, when one exists.
    pub config_key: Option<String>,
    pub default_enabled: bool,
    pub summary: String,
}

/// Which build is answering.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct BuildInfo {
    /// Workspace version of the build. `0.0.0` for an unreleased source build.
    pub version: String,
    /// Full commit sha, or [`UNKNOWN`].
    pub commit: String,
    /// `clean`, `dirty`, or [`UNKNOWN`].
    pub commit_state: String,
}

impl Default for BuildInfo {
    fn default() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            commit: BUILD_COMMIT.to_string(),
            commit_state: BUILD_COMMIT_STATE.to_string(),
        }
    }
}

impl BuildInfo {
    /// Whether this build can be identified by commit alone — a known commit
    /// with no uncommitted changes on top of it.
    ///
    /// A `dirty` or `unknown` build is not reproducible from its commit, so a
    /// caller pinning behaviour to a commit must not treat it as pinned.
    pub fn is_pinned(&self) -> bool {
        self.commit != UNKNOWN && self.commit_state == "clean"
    }

    /// First 12 characters of the commit, for display.
    pub fn short_commit(&self) -> &str {
        if self.commit == UNKNOWN {
            return UNKNOWN;
        }
        let end = self
            .commit
            .char_indices()
            .nth(12)
            .map_or(self.commit.len(), |(index, _)| index);
        &self.commit[..end]
    }
}

impl ForkManifest {
    /// Whether the build declares the capability with this id.
    pub fn has_capability(&self, id: &str) -> bool {
        self.capability(id).is_some()
    }

    pub fn capability(&self, id: &str) -> Option<&Capability> {
        self.capabilities.iter().find(|entry| entry.id == id)
    }
}

/// The manifest for this build.
///
/// Panics if the embedded JSON does not parse, which a shipped binary cannot
/// do: `embedded_manifest_parses` below fails the build first.
pub fn manifest() -> &'static ForkManifest {
    static MANIFEST: OnceLock<ForkManifest> = OnceLock::new();
    MANIFEST.get_or_init(|| match parse(MANIFEST_JSON) {
        Ok(manifest) => manifest,
        Err(error) => panic!("embedded fork manifest is not valid: {error}"),
    })
}

/// Which build is answering. Shorthand for `manifest().build`.
pub fn build_info() -> &'static BuildInfo {
    &manifest().build
}

/// The manifest as the JSON a caller outside the process should read.
pub fn manifest_json() -> String {
    match serde_json::to_string_pretty(manifest()) {
        Ok(json) => json,
        // Every field is a plain string, number, bool, or Vec of those, so
        // this is unreachable; returning the embedded text keeps a caller
        // parsing something valid rather than nothing.
        Err(_) => MANIFEST_JSON.to_string(),
    }
}

fn parse(json: &str) -> Result<ForkManifest, String> {
    let mut manifest: ForkManifest =
        serde_json::from_str(json).map_err(|error| error.to_string())?;
    if manifest.schema_version != SCHEMA_VERSION {
        return Err(format!(
            "schema_version {} is not the {SCHEMA_VERSION} this build reads",
            manifest.schema_version
        ));
    }
    manifest.build = BuildInfo::default();
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn embedded_manifest_parses() {
        let manifest = manifest();
        assert_eq!(manifest.schema_version, SCHEMA_VERSION);
        assert_eq!(manifest.fork.slug, "Nitjsefnie-OSC/codex");
        assert_eq!(manifest.upstream.slug, "openai/codex");
    }

    #[test]
    fn the_release_channel_never_points_at_upstream() {
        let channel = &manifest().release_channel;
        let upstream_slug = &manifest().upstream.slug;

        assert!(
            !channel.latest_release_api_url.contains(upstream_slug),
            "update channel would pull an upstream build over a fork build"
        );
        assert!(
            !channel.releases_page_url.contains(upstream_slug),
            "release page would send a user to upstream artifacts"
        );
        assert!(
            channel
                .latest_release_api_url
                .contains(&manifest().fork.slug)
        );
    }

    #[test]
    fn every_fork_added_capability_is_declared() {
        let manifest = manifest();
        assert!(manifest.has_capability("tool.whoami"));
        assert!(manifest.has_capability("tool.monitor"));
        assert!(manifest.has_capability("hook.stop_background_state"));

        let skill_activations = manifest
            .capability("hook.skill_activations")
            .unwrap_or_else(|| panic!("skill_activations hook capability should be declared"));
        assert_eq!(skill_activations.kind, CapabilityKind::HookField);
        assert_eq!(skill_activations.name, "skill_activations");
        assert_eq!(skill_activations.config_key, None);
        assert!(skill_activations.default_enabled);
        assert!(skill_activations.summary.contains("PreToolUse"));
        assert!(skill_activations.summary.contains("PostToolUse"));

        assert!(!manifest.has_capability("tool.not-a-thing"));
    }

    #[test]
    fn capability_ids_are_unique() {
        let mut ids: Vec<&str> = manifest()
            .capabilities
            .iter()
            .map(|capability| capability.id.as_str())
            .collect();
        let total = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), total, "duplicate capability id in the manifest");
    }

    #[test]
    fn a_gated_capability_names_the_key_that_turns_it_off() {
        let monitor = manifest()
            .capability("tool.monitor")
            .unwrap_or_else(|| panic!("monitor capability should be declared"));
        assert_eq!(monitor.config_key.as_deref(), Some("monitor_tool"));
        assert_eq!(monitor.kind, CapabilityKind::Tool);
    }

    #[test]
    fn a_schema_version_this_build_cannot_read_is_rejected() {
        let json = MANIFEST_JSON.replacen("\"schema_version\": 1", "\"schema_version\": 2", 1);
        let error = parse(&json).err().unwrap_or_default();
        assert!(error.contains("schema_version 2"), "got: {error}");
    }

    #[test]
    fn an_unknown_or_dirty_build_is_not_pinned() {
        let pinned = BuildInfo {
            version: "0.1.0".to_string(),
            commit: "a".repeat(40),
            commit_state: "clean".to_string(),
        };
        assert!(pinned.is_pinned());
        assert_eq!(pinned.short_commit(), "aaaaaaaaaaaa");

        assert!(
            !BuildInfo {
                commit_state: "dirty".to_string(),
                ..pinned.clone()
            }
            .is_pinned()
        );
        assert!(
            !BuildInfo {
                commit: UNKNOWN.to_string(),
                ..pinned
            }
            .is_pinned()
        );
    }

    #[test]
    fn manifest_json_round_trips() {
        let json = manifest_json();
        let parsed: ForkManifest =
            serde_json::from_str(&json).unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(&parsed, manifest());
    }
}
