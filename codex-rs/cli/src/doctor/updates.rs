//! Diagnoses whether Codex update paths target the running installation.
//!
//! Update diagnostics combine cached version metadata, install-channel hints,
//! and bounded latest-version probes. For npm-managed launches, this module also
//! verifies that npm install -g would update the package root that launched the
//! current process, which catches PATH and prefix mismatches before the user runs
//! an update command.

use std::path::Path;

use codex_core::config::Config;
use codex_install_context::InstallContext;
use codex_install_context::InstallMethod;
use serde::Deserialize;

use super::CheckStatus;
use super::DoctorCheck;
use super::NpmRootCheck;
use super::doctor_install_context;
use super::doctor_managed_by_npm;
use super::npm_global_root_check;
use super::run_command;

const VERSION_FILE_NAME: &str = "version.json";
const HOMEBREW_CASK_API_URL: &str = "https://formulae.brew.sh/api/cask/codex.json";

/// The release channel this build may upgrade from, and the tag prefix that
/// identifies it. Both come from the fork manifest: probing upstream's releases
/// would report an "available update" that, if taken, replaces this build with
/// one that has none of its capabilities.
fn github_latest_release_url() -> &'static str {
    &codex_fork_manifest::manifest()
        .release_channel
        .latest_release_api_url
}

fn release_tag_prefix() -> &'static str {
    &codex_fork_manifest::manifest().release_channel.tag_prefix
}

/// Whether upstream's package managers and installer scripts ship this build.
fn upstream_installers_ship_this_build() -> bool {
    codex_fork_manifest::manifest()
        .release_channel
        .package_manager_releases
}

/// Builds the update-health row for the current installation.
///
/// Network failures while fetching latest-version metadata degrade the row to a
/// warning instead of failing doctor outright; update freshness is useful
/// support context but should not mask more direct install/config failures.
pub(super) fn updates_check(config: &Config) -> DoctorCheck {
    let current_exe = std::env::current_exe().ok();
    let install_context = doctor_install_context(current_exe.as_deref());
    let fork_manifest = codex_fork_manifest::manifest();
    let mut details = vec![
        // Which build this is, first: every line below is only interpretable
        // once you know whether the running binary is the fork or upstream.
        format!("fork: {}", fork_manifest.fork.slug),
        format!(
            "build commit: {} ({})",
            fork_manifest.build.commit, fork_manifest.build.commit_state
        ),
        format!("release channel: {}", github_latest_release_url()),
        format!(
            "check for update on startup: {}",
            config.check_for_update_on_startup
        ),
        format!("update action: {}", update_action_label(&install_context)),
    ];
    let version_file = config.codex_home.join(VERSION_FILE_NAME);
    push_cached_version_details(&mut details, &version_file);

    let mut status = CheckStatus::Ok;
    let mut summary = "update configuration is locally consistent".to_string();
    let mut remediation = None;

    if doctor_managed_by_npm(current_exe.as_deref()) {
        match npm_global_root_check() {
            NpmRootCheck::Match { package_root } => {
                details.push(format!("npm update target: {}", package_root.display()));
            }
            NpmRootCheck::Mismatch {
                running_package_root,
                npm_package_root,
            } => {
                status = CheckStatus::Fail;
                summary = "update would target a different npm install".to_string();
                details.push(format!(
                    "running package root: {}",
                    running_package_root.display()
                ));
                details.push(format!("npm package root: {}", npm_package_root.display()));
                remediation = Some(format!(
                    "Fix PATH or npm prefix so the running package root ({}) matches the npm global package root ({}).",
                    running_package_root.display(),
                    npm_package_root.display()
                ));
            }
            NpmRootCheck::MissingPackageRoot => {
                status = status.max(CheckStatus::Warning);
                summary = "npm update target could not be proven".to_string();
                remediation = Some(
                    "Reinstall or update Codex so the JS shim provides CODEX_MANAGED_PACKAGE_ROOT."
                        .to_string(),
                );
            }
            NpmRootCheck::NpmUnavailable(error) => {
                status = status.max(CheckStatus::Warning);
                summary = "npm update target could not be inspected".to_string();
                details.push(format!("npm root -g failed: {error}"));
            }
        }
    }

    match fetch_latest_version(&install_context) {
        Ok(latest_version) => {
            details.push(format!("latest version: {latest_version}"));
            if is_newer(&latest_version, env!("CARGO_PKG_VERSION")) == Some(true) {
                details.push("latest version status: newer version is available".to_string());
            } else {
                details.push("latest version status: current version is not older".to_string());
            }
        }
        Err(err) => {
            status = status.max(CheckStatus::Warning);
            details.push(format!("latest version probe: {err}"));
        }
    }

    let mut check = DoctorCheck::new("updates.status", "updates", status, summary).details(details);
    if let Some(remediation) = remediation {
        check = check.remediation(remediation);
    }
    check
}

fn push_cached_version_details(details: &mut Vec<String>, version_file: &Path) {
    details.push(format!("version cache: {}", version_file.display()));
    match std::fs::read_to_string(version_file) {
        Ok(contents) => match serde_json::from_str::<VersionInfo>(&contents) {
            Ok(info) => {
                details.push(format!("cached latest version: {}", info.latest_version));
                if let Some(last_checked_at) = info.last_checked_at {
                    details.push(format!("last checked at: {last_checked_at}"));
                }
                if let Some(dismissed_version) = info.dismissed_version {
                    details.push(format!("dismissed version: {dismissed_version}"));
                }
            }
            Err(err) => details.push(format!("version cache parse: {err}")),
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            details.push("version cache: missing".to_string());
        }
        Err(err) => details.push(format!("version cache read: {err}")),
    }
}

fn update_action_label(context: &InstallContext) -> String {
    if !upstream_installers_ship_this_build() {
        // Naming the upstream command anyway would read as a recommendation.
        // Say what it would actually do, and where the real upgrade lives.
        return format!(
            "manual: download from {} ({} would install an upstream build)",
            codex_fork_manifest::manifest()
                .release_channel
                .releases_page_url,
            upstream_install_command(context),
        );
    }
    upstream_install_command(context).to_string()
}

fn upstream_install_command(context: &InstallContext) -> &'static str {
    match &context.method {
        InstallMethod::Npm => "npm install -g @openai/codex",
        InstallMethod::Bun => "bun install -g @openai/codex",
        InstallMethod::Pnpm => "pnpm add -g @openai/codex",
        InstallMethod::Brew => "brew upgrade --cask codex",
        InstallMethod::Standalone { .. } => "standalone installer",
        InstallMethod::Other => "manual or unknown",
    }
}

fn fetch_latest_version(context: &InstallContext) -> Result<String, String> {
    // The Homebrew cask tracks upstream. Consulting it for a build Homebrew
    // does not ship would report an upstream version as this build's available
    // update, so only the fork's own release channel is asked.
    match &context.method {
        InstallMethod::Brew if upstream_installers_ship_this_build() => {
            fetch_homebrew_cask_version()
        }
        InstallMethod::Brew
        | InstallMethod::Npm
        | InstallMethod::Bun
        | InstallMethod::Pnpm
        | InstallMethod::Standalone { .. }
        | InstallMethod::Other => fetch_latest_github_release_version(),
    }
}

fn fetch_latest_github_release_version() -> Result<String, String> {
    #[derive(Deserialize)]
    struct ReleaseInfo {
        tag_name: String,
    }

    let info = http_get_json::<ReleaseInfo>(github_latest_release_url())?;
    info.tag_name
        .strip_prefix(release_tag_prefix())
        .map(str::to_string)
        .ok_or_else(|| format!("failed to parse latest tag {}", info.tag_name))
}

fn fetch_homebrew_cask_version() -> Result<String, String> {
    #[derive(Deserialize)]
    struct HomebrewCaskInfo {
        version: String,
    }

    http_get_json::<HomebrewCaskInfo>(HOMEBREW_CASK_API_URL).map(|info| info.version)
}

fn http_get_json<T>(url: &str) -> Result<T, String>
where
    T: for<'de> Deserialize<'de>,
{
    let body = run_command("curl", ["-fsSL", "--max-time", "5", url])?;
    serde_json::from_str::<T>(&body).map_err(|err| err.to_string())
}

fn is_newer(latest: &str, current: &str) -> Option<bool> {
    match (parse_version(latest), parse_version(current)) {
        (Some(latest), Some(current)) => Some(latest > current),
        (Some(_), None) | (None, Some(_)) | (None, None) => None,
    }
}

fn parse_version(value: &str) -> Option<(u64, u64, u64)> {
    let mut parts = value.trim().split('.');
    let major = parts.next()?.parse::<u64>().ok()?;
    let minor = parts.next()?.parse::<u64>().ok()?;
    let patch = parts.next()?.parse::<u64>().ok()?;
    Some((major, minor, patch))
}

#[derive(Deserialize)]
struct VersionInfo {
    latest_version: String,
    #[serde(default)]
    last_checked_at: Option<String>,
    #[serde(default)]
    dismissed_version: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_newer_compares_plain_semver() {
        assert_eq!(is_newer("1.2.4", "1.2.3"), Some(true));
        assert_eq!(is_newer("1.2.3", "1.2.4"), Some(false));
        assert_eq!(is_newer("1.2.3-beta.1", "1.2.2"), None);
    }

    #[test]
    fn update_action_labels_install_contexts() {
        assert_eq!(
            upstream_install_command(&InstallContext {
                method: InstallMethod::Npm,
                package_layout: None,
            }),
            "npm install -g @openai/codex"
        );
        assert_eq!(
            upstream_install_command(&InstallContext {
                method: InstallMethod::Pnpm,
                package_layout: None,
            }),
            "pnpm add -g @openai/codex"
        );
        assert_eq!(
            upstream_install_command(&InstallContext {
                method: InstallMethod::Other,
                package_layout: None,
            }),
            "manual or unknown"
        );
    }

    #[test]
    fn the_label_does_not_recommend_a_command_that_installs_upstream() {
        // The doctor row is read as advice. On a build upstream does not ship,
        // printing the bare npm command tells the user to overwrite their fork
        // install with an upstream binary.
        assert!(!upstream_installers_ship_this_build());

        let label = update_action_label(&InstallContext {
            method: InstallMethod::Npm,
            package_layout: None,
        });

        assert!(label.starts_with("manual: download from "), "got: {label}");
        assert!(
            label.contains(
                &codex_fork_manifest::manifest()
                    .release_channel
                    .releases_page_url
            ),
            "got: {label}"
        );
        assert!(
            label.contains("would install an upstream build"),
            "got: {label}"
        );
    }

    #[test]
    fn the_release_probe_targets_the_fork_channel() {
        let manifest = codex_fork_manifest::manifest();
        assert!(github_latest_release_url().contains(&manifest.fork.slug));
        assert!(!github_latest_release_url().contains(&manifest.upstream.slug));
        assert_eq!(release_tag_prefix(), manifest.release_channel.tag_prefix);
    }
}
