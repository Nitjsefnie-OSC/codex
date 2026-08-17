#[cfg(any(not(debug_assertions), test))]
use codex_install_context::InstallContext;
#[cfg(any(not(debug_assertions), test))]
use codex_install_context::InstallMethod;
#[cfg(any(not(debug_assertions), test))]
use codex_install_context::StandalonePlatform;

/// Whether this fork ships builds through the package managers and installer
/// scripts the upstream update actions drive.
///
/// It does not, so this is `false` and every such action is withheld; the
/// constant is read from the manifest rather than inlined so turning it on
/// later is a manifest edit, not a code hunt.
#[cfg(any(not(debug_assertions), test))]
fn fork_publishes_package_manager_releases() -> bool {
    codex_fork_manifest::manifest()
        .release_channel
        .package_manager_releases
}

/// Update action the CLI should perform after the TUI exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateAction {
    /// Update via `npm install -g @openai/codex@latest`.
    NpmGlobalLatest,
    /// Update via `bun install -g @openai/codex@latest`.
    BunGlobalLatest,
    /// Update via `pnpm add -g @openai/codex@latest`.
    PnpmGlobalLatest,
    /// Update via `brew upgrade codex`.
    BrewUpgrade,
    /// Update via `curl -fsSL https://chatgpt.com/codex/install.sh | CODEX_NON_INTERACTIVE=1 sh`.
    StandaloneUnix,
    /// Update via `$env:CODEX_NON_INTERACTIVE=1; irm https://chatgpt.com/codex/install.ps1 | iex`.
    StandaloneWindows,
}

impl UpdateAction {
    #[cfg(any(not(debug_assertions), test))]
    pub(crate) fn from_install_context(context: &InstallContext) -> Option<Self> {
        Self::from_install_context_with_channel(context, fork_publishes_package_manager_releases())
    }

    #[cfg(any(not(debug_assertions), test))]
    fn from_install_context_with_channel(
        context: &InstallContext,
        upstream_installers_ship_this_build: bool,
    ) -> Option<Self> {
        // Every action below installs an *upstream* build: the npm package, the
        // Homebrew cask, and the install.sh/install.ps1 scripts all resolve to
        // openai/codex. Running one on a fork install replaces the binary with
        // one that has none of the fork's capabilities, and nothing about the
        // upgrade says so. When the fork does not ship through them there is no
        // in-place update action; the caller points at the fork releases page
        // instead.
        if !upstream_installers_ship_this_build {
            return None;
        }

        match &context.method {
            InstallMethod::Npm => Some(UpdateAction::NpmGlobalLatest),
            InstallMethod::Bun => Some(UpdateAction::BunGlobalLatest),
            InstallMethod::Pnpm => Some(UpdateAction::PnpmGlobalLatest),
            InstallMethod::Brew => Some(UpdateAction::BrewUpgrade),
            InstallMethod::Standalone { platform, .. } => Some(match platform {
                StandalonePlatform::Unix => UpdateAction::StandaloneUnix,
                StandalonePlatform::Windows => UpdateAction::StandaloneWindows,
            }),
            InstallMethod::Other => None,
        }
    }

    /// Returns the list of command-line arguments for invoking the update.
    pub fn command_args(self) -> (&'static str, &'static [&'static str]) {
        match self {
            UpdateAction::NpmGlobalLatest => ("npm", &["install", "-g", "@openai/codex"]),
            UpdateAction::BunGlobalLatest => ("bun", &["install", "-g", "@openai/codex"]),
            UpdateAction::PnpmGlobalLatest => ("pnpm", &["add", "-g", "@openai/codex"]),
            UpdateAction::BrewUpgrade => ("brew", &["upgrade", "--cask", "codex"]),
            UpdateAction::StandaloneUnix => (
                "sh",
                &[
                    "-c",
                    "curl -fsSL https://chatgpt.com/codex/install.sh | CODEX_NON_INTERACTIVE=1 sh",
                ],
            ),
            UpdateAction::StandaloneWindows => (
                "powershell",
                &[
                    "-ExecutionPolicy",
                    "Bypass",
                    "-c",
                    "$env:CODEX_NON_INTERACTIVE=1; irm https://chatgpt.com/codex/install.ps1 | iex",
                ],
            ),
        }
    }

    /// Returns string representation of the command-line arguments for invoking the update.
    pub fn command_str(self) -> String {
        let (command, args) = self.command_args();
        shlex::try_join(std::iter::once(command).chain(args.iter().copied()))
            .unwrap_or_else(|_| format!("{command} {}", args.join(" ")))
    }
}

#[cfg(not(debug_assertions))]
pub fn get_update_action() -> Option<UpdateAction> {
    UpdateAction::from_install_context(InstallContext::current())
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_utils_absolute_path::AbsolutePathBuf;
    use pretty_assertions::assert_eq;

    fn native_release_dir() -> AbsolutePathBuf {
        AbsolutePathBuf::from_absolute_path(std::env::temp_dir().join("native-release"))
            .expect("temp dir path should be absolute")
    }

    fn install_methods() -> Vec<InstallMethod> {
        let release_dir = native_release_dir();
        vec![
            InstallMethod::Npm,
            InstallMethod::Bun,
            InstallMethod::Pnpm,
            InstallMethod::Brew,
            InstallMethod::Standalone {
                platform: StandalonePlatform::Unix,
                release_dir: release_dir.clone(),
                resources_dir: Some(release_dir.join("codex-resources")),
            },
            InstallMethod::Standalone {
                platform: StandalonePlatform::Windows,
                release_dir: release_dir.clone(),
                resources_dir: Some(release_dir.join("codex-resources")),
            },
            InstallMethod::Other,
        ]
    }

    fn action_for(method: InstallMethod, upstream_ships_this_build: bool) -> Option<UpdateAction> {
        UpdateAction::from_install_context_with_channel(
            &InstallContext {
                method,
                package_layout: None,
            },
            upstream_ships_this_build,
        )
    }

    #[test]
    fn maps_install_context_to_update_action() {
        let release_dir = native_release_dir();

        assert_eq!(action_for(InstallMethod::Other, true), None);
        assert_eq!(
            action_for(InstallMethod::Npm, true),
            Some(UpdateAction::NpmGlobalLatest)
        );
        assert_eq!(
            action_for(InstallMethod::Bun, true),
            Some(UpdateAction::BunGlobalLatest)
        );
        assert_eq!(
            action_for(InstallMethod::Pnpm, true),
            Some(UpdateAction::PnpmGlobalLatest)
        );
        assert_eq!(
            action_for(InstallMethod::Brew, true),
            Some(UpdateAction::BrewUpgrade)
        );
        assert_eq!(
            action_for(
                InstallMethod::Standalone {
                    platform: StandalonePlatform::Unix,
                    release_dir: release_dir.clone(),
                    resources_dir: Some(release_dir.join("codex-resources")),
                },
                true
            ),
            Some(UpdateAction::StandaloneUnix)
        );
        assert_eq!(
            action_for(
                InstallMethod::Standalone {
                    platform: StandalonePlatform::Windows,
                    release_dir: release_dir.clone(),
                    resources_dir: Some(release_dir.join("codex-resources")),
                },
                true
            ),
            Some(UpdateAction::StandaloneWindows)
        );
    }

    #[test]
    fn no_install_method_upgrades_in_place_when_upstream_does_not_ship_this_build() {
        for method in install_methods() {
            assert_eq!(
                action_for(method.clone(), false),
                None,
                "{method:?} would have installed an upstream build over this one"
            );
        }
    }

    #[test]
    fn this_fork_withholds_every_upstream_update_action() {
        // Guards the wiring, not just the helper: if the manifest ever claims
        // the fork ships through upstream's installers while it does not, every
        // update path silently reverts a user to an upstream build.
        assert!(!fork_publishes_package_manager_releases());
        for method in install_methods() {
            assert_eq!(
                UpdateAction::from_install_context(&InstallContext {
                    method: method.clone(),
                    package_layout: None,
                }),
                None,
                "{method:?} offered an upstream update action"
            );
        }
    }

    #[test]
    fn standalone_update_commands_rerun_latest_installer() {
        assert_eq!(
            UpdateAction::StandaloneUnix.command_args(),
            (
                "sh",
                &[
                    "-c",
                    "curl -fsSL https://chatgpt.com/codex/install.sh | CODEX_NON_INTERACTIVE=1 sh"
                ][..],
            )
        );
        assert_eq!(
            UpdateAction::StandaloneWindows.command_args(),
            (
                "powershell",
                &[
                    "-ExecutionPolicy",
                    "Bypass",
                    "-c",
                    "$env:CODEX_NON_INTERACTIVE=1; irm https://chatgpt.com/codex/install.ps1 | iex"
                ][..],
            )
        );
    }
}
