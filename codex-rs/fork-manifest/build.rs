use std::path::Path;
use std::process::Command;

/// Stamps the commit this binary was built from into the compiled crate.
///
/// A release build pins the commit explicitly through `CODEX_FORK_BUILD_SHA`,
/// which is what the fork release workflow sets. A developer build falls back
/// to asking git, and a build with neither (a vendored source tarball, or a
/// hermetic sandbox with no git) reports `unknown` rather than guessing.
fn main() {
    println!("cargo:rerun-if-env-changed=CODEX_FORK_BUILD_SHA");
    println!("cargo:rerun-if-changed=fork-manifest.json");
    println!("cargo:rerun-if-changed=build.rs");

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
    let repo_root = Path::new(&manifest_dir).join("..").join("..");
    for head_path in ["HEAD", "refs/heads"] {
        let path = repo_root.join(".git").join(head_path);
        if path.exists() {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }

    let (commit, state) = match std::env::var("CODEX_FORK_BUILD_SHA") {
        // An explicitly pinned commit is authoritative: the workflow that set
        // it checked out exactly that tree.
        Ok(sha) if !sha.trim().is_empty() => (sha.trim().to_string(), "clean".to_string()),
        _ => match git_commit(&manifest_dir) {
            Some(commit) => {
                let state = match git_is_dirty(&manifest_dir) {
                    Some(true) => "dirty",
                    Some(false) => "clean",
                    None => "unknown",
                };
                (commit, state.to_string())
            }
            None => ("unknown".to_string(), "unknown".to_string()),
        },
    };

    println!("cargo:rustc-env=CODEX_FORK_BUILD_COMMIT={commit}");
    println!("cargo:rustc-env=CODEX_FORK_BUILD_COMMIT_STATE={state}");
}

fn git_commit(cwd: &str) -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(cwd)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let commit = String::from_utf8(output.stdout).ok()?.trim().to_string();
    if commit.is_empty() {
        None
    } else {
        Some(commit)
    }
}

/// `None` when git could not answer at all, which is different from a clean
/// tree and must not be reported as one.
fn git_is_dirty(cwd: &str) -> Option<bool> {
    let output = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .current_dir(cwd)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(!String::from_utf8_lossy(&output.stdout).trim().is_empty())
}
