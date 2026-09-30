# Codex Exec Agent Role Selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add first-class `codex exec --agent <ROLE>` selection for new headless sessions, using the same installed role profiles and precedence rules as native agent spawn.

**Architecture:** Parse the role name in `codex-exec`, validate that it is used only for a new session, and delegate role resolution/application to a narrow public `codex-core` adapter. The adapter reuses the existing role layer loader, then restores invocation-owned runtime safety and process state without overriding role-owned identity fields.

**Tech Stack:** Rust, Clap, Tokio, existing `codex-core` config layers and role registry, Wiremock-based exec integration tests, GitHub Actions.

## Global Constraints

- Do not parse role TOML in `codex-exec`; `codex-core` remains the single role authority.
- A role owns `model`, `model_reasoning_effort`, `developer_instructions`, and any other values explicitly present in its role layer.
- The invocation owns approval/sandbox/permission state, CWD and writable roots, executable paths, hook-trust bypass, persistence mode, and PSP routing.
- Reject `--agent` with `resume`, `fork`, or `review`; never mutate an existing session's recorded role.
- Unknown and malformed roles must fail before login enforcement or any Responses request.
- Do not run Cargo, Rust builds, Rust tests, formatting, schema generation, or release builds locally. Put every Rust proof in `.github/workflows/fork-verify.yml` and inspect it through GitHub Actions.
- Keep the exact commit trailer `Co-authored-by: GPT-5.6 Sol <noreply@openai.com>`.
- Never read, edit, stage, or remove the user's untracked `TOOL-ARCHITECTURE.md`.

---

### Task 1: Pin the CLI and compatibility contract with failing tests

**Files:**

- Modify: `codex-rs/exec/src/cli.rs`
- Modify: `codex-rs/exec/src/lib_tests.rs`
- Modify: `.github/workflows/fork-verify.yml`

**Step 1: Add parser-level RED tests**

Add focused tests that parse the real `Cli` shape and assert:

- `codex exec --agent adversary -- "Review this claim"` stores `Some("adversary")`.
- `--agent` is global enough to appear before a new-session prompt but is not silently accepted as prompt data.
- `--agent ""` is rejected.
- `--agent adversary resume --last`, `fork <id>`, and `review --uncommitted` are rejected by the startup compatibility validator with the command name in the error.

Extract a pure `validate_agent_role_command(agent, command)` helper if necessary so compatibility is executable without starting auth or a session.

**Step 2: Add an exact workflow filter**

Add a bounded workflow step named `headless agent role contract tests` that runs only the new parser/compatibility tests, captures test output, requires every named filter to match at least one passing test, and exits nonzero on any failure.

**Step 3: Push a diagnostic RED commit**

Commit only tests and workflow wiring, push to `origin/main`, and verify through `gh api` that the exact step fails for the missing CLI field/behavior while earlier workspace checks pass. Record the run and job IDs.

**Step 4: Keep history clean**

After implementation turns the same proof green, amend this diagnostic commit rather than retaining a test-only failure commit. Push with `--force-with-lease` only.

---

### Task 2: Add the headless CLI surface

**Files:**

- Modify: `codex-rs/exec/src/cli.rs`
- Modify: `codex-rs/exec/src/lib.rs`
- Test: `codex-rs/exec/src/lib_tests.rs`

**Step 1: Add the root option**

Add:

```rust
/// Agent role profile to apply to a new headless session.
#[arg(long = "agent", value_name = "ROLE", global = true)]
pub agent: Option<String>,
```

Use a non-empty value parser or normalize-and-reject blank/whitespace-only values before configuration loading.

**Step 2: Validate operation compatibility immediately**

Destructure `agent` in `run_main` and call the pure compatibility validator before bootstrap authentication, cloud-config fetching, login enforcement, or app-server startup. Report explicit errors such as ``--agent cannot be used with `resume` ``.

**Step 3: Make the parser tests green**

Do not add role-file parsing to the exec crate. At this checkpoint only the CLI and operation contract should pass.

---

### Task 3: Expose one core role adapter for new headless sessions

**Files:**

- Modify: `codex-rs/core/src/agent/mod.rs`
- Modify: `codex-rs/core/src/agent/role.rs`
- Test: `codex-rs/core/src/agent/role.rs`

**Step 1: Add a core RED test using a real temporary role**

Create a temporary `$CODEX_HOME/agents/adversary.toml` through the existing test fixture path. Build a normal `Config`, apply the proposed headless adapter, and assert the role changes:

- `model`
- `model_reasoning_effort`
- `developer_instructions`

At the same time, seed invocation-owned values and assert they survive:

- `approval_policy`
- sandbox and permission profile
- `cwd` and additional writable roots
- `bypass_hook_trust`
- `ephemeral`
- `psp`
- sandbox/helper executable paths

Add a negative test for an unknown role with the exact role name in the error.

**Step 2: Implement a narrow public adapter**

Expose a function from `codex_core::agent`, for example:

```rust
pub async fn apply_exec_agent_role(
    config: &mut Config,
    role_name: &str,
) -> Result<(), String>
```

It must call the existing role resolver/layer application. Do not duplicate `parse_agent_role_file_contents`, built-in role lookup, or layer precedence.

**Step 3: Preserve runtime fields deliberately**

Before role application, snapshot the invocation-owned runtime fields listed above. After the existing role rebuild completes, restore only those fields. Do not restore model, effort, developer instructions, base instructions, personality, service tier, or provider if the role explicitly supplies them.

Prefer a small typed snapshot structure in `role.rs` over cloning all `ConfigOverrides`; that makes the identity/runtime ownership boundary reviewable and prevents a future field from being restored accidentally.

**Step 4: Make core tests green and mutation-resistant**

The tests must fail if role application is removed and independently fail if runtime restoration is removed. Add both core filters to the exact workflow step with pass-count gates.

---

### Task 4: Apply the role before headless session startup

**Files:**

- Modify: `codex-rs/exec/src/lib.rs`
- Test: `codex-rs/exec/src/lib_tests.rs`
- Test: `codex-rs/exec/tests/suite/exec.rs` or the closest existing mocked Responses integration module

**Step 1: Apply after final exec config resolution**

Change the resolved config binding to mutable, then invoke `codex_core::agent::apply_exec_agent_role(&mut config, role_name).await?` after `build_exec_config` and before login enforcement, telemetry/session construction, or any network-backed turn operation.

This order ensures the role sees the fully loaded user registry while a bad role still fails before an API request.

**Step 2: Prove no network request on failure**

Using the existing mocked Responses server, run startup with an unknown role and assert:

- nonzero result with `unknown agent_type 'missing-role'` (or the final intentional diagnostic), and
- the mock server received zero requests.

Repeat the zero-request assertion for an incompatible resume/review case if the integration harness reaches startup validation.

**Step 3: Prove the role reaches the turn**

Create a temporary role with a distinct model/effort and developer-instruction marker. Start a new exec session against the mock server and assert the outbound request uses the role model and includes the role developer instructions. Assert the app-server/session configuration retains never-approve, danger-full-access, requested CWD, and hook-trust bypass.

**Step 4: Prove CLI identity overrides cannot defeat the role**

Run the test with both `--agent adversary` and a conflicting `--model`/reasoning override. Assert the explicit role layer wins, matching native spawn behavior.

---

### Task 5: Complete GitHub Actions verification and contract documentation

**Files:**

- Modify: `.github/workflows/fork-verify.yml`
- Modify: `fork-manifest.json`
- Modify: `codex-rs/fork-manifest/src/lib.rs` or the current manifest contract test location
- Modify: `docs/superpowers/specs/2026-08-10-codex-exec-agent-role-design.md` only if implementation changes the accepted design

**Step 1: Declare the fork capability**

Add a manifest capability for named headless role selection, such as `cli.exec_agent_role`, with a test proving `codex fork-manifest --capabilities` includes it. Keep the capability name aligned with existing manifest kind/name conventions rather than inventing an incompatible schema shape.

**Step 2: Run the full workflow on the amended implementation commit**

Push with the exact coauthor trailer. Through `gh api`, verify terminal success for:

- workspace check
- headless agent role contract tests
- core role tests
- exec integration tests
- manifest tests
- all pre-existing fork verification families

Cancel superseded workflow runs to avoid wasting repository minutes.

**Step 3: Review the final diff**

Check `git diff --check`, the exact commit message, trailer, and changed-file scope. Confirm `codex-rs/target` is absent and `TOOL-ARCHITECTURE.md` remains untouched/untracked.

---

### Task 6: Release, install, and perform a real subscription-backed smoke test

**Files:**

- Modify: release tag/release metadata only after workflow green
- Modify: bundle fork lock/update metadata if required by its installer contract

**Step 1: Publish the next fork release**

Tag the verified commit as the next fork version, run the existing GitHub release workflow, and verify the Linux and Windows archive contents and `SHA256SUMS` from GitHub-served artifacts.

**Step 2: Install the verified Linux artifact**

Use the bundle's hardened installer path and dry-run first. Do not locally compile. Preserve the live Sol/high, never-approve, danger-full-access, prompt-suggestion-disabled defaults.

**Step 3: Live-test a real installed role**

Run, with stdin closed:

```bash
codex exec --agent adversary -- "Call whoami and report only its JSON." < /dev/null
```

Assert the returned server-verified identity matches the installed `adversary.toml` model/effort and that a developer-instruction marker is obeyed. Run an unknown-role command and confirm it fails locally without an API request.

**Step 4: Close the tracking issue only after deployment**

Update the bundle issue through `gh api` with the verified commit, release, checksums, workflow run, and live probe evidence. Use `Closes #<number>` in the final bundle-side implementation commit when the issue number is known; do not rewrite unrelated history solely to add it.
