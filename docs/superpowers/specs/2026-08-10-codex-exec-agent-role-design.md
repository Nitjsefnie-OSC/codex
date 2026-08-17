# Codex Exec Agent Role Selection Design

## Decision

Add `--agent <ROLE>` to `codex exec` for a new headless session. The option
selects one of the role profiles already discovered from `$CODEX_HOME/agents`
and applies that role through the same core role-layer machinery used by native
`spawn_agent`.

The option is rejected for `resume`, `fork`, and `review`. Resume and fork keep
the identity recorded by the original session; review remains its dedicated
mode rather than becoming an arbitrary role launch.

## Context

The bundle installs role files such as `adversary.toml`, `implementer.toml`, and
`code-reviewer.toml`, but `codex exec` has no option that selects one. The files
are loaded into `Config.agent_roles`; production role application currently
happens only inside the multi-agent spawn path. A caller can manually repeat a
role's model, reasoning effort, and developer instructions through unrelated
config overrides, but that does not select or validate the named role.

## Alternatives considered

1. **First-class `--agent` (chosen).** Reuses role discovery, validation, and
   config layering. It gives the CLI a stable named identity surface.
2. **Map roles onto `--profile`.** Requires duplicate files outside
   `$CODEX_HOME/agents` and does not validate the role registry.
3. **Translate role files into `-c` overrides in the exec crate.** Duplicates
   parsing and precedence rules already owned by `codex-core`.

## Command contract

```text
codex exec --agent adversary -- "Review this claim" < /dev/null
```

- `--agent` accepts a non-empty installed or built-in role name.
- The role profile owns `model`, `model_reasoning_effort`, and
  `developer_instructions`, matching native spawn semantics.
- Headless invocation state remains sticky across role application:
  `approval_policy`, sandbox/permission selection, CWD and writable roots,
  executable paths, hook-trust bypass, persistence mode, and PSP routing.
- An unknown or malformed role fails before authentication or a Responses API
  request.
- `--agent` with `resume`, `fork`, or `review` returns a usage error. An existing
  session's role is never silently replaced.

## Architecture

### CLI surface

`codex-rs/exec/src/cli.rs` adds `agent: Option<String>` to the root exec CLI.
Argument validation keeps the option on new sessions only.

### Core boundary

`codex-core` exposes one narrowly-scoped exec role application function. It
resolves the already-loaded role registry and invokes the existing role-layer
implementation. The exec crate does not parse agent TOML itself.

### Runtime precedence

Role application rebuilds config because role files are config layers. The exec
adapter snapshots and reapplies invocation-owned runtime overrides after that
rebuild. Role-owned identity fields remain authoritative, while runtime safety
and process selections cannot be dropped accidentally.

## Error handling

- Empty role: Clap value validation error.
- Unknown role: `unknown agent_type '<name>'`, before session startup.
- Malformed role file: existing role loader diagnostic, promoted to a nonzero
  exec startup failure.
- Role used with a non-new-session subcommand: explicit usage error naming the
  incompatible command.

## Verification

1. Parser test proves `--agent adversary` reaches the exec CLI value and rejects
   an empty value.
2. Core test applies a real temporary role file and proves model, effort, and
   developer instructions change.
3. Exec test proves danger-full-access/never-approve/CWD/hook-trust state survives
   role application.
4. Negative tests prove unknown roles and resume/fork/review combinations fail
   before a mocked Responses endpoint receives a request.
5. End-to-end GitHub Actions smoke runs a built `codex exec --agent adversary`
   against a temporary role and inspects `whoami` plus the developer instruction
   marker.

## Out of scope

- Changing native `spawn_agent` role precedence.
- Adding role switching to an already-running session.
- Treating config profiles and agent roles as aliases.
- Making role files a filesystem security boundary.

