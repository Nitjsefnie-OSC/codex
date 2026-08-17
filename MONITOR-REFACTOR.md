# `monitor` on unified exec — implementation report

Branch `feat/monitor-unified-exec`, two commits on top of fork `main`
(`180f2b8f88`):

- `37a2def789` — Rebuild monitor on the unified exec process manager
- `766a4ddddb` — monitor: keep the head of the output and record terminal state

## The design

### Processes live in `UnifiedExecProcessManager`, and only there

`monitor` no longer spawns anything itself. The handler builds an
`ExecCommandRequest` — the same struct `exec_command` builds — and calls
`UnifiedExecProcessManager::start_monitor`, which is `exec_command` with a
watcher attached. `exec_command` and `start_monitor` are both thin wrappers over
one `exec_command_inner`, so there is exactly one spawn path.

Everything the gap analysis listed as missing is therefore inherited rather than
reimplemented:

| Capability | Where it comes from |
|---|---|
| process id | `allocate_process_id` / `ProcessStore` |
| bounded retained output | the `HeadTailBuffer` transcript `exec_command` already creates |
| list / terminate | `list_processes`, `terminate_process`, `terminate_all_processes` |
| `write_stdin` reads | unchanged — a monitor is an ordinary entry in the process store |
| survival across turn interruption | the manager owns the process; nothing ties it to the turn's cancellation token |
| approvals, sandboxing, network policy | `open_session_with_sandbox` via `ToolOrchestrator` |

There is **no second process registry**. What was added is a metadata table
(`unified_exec/monitors.rs`) keyed by the manager's own process ids, living
inside `UnifiedExecProcessManager` next to `process_store`, holding only what
unified exec has no opinion about: watcher classification, ownership, terminal
state, notification counters, and the read watermark. It holds an `Arc` of the
*same* transcript buffer unified exec fills — not a copy — which is what lets a
finished or stopped monitor still be read after its process-store entry is gone.

### The model-facing channel

`unified_exec/async_watcher.rs` streams output to the *client* as
`ExecCommandOutputDelta` events. The model sees none of that. The new
`unified_exec/monitor_watcher.rs` runs beside it on the same process:

- reads the process's broadcast output channel (no polling, no second copy);
- accumulates **complete lines**, holding back a trailing partial line until it
  terminates, and force-flushing a line that never terminates (a `\r` progress
  bar) once it reaches 4 KiB;
- batches on a 500 ms interval, or immediately once a batch reaches 40 lines;
- injects each batch as a `monitor_notification` developer message through
  `Session::inject_no_new_turn`, which delivers into the *active* turn if one is
  running and otherwise records into history for the next one.

Bounds, all in `unified_exec/monitors.rs`:

| Bound | Value | Behaviour past it |
|---|---|---|
| lines per notification | 40 | remainder reported as `omitted_lines`; stays in retained output |
| bytes per notification | 4096 | a single over-long line is truncated on a char boundary rather than dropped |
| notifications per monitor | 20 | batches counted as `suppressed_notifications`, not sent |

The terminal notification is **never** capped and is delivered exactly once on
every path — clean exit, non-zero exit, spawn failure, explicit stop, session
teardown, timeout. Uniqueness is a `compare_exchange` on the handle
(`claim_terminal`), so a second attempt is a no-op rather than a second message.

### Watcher semantics

- **Classification** — `kind: "job" | "watcher"`. A job is expected to finish and
  gets a 10-minute default ceiling; a watcher is persistent and gets no default
  ceiling, running until stopped or until the session tears its processes down.
- **Ownership** — `MonitorOwner { model_slug, sub_id, call_id }`, taken from the
  turn that started it.
- **Terminal state** — `running | exited{exit_code} | failed{message} | stopped |
  timed_out`, published on a `watch` channel so `wait` is edge-free.
- **Read acknowledgement** — every notification carries a dense sequence number;
  `read` advances an acknowledgement watermark (monotonic, never past what was
  delivered), and `list` exposes `unacknowledged_notifications`. That is the
  "prove the output was consumed" bit.

### Control surface

To the model, as `monitor` actions: `start` (default), `list`, `read`, `stop`,
`wait`. All return JSON.

To the app-server, on `CodexThread` — the same public core API that already
carries `list_background_terminals` / `terminate_background_terminal`:
`list_monitors`, `read_monitor_output`, `stop_monitor`, `wait_for_monitor`, with
`MonitorInfo` / `MonitorOutput` / `MonitorState` / `MonitorKind` /
`MonitorOwner` / `MonitorAcknowledgement` / `MonitorWaitOutcome` re-exported from
`codex_core`.

**Deliberately not done:** no new JSON-RPC methods on `app-server-protocol`.
Adding a `ClientRequest` variant there requires regenerating the vendored schema
fixtures (`app-server-protocol/schema/json`, `/typescript`, and two zstd-compressed
precomputed export blobs), which needs `cargo test -p codex-app-server-protocol`
plus Prettier — a build heavier than this machine's stated ceiling permits, and
the fork-verify workflow only runs `cargo check`. The core-side API is complete,
so wiring the JSON-RPC methods later is mechanical.

## What was reused vs added

**Reused, unchanged:** `UnifiedExecProcessManager` and its process store,
`UnifiedExecProcess`, `HeadTailBuffer`, `ToolOrchestrator` / sandbox / approval
path, `async_watcher::start_streaming_output` and `spawn_exit_watcher`,
`ToolEmitter`, `ContextualUserFragment` + `Session::inject_no_new_turn`.

**Added:**

- `core/src/unified_exec/monitors.rs` — metadata types, counters, store
- `core/src/unified_exec/monitor_watcher.rs` — line batching + notification pump
- `core/src/context/monitor_notification.rs` — the model-visible fragment
- `start_monitor` / `attach_monitor` / `list_monitors` / `read_monitor_output` /
  `stop_monitor` / `wait_for_monitor` on the manager
- monitor methods on `Session` and `CodexThread`

**Removed:** `core/src/exec_output_deltas.rs` and its tests, and the
`StdoutStream::chunking` field. That module existed only so the old launcher
could emit line-aligned deltas through `process_exec_tool_call`; the new monitor
does not use that path, so its `Lines` mode had no reachable caller and keeping
it would have left a second line splitter in the tree. `exec.rs`,
`tasks/user_shell.rs` and `tools/runtimes/shell.rs` are back to upstream.

**Refactored:** `get_command`'s shell-wrapping half is extracted as
`resolve_shell_command`, so a monitored command is wrapped exactly the way
`exec_command` wraps one (including zsh-fork mode) instead of being duplicated.

**Unrelated but required:** `#![recursion_limit = "256"]` on `codex-core`.
`monitor` reaches the orchestrator through two more async layers than
`exec_command`, and auto-trait solving for the resulting future exceeds the
default limit. The compiler's own suggested fix.

## `EventMsg` — not touched

No `EventMsg` variant was added, changed, or removed. The exhaustive consumer at
`mcp-server/src/codex_tool_runner.rs` and the partial one at
`app-server-protocol/src/protocol/event_mapping.rs` are both untouched.
Model-facing notifications ride `ResponseItem::Message`, not the event stream;
client-facing output still rides the existing `ExecCommandOutputDelta` /
`ExecCommandEnd` family that unified exec already emits. `cargo check --workspace`
on CI confirms no consumer broke.

## Feature flag

`features/src/lib.rs`, `Feature::MonitorTool` / key `monitor_tool`:

| | before | after |
|---|---|---|
| `stage` | `Stage::UnderDevelopment` | `Stage::Stable` |
| `default_enabled` | `false` | `true` |

**Effective default: on.** `spec_plan.rs` registers `MonitorHandler` when
`environment_mode.has_environment() && features.enabled(Feature::MonitorTool)`,
so with the flag defaulting to `true` the tool is present in every session that
has an environment. `monitor_tool = false` in config still turns it off.
`core/tests/suite/prompt_caching.rs`'s golden tool-name list gains `monitor`
between `apply_patch` and `view_image` to match; that test passes on CI.

## Tests

34 tests, all executed on CI (see below). They assert behaviour, not compilation:

| Contract | Test |
|---|---|
| lines arrive as separate sequenced notifications | `output_lines_arrive_as_separate_sequenced_notifications` — asserts ≥2 batches, dense `seq` from 1, and that the lines reassemble to `first/second/third` |
| the cap holds | `a_noisy_monitor_is_capped_but_its_output_stays_readable` — 2000 lines, asserts ≤ `MAX_MONITOR_NOTIFICATIONS + 1` notifications |
| the head of the output is never lost | same test — asserts the first notification opens on `line-1` |
| terminal notification always fires, exactly once | `a_successful_job_always_delivers_exactly_one_terminal_notification`, `a_failing_job_reports_its_exit_code_in_the_terminal_notification`, `a_job_that_outlives_its_timeout_still_terminates_and_says_so` |
| a stopped monitor leaves no orphan | `stopping_a_watcher_kills_the_process_rather_than_orphaning_it` — the watcher appends to a file in a loop; after `stop`, the file's size must not grow for a further second, and the process must be gone from the process store |
| retained output is readable after the fact | same firehose test, plus `list_reports_classification_ownership_and_unread_notifications` reading `hello\n` back from a finished monitor |
| watcher classification / ownership / ack | `list_reports_classification_ownership_and_unread_notifications` |
| wait | `wait_blocks_until_a_job_finishes_and_reports_it_did`, `wait_gives_up_on_a_persistent_watcher_without_stopping_it` |
| interruption survival | `a_monitor_survives_the_interruption_of_the_turn_that_started_it` |
| units | `unified_exec/monitors_tests.rs` (sequence density, cap, ack monotonicity), `unified_exec/monitor_watcher_tests.rs` (line splitting, `\r\n`, runaway line, byte/line caps, char-boundary truncation), `context/monitor_notification_tests.rs` (payload shape) |

Because this machine may not run `cargo test`, `fork-verify` now runs
`cargo test -p codex-core --lib monitor` on GitHub's runners. That is where the
contract is actually executed.

## Two defects CI caught

The first push failed 3 of 34. Both root causes were product defects, not test
problems, and both are fixed in `766a4ddddb` with regression tests:

1. **The head of the output was dropped.** A process starts writing when it is
   spawned; approvals and sandbox selection sit between that and the point where
   the watcher subscribes, and a `tokio::sync::broadcast` receiver only sees what
   is sent after it exists. The first line vanished, and a command that finished
   before the watcher attached lost everything — the firehose test read back zero
   bytes. The bytes were never missing, only unreachable: the process appends
   every chunk to its own output buffer *before* broadcasting it. The monitor now
   seeds itself from that buffer and subscribes to the broadcast **under the same
   lock the reader takes to append**, so nothing can be published between the two.
   The seed feeds the transcript as well as the first notification. Seeding drains
   bytes the initial yield would otherwise have collected, so they are put back in
   front of the collected output — the tool result and the sandbox-denial check
   are unchanged.
2. **Terminal state was never stored.** `watch::Sender::send` is a no-op when
   nothing is subscribed, which is the common case (a monitor usually finishes
   before anyone waits on it), so `list` reported a finished monitor as still
   running. `send_replace` stores it unconditionally.

## CI

| Run | Commit | Result |
|---|---|---|
| [`31083459109`](https://github.com/Nitjsefnie-OSC/codex/actions/runs/31083459109) | `37a2def789` | **failure** — `cargo check --workspace` green, monitor tests 31 passed / 3 failed |
| [`31084818484`](https://github.com/Nitjsefnie-OSC/codex/actions/runs/31084818484) | `766a4ddddb` | **success** — workspace check green, monitor tests 34 passed / 0 failed, golden tool-name list green |

## Machine budget

`df -h /` before any cargo invocation: **116G free** (394G total, 262G used).
`df -h /` after all local work: **109G free** (269G used); `codex-rs/target` is
11G. Never below the 40G floor. Nothing heavier than
`cargo check -p codex-core --all-targets` and `cargo clippy -p codex-core
--all-targets` ran locally — no `cargo build`, no `cargo test`, no `--workspace`.

## Unverified / known limitations

- **The subscribe race is narrowed, not mathematically eliminated.** The reader
  appends under the output-buffer lock and then sends to the broadcast *after*
  releasing it. Taking the seed and subscribing under that same lock removes the
  large window (the whole approval/sandbox phase) but leaves an
  instruction-level one in which a chunk could in principle be both seeded and
  broadcast, showing up as one duplicated line. I have not constructed a case
  that hits it, and I have not proven it cannot happen.
- **Broadcast lag.** The channel has capacity 64. A monitor whose watcher falls
  that far behind records `lagged` and says so in the terminal notification; the
  retained transcript is authoritative in that case. Not exercised by a test.
- **Process-id reuse.** A short-lived monitor's id is released back to the
  allocator and could later be handed to a new process. Monitor records are only
  written by monitors, so the only reachable effect is a new monitor replacing an
  old record under the same id — which is correct — but I have not tested it.
- **`MAX_RETAINED_MONITORS` eviction** (64, oldest terminal record first) is
  implemented but has no direct test; constructing 64 real processes in a unit
  test was not worth the runtime.
- **App-server JSON-RPC methods** are not registered, for the reason given above.
  Only the `CodexThread` API is exposed.
- **Remote environments.** `start_monitor` passes the turn environment straight
  through, so a monitor on a remote environment should behave like
  `exec_command` there. Untested — CI runs on a local environment only.
