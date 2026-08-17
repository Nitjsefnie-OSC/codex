//! Background work that outlives the turn, as seen by a stop hook.
//!
//! A `Stop` or `SubagentStop` hook is the last point at which anything can
//! object to a turn ending, and the usual thing worth objecting to is work the
//! agent started and walked away from: a monitor still running, or output it
//! was notified about and never read. Until this payload existed a hook had no
//! way to know — it could see the model, the cwd, and the transcript path, none
//! of which say whether a build is still going. Reconstructing it from the
//! transcript means re-deriving live process state from a log, which is both
//! racy and wrong after a process exits.
//!
//! These types are the authoritative answer, read from the process manager at
//! the moment the hook fires.

use schemars::JsonSchema;
use serde::Serialize;

/// Everything still outstanding when the turn tried to end.
#[derive(Debug, Clone, Default, PartialEq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "background-state")]
pub struct BackgroundState {
    /// Every monitor this session started, running or finished. A finished one
    /// still matters: its output may never have been read.
    pub monitors: Vec<MonitorSnapshot>,
    /// How many of them are still running.
    pub running_monitors: u32,
    /// Notifications delivered to the model across all monitors that no `read`
    /// has acknowledged. Non-zero means output was produced and not consumed.
    pub unacknowledged_notifications: u64,
    /// Background terminals held open by `exec_command`, which are processes
    /// the session can still write to and read from.
    pub background_terminals: Vec<BackgroundTerminalSnapshot>,
    /// How many of those there are, so a hook can branch without walking the
    /// list.
    pub running_background_terminals: u32,
}

impl BackgroundState {
    /// Whether anything is still running. A hook that only gates on "did you
    /// leave something running" needs nothing else.
    pub fn has_running_work(&self) -> bool {
        self.running_monitors > 0 || self.running_background_terminals > 0
    }
}

/// One monitor at the moment the hook fired.
#[derive(Debug, Clone, PartialEq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "background-state.monitor")]
pub struct MonitorSnapshot {
    pub process_id: i32,
    pub command: String,
    pub cwd: String,
    /// `job` for something expected to finish, `watcher` for something that
    /// runs until stopped. A watcher left running is normal; a job left
    /// running usually is not.
    pub kind: String,
    /// `running`, `exited`, `failed`, `stopped`, or `timed_out`.
    pub state: String,
    pub running: bool,
    /// Set when `state` is `exited`.
    pub exit_code: Option<i32>,
    /// Set when `state` is `failed`.
    pub failure_message: Option<String>,
    pub age_seconds: f64,
    pub notifications_delivered: u64,
    pub notifications_suppressed: u64,
    /// Delivered notifications this monitor's output has not been read back
    /// for.
    pub unacknowledged_notifications: u64,
    /// The turn that started the monitor, so a hook can tell a subagent's
    /// watcher from its parent's.
    pub owner_model_slug: String,
    pub owner_turn_id: String,
    pub owner_call_id: String,
}

/// One background terminal at the moment the hook fired.
#[derive(Debug, Clone, PartialEq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "background-state.background-terminal")]
pub struct BackgroundTerminalSnapshot {
    pub process_id: String,
    pub command: String,
    pub cwd: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn monitor(state: &str, running: bool) -> MonitorSnapshot {
        MonitorSnapshot {
            process_id: 1,
            command: "cargo build".to_string(),
            cwd: "/repo".to_string(),
            kind: "job".to_string(),
            state: state.to_string(),
            running,
            exit_code: None,
            failure_message: None,
            age_seconds: 1.5,
            notifications_delivered: 3,
            notifications_suppressed: 0,
            unacknowledged_notifications: 3,
            owner_model_slug: "gpt-5".to_string(),
            owner_turn_id: "turn-1".to_string(),
            owner_call_id: "call-1".to_string(),
        }
    }

    #[test]
    fn an_empty_state_has_no_running_work() {
        assert!(!BackgroundState::default().has_running_work());
    }

    #[test]
    fn a_running_monitor_is_running_work() {
        let state = BackgroundState {
            monitors: vec![monitor("running", true)],
            running_monitors: 1,
            unacknowledged_notifications: 3,
            ..Default::default()
        };
        assert!(state.has_running_work());
    }

    #[test]
    fn a_finished_monitor_with_unread_output_is_not_running_work() {
        // The distinction matters: a hook that blocks on unread output is a
        // different policy from one that blocks on live processes, and this
        // payload has to let a hook implement either.
        let state = BackgroundState {
            monitors: vec![monitor("exited", false)],
            running_monitors: 0,
            unacknowledged_notifications: 3,
            ..Default::default()
        };
        assert!(!state.has_running_work());
        assert_eq!(state.unacknowledged_notifications, 3);
    }

    #[test]
    fn a_background_terminal_alone_is_running_work() {
        let state = BackgroundState {
            background_terminals: vec![BackgroundTerminalSnapshot {
                process_id: "7".to_string(),
                command: "bash".to_string(),
                cwd: "/repo".to_string(),
            }],
            running_background_terminals: 1,
            ..Default::default()
        };
        assert!(state.has_running_work());
    }

    #[test]
    fn a_snapshot_serializes_every_field_a_hook_reads() {
        let json = serde_json::to_value(BackgroundState {
            monitors: vec![monitor("running", true)],
            running_monitors: 1,
            unacknowledged_notifications: 3,
            ..Default::default()
        })
        .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(json["running_monitors"], 1);
        assert_eq!(json["unacknowledged_notifications"], 3);
        assert_eq!(json["running_background_terminals"], 0);
        assert_eq!(json["monitors"][0]["state"], "running");
        assert_eq!(json["monitors"][0]["kind"], "job");
        assert_eq!(json["monitors"][0]["running"], true);
        // Present-but-null rather than absent, so a hook can read the key
        // unconditionally.
        assert!(json["monitors"][0]["exit_code"].is_null());
        assert!(json["monitors"][0]["failure_message"].is_null());
        assert_eq!(json["monitors"][0]["owner_turn_id"], "turn-1");
    }
}
