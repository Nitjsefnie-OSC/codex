use super::*;
use pretty_assertions::assert_eq;

#[test]
fn resume_parses_prompt_after_global_flags() {
    const PROMPT: &str = "echo resume-with-global-flags-after-subcommand";
    let cli = Cli::parse_from([
        "codex-exec",
        "resume",
        "--last",
        "--json",
        "--model",
        "gpt-5.2-codex",
        "--dangerously-bypass-approvals-and-sandbox",
        "--skip-git-repo-check",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        PROMPT,
    ]);

    assert!(cli.ephemeral);
    assert!(cli.ignore_user_config);
    assert!(cli.ignore_rules);
    let Some(Command::Resume(args)) = cli.command else {
        panic!("expected resume command");
    };
    let effective_prompt = args.prompt.clone().or_else(|| {
        if args.last {
            args.session_id.clone()
        } else {
            None
        }
    });
    assert_eq!(effective_prompt.as_deref(), Some(PROMPT));
}

#[test]
fn resume_accepts_output_flags_after_subcommand() {
    const PROMPT: &str = "echo resume-with-output-file";
    let cli = Cli::parse_from([
        "codex-exec",
        "resume",
        "session-123",
        "-o",
        "/tmp/resume-output.md",
        "--output-schema",
        "/tmp/schema.json",
        PROMPT,
    ]);

    assert_eq!(
        cli.last_message_file,
        Some(PathBuf::from("/tmp/resume-output.md"))
    );
    assert_eq!(cli.output_schema, Some(PathBuf::from("/tmp/schema.json")));
    let Some(Command::Resume(args)) = cli.command else {
        panic!("expected resume command");
    };
    assert_eq!(args.session_id.as_deref(), Some("session-123"));
    assert_eq!(args.prompt.as_deref(), Some(PROMPT));
}

#[test]
fn fork_parses_prompt_after_global_flags() {
    const PROMPT: &str = "continue on the fork";
    let cli = Cli::parse_from([
        "codex-exec",
        "fork",
        "session-123",
        "--json",
        "--model",
        "gpt-5.2-codex",
        "--skip-git-repo-check",
        "--ephemeral",
        PROMPT,
    ]);

    assert!(cli.json);
    assert!(cli.ephemeral);
    let Some(Command::Fork(args)) = cli.command else {
        panic!("expected fork command");
    };
    assert_eq!(args.session_id, "session-123");
    assert_eq!(args.prompt.as_deref(), Some(PROMPT));
}

#[test]
fn parses_config_isolation_flags() {
    let cli = Cli::parse_from([
        "codex-exec",
        "--ignore-user-config",
        "--ignore-rules",
        "summarize",
    ]);

    assert!(cli.ignore_user_config);
    assert!(cli.ignore_rules);
}

#[test]
fn headless_agent_role_parses_for_new_session() {
    let cli = Cli::parse_from([
        "codex-exec",
        "--agent",
        "adversary",
        "Review this claim",
    ]);

    assert_eq!(cli.agent.as_deref(), Some("adversary"));
    assert!(cli.command.is_none());
    assert_eq!(cli.prompt.as_deref(), Some("Review this claim"));
}

#[test]
fn headless_agent_role_rejects_blank_value() {
    let error = Cli::try_parse_from(["codex-exec", "--agent", "", "Review this claim"])
        .expect_err("blank agent role should be rejected");

    assert_eq!(error.kind(), clap::error::ErrorKind::InvalidValue);
}

#[test]
fn headless_agent_role_rejects_existing_session_commands() {
    for (args, command_name) in [
        (
            vec!["codex-exec", "--agent", "adversary", "resume", "--last"],
            "resume",
        ),
        (
            vec![
                "codex-exec",
                "--agent",
                "adversary",
                "fork",
                "session-123",
            ],
            "fork",
        ),
        (
            vec![
                "codex-exec",
                "--agent",
                "adversary",
                "review",
                "--uncommitted",
            ],
            "review",
        ),
    ] {
        let cli = Cli::parse_from(args);
        let error = validate_agent_role_command(cli.agent.as_deref(), cli.command.as_ref())
            .expect_err("existing-session command should reject --agent");

        assert!(error.contains("--agent"), "unexpected error: {error}");
        assert!(error.contains(command_name), "unexpected error: {error}");
    }
}

#[test]
fn approve_for_me_flag_applies_to_resume_when_passed_at_exec_root() {
    for flag in ["--approve-for-me", "--not-so-yolo"] {
        let cli = Cli::parse_from(["codex-exec", flag, "resume", "--last"]);

        assert!(cli.auto_review);
    }
}

#[test]
fn approve_for_me_flag_conflicts_with_other_sandbox_modes() {
    for conflicting_args in [
        vec!["--sandbox", "read-only"],
        vec!["--dangerously-bypass-approvals-and-sandbox"],
    ] {
        let mut args = vec!["codex-exec", "--approve-for-me"];
        args.extend(conflicting_args);
        args.push("summarize");

        let error = Cli::try_parse_from(args).expect_err("flags should conflict");
        assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
    }
}
