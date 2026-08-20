//! Background terminal interaction and process-summary history cells.

use super::*;
use crate::width::display_width;

#[derive(Debug)]
pub(crate) struct UnifiedExecInteractionCell {
    command_display: Option<String>,
    stdin: String,
}

impl UnifiedExecInteractionCell {
    pub(crate) fn new(command_display: Option<String>, stdin: String) -> Self {
        Self {
            command_display,
            stdin,
        }
    }
}

impl HistoryCell for UnifiedExecInteractionCell {
    fn display_lines(&self, width: u16) -> Vec<Line<'static>> {
        if width == 0 {
            return Vec::new();
        }
        let wrap_width = width as usize;
        let waited_only = self.stdin.is_empty();

        let mut header_spans = if waited_only {
            vec!["• Waited for background terminal".bold()]
        } else {
            vec!["↳ ".dim(), "Interacted with background terminal".bold()]
        };
        if let Some(command) = &self.command_display
            && !command.is_empty()
        {
            header_spans.push(" · ".dim());
            header_spans.push(command.clone().dim());
        }
        let header = Line::from(header_spans);

        let mut out: Vec<Line<'static>> = Vec::new();
        let header_wrapped = adaptive_wrap_line(&header, RtOptions::new(wrap_width));
        push_owned_lines(&header_wrapped, &mut out);

        if waited_only {
            return out;
        }

        let input_lines: Vec<Line<'static>> = self
            .stdin
            .lines()
            .map(|line| Line::from(line.to_string()))
            .collect();

        let input_wrapped = adaptive_wrap_lines(
            input_lines,
            RtOptions::new(wrap_width)
                .initial_indent(Line::from("  └ ".dim()))
                .subsequent_indent(Line::from("    ".dim())),
        );
        out.extend(input_wrapped);
        out
    }

    fn raw_lines(&self) -> Vec<Line<'static>> {
        let mut out = Vec::new();
        if self.stdin.is_empty() {
            if let Some(command) = self
                .command_display
                .as_ref()
                .filter(|command| !command.is_empty())
            {
                out.push(Line::from(format!(
                    "Waited for background terminal: {command}"
                )));
            } else {
                out.push(Line::from("Waited for background terminal"));
            }
            return out;
        }

        if let Some(command) = self
            .command_display
            .as_ref()
            .filter(|command| !command.is_empty())
        {
            out.push(Line::from(format!(
                "Interacted with background terminal: {command}"
            )));
        } else {
            out.push(Line::from("Interacted with background terminal"));
        }
        out.extend(raw_lines_from_source(&self.stdin));
        out
    }
}

pub(crate) fn new_unified_exec_interaction(
    command_display: Option<String>,
    stdin: String,
) -> UnifiedExecInteractionCell {
    UnifiedExecInteractionCell::new(command_display, stdin)
}

#[derive(Debug)]
struct ProcessListCell {
    native_agents: Vec<NativeAgentDetails>,
    exec_command_processes: Vec<UnifiedExecProcessDetails>,
    monitor_processes: Vec<UnifiedExecProcessDetails>,
}

impl ProcessListCell {
    fn new(
        native_agents: Vec<NativeAgentDetails>,
        exec_command_processes: Vec<UnifiedExecProcessDetails>,
        monitor_processes: Vec<UnifiedExecProcessDetails>,
    ) -> Self {
        Self {
            native_agents,
            exec_command_processes,
            monitor_processes,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NativeAgentDetails {
    pub(crate) agent_path: String,
    pub(crate) label: String,
}

#[derive(Debug, Clone)]
pub(crate) struct UnifiedExecProcessDetails {
    pub(crate) command_display: String,
    pub(crate) recent_chunks: Vec<String>,
}

impl ProcessListCell {
    fn append_process_section(
        out: &mut Vec<Line<'static>>,
        title: &'static str,
        empty_message: &'static str,
        processes: &[UnifiedExecProcessDetails],
        wrap_width: usize,
    ) {
        let max_processes = 16usize;
        out.push(vec![title.bold()].into());
        out.push("".into());

        if processes.is_empty() {
            out.push(format!("  • {empty_message}").italic().into());
            return;
        }

        let prefix = "  • ";
        let prefix_width = display_width(prefix);
        let truncation_suffix = " [...]";
        let truncation_suffix_width = display_width(truncation_suffix);
        let mut shown = 0usize;
        for process in processes {
            if shown >= max_processes {
                break;
            }
            let command = &process.command_display;
            let (snippet, snippet_truncated) = {
                let (first_line, has_more_lines) = match command.split_once('\n') {
                    Some((first, _)) => (first, true),
                    None => (command.as_str(), false),
                };
                let max_graphemes = 80;
                let mut graphemes = first_line.grapheme_indices(true);
                if let Some((byte_index, _)) = graphemes.nth(max_graphemes) {
                    (first_line[..byte_index].to_string(), true)
                } else {
                    (first_line.to_string(), has_more_lines)
                }
            };
            if wrap_width <= prefix_width {
                out.push(Line::from(prefix.dim()));
                shown += 1;
                continue;
            }
            let budget = wrap_width.saturating_sub(prefix_width);
            let mut needs_suffix = snippet_truncated;
            if !needs_suffix {
                let (_, remainder, _) = take_prefix_by_width(&snippet, budget);
                if !remainder.is_empty() {
                    needs_suffix = true;
                }
            }
            if needs_suffix && budget > truncation_suffix_width {
                let available = budget.saturating_sub(truncation_suffix_width);
                let (truncated, _, _) = take_prefix_by_width(&snippet, available);
                out.push(vec![prefix.dim(), truncated.cyan(), truncation_suffix.dim()].into());
            } else {
                let (truncated, _, _) = take_prefix_by_width(&snippet, budget);
                out.push(vec![prefix.dim(), truncated.cyan()].into());
            }

            let chunk_prefix_first = "    ↳ ";
            let chunk_prefix_next = "      ";
            for (idx, chunk) in process.recent_chunks.iter().enumerate() {
                let chunk_prefix = if idx == 0 {
                    chunk_prefix_first
                } else {
                    chunk_prefix_next
                };
                let chunk_prefix_width = display_width(chunk_prefix);
                if wrap_width <= chunk_prefix_width {
                    out.push(Line::from(chunk_prefix.dim()));
                    continue;
                }
                let budget = wrap_width.saturating_sub(chunk_prefix_width);
                let (truncated, remainder, _) = take_prefix_by_width(chunk, budget);
                if !remainder.is_empty() && budget > truncation_suffix_width {
                    let available = budget.saturating_sub(truncation_suffix_width);
                    let (shorter, _, _) = take_prefix_by_width(chunk, available);
                    out.push(
                        vec![chunk_prefix.dim(), shorter.dim(), truncation_suffix.dim()].into(),
                    );
                } else {
                    out.push(vec![chunk_prefix.dim(), truncated.dim()].into());
                }
            }
            shown += 1;
        }

        let remaining = processes.len().saturating_sub(shown);
        if remaining > 0 {
            let more_text = format!("... and {remaining} more running");
            if wrap_width <= prefix_width {
                out.push(Line::from(prefix.dim()));
            } else {
                let budget = wrap_width.saturating_sub(prefix_width);
                let (truncated, _, _) = take_prefix_by_width(&more_text, budget);
                out.push(vec![prefix.dim(), truncated.dim()].into());
            }
        }
    }
}

impl HistoryCell for ProcessListCell {
    fn display_lines(&self, width: u16) -> Vec<Line<'static>> {
        if width == 0 {
            return Vec::new();
        }

        let wrap_width = width as usize;
        let max_agents = 16usize;
        let prefix = "  • ";
        let prefix_width = display_width(prefix);
        let mut out: Vec<Line<'static>> = Vec::new();

        out.push(vec!["Native agents".bold()].into());
        out.push("".into());
        if self.native_agents.is_empty() {
            out.push("  • No native agents running.".italic().into());
        } else {
            for agent in self.native_agents.iter().take(max_agents) {
                if wrap_width <= prefix_width {
                    out.push(Line::from(prefix.dim()));
                    continue;
                }
                let row = format!("{} — {} — running", agent.agent_path, agent.label);
                let budget = wrap_width.saturating_sub(prefix_width);
                let (truncated, _, _) = take_prefix_by_width(&row, budget);
                out.push(vec![prefix.dim(), truncated.cyan()].into());
            }
            let remaining = self.native_agents.len().saturating_sub(max_agents);
            if remaining > 0 {
                let more_text = format!("... and {remaining} more running");
                let budget = wrap_width.saturating_sub(prefix_width);
                let (truncated, _, _) = take_prefix_by_width(&more_text, budget);
                out.push(vec![prefix.dim(), truncated.dim()].into());
            }
        }

        out.push("".into());
        Self::append_process_section(
            &mut out,
            "exec_command terminals",
            "No exec_command terminals running.",
            &self.exec_command_processes,
            wrap_width,
        );
        out.push("".into());
        Self::append_process_section(
            &mut out,
            "Monitor jobs and watchers",
            "No monitor jobs or watchers running.",
            &self.monitor_processes,
            wrap_width,
        );

        out
    }

    fn raw_lines(&self) -> Vec<Line<'static>> {
        plain_lines(self.display_lines(u16::MAX))
    }

    fn desired_height(&self, width: u16) -> u16 {
        self.display_lines(width).len() as u16
    }
}

pub(crate) fn new_process_list_output(
    native_agents: Vec<NativeAgentDetails>,
    exec_command_processes: Vec<UnifiedExecProcessDetails>,
    monitor_processes: Vec<UnifiedExecProcessDetails>,
) -> CompositeHistoryCell {
    let command = PlainHistoryCell::new(vec!["/ps".magenta().into()]);
    let summary = ProcessListCell::new(native_agents, exec_command_processes, monitor_processes);
    CompositeHistoryCell::new(vec![Box::new(command), Box::new(summary)])
}
