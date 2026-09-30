//! `/ps` aggregation across app-owned native agents and chat-owned processes.

use super::*;

impl App {
    pub(super) fn add_process_list_output(&mut self) {
        let native_agents = self
            .agent_navigation
            .ordered_path_backed_subagent_threads(self.primary_thread_id)
            .into_iter()
            .filter_map(|(_, entry)| {
                if !entry.is_running || entry.is_closed {
                    return None;
                }
                Some(history_cell::NativeAgentDetails {
                    agent_path: entry.agent_path.as_deref()?.trim().to_string(),
                    label: format_agent_picker_item_name(
                        entry.agent_nickname.as_deref(),
                        entry.agent_role.as_deref(),
                        /*is_primary*/ false,
                    ),
                })
            })
            .collect();
        self.chat_widget.add_ps_output(native_agents);
    }
}
