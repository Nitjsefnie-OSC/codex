use std::sync::Arc;

use crate::agents_md::LoadedAgentsMd;
use crate::environment_selection::TurnEnvironmentSnapshot;
use crate::session::turn_context::TurnContext;
use crate::tools::router::ToolRouter;
use codex_exec_server::ExecutorCapabilityDiscoverySnapshot;
use codex_exec_server::ResolvedSelectedCapabilityRoot;
use codex_mcp::McpBinding;
use tokio::sync::Mutex;

/// Response-scoped server identity observed while a sampling step is in flight.
///
/// A new generation is created for every sampling attempt. This prevents a
/// late event from a cancelled/retried response from overwriting the identity
/// of the current response before a tool call reads it.
#[derive(Debug, Default)]
pub(crate) struct ResponseIdentityState {
    state: Mutex<ResponseIdentitySnapshot>,
}

#[derive(Debug, Default)]
struct ResponseIdentitySnapshot {
    generation: u64,
    latest_server_model: Option<String>,
}

impl ResponseIdentityState {
    pub(crate) async fn begin_response(&self) -> u64 {
        let mut state = self.state.lock().await;
        state.generation = state.generation.wrapping_add(1);
        state.latest_server_model = None;
        state.generation
    }

    pub(crate) async fn record_server_model_for_response(
        &self,
        response_generation: u64,
        server_model: String,
    ) {
        let mut state = self.state.lock().await;
        if state.generation == response_generation {
            state.latest_server_model = Some(server_model);
        }
    }

    pub(crate) async fn latest_server_model(&self) -> Option<String> {
        self.state.lock().await.latest_server_model.clone()
    }
}

/// Request-scoped state that may change between model sampling requests.
pub(crate) struct StepContext {
    pub(crate) turn: Arc<TurnContext>,
    pub(crate) response_identity: Arc<ResponseIdentityState>,
    pub(crate) environments: TurnEnvironmentSnapshot,
    /// Capability roots bound to ready environments in this exact step.
    pub(crate) selected_capability_roots: Vec<ResolvedSelectedCapabilityRoot>,
    /// Executor-materialized capability files shared by MCP and skills in this exact step.
    pub(crate) executor_capability_discovery: Option<Arc<ExecutorCapabilityDiscoverySnapshot>>,
    /// The exact MCP connections, configuration, and catalog captured for this step.
    pub(crate) mcp: Arc<McpBinding>,
    /// The finalized tool plan advertised and executed for this exact sampling request.
    pub(crate) tool_router: Arc<ToolRouter>,
    /// The canonical AGENTS.md value observed with this environment snapshot.
    pub(crate) loaded_agents_md: Option<Arc<LoadedAgentsMd>>,
}
