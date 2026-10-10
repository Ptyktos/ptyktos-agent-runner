//! Ptyktos substrate trait surface — the adapters the runner needs.
//!
//! Each trait is the boundary between the runner and one Ptyktos substrate
//! crate. The default impls are no-ops so the lib builds without the
//! substrate feature; with `--features substrate` the real adapters
//! wire the actual Ptyktos crates in `src/adapters/`.
//!
//! The five traits correspond to the five Ptyktos surfaces the factory
//! composes:
//!   - [`AgentWorkspace`]   — `ptyktos/agentvfs` (the agent's mountable VFS)
//!   - [`AgentRuntime`]    — `ptyktos/intent` (the RRSI agent harness)
//!   - [`TraceSubstrate`]  — `ptyktos/memoryfold` (the trace-ontology substrate)
//!   - [`ReviewSurface`]   — `ptyktos/caretaker` (PR review + ship)
//!   - [`GovernanceGate`]  — `ptyktos/telos` (the Done-event gate)
//!
//! Each trait returns a `SubstrateResult<T>` so the runner can fail
//! the phase on a substrate error and surface the failure to the
//! saga-orchestrator with a [`hosting_core::FailureCategory`].
//!
//! See `src/adapters.rs` for the real implementations.

use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use hosting_core::task::{FailureCategory, RunnerJob, RunnerPhase, RunnerRecipeRef};

/// Crate-wide result for substrate adapters. The `category` lets the
/// runner map a substrate failure to the right [`FailureCategory`]
/// the saga branches on.
pub type SubstrateResult<T> = std::result::Result<T, SubstrateError>;

#[derive(Debug, Error)]
pub enum SubstrateError {
    #[error("workspace mount failed: {0}")]
    WorkspaceMount(String),
    #[error("agent invocation failed: {0}")]
    AgentInvocation(String),
    #[error("trace substrate failed: {0}")]
    TraceSubstrate(String),
    #[error("review surface failed: {0}")]
    ReviewSurface(String),
    #[error("governance gate failed: {0}")]
    GovernanceGate(String),

    #[error("transient infrastructure failure: {0}")]
    Transient(String),
    #[error("precondition not met: {0}")]
    Precondition(String),
    #[error("policy denial: {0}")]
    Policy(String),
}

impl SubstrateError {
    /// Map a substrate error to the wire [`FailureCategory`] the saga
    /// branches on.
    pub fn category(&self) -> FailureCategory {
        match self {
            Self::WorkspaceMount(_)
            | Self::AgentInvocation(_)
            | Self::TraceSubstrate(_)
            | Self::ReviewSurface(_)
            | Self::GovernanceGate(_)
            | Self::Transient(_)
            | Self::Precondition(_)
            | Self::Policy(_) => {
                // The error variants already encode the category; the
                // match is exhaustive over the union.
                match self {
                    Self::Transient(_) => FailureCategory::Transient,
                    Self::Precondition(_) => FailureCategory::Precondition,
                    Self::Policy(_) => FailureCategory::Policy,
                    _ => FailureCategory::ProviderInternal,
                }
            }
        }
    }
}

/// The task description the agent is asked to work on. Extracted from
/// the [`RunnerJob`]'s `recipe.rendered_template` (the Tera-rendered
/// recipe body) and the audit context.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentTask {
    /// The Ptyktos repo to work in (e.g. `"ptyktos/memoryfold"`).
    pub repo: String,
    /// The branch the agent checks out.
    pub branch: String,
    /// The commit SHA to base on (or HEAD if absent).
    pub base_sha: Option<String>,
    /// The natural-language task description (Operately task body).
    pub task: String,
    /// A diff to apply, if the runner is in the `apply` phase.
    pub diff: Option<String>,
}

/// The result of the agent's work. The runner stores this in
/// [`AgentWorkspace`] so the next phase (verify) can read it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentOutput {
    /// The branch the agent pushed (or `None` if the agent didn't push).
    pub branch: Option<String>,
    /// The PR the agent opened (or `None`).
    pub pr_url: Option<String>,
    /// The trace substrate ids the agent emitted (for the audit chain).
    pub trace_ids: Vec<String>,
    /// The bench result the agent reported (e.g. `cargo test` summary).
    pub bench_summary: Option<String>,
    /// The agent's internal claim of completion (text).
    pub agent_note: Option<String>,
}

/// The agent's mountable workspace. The runner mounts the source tree
/// at `/srv/repo` (read-write) and the agent's scratch at `/tmp/agent`
/// (read-write, tmpfs-style). The `snapshot` / `restore` methods let
/// the runner rollback the apply phase on a verify failure.
#[async_trait]
pub trait AgentWorkspace: Send + Sync {
    /// Mount the workspace for a new job. Returns the workspace id
    /// (used in subsequent calls).
    async fn mount(&self, job: &RunnerJob) -> SubstrateResult<String>;

    /// Snapshot the current workspace state (for rollback on verify
    /// failure). The returned id is opaque; pass it to `restore`.
    async fn snapshot(&self, workspace_id: &str) -> SubstrateResult<String>;

    /// Restore the workspace to a previous snapshot. Idempotent.
    async fn restore(&self, workspace_id: &str, snapshot_id: &str) -> SubstrateResult<()>;

    /// Remove the workspace (called in the `destroy` phase).
    async fn destroy(&self, workspace_id: &str) -> SubstrateResult<()>;

    /// Read a file from the workspace (used in the verify phase to
    /// read the agent's report).
    async fn read(&self, workspace_id: &str, path: &str) -> SubstrateResult<String>;
}

/// The agent runtime. The runner invokes the agent with the task
/// description; the agent returns its output (or a structured error
/// the runner maps to a `SubstrateError`).
#[async_trait]
pub trait AgentRuntime: Send + Sync {
    /// Run the agent in the given workspace, on the given task, for
    /// the given phase. The agent writes its trace events to the
    /// trace substrate; the runner picks them up via
    /// [`TraceSubstrate::poll_for_job`].
    async fn run_phase(
        &self,
        workspace_id: &str,
        task: &AgentTask,
        phase: RunnerPhase,
    ) -> SubstrateResult<AgentOutput>;
}

/// The trace substrate. The runner uses this to record the cycle's
/// audit (one trace node per phase transition, plus the diff/result
/// for each) and to read the agent's trace events.
#[async_trait]
pub trait TraceSubstrate: Send + Sync {
    /// Record a trace node. The id is the content hash (stable opaque
    /// id; the Ptyktos OBO discipline).
    async fn record(
        &self,
        job: &RunnerJob,
        kind: &str,
        note: &str,
    ) -> SubstrateResult<String>;

    /// Read the trace ids the agent emitted for this job (for the
    /// verify phase's audit).
    async fn list_for_job(&self, job_id: &str) -> SubstrateResult<Vec<String>>;
}

/// The review surface. The runner calls this in the verify phase to
/// run the agent's PR through the Ptyktos review pipeline.
#[async_trait]
pub trait ReviewSurface: Send + Sync {
    /// Submit the agent's branch for review and return the review id.
    async fn submit_for_review(
        &self,
        job: &RunnerJob,
        branch: &str,
    ) -> SubstrateResult<String>;

    /// Poll the review; return the verdict (pass/fail) once it's
    /// decided. The runner calls this repeatedly with a backoff.
    async fn await_verdict(&self, review_id: &str) -> SubstrateResult<ReviewVerdict>;
}

/// The review verdict — pass or fail, with the rule that fired.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReviewVerdict {
    pub passed: bool,
    /// The rule that fired (or `None` if passed).
    pub rule: Option<String>,
    /// Human-readable explanation.
    pub reason: String,
}

/// The governance gate. The runner calls this in the verify phase
/// after the review passes. The gate is the authority on whether
/// the agent's `Done` event is allowed; a deny downgrades the cycle
/// to `Failed` and surfaces the reason.
#[async_trait]
pub trait GovernanceGate: Send + Sync {
    /// Submit the agent's `Done` event for gating.
    async fn gate(
        &self,
        job: &RunnerJob,
        review: &ReviewVerdict,
        output: &AgentOutput,
    ) -> SubstrateResult<GateDecision>;
}

/// The gate decision. `allowed = true` lets the cycle proceed to
/// `merge`; `allowed = false` downgrades to `Failed` with the `reason`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GateDecision {
    pub allowed: bool,
    pub reason: String,
}

// ─── no-op default impls (for the `minimal` feature) ─────────────────
//
// When the runner is built without `--features substrate`, the lib
// still compiles: every adapter defaults to a no-op that returns the
// documented success shape. The binary still reads RunnerJob + emits
// RunnerEvent; the actual agent work is stubbed. This is the
// `--features minimal` build, used for CI and contract tests.

/// The no-op substrate set. Used by the default trait impls.
#[derive(Debug, Default, Clone)]
pub struct NoopSubstrate;

#[async_trait]
impl AgentWorkspace for NoopSubstrate {
    async fn mount(&self, _job: &RunnerJob) -> SubstrateResult<String> {
        Ok("noop-workspace".to_string())
    }
    async fn snapshot(&self, _id: &str) -> SubstrateResult<String> {
        Ok("noop-snapshot".to_string())
    }
    async fn restore(&self, _id: &str, _s: &str) -> SubstrateResult<()> {
        Ok(())
    }
    async fn destroy(&self, _id: &str) -> SubstrateResult<()> {
        Ok(())
    }
    async fn read(&self, _id: &str, _p: &str) -> SubstrateResult<String> {
        Ok(String::new())
    }
}

#[async_trait]
impl AgentRuntime for NoopSubstrate {
    async fn run_phase(
        &self,
        _w: &str,
        _t: &AgentTask,
        _p: RunnerPhase,
    ) -> SubstrateResult<AgentOutput> {
        Ok(AgentOutput {
            branch: None,
            pr_url: None,
            trace_ids: vec![],
            bench_summary: None,
            agent_note: Some("noop: --features minimal".to_string()),
        })
    }
}

#[async_trait]
impl TraceSubstrate for NoopSubstrate {
    async fn record(
        &self,
        _j: &RunnerJob,
        _k: &str,
        _n: &str,
    ) -> SubstrateResult<String> {
        Ok("noop-trace".to_string())
    }
    async fn list_for_job(&self, _id: &str) -> SubstrateResult<Vec<String>> {
        Ok(vec![])
    }
}

#[async_trait]
impl ReviewSurface for NoopSubstrate {
    async fn submit_for_review(
        &self,
        _j: &RunnerJob,
        _b: &str,
    ) -> SubstrateResult<String> {
        Ok("noop-review".to_string())
    }
    async fn await_verdict(&self, _id: &str) -> SubstrateResult<ReviewVerdict> {
        Ok(ReviewVerdict {
            passed: true,
            rule: None,
            reason: "noop: --features minimal".to_string(),
        })
    }
}

#[async_trait]
impl GovernanceGate for NoopSubstrate {
    async fn gate(
        &self,
        _j: &RunnerJob,
        _r: &ReviewVerdict,
        _o: &AgentOutput,
    ) -> SubstrateResult<GateDecision> {
        Ok(GateDecision {
            allowed: true,
            reason: "noop: --features minimal".to_string(),
        })
    }
}

/// The full substrate bundle. Constructors return the no-op set by
/// default; with `--features substrate` they're overridden in
/// `src/adapters.rs` to wire the real Ptyktos crates.
#[derive(Clone)]
pub struct PtyktosSubstrate {
    pub workspace: std::sync::Arc<dyn AgentWorkspace>,
    pub runtime: std::sync::Arc<dyn AgentRuntime>,
    pub trace: std::sync::Arc<dyn TraceSubstrate>,
    pub review: std::sync::Arc<dyn ReviewSurface>,
    pub gate: std::sync::Arc<dyn GovernanceGate>,
}

impl PtyktosSubstrate {
    /// The no-op substrate — for `--features minimal` builds and for
    /// unit tests that don't need the real stack.
    pub fn noop() -> Self {
        let n = std::sync::Arc::new(NoopSubstrate);
        Self {
            workspace: n.clone(),
            runtime: n.clone(),
            trace: n.clone(),
            review: n.clone(),
            gate: n,
        }
    }
}

impl std::fmt::Debug for PtyktosSubstrate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PtyktosSubstrate").finish_non_exhaustive()
    }
}
