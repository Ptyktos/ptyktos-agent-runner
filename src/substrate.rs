//! Substrate adapter traits: the boundary between the runner and the
//! Ptyktos crates it drives.
//!
//! The five traits correspond to the five Ptyktos surfaces:
//!   - [`AgentWorkspace`]  — `agentvfs` (mountable workspace)
//!   - [`AgentRuntime`]    — `intent` (agent harness)
//!   - [`TraceSubstrate`]  — `memoryfold` (trace records)
//!   - [`ReviewSurface`]   — `caretaker` (PR review)
//!   - [`GovernanceGate`]  — `telos` (gate on the terminal event)
//!
//! No production adapter exists in this crate yet. [`PtyktosSubstrate`]
//! has to be built by a composition root; the binary refuses to run
//! without one. Test doubles live in [`crate::testing`] behind the
//! `test-substrate` feature.

use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::contract::{FailureCategory, RunnerJob, RunnerPhase};

/// Result alias for substrate adapters.
pub type SubstrateResult<T> = Result<T, SubstrateError>;

/// Errors from the substrate adapters, and from the runner's own state
/// handling. [`SubstrateError::category`] gives the saga-facing category.
#[derive(Debug, Error)]
pub enum SubstrateError {
    /// The workspace could not be mounted.
    #[error("workspace mount failed: {0}")]
    WorkspaceMount(String),
    /// The agent failed to run.
    #[error("agent invocation failed: {0}")]
    AgentInvocation(String),
    /// The trace substrate failed.
    #[error("trace substrate failed: {0}")]
    TraceSubstrate(String),
    /// The review surface failed.
    #[error("review surface failed: {0}")]
    ReviewSurface(String),
    /// The governance gate failed (not a denial).
    #[error("governance gate failed: {0}")]
    GovernanceGate(String),
    /// Retryable infrastructure failure.
    #[error("transient infrastructure failure: {0}")]
    Transient(String),
    /// A precondition is not met (missing config, phase out of order).
    #[error("precondition not met: {0}")]
    Precondition(String),
    /// Refused by policy before any change was made.
    #[error("policy denial: {0}")]
    Policy(String),
    /// Refused after the apply phase may have changed something: the saga
    /// must compensate. Used for review rejection and gate denial.
    #[error("denied, compensation required: {0}")]
    Denied(String),
    /// The runner's checkpoint could not be parsed.
    #[error("checkpoint is unreadable: {0}")]
    CorruptState(String),
}

impl SubstrateError {
    /// The wire [`FailureCategory`] for this error.
    pub fn category(&self) -> FailureCategory {
        match self {
            Self::Transient(_) => FailureCategory::Transient,
            Self::Precondition(_) => FailureCategory::Precondition,
            Self::Policy(_) | Self::Denied(_) => FailureCategory::Policy,
            Self::CorruptState(_) => FailureCategory::CorruptState,
            Self::WorkspaceMount(_)
            | Self::AgentInvocation(_)
            | Self::TraceSubstrate(_)
            | Self::ReviewSurface(_)
            | Self::GovernanceGate(_) => FailureCategory::ProviderInternal,
        }
    }
}

/// The task the agent is asked to work on, taken from the rendered recipe.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentTask {
    /// The repo to work in.
    pub repo: String,
    /// The branch the agent checks out.
    pub branch: String,
    /// The commit to base on, if pinned.
    pub base_sha: Option<String>,
    /// The natural-language task description.
    pub task: String,
    /// A diff to apply, if any.
    pub diff: Option<String>,
}

/// What the agent produced. Stored in the checkpoint and emitted as the
/// `apply` outputs.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentOutput {
    /// The branch the agent pushed, if any.
    pub branch: Option<String>,
    /// The PR the agent opened, if any.
    pub pr_url: Option<String>,
    /// Trace ids the agent emitted.
    pub trace_ids: Vec<String>,
    /// Summary of the bench / test run, if reported.
    pub bench_summary: Option<String>,
    /// The agent's own note on completion.
    pub agent_note: Option<String>,
}

/// The agent's mountable workspace.
#[async_trait]
pub trait AgentWorkspace: Send + Sync {
    /// Mount a workspace for the job. Returns its id.
    async fn mount(&self, job: &RunnerJob) -> SubstrateResult<String>;
    /// Snapshot the workspace for rollback. Returns the snapshot id.
    async fn snapshot(&self, workspace_id: &str) -> SubstrateResult<String>;
    /// Restore a snapshot. Idempotent.
    async fn restore(&self, workspace_id: &str, snapshot_id: &str) -> SubstrateResult<()>;
    /// Remove the workspace.
    async fn destroy(&self, workspace_id: &str) -> SubstrateResult<()>;
    /// Read a file from the workspace.
    async fn read(&self, workspace_id: &str, path: &str) -> SubstrateResult<String>;
}

/// The agent runtime.
#[async_trait]
pub trait AgentRuntime: Send + Sync {
    /// Run the agent in the workspace for one phase.
    async fn run_phase(
        &self,
        workspace_id: &str,
        task: &AgentTask,
        phase: RunnerPhase,
    ) -> SubstrateResult<AgentOutput>;
}

/// The trace substrate.
#[async_trait]
pub trait TraceSubstrate: Send + Sync {
    /// Record one trace node for this job. Returns its id.
    async fn record(&self, job: &RunnerJob, kind: &str, note: &str) -> SubstrateResult<String>;
    /// Trace ids the agent emitted for this job.
    async fn list_for_job(&self, job_id: &str) -> SubstrateResult<Vec<String>>;
}

/// The review surface.
#[async_trait]
pub trait ReviewSurface: Send + Sync {
    /// Submit a branch for review. Returns the review id.
    async fn submit_for_review(&self, job: &RunnerJob, branch: &str) -> SubstrateResult<String>;
    /// Current state of the review. `Pending` means no decision yet.
    async fn check_review(&self, review_id: &str) -> SubstrateResult<ReviewStatus>;
}

/// State of a review.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReviewStatus {
    /// No decision yet; poll again.
    Pending,
    /// Decided.
    Decided(ReviewVerdict),
}

/// A decided review.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReviewVerdict {
    /// Whether the review passed.
    pub passed: bool,
    /// The rule that fired, if it failed.
    pub rule: Option<String>,
    /// Human-readable explanation.
    pub reason: String,
}

/// The governance gate, consulted after a passing review.
#[async_trait]
pub trait GovernanceGate: Send + Sync {
    /// Decide whether the agent's result may be reported as done.
    async fn gate(
        &self,
        job: &RunnerJob,
        review: &ReviewVerdict,
        output: &AgentOutput,
    ) -> SubstrateResult<GateDecision>;
}

/// The gate's decision.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GateDecision {
    /// `true` lets the result be reported as done.
    pub allowed: bool,
    /// Why.
    pub reason: String,
}

/// How long and how often the verify phase polls the review.
#[derive(Clone, Copy, Debug)]
pub struct PollPolicy {
    /// Wait between two checks.
    pub interval: Duration,
    /// Checks before giving up with a transient failure.
    pub max_attempts: u32,
}

impl Default for PollPolicy {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            max_attempts: 60,
        }
    }
}

/// The full substrate bundle the runner is given.
#[derive(Clone)]
pub struct PtyktosSubstrate {
    /// Workspace adapter.
    pub workspace: std::sync::Arc<dyn AgentWorkspace>,
    /// Agent runtime adapter.
    pub runtime: std::sync::Arc<dyn AgentRuntime>,
    /// Trace adapter.
    pub trace: std::sync::Arc<dyn TraceSubstrate>,
    /// Review adapter.
    pub review: std::sync::Arc<dyn ReviewSurface>,
    /// Gate adapter.
    pub gate: std::sync::Arc<dyn GovernanceGate>,
    /// Review polling policy.
    pub poll: PollPolicy,
}

impl PtyktosSubstrate {
    /// Bundle the five adapters with the default poll policy.
    pub fn new(
        workspace: std::sync::Arc<dyn AgentWorkspace>,
        runtime: std::sync::Arc<dyn AgentRuntime>,
        trace: std::sync::Arc<dyn TraceSubstrate>,
        review: std::sync::Arc<dyn ReviewSurface>,
        gate: std::sync::Arc<dyn GovernanceGate>,
    ) -> Self {
        Self {
            workspace,
            runtime,
            trace,
            review,
            gate,
            poll: PollPolicy::default(),
        }
    }
}

impl std::fmt::Debug for PtyktosSubstrate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PtyktosSubstrate").finish_non_exhaustive()
    }
}
