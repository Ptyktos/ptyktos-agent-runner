//! `ptyktos-agent-runner`: the Ptyktos substrate as a saga runner.
//!
//! Reads one [`contract::RunnerJob`] from stdin, runs one lifecycle phase
//! (`plan` / `apply` / `verify` / `compensate` / `destroy`), and writes
//! [`contract::RunnerEvent`]s to stdout. Exit codes follow
//! [`contract::RunnerOutcome::exit_code`].
//!
//! # Fail closed
//!
//! The binary has no substrate of its own. Without one configured it exits
//! 2 with an `aborted` / `precondition` event. It never reports `succeeded`
//! for work it did not do. The only substrate this crate can build is the
//! test double behind the `test-substrate` feature ([`testing`]).
//!
//! # Checkpoints
//!
//! Every attempt is a new process with a new `job_id`, so the state is
//! keyed by `TAS_RUNNER_IDEMPOTENCY_KEY` and stored in
//! `TAS_RUNNER_CHECKPOINT_DIR`. A retried `apply` finds the workspace and
//! the agent output it already recorded and does not repeat them. A
//! `verify` or `compensate` in a later process reads what `apply` recorded.
//!
//! # The recipe
//!
//! The rendered template must be a JSON object with an `agent` section
//! ([`RenderedTemplate`]). Anything else is a `precondition` failure.

#![deny(missing_docs)]

pub mod contract;
pub mod substrate;
#[cfg(feature = "test-substrate")]
pub mod testing;

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::contract::{FailureCategory, RunnerJob, RunnerOutcome, RunnerPhase, RunnerRecipeRef};
use crate::substrate::{
    AgentOutput, AgentTask, PollPolicy, PtyktosSubstrate, ReviewStatus, ReviewVerdict,
    SubstrateError, SubstrateResult,
};

/// Env var holding the saga's idempotency key for this step.
pub const IDEMPOTENCY_KEY_ENV: &str = "TAS_RUNNER_IDEMPOTENCY_KEY";
/// Env var naming the directory checkpoints are written to.
pub const CHECKPOINT_DIR_ENV: &str = "TAS_RUNNER_CHECKPOINT_DIR";
/// Env var that selects a substrate for the binary. Only `local-test`
/// is recognised, and only in builds with the `test-substrate` feature.
pub const SUBSTRATE_ENV: &str = "PTYKTOS_AGENT_RUNNER_SUBSTRATE";

// ─── the rendered recipe shape ──────────────────────────────────────────

/// The recipe's rendered `template`. Anything else is a precondition failure.
#[derive(Clone, Debug, Deserialize)]
pub struct RenderedTemplate {
    /// The agent work to perform.
    pub agent: AgentSection,
}

/// The `agent` section of the rendered template.
#[derive(Clone, Debug, Deserialize)]
pub struct AgentSection {
    /// The repo the agent works in (e.g. `"ptyktos/memoryfold"`).
    pub repo: String,
    /// The branch the agent checks out and pushes to.
    pub branch: String,
    /// The base commit, if pinned.
    #[serde(default)]
    pub base_sha: Option<String>,
    /// The natural-language task.
    pub task: String,
}

impl RenderedTemplate {
    /// Parse the recipe. A wrong shape is a `Precondition` error.
    pub fn from_recipe(recipe: &RunnerRecipeRef) -> SubstrateResult<Self> {
        serde_json::from_value(recipe.rendered_template.clone()).map_err(|e| {
            SubstrateError::Precondition(format!(
                "ptyktos-agent-runner: rendered_template is not a RenderedTemplate: {e}"
            ))
        })
    }
}

// ─── state and checkpoint ───────────────────────────────────────────────

/// What the phases have recorded so far for one idempotency key.
///
/// When opened with [`RunnerState::open`], every change is written to the
/// checkpoint file by [`RunnerState::persist`] before the phase moves on.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RunnerState {
    /// The mounted workspace (set by `apply`).
    pub workspace_id: Option<String>,
    /// The snapshot taken before the agent ran (set by `apply`).
    pub snapshot_id: Option<String>,
    /// The agent's output (set by `apply`; its presence means apply is done).
    pub agent_output: Option<AgentOutput>,
    /// The review submitted by `verify`.
    pub review_id: Option<String>,
    #[serde(skip)]
    checkpoint: Option<PathBuf>,
}

impl RunnerState {
    /// Load the state at `path`. A missing file is an empty state. The
    /// state will be written back to `path` on [`RunnerState::persist`].
    pub fn open(path: &Path) -> SubstrateResult<Self> {
        let mut state = match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice::<RunnerState>(&bytes)
                .map_err(|e| SubstrateError::CorruptState(format!("{}: {e}", path.display())))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => RunnerState::default(),
            Err(e) => {
                return Err(SubstrateError::Transient(format!(
                    "read checkpoint {}: {e}",
                    path.display()
                )))
            }
        };
        state.checkpoint = Some(path.to_path_buf());
        Ok(state)
    }

    /// Write the state to its checkpoint file (no-op for an unopened state).
    /// The file is written to a temporary name, synced, then renamed, so a
    /// crash leaves either the old or the new state.
    pub fn persist(&self) -> SubstrateResult<()> {
        let Some(path) = &self.checkpoint else {
            return Ok(());
        };
        let io = |what: &str, e: std::io::Error| {
            SubstrateError::Transient(format!("{what} checkpoint {}: {e}", path.display()))
        };
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| io("create dir for", e))?;
        }
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|e| SubstrateError::CorruptState(format!("encode state: {e}")))?;
        let tmp = path.with_extension("json.tmp");
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp).map_err(|e| io("write", e))?;
            f.write_all(&bytes).map_err(|e| io("write", e))?;
            f.sync_all().map_err(|e| io("sync", e))?;
        }
        std::fs::rename(&tmp, path).map_err(|e| io("rename", e))
    }
}

/// The checkpoint file for an idempotency key inside `dir`. The key is
/// hex-encoded, so any key maps to a safe, unambiguous file name.
pub fn checkpoint_path(dir: &Path, idempotency_key: &str) -> PathBuf {
    let hex: String = idempotency_key
        .bytes()
        .map(|b| format!("{b:02x}"))
        .collect();
    dir.join(format!("{hex}.json"))
}

// ─── the phase driver ───────────────────────────────────────────────────

/// Run one phase of one job. Never panics: every error becomes a
/// terminal [`RunnerOutcome`].
pub async fn run(
    job: &RunnerJob,
    substrate: &PtyktosSubstrate,
    state: &mut RunnerState,
) -> RunnerOutcome {
    let attempt = job
        .previous_attempt
        .as_ref()
        .map_or(1u32, |p| p.attempt.saturating_add(1));
    info!(job_id = %job.job_id, phase = ?job.phase, attempt, "ptyktos-agent-runner: phase start");

    let template = match job.recipe.as_ref() {
        Some(r) => match RenderedTemplate::from_recipe(r) {
            Ok(t) => t,
            Err(e) => return RunnerOutcome::aborted(format!("{e}"), FailureCategory::Precondition),
        },
        None => {
            return RunnerOutcome::aborted(
                "ptyktos-agent-runner: RunnerJob.recipe is required (the workflow worker resolves and renders it)",
                FailureCategory::Precondition,
            )
        }
    };

    let result = match job.phase {
        RunnerPhase::Plan => phase_plan(job, &template).await,
        RunnerPhase::Apply => phase_apply(job, &template, substrate, state).await,
        RunnerPhase::Verify => phase_verify(job, substrate, state).await,
        RunnerPhase::Compensate => phase_compensate(job, substrate, state).await,
        RunnerPhase::Destroy => phase_destroy(job, substrate, state).await,
    };

    match result {
        Ok(()) => RunnerOutcome::succeeded_with(outputs_for(job.phase, state)),
        Err(SubstrateError::Precondition(reason)) => {
            RunnerOutcome::aborted(reason, FailureCategory::Precondition)
        }
        Err(SubstrateError::Policy(reason)) => {
            RunnerOutcome::aborted(reason, FailureCategory::Policy)
        }
        Err(SubstrateError::Denied(reason)) => {
            // The apply phase may already have pushed; the saga must undo it.
            warn!(job_id = %job.job_id, %reason, "ptyktos-agent-runner: denied, compensation required");
            RunnerOutcome::NeedsCompensation {
                reason,
                category: FailureCategory::Policy,
            }
        }
        Err(e) => {
            let reason = format!("ptyktos-agent-runner: phase {:?} failed: {e}", job.phase);
            RunnerOutcome::failed(reason, e.category())
        }
    }
}

/// The payload emitted on the terminal `succeeded` event.
fn outputs_for(phase: RunnerPhase, state: &RunnerState) -> serde_json::Value {
    match phase {
        RunnerPhase::Apply => state
            .agent_output
            .as_ref()
            .and_then(|o| serde_json::to_value(o).ok())
            .unwrap_or_else(|| serde_json::json!({})),
        RunnerPhase::Verify => serde_json::json!({ "review_id": state.review_id }),
        _ => serde_json::json!({}),
    }
}

// ─── phase: plan ──────────────────────────────────────────────────────────

async fn phase_plan(job: &RunnerJob, template: &RenderedTemplate) -> SubstrateResult<()> {
    info!(
        job_id = %job.job_id,
        repo = %template.agent.repo,
        branch = %template.agent.branch,
        "ptyktos-agent-runner: plan composed",
    );
    Ok(())
}

// ─── phase: apply ─────────────────────────────────────────────────────────

fn agent_task(template: &RenderedTemplate) -> AgentTask {
    AgentTask {
        repo: template.agent.repo.clone(),
        branch: template.agent.branch.clone(),
        base_sha: template.agent.base_sha.clone(),
        task: template.agent.task.clone(),
        diff: None,
    }
}

async fn phase_apply(
    job: &RunnerJob,
    template: &RenderedTemplate,
    substrate: &PtyktosSubstrate,
    state: &mut RunnerState,
) -> SubstrateResult<()> {
    // A retry after the agent finished: nothing to repeat.
    if state.agent_output.is_some() {
        info!(job_id = %job.job_id, "ptyktos-agent-runner: apply already recorded for this key");
        return Ok(());
    }

    // Mount once per idempotency key. Persist before anything else so a
    // retry finds the workspace instead of mounting a second one.
    if state.workspace_id.is_none() {
        let ws = substrate.workspace.mount(job).await?;
        state.workspace_id = Some(ws);
        state.persist()?;
    }
    let workspace_id = state
        .workspace_id
        .clone()
        .ok_or_else(|| SubstrateError::Precondition("apply: no workspace after mount".into()))?;

    if state.snapshot_id.is_none() {
        let snapshot = substrate.workspace.snapshot(&workspace_id).await?;
        state.snapshot_id = Some(snapshot);
        state.persist()?;
    }

    substrate
        .trace
        .record(
            job,
            "ptyktos-agent-runner.apply.start",
            &format!("workspace={workspace_id}"),
        )
        .await?;

    let output = substrate
        .runtime
        .run_phase(&workspace_id, &agent_task(template), RunnerPhase::Apply)
        .await?;
    state.agent_output = Some(output.clone());
    state.persist()?;

    substrate
        .trace
        .record(
            job,
            "ptyktos-agent-runner.apply.end",
            &format!(
                "branch={:?} pr={:?} traces={}",
                output.branch,
                output.pr_url,
                output.trace_ids.len()
            ),
        )
        .await?;
    Ok(())
}

// ─── phase: verify ────────────────────────────────────────────────────────

async fn phase_verify(
    job: &RunnerJob,
    substrate: &PtyktosSubstrate,
    state: &mut RunnerState,
) -> SubstrateResult<()> {
    let output = state.agent_output.clone().ok_or_else(|| {
        SubstrateError::Precondition(
            "ptyktos-agent-runner.verify: no agent_output from the apply phase".to_string(),
        )
    })?;
    let branch = output.branch.clone().ok_or_else(|| {
        SubstrateError::Precondition(
            "ptyktos-agent-runner.verify: apply did not produce a branch".to_string(),
        )
    })?;

    // Submit once; a retry polls the same review.
    let review_id = match state.review_id.clone() {
        Some(id) => id,
        None => {
            let id = substrate.review.submit_for_review(job, &branch).await?;
            state.review_id = Some(id.clone());
            state.persist()?;
            id
        }
    };

    let verdict = poll_review(substrate.review.as_ref(), &review_id, substrate.poll).await?;
    if !verdict.passed {
        return Err(SubstrateError::Denied(format!(
            "review {review_id} rejected{}: {}",
            verdict
                .rule
                .as_deref()
                .map(|r| format!(" by rule {r}"))
                .unwrap_or_default(),
            verdict.reason
        )));
    }

    let decision = substrate.gate.gate(job, &verdict, &output).await?;
    if !decision.allowed {
        return Err(SubstrateError::Denied(format!(
            "governance gate denied: {}",
            decision.reason
        )));
    }

    substrate
        .trace
        .record(
            job,
            "ptyktos-agent-runner.verify.pass",
            &format!("review={review_id} gate=ok"),
        )
        .await?;
    Ok(())
}

/// Poll the review until it is decided. A pending review is polled again
/// after `policy.interval`; after `policy.max_attempts` the phase fails
/// transiently (the saga retries from the checkpoint).
async fn poll_review(
    review: &dyn substrate::ReviewSurface,
    review_id: &str,
    policy: PollPolicy,
) -> SubstrateResult<ReviewVerdict> {
    let attempts = policy.max_attempts.max(1);
    for attempt in 1..=attempts {
        match review.check_review(review_id).await? {
            ReviewStatus::Decided(verdict) => return Ok(verdict),
            ReviewStatus::Pending => {
                if attempt < attempts {
                    tokio::time::sleep(policy.interval).await;
                }
            }
        }
    }
    Err(SubstrateError::Transient(format!(
        "ptyktos-agent-runner.verify: review {review_id} still pending after {attempts} checks"
    )))
}

// ─── phase: compensate ────────────────────────────────────────────────────

async fn phase_compensate(
    job: &RunnerJob,
    substrate: &PtyktosSubstrate,
    state: &mut RunnerState,
) -> SubstrateResult<()> {
    let workspace_id = state.workspace_id.as_ref().ok_or_else(|| {
        SubstrateError::Precondition(
            "ptyktos-agent-runner.compensate: no workspace_id from the apply phase".to_string(),
        )
    })?;
    let snapshot_id = state.snapshot_id.as_ref().ok_or_else(|| {
        SubstrateError::Precondition(
            "ptyktos-agent-runner.compensate: no snapshot_id from the apply phase".to_string(),
        )
    })?;
    substrate
        .workspace
        .restore(workspace_id, snapshot_id)
        .await?;
    substrate
        .trace
        .record(
            job,
            "ptyktos-agent-runner.compensate.restored",
            &format!("workspace={workspace_id} snapshot={snapshot_id}"),
        )
        .await?;
    Ok(())
}

// ─── phase: destroy ───────────────────────────────────────────────────────

async fn phase_destroy(
    job: &RunnerJob,
    substrate: &PtyktosSubstrate,
    state: &mut RunnerState,
) -> SubstrateResult<()> {
    if let Some(workspace_id) = state.workspace_id.as_ref() {
        substrate.workspace.destroy(workspace_id).await?;
    }
    substrate
        .trace
        .record(
            job,
            "ptyktos-agent-runner.destroy.sealed",
            &format!("job={}", job.job_id),
        )
        .await?;
    Ok(())
}

/// Default time the binary waits between review polls when the test
/// substrate does not override it.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// The substrate the binary should use, from the environment.
///
/// Returns `None` in every build unless the `test-substrate` feature is on
/// and `PTYKTOS_AGENT_RUNNER_SUBSTRATE=local-test` is set. A `None` makes the
/// binary exit 2. There is deliberately no default.
pub fn substrate_from_env() -> Option<PtyktosSubstrate> {
    #[cfg(feature = "test-substrate")]
    {
        if std::env::var(SUBSTRATE_ENV).ok().as_deref() == Some("local-test") {
            let root = std::env::var("PTYKTOS_AGENT_RUNNER_TEST_ROOT").ok()?;
            let interval = std::env::var("PTYKTOS_AGENT_RUNNER_POLL_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .map_or(DEFAULT_POLL_INTERVAL, Duration::from_millis);
            warn!("ptyktos-agent-runner: using the LOCAL TEST substrate (not for production)");
            let mut s = testing::local_substrate(root);
            s.poll.interval = interval;
            return Some(s);
        }
        None
    }
    #[cfg(not(feature = "test-substrate"))]
    {
        None
    }
}
