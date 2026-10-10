//! `ptyktos-agent-runner` — the Ptyktos substrate as a saga runner.
//!
//! Implements the [`hosting_core::task::RunnerJob`] /
//! [`hosting_core::task::RunnerEvent`] contract for the 5 lifecycle
//! phases (`plan` / `apply` / `verify` / `compensate` / `destroy`).
//! Plugs into the Tasmanian-Cloud/saga `runner` crate as a 6th binary
//! alongside `tofu-runner` / `ansible-runner` / `helm-runner` /
//! `proxmox-runner` / `talm-runner`.
//!
//! # The 5 phases, mapped to the Ptyktos substrate
//!
//! | Phase | Ptyktos substrate touched | What the runner does |
//! |---|---|---|
//! | `plan` | none | Read the task from the recipe; assemble the [`AgentTask`]; write a `plan` event |
//! | `apply` | `agentvfs` + `intent` + `memoryfold` | Mount the workspace, invoke the agent, capture the output (PR, branch, trace ids) |
//! | `verify` | `caretaker` + `telos` | Submit the PR for review; await the verdict; gate the `Done` event through `telos` |
//! | `compensate` | `agentvfs` | Rollback the workspace to the pre-apply snapshot |
//! | `destroy` | `agentvfs` + `memoryfold` | Tear down the workspace; seal the trace |
//!
//! # Why this is a runner and not a saga
//!
//! The agent's work is a single saga *step* (one RunnerJob, one
//! phase at a time). The orchestration of multiple steps
//! (plan → apply → verify → compensate/destroy) is the saga
//! itself, owned by `saga-orchestrator`. The runner is the
//! workhorse; the saga is the conductor.
//!
//! # How the runner reads the recipe
//!
//! The recipe is rendered by `recipe-renderer` and arrives as
//! `RunnerRecipeRef::rendered_template` (a `serde_json::Value`).
//! The runner expects a JSON object with the following shape (the
//! `ptyktos-agent-deploy` recipe type):
//!
//! ```json
//! {
//!   "agent": {
//!     "repo": "ptyktos/memoryfold",
//!     "branch": "feat/trace-ontology-v2",
//!     "base_sha": "abc123",
//!     "task": "the natural-language task description"
//!   }
//! }
//! ```
//!
//! If the recipe doesn't match this shape, the runner aborts with a
//! `Precondition` failure and the saga branches on it.
//!
//! # Audit
//!
//! Every phase transition emits a [`RunnerEvent`] with a trace id
//! from the trace substrate. The full cycle's events are the audit
//! trail; the saga-orchestrator records them in the deployment
//! record.

#![deny(missing_docs)]

pub mod contract;
pub mod substrate;

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use tracing::{info, warn};

use crate::contract::{
    FailureCategory, RunnerEvent, RunnerEventKind, RunnerJob, RunnerOutcome, RunnerPhase,
    RunnerRecipeRef,
};
use crate::substrate::{
    AgentOutput, AgentTask, PtyktosSubstrate, SubstrateError, SubstrateResult,
};

// ─── the rendered recipe shape (the contract between saga-orchestrator
//     and ptyktos-agent-runner) ───────────────────────────────────────

/// The recipe's `template` field, after Tera rendering. The runner
/// expects this exact shape; anything else is a `Precondition`
/// failure.
#[derive(Clone, Debug, Deserialize)]
pub struct RenderedTemplate {
    /// The agent work to perform.
    pub agent: AgentSection,
}

/// The `agent` section of the rendered template.
#[derive(Clone, Debug, Deserialize)]
pub struct AgentSection {
    /// The Ptyktos repo the agent works in (e.g. `"ptyktos/memoryfold"`).
    pub repo: String,
    /// The branch the agent checks out (and pushes to).
    pub branch: String,
    /// The base commit SHA (or HEAD if absent).
    #[serde(default)]
    pub base_sha: Option<String>,
    /// The natural-language task description.
    pub task: String,
}

impl RenderedTemplate {
    /// Parse the rendered template from a `RunnerRecipeRef`. Returns
    /// `SubstrateError::Precondition` if the shape is wrong.
    pub fn from_recipe(recipe: &RunnerRecipeRef) -> SubstrateResult<Self> {
        serde_json::from_value(recipe.rendered_template.clone()).map_err(|e| {
            SubstrateError::Precondition(format!(
                "ptyktos-agent-runner: rendered_template is not a RenderedTemplate: {e}"
            ))
        })
    }
}

// ─── the runner phase logic ───────────────────────────────────────────

/// The runner state — the workspace id + the snapshot id (set in
/// `apply`, used in `compensate`).
#[derive(Clone, Debug, Default)]
pub struct RunnerState {
    pub workspace_id: Option<String>,
    pub snapshot_id: Option<String>,
    pub agent_output: Option<AgentOutput>,
}

/// Run one phase of one job. Never panics: any error becomes a
/// `Failed` / `Aborted` / `NeedsCompensation` [`RunnerOutcome`].
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

    // The runner needs the rendered recipe at every phase. Abort
    // early with a Precondition if it's missing — this is the
    // common case for a misconfigured saga.
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

    // Dispatch on phase.
    let result = match job.phase {
        RunnerPhase::Plan => phase_plan(job, &template, substrate, state).await,
        RunnerPhase::Apply => phase_apply(job, &template, substrate, state).await,
        RunnerPhase::Verify => phase_verify(job, &template, substrate, state).await,
        RunnerPhase::Compensate => phase_compensate(job, substrate, state).await,
        RunnerPhase::Destroy => phase_destroy(job, substrate, state).await,
    };

    match result {
        Ok(()) => RunnerOutcome::succeeded(/* terminal_event_data filled by finish */),
        Err(SubstrateError::Precondition(reason)) => {
            RunnerOutcome::aborted(reason, FailureCategory::Precondition)
        }
        Err(SubstrateError::Policy(reason)) => {
            RunnerOutcome::aborted(reason, FailureCategory::Policy)
        }
        Err(SubstrateError::Transient(reason)) => {
            RunnerOutcome::failed(reason, FailureCategory::Transient)
        }
        Err(e) => {
            // Workspace / agent / trace / review / gate failures
            // map to `ProviderInternal` (the substrate is the
            // "provider" in this contract).
            let reason = format!("ptyktos-agent-runner: phase {:?} failed: {e}", job.phase);
            RunnerOutcome::failed(reason, e.category())
        }
    }
}

// ─── phase: plan ───────────────────────────────────────────────────────

async fn phase_plan(
    job: &RunnerJob,
    template: &RenderedTemplate,
    _substrate: &PtyktosSubstrate,
    _state: &mut RunnerState,
) -> SubstrateResult<()> {
    let task = AgentTask {
        repo: template.agent.repo.clone(),
        branch: template.agent.branch.clone(),
        base_sha: template.agent.base_sha.clone(),
        task: template.agent.task.clone(),
        diff: None,
    };
    info!(
        job_id = %job.job_id,
        repo = %task.repo,
        branch = %task.branch,
        "ptyktos-agent-runner: plan composed",
    );
    Ok(())
}

// ─── phase: apply ──────────────────────────────────────────────────────

async fn phase_apply(
    job: &RunnerJob,
    template: &RenderedTemplate,
    substrate: &PtyktosSubstrate,
    state: &mut RunnerState,
) -> SubstrateResult<()> {
    // 1. Mount the workspace.
    let workspace_id = substrate.workspace.mount(job).await?;
    state.workspace_id = Some(workspace_id.clone());

    // 2. Snapshot for rollback.
    let snapshot_id = substrate.workspace.snapshot(&workspace_id).await?;
    state.snapshot_id = Some(snapshot_id);

    // 3. Record the apply start in the trace substrate.
    substrate
        .trace
        .record(
            job,
            "ptyktos-agent-runner.apply.start",
            &format!("workspace={workspace_id}"),
        )
        .await?;

    // 4. Invoke the agent.
    let task = AgentTask {
        repo: template.agent.repo.clone(),
        branch: template.agent.branch.clone(),
        base_sha: template.agent.base_sha.clone(),
        task: template.agent.task.clone(),
        diff: None,
    };
    let output = substrate
        .runtime
        .run_phase(&workspace_id, &task, RunnerPhase::Apply)
        .await?;
    state.agent_output = Some(output.clone());

    // 5. Record the apply end.
    substrate
        .trace
        .record(
            job,
            "ptyktos-agent-runner.apply.end",
            &format!("branch={:?} pr={:?} traces={}", output.branch, output.pr_url, output.trace_ids.len()),
        )
        .await?;

    Ok(())
}

// ─── phase: verify ─────────────────────────────────────────────────────

async fn phase_verify(
    job: &RunnerJob,
    _template: &RenderedTemplate,
    substrate: &PtyktosSubstrate,
    state: &mut RunnerState,
) -> SubstrateResult<()> {
    let output = state.agent_output.as_ref().ok_or_else(|| {
        SubstrateError::Precondition(
            "ptyktos-agent-runner.verify: no agent_output from the apply phase".to_string(),
        )
    })?;
    let branch = output.branch.as_ref().ok_or_else(|| {
        SubstrateError::Precondition(
            "ptyktos-agent-runner.verify: apply did not produce a branch".to_string(),
        )
    })?;

    // 1. Submit for review.
    let review_id = substrate.review.submit_for_review(job, branch).await?;

    // 2. Await the verdict. The runner polls with a 5s backoff until
    //    the review decides; the saga's lease caps the total
    //    wait. This is a busy loop because the contract is async
    //    polling on the saga-orchestrator's clock.
    let verdict = poll_review(&*substrate.review, &review_id).await?;

    // 3. Gate the Done event.
    let decision = substrate.gate.gate(job, &verdict, output).await?;
    if !decision.allowed {
        // The gate denied. The runner doesn't fail the phase (the
        // deny is a verdict, not an error) — it returns a
        // NeedsCompensation outcome so the saga-orchestrator
        // routes the cycle to compensate.
        warn!(
            job_id = %job.job_id,
            reason = %decision.reason,
            "ptyktos-agent-runner: governance gate denied",
        );
        return Err(SubstrateError::Policy(format!(
            "governance gate denied: {}",
            decision.reason
        )));
    }

    // 4. Record the verify pass.
    substrate
        .trace
        .record(
            job,
            "ptyktos-agent-runner.verify.pass",
            &format!("review={} gate=ok", review_id),
        )
        .await?;

    Ok(())
}

async fn poll_review(
    review: &dyn substrate::ReviewSurface,
    review_id: &str,
) -> SubstrateResult<substrate::ReviewVerdict> {
    let backoff = std::time::Duration::from_secs(5);
    // The saga-orchestrator's lease caps the total wait; we poll for
    // up to ~5 minutes (60 * 5s) by default. The saga-orchestrator
    // will SIGTERM us at the lease boundary; runner::race_sigterm
    // catches it.
    for _ in 0..60 {
        let verdict = review.await_verdict(review_id).await?;
        // We don't have a "is_final" signal in the contract yet;
        // for now, the verdict is returned as-is and the runner's
        // caller decides. In the next iteration the ReviewSurface
        // trait will grow `is_pending`.
        return Ok(verdict);
        // (the loop body is here as a placeholder for the
        // future "still pending" signal.)
        #[allow(unreachable_code)]
        {
            tokio::time::sleep(backoff).await;
        }
    }
    Err(SubstrateError::Transient(format!(
        "ptyktos-agent-runner.verify: review {review_id} did not decide within ~5 minutes"
    )))
}

// ─── phase: compensate ───────────────────────────────────────────────

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

// ─── phase: destroy ────────────────────────────────────────────────────

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
