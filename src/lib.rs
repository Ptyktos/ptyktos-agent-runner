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

use std::collections::BTreeMap;
use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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

// ─── idempotency keys ─────────────────────────────────────────────────

/// Split a saga idempotency key `<lifecycle>:<phase>:<ordinal>` into its
/// parts. The lifecycle id names the checkpoint file, so every phase of one
/// saga shares one checkpoint. Exactly three non-empty parts are required; a
/// lifecycle id containing `:` is refused rather than split ambiguously.
pub fn parse_idempotency_key(key: &str) -> Result<(&str, &str, &str), String> {
    let parts: Vec<&str> = key.split(':').collect();
    match parts.as_slice() {
        [lifecycle, phase, ordinal]
            if !lifecycle.is_empty() && !phase.is_empty() && !ordinal.is_empty() =>
        {
            Ok((lifecycle, phase, ordinal))
        }
        _ => Err(format!(
            "{IDEMPOTENCY_KEY_ENV} must look like <lifecycle>:<phase>:<ordinal> with no ':' inside the lifecycle id"
        )),
    }
}

/// The wire name of a phase, as used in idempotency keys.
pub fn phase_name(phase: RunnerPhase) -> &'static str {
    match phase {
        RunnerPhase::Plan => "plan",
        RunnerPhase::Apply => "apply",
        RunnerPhase::Verify => "verify",
        RunnerPhase::Compensate => "compensate",
        RunnerPhase::Destroy => "destroy",
    }
}

fn check_key_matches_phase(key: &str, phase: RunnerPhase) -> Result<(), String> {
    let (_, key_phase, _) = parse_idempotency_key(key)?;
    if key_phase != phase_name(phase) {
        return Err(format!(
            "idempotency key names phase '{key_phase}' but the job is phase '{}'",
            phase_name(phase)
        ));
    }
    Ok(())
}

// ─── state and checkpoint ───────────────────────────────────────────────

/// Source of unique temp-file suffixes within this process.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// What the phases have recorded so far for one saga lifecycle.
///
/// [`RunnerState::open`] takes an exclusive lock on the checkpoint and holds
/// it until this value is dropped, so the load, the run and every persist
/// form one critical section. Changes are written with
/// [`RunnerState::persist`] before the phase moves on.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RunnerState {
    /// The mounted workspace (set by `apply`, cleared by `destroy`).
    pub workspace_id: Option<String>,
    /// The snapshot taken before the agent ran (set by `apply`).
    pub snapshot_id: Option<String>,
    /// The agent's output (set by `apply`; its presence means apply is done).
    pub agent_output: Option<AgentOutput>,
    /// The review submitted by `verify`.
    pub review_id: Option<String>,
    /// For each phase, the full idempotency key that last completed it. A
    /// repeat of that exact key (same phase and ordinal) replays the outcome
    /// instead of running the phase again.
    pub phase_keys: BTreeMap<String, String>,
    #[serde(skip)]
    checkpoint: Option<PathBuf>,
    #[serde(skip)]
    lock: Option<Arc<File>>,
}

impl RunnerState {
    /// Lock and load the checkpoint at `path`. Blocks until any other holder
    /// has released the lock. A missing file is an empty state.
    pub fn open(path: &Path) -> SubstrateResult<Self> {
        let lock = Self::lock_file(path)?;
        lock.lock().map_err(|e| {
            SubstrateError::Transient(format!(
                "lock checkpoint {}: {e}",
                path.with_extension("lock").display()
            ))
        })?;
        Self::load(path, lock)
    }

    /// Like [`RunnerState::open`], but never blocks on the lock. `Ok(None)`
    /// means another holder has it, and the caller may retry.
    pub fn try_open(path: &Path) -> SubstrateResult<Option<Self>> {
        let lock = Self::lock_file(path)?;
        match lock.try_lock() {
            Ok(()) => Self::load(path, lock).map(Some),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(e)) => Err(SubstrateError::Transient(format!(
                "lock checkpoint {}: {e}",
                path.with_extension("lock").display()
            ))),
        }
    }

    /// The lock file beside the checkpoint, created if needed.
    fn lock_file(path: &Path) -> SubstrateResult<File> {
        let lock_path = path.with_extension("lock");
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| {
                SubstrateError::Transient(format!("create checkpoint dir {}: {e}", dir.display()))
            })?;
        }
        std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| {
                SubstrateError::Transient(format!("open lock {}: {e}", lock_path.display()))
            })
    }

    /// Read the checkpoint under a lock that the caller already holds.
    fn load(path: &Path, lock: File) -> SubstrateResult<Self> {
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
        state.lock = Some(Arc::new(lock));
        Ok(state)
    }

    /// Write the state to its checkpoint. The bytes go to a temp file whose
    /// name is unique to this write (`create_new`, pid + counter + clock), are
    /// fsynced, then renamed over the checkpoint, and the directory is fsynced
    /// so the rename survives power loss. A crash leaves the old or the new
    /// state, never a mix. No-op for a state that was not opened.
    pub fn persist(&self) -> SubstrateResult<()> {
        let Some(path) = &self.checkpoint else {
            return Ok(());
        };
        let io = |what: &str, e: std::io::Error| {
            SubstrateError::Transient(format!("{what} checkpoint {}: {e}", path.display()))
        };
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|e| SubstrateError::CorruptState(format!("encode state: {e}")))?;

        let file_name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "checkpoint".into());
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let tmp = path.with_file_name(format!(
            "{file_name}.{}.{}.{nanos}.tmp",
            std::process::id(),
            TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp)
                .map_err(|e| io("create temp for", e))?;
            let written = f.write_all(&bytes).and_then(|()| f.sync_all());
            if let Err(e) = written {
                let _ = std::fs::remove_file(&tmp);
                return Err(io("write", e));
            }
        }
        std::fs::rename(&tmp, path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            io("rename", e)
        })?;
        // The rename is a change to the directory. Until the directory is
        // fsynced, a power loss can bring back the old entry, so do not report
        // success before this.
        let dir = match path.parent() {
            Some(d) if !d.as_os_str().is_empty() => d,
            _ => Path::new("."),
        };
        File::open(dir)
            .and_then(|d| d.sync_all())
            .map_err(|e| io("sync directory of", e))
    }
}

/// The checkpoint file for a saga lifecycle inside `dir`. The lifecycle id
/// is hex-encoded, so any id maps to a safe, unambiguous file name. All
/// phases of one lifecycle share this file.
pub fn checkpoint_path(dir: &Path, lifecycle_id: &str) -> PathBuf {
    let hex: String = lifecycle_id.bytes().map(|b| format!("{b:02x}")).collect();
    dir.join(format!("{hex}.json"))
}

// ─── the phase driver ───────────────────────────────────────────────────

/// Run one phase of one job without idempotency-key bookkeeping. Never
/// panics: every error becomes a terminal [`RunnerOutcome`]. Use
/// [`run_attempt`] from the binary, which binds the phase to its key.
pub async fn run(
    job: &RunnerJob,
    substrate: &PtyktosSubstrate,
    state: &mut RunnerState,
) -> RunnerOutcome {
    run_inner(job, substrate, state, None).await
}

/// Run one phase of one job for the given idempotency key
/// `<lifecycle>:<phase>:<ordinal>`. The key's phase must match the job's.
/// If this exact key already completed the phase, its outcome is replayed.
pub async fn run_attempt(
    job: &RunnerJob,
    substrate: &PtyktosSubstrate,
    state: &mut RunnerState,
    idempotency_key: &str,
) -> RunnerOutcome {
    if let Err(reason) = check_key_matches_phase(idempotency_key, job.phase) {
        return RunnerOutcome::aborted(reason, FailureCategory::Precondition);
    }
    run_inner(job, substrate, state, Some(idempotency_key)).await
}

async fn run_inner(
    job: &RunnerJob,
    substrate: &PtyktosSubstrate,
    state: &mut RunnerState,
    key: Option<&str>,
) -> RunnerOutcome {
    let attempt = job
        .previous_attempt
        .as_ref()
        .map_or(1u32, |p| p.attempt.saturating_add(1));
    info!(job_id = %job.job_id, phase = ?job.phase, attempt, "ptyktos-agent-runner: phase start");

    let name = phase_name(job.phase);
    if let Some(k) = key {
        if state.phase_keys.get(name).map(String::as_str) == Some(k) {
            info!(job_id = %job.job_id, key = %k, "ptyktos-agent-runner: phase already completed for this key; replaying");
            return RunnerOutcome::succeeded_with(outputs_for(job.phase, state));
        }
    }

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
        Ok(()) => {
            if let Some(k) = key {
                state.phase_keys.insert(name.to_string(), k.to_string());
                if let Err(e) = state.persist() {
                    return RunnerOutcome::failed(
                        format!("ptyktos-agent-runner: cannot record phase completion: {e}"),
                        e.category(),
                    );
                }
            }
            RunnerOutcome::succeeded_with(outputs_for(job.phase, state))
        }
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
    if let Some(workspace_id) = state.workspace_id.clone() {
        substrate.workspace.destroy(&workspace_id).await?;
        state.workspace_id = None;
        state.persist()?;
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
