//! `ptyktos-agent-runner` CLI. Reads one `RunnerJob` from stdin, runs the
//! phase, writes line-delimited `RunnerEvent`s to stdout, and exits with the
//! contract code (0 succeeded, 1 transient / internal, 2 precondition,
//! 3 policy, 64 unreadable job, 143 SIGTERM).
//!
//! Environment:
//! - `TAS_RUNNER_IDEMPOTENCY_KEY` (required): the saga's key for this step.
//! - `TAS_RUNNER_CHECKPOINT_DIR` (required): where state is checkpointed.
//! - a substrate (see `ptyktos_agent_runner::substrate_from_env`). None is
//!   configured in a default build, so the runner exits 2 with a clear event.

use std::path::Path;
use std::process::ExitCode;

use ptyktos_agent_runner::contract::{
    FailureCategory, RunnerEventKind, RunnerJob, RunnerOutcome, RunnerPhase,
};
use ptyktos_agent_runner::{
    checkpoint_path, run, substrate_from_env, RunnerState, CHECKPOINT_DIR_ENV, IDEMPOTENCY_KEY_ENV,
    SUBSTRATE_ENV,
};

#[tokio::main]
async fn main() -> ExitCode {
    let Some(job) = read_job_from_stdin() else {
        return ExitCode::from(64);
    };
    let emitter = EventEmitter::new(&job);

    let Some(key) = non_empty_env(IDEMPOTENCY_KEY_ENV) else {
        return finish(
            &emitter,
            RunnerOutcome::aborted(
                format!(
                    "{IDEMPOTENCY_KEY_ENV} is not set; the runner cannot checkpoint without it"
                ),
                FailureCategory::Precondition,
            ),
        );
    };
    let Some(dir) = non_empty_env(CHECKPOINT_DIR_ENV) else {
        return finish(
            &emitter,
            RunnerOutcome::aborted(
                format!("{CHECKPOINT_DIR_ENV} is not set; the runner cannot checkpoint"),
                FailureCategory::Precondition,
            ),
        );
    };
    let Some(substrate) = substrate_from_env() else {
        return finish(
            &emitter,
            RunnerOutcome::aborted(
                format!(
                    "no substrate is configured for this runner ({SUBSTRATE_ENV} is unset or not recognised); refusing to run"
                ),
                FailureCategory::Precondition,
            ),
        );
    };

    let path = checkpoint_path(Path::new(&dir), &key);
    let mut state = match RunnerState::open(&path) {
        Ok(s) => s,
        Err(e) => {
            return finish(
                &emitter,
                RunnerOutcome::failed(
                    format!("ptyktos-agent-runner: cannot load checkpoint: {e}"),
                    e.category(),
                ),
            )
        }
    };

    match race_sigterm(run(&job, &substrate, &mut state)).await {
        RaceOutcome::Completed(outcome) => finish(&emitter, outcome),
        RaceOutcome::SigTerm => handle_sigterm(&job),
    }
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

/// Read the job from stdin. `None` means it did not parse (exit 64).
fn read_job_from_stdin() -> Option<RunnerJob> {
    use std::io::Read;
    let mut s = String::new();
    std::io::stdin().read_to_string(&mut s).ok()?;
    serde_json::from_str(&s).ok()
}

/// Writes one JSON event per line on stdout. Logs go to stderr.
struct EventEmitter<'a> {
    job_id: &'a str,
    phase: RunnerPhase,
    seq: std::sync::atomic::AtomicU64,
}

impl<'a> EventEmitter<'a> {
    fn new(job: &'a RunnerJob) -> Self {
        Self {
            job_id: &job.job_id,
            phase: job.phase,
            seq: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn emit(&self, kind: RunnerEventKind, data: serde_json::Value) {
        let seq = self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let event = ptyktos_agent_runner::contract::RunnerEvent {
            version: "1".to_string(),
            ts: chrono::Utc::now(),
            job_id: self.job_id.to_string(),
            kind,
            phase: self.phase,
            seq,
            decision_id: None,
            data,
        };
        if let Ok(s) = serde_json::to_string(&event) {
            println!("{s}");
        }
    }
}

enum RaceOutcome {
    Completed(RunnerOutcome),
    SigTerm,
}

/// Race the phase against SIGTERM. On SIGTERM the phase future is dropped;
/// whatever it had checkpointed stays on disk for the next attempt.
async fn race_sigterm<F>(f: F) -> RaceOutcome
where
    F: std::future::Future<Output = RunnerOutcome>,
{
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(_) => return RaceOutcome::Completed(f.await),
    };
    tokio::select! {
        outcome = f => RaceOutcome::Completed(outcome),
        _ = sigterm.recv() => RaceOutcome::SigTerm,
    }
}

fn finish(emitter: &EventEmitter<'_>, outcome: RunnerOutcome) -> ExitCode {
    let (kind, data) = outcome.terminal_event_data();
    emitter.emit(kind, data);
    ExitCode::from(outcome.exit_code() as u8)
}

fn handle_sigterm(job: &RunnerJob) -> ExitCode {
    let emitter = EventEmitter::new(job);
    emitter.emit(
        RunnerEventKind::NeedsCompensation,
        serde_json::json!({ "reason": "SIGTERM during phase", "category": FailureCategory::RestartDuringApply.as_str() }),
    );
    ExitCode::from(143)
}
