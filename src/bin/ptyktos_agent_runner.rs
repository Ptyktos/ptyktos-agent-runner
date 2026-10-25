//! `ptyktos-agent-runner` CLI. Reads one `RunnerJob` from stdin, runs the
//! phase, writes line-delimited `RunnerEvent`s to stdout, and exits with the
//! contract code (0 succeeded, 1 transient / internal, 2 precondition,
//! 3 policy, 64 unreadable job, 143 SIGTERM).
//!
//! Environment:
//! - `TAS_RUNNER_IDEMPOTENCY_KEY` (required): `<lifecycle>:<phase>:<ordinal>`.
//!   The lifecycle names the checkpoint, shared by all phases of one saga.
//! - `TAS_RUNNER_CHECKPOINT_DIR` (required): where state is checkpointed.
//! - a substrate (see `ptyktos_agent_runner::substrate_from_env`). None is
//!   configured in a default build, so the runner exits 2 with a clear event.

use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

use ptyktos_agent_runner::contract::{
    FailureCategory, RunnerEventKind, RunnerJob, RunnerOutcome, RunnerPhase,
};
use ptyktos_agent_runner::substrate::SubstrateResult;
use ptyktos_agent_runner::{
    checkpoint_path, parse_idempotency_key, run_attempt, substrate_from_env, RunnerState,
    CHECKPOINT_DIR_ENV, IDEMPOTENCY_KEY_ENV, SUBSTRATE_ENV,
};
use tokio::signal::unix::{signal, Signal, SignalKind};

/// How often an attempt waiting on the checkpoint lock tries again.
const LOCK_POLL: Duration = Duration::from_millis(50);

#[tokio::main]
async fn main() -> ExitCode {
    // Installed before anything that can block (reading the job, waiting on
    // the checkpoint lock). A SIGTERM during those waits is then seen here
    // rather than ending the process with the default action, which emits no
    // terminal event. `None` only if the handler cannot be installed.
    let mut sigterm = signal(SignalKind::terminate()).ok();

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

    let lifecycle = match parse_idempotency_key(&key) {
        Ok((lifecycle, _, _)) => lifecycle,
        Err(reason) => {
            return finish(
                &emitter,
                RunnerOutcome::aborted(reason, FailureCategory::Precondition),
            )
        }
    };
    // One checkpoint per saga lifecycle: every phase of the saga opens the same
    // file, and its lock serialises concurrent attempts.
    let path = checkpoint_path(Path::new(&dir), lifecycle);
    let mut state = match acquire_checkpoint(&path, &mut sigterm).await {
        Ok(Some(state)) => state,
        Ok(None) => {
            return handle_sigterm(&emitter, "SIGTERM while waiting for the checkpoint lock")
        }
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

    // Prefer a SIGTERM that is already pending over starting the phase.
    tokio::select! {
        biased;
        () = recv_sigterm(&mut sigterm) => {
            handle_sigterm(&emitter, "SIGTERM during phase")
        }
        outcome = run_attempt(&job, &substrate, &mut state, &key) => finish(&emitter, outcome),
    }
}

/// Take the checkpoint lock, polling so that SIGTERM can interrupt the wait.
/// `Ok(None)` means SIGTERM arrived before the lock was taken.
async fn acquire_checkpoint(
    path: &Path,
    sigterm: &mut Option<Signal>,
) -> SubstrateResult<Option<RunnerState>> {
    loop {
        if let Some(state) = RunnerState::try_open(path)? {
            return Ok(Some(state));
        }
        tokio::select! {
            biased;
            () = recv_sigterm(sigterm) => return Ok(None),
            () = tokio::time::sleep(LOCK_POLL) => {}
        }
    }
}

/// Resolves when SIGTERM is delivered. Never resolves without a handler.
async fn recv_sigterm(sigterm: &mut Option<Signal>) {
    match sigterm {
        Some(s) => {
            s.recv().await;
        }
        None => std::future::pending().await,
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

fn finish(emitter: &EventEmitter<'_>, outcome: RunnerOutcome) -> ExitCode {
    let (kind, data) = outcome.terminal_event_data();
    emitter.emit(kind, data);
    ExitCode::from(outcome.exit_code() as u8)
}

/// On SIGTERM the phase future has been dropped. Whatever it checkpointed
/// stays on disk for the next attempt. Emits the contract's terminal event
/// and exits 143.
fn handle_sigterm(emitter: &EventEmitter<'_>, reason: &str) -> ExitCode {
    emitter.emit(
        RunnerEventKind::NeedsCompensation,
        serde_json::json!({ "reason": reason, "category": FailureCategory::RestartDuringApply.as_str() }),
    );
    ExitCode::from(143)
}
