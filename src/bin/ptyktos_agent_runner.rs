//! `ptyktos-agent-runner` CLI entrypoint per `docs/runner-contract.md`.
//!
//! Reads one [`hosting_core::task::RunnerJob`] from stdin, runs the phase,
//! emits line-delimited [`hosting_core::task::RunnerEvent`]s on stdout, and
//! exits with the contract-defined code. See the crate root doc for the
//! 5-phase lifecycle (plan / apply / verify / compensate / destroy).
//!
//! The shared stdin/stdout/SIGTERM/exit-code skeleton mirrors the existing
//! `tofu-runner` / `ansible-runner` / `helm-runner` / `proxmux-runner` /
//! `talm-runner` binaries; the agent-specific phase logic lives in
//! `ptyktos_agent_runner::run`.

use std::process::ExitCode;

use ptyktos_agent_runner::contract::RunnerJob;
use ptyktos_agent_runner::{run, PtyktosSubstrate, RunnerState};

#[tokio::main]
async fn main() -> ExitCode {
    let Some(job) = read_job_from_stdin() else {
        return ExitCode::from(64);
    };

    let emitter = EventEmitter::new(&job);
    let substrate = PtyktosSubstrate::noop();
    let mut state = RunnerState::default();

    // The Ptyktos substrate is a no-op when no substrate adapter is
    // wired in. The saga-orchestrator runs the binary on the agent
    // host; production builds replace PtyktosSubstrate::noop() with
    // PtyktosSubstrate::ptyktos() (the real adapter set).
    match race_sigterm(run(&job, &substrate, &mut state)).await {
        RaceOutcome::Completed(outcome) => finish(&emitter, outcome),
        RaceOutcome::SigTerm => handle_sigterm(&job),
    }
}

// ─── the runner skeleton (mirrors the `runner` crate's lib.rs) ──────
//
// These helpers are the minimum needed to match the runner contract
// (`docs/runner-contract.md`): one JSON job from stdin, line-delimited
// JSON events on stdout, free-form logs on stderr, exit code 0/2/3/64
// per the contract's table. They're duplicated here (rather than
// imported from the `runner` crate) to keep `ptyktos-agent-runner`
// self-contained: a one-crate build that doesn't require the full
// saga/hosting-runtime to compile.

/// Read the [`RunnerJob`] from stdin. Returns `None` on parse error
/// (the caller exits with 64 per the contract).
fn read_job_from_stdin() -> Option<RunnerJob> {
    use std::io::Read;
    let mut s = String::new();
    std::io::stdin().read_to_string(&mut s).ok()?;
    serde_json::from_str(&s).ok()
}

/// One event per `kind` + data payload, written as one line of JSON
/// to stdout.
struct EventEmitter<'a> {
    job_id: &'a str,
    phase: hosting_core::task::RunnerPhase,
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

    fn emit(
        &self,
        kind: ptyktos_agent_runner::contract::RunnerEventKind,
        data: serde_json::Value,
    ) {
        let seq = self
            .seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
        // Stdout is events only; logs go to stderr. One line per event.
        if let Ok(s) = serde_json::to_string(&event) {
            println!("{s}");
        }
    }
}

enum RaceOutcome {
    Completed(ptyktos_agent_runner::RunnerOutcome),
    SigTerm,
}

/// `tokio::select!` over the runner future and a SIGTERM signal. The
/// saga-orchestrator sends SIGTERM at the lease boundary; on receipt
/// we transition to the `compensate` phase via the saga's next call.
async fn race_sigterm<F>(f: F) -> RaceOutcome
where
    F: std::future::Future<Output = ptyktos_agent_runner::RunnerOutcome>,
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

fn finish(
    emitter: &EventEmitter<'_>,
    outcome: ptyktos_agent_runner::RunnerOutcome,
) -> ExitCode {
    let (kind, data) = outcome.terminal_event_data();
    emitter.emit(kind, data);
    ExitCode::from(outcome.exit_code() as u8)
}

fn handle_sigterm(job: &RunnerJob) -> ExitCode {
    // Per the contract, exit 143 on SIGTERM and emit a NeedsCompensation
    // event so the saga-orchestrator knows to retry with the
    // compensate phase.
    let emitter = EventEmitter::new(job);
    emitter.emit(
        ptyktos_agent_runner::contract::RunnerEventKind::NeedsCompensation,
        serde_json::json!({ "reason": "SIGTERM during phase" }),
    );
    ExitCode::from(143)
}
