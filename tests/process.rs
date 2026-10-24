//! Process-level tests of the `ptyktos-agent-runner` binary.
//!
//! Each call starts a fresh process, the way the saga does. State is shared
//! only through the checkpoint directory and the local test substrate root.
//! The substrate is the test double (`test-substrate` feature), so nothing
//! here touches a real workspace, review or gate.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use ptyktos_agent_runner::contract::{
    FailureCategory, RunnerDestination, RunnerJob, RunnerPhase, RunnerRecipeRef,
};
use ptyktos_agent_runner::{checkpoint_path, RunnerState, CHECKPOINT_DIR_ENV, IDEMPOTENCY_KEY_ENV};
use serde_json::{json, Value};

struct Env {
    _tmp: tempfile::TempDir,
    checkpoints: PathBuf,
    substrate_root: PathBuf,
}

impl Env {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let checkpoints = tmp.path().join("checkpoints");
        let substrate_root = tmp.path().join("substrate");
        Self {
            _tmp: tmp,
            checkpoints,
            substrate_root,
        }
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.substrate_root.join("log.txt")).unwrap_or_default()
    }

    fn set_flag(&self, name: &str, value: &str) {
        std::fs::create_dir_all(&self.substrate_root).unwrap();
        std::fs::write(self.substrate_root.join(name), value).unwrap();
    }
}

fn job(phase: RunnerPhase, job_id: &str) -> RunnerJob {
    RunnerJob {
        version: "1".into(),
        job_id: job_id.into(),
        tenant: "tenant-a".into(),
        deployment_ref: "deploy-ref".into(),
        phase,
        authz: None,
        destination: Some(RunnerDestination {
            name: "dest".into(),
            endpoint: "https://example.test".into(),
            credentials_ref: "vault://example/cred".into(),
        }),
        recipe: Some(RunnerRecipeRef {
            id: "ptyktos-agent-deploy@0.1.0".into(),
            version: "0.1.0".into(),
            rendered_template: json!({
                "agent": {
                    "repo": "ptyktos/example",
                    "branch": "feat/phase0",
                    "base_sha": "abc123",
                    "task": "make the fail-open fixes"
                }
            }),
        }),
        requires: Default::default(),
        previous_attempt: None,
    }
}

struct Ran {
    code: i32,
    events: Vec<Value>,
}

impl Ran {
    fn last_kind(&self) -> &str {
        self.events
            .last()
            .and_then(|e| e["kind"].as_str())
            .unwrap_or("<none>")
    }
    fn last_data(&self) -> &Value {
        &self.events.last().unwrap()["data"]
    }
}

/// Start the runner binary with `envs` set and `job` on stdin.
fn run_runner(job: &RunnerJob, envs: &[(&str, &str)]) -> Ran {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ptyktos-agent-runner"));
    cmd.env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().expect("spawn runner");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(serde_json::to_string(job).unwrap().as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    let events = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).expect("each stdout line is a RunnerEvent"))
        .collect();
    Ran {
        code: out.status.code().unwrap_or(-1),
        events,
    }
}

/// Run a phase with the local test substrate and the given idempotency key.
fn phase(env: &Env, key: &str, phase: RunnerPhase, job_id: &str) -> Ran {
    let checkpoints = env.checkpoints.to_string_lossy().into_owned();
    let root = env.substrate_root.to_string_lossy().into_owned();
    run_runner(
        &job(phase, job_id),
        &[
            (IDEMPOTENCY_KEY_ENV, key),
            (CHECKPOINT_DIR_ENV, &checkpoints),
            ("PTYKTOS_AGENT_RUNNER_SUBSTRATE", "local-test"),
            ("PTYKTOS_AGENT_RUNNER_TEST_ROOT", &root),
            ("PTYKTOS_AGENT_RUNNER_POLL_MS", "1"),
        ],
    )
}

#[test]
fn apply_then_verify_in_separate_processes() {
    let env = Env::new();
    let apply = phase(&env, "saga-1:apply:1", RunnerPhase::Apply, "job-a1");
    assert_eq!(apply.code, 0, "{:?}", apply.events);
    assert_eq!(apply.last_kind(), "succeeded");
    assert_eq!(
        apply.last_data()["branch"],
        "feat/phase0",
        "apply emits its outputs"
    );

    // A different process and a different job_id, same idempotency key.
    let verify = phase(&env, "saga-1:apply:1", RunnerPhase::Verify, "job-v1");
    assert_eq!(verify.code, 0, "{:?}", verify.events);
    assert_eq!(verify.last_kind(), "succeeded");
    assert!(verify.last_data()["review_id"]
        .as_str()
        .unwrap()
        .starts_with("rev-"));
}

#[test]
fn verify_without_a_checkpoint_is_a_precondition_failure() {
    let env = Env::new();
    let r = phase(&env, "saga-unknown:apply:1", RunnerPhase::Verify, "job-v");
    assert_eq!(r.code, 2);
    assert_eq!(r.last_kind(), "aborted");
    assert_eq!(r.last_data()["category"], "precondition");
}

#[test]
fn gate_denial_yields_needs_compensation() {
    let env = Env::new();
    assert_eq!(
        phase(&env, "saga-2:apply:1", RunnerPhase::Apply, "job-a2").code,
        0
    );
    env.set_flag("gate", "deny");

    let verify = phase(&env, "saga-2:apply:1", RunnerPhase::Verify, "job-v2");
    assert_eq!(verify.code, 1, "{:?}", verify.events);
    assert_eq!(verify.last_kind(), "needs_compensation");
    assert_eq!(verify.last_data()["category"], "policy");
    assert!(verify.last_data()["reason"]
        .as_str()
        .unwrap()
        .contains("denied"));

    // The saga then runs compensate from the same checkpoint and it restores.
    let comp = phase(&env, "saga-2:apply:1", RunnerPhase::Compensate, "job-c2");
    assert_eq!(comp.code, 0, "{:?}", comp.events);
    assert!(env.log().contains("restore "));
}

#[test]
fn review_rejection_also_yields_needs_compensation() {
    let env = Env::new();
    assert_eq!(
        phase(&env, "saga-3:apply:1", RunnerPhase::Apply, "job-a3").code,
        0
    );
    env.set_flag("review", "fail");
    let verify = phase(&env, "saga-3:apply:1", RunnerPhase::Verify, "job-v3");
    assert_eq!(verify.code, 1);
    assert_eq!(verify.last_kind(), "needs_compensation");
    assert!(verify.last_data()["reason"]
        .as_str()
        .unwrap()
        .contains("test-rule"));
}

#[test]
fn retried_apply_does_not_mount_a_second_workspace() {
    let env = Env::new();
    let first = phase(
        &env,
        "saga-4:apply:1",
        RunnerPhase::Apply,
        "job-a4-attempt1",
    );
    assert_eq!(first.code, 0);
    // The saga retries with a new job_id after the first attempt's exit.
    let retry = phase(
        &env,
        "saga-4:apply:1",
        RunnerPhase::Apply,
        "job-a4-attempt2",
    );
    assert_eq!(retry.code, 0, "{:?}", retry.events);
    assert_eq!(retry.last_data()["branch"], "feat/phase0");

    let mounts = env
        .log()
        .lines()
        .filter(|l| l.starts_with("mount "))
        .count();
    assert_eq!(mounts, 1, "log:\n{}", env.log());
    let agent_runs = env
        .log()
        .lines()
        .filter(|l| l.starts_with("agent "))
        .count();
    assert_eq!(agent_runs, 1, "the agent must not run twice for one key");
}

#[test]
fn retry_after_a_failure_mid_apply_reuses_the_workspace() {
    // The mount succeeds, then the snapshot fails. The workspace must be
    // on the checkpoint, so the retry does not mount a second one.
    let env = Env::new();
    env.set_flag("snapshot", "fail");
    let first = phase(&env, "saga-4b:apply:1", RunnerPhase::Apply, "job-4b-1");
    assert_eq!(first.code, 1, "{:?}", first.events);
    assert_eq!(first.last_kind(), "failed");
    assert_eq!(first.last_data()["category"], "transient");

    env.set_flag("snapshot", "ok");
    let retry = phase(&env, "saga-4b:apply:1", RunnerPhase::Apply, "job-4b-2");
    assert_eq!(retry.code, 0, "{:?}", retry.events);
    let mounts = env.log().lines().filter(|l| l.starts_with("mount ")).count();
    assert_eq!(mounts, 1, "log:\n{}", env.log());
}

#[test]
fn a_different_idempotency_key_gets_its_own_workspace() {
    let env = Env::new();
    assert_eq!(
        phase(&env, "saga-5:apply:1", RunnerPhase::Apply, "j1").code,
        0
    );
    assert_eq!(
        phase(&env, "saga-5:apply:2", RunnerPhase::Apply, "j2").code,
        0
    );
    let mounts = env
        .log()
        .lines()
        .filter(|l| l.starts_with("mount "))
        .count();
    assert_eq!(mounts, 2);
}

#[test]
fn verify_polls_a_pending_review_until_decided() {
    let env = Env::new();
    assert_eq!(
        phase(&env, "saga-6:apply:1", RunnerPhase::Apply, "j-a6").code,
        0
    );
    env.set_flag("review", "pending:2");
    let verify = phase(&env, "saga-6:apply:1", RunnerPhase::Verify, "j-v6");
    assert_eq!(verify.code, 0, "{:?}", verify.events);
    assert_eq!(verify.last_kind(), "succeeded");
    // Two Pending answers were consumed before the decision.
    assert_eq!(
        std::fs::read_to_string(env.substrate_root.join("review")).unwrap(),
        "pending:0"
    );
}

#[test]
fn no_substrate_configured_exits_2_with_a_clear_event() {
    let env = Env::new();
    let checkpoints = env.checkpoints.to_string_lossy().into_owned();
    let r = run_runner(
        &job(RunnerPhase::Apply, "job-none"),
        &[
            (IDEMPOTENCY_KEY_ENV, "saga-7:apply:1"),
            (CHECKPOINT_DIR_ENV, &checkpoints),
        ],
    );
    assert_eq!(
        r.code, 2,
        "a runner with no substrate must not report success"
    );
    assert_eq!(r.last_kind(), "aborted");
    assert_eq!(r.last_data()["category"], "precondition");
    assert!(r.last_data()["reason"]
        .as_str()
        .unwrap()
        .contains("no substrate"));
    // Nothing was checkpointed or mounted.
    assert!(!checkpoint_path(Path::new(&checkpoints), "saga-7:apply:1").exists());
}

#[test]
fn an_unknown_substrate_name_also_exits_2() {
    let env = Env::new();
    let checkpoints = env.checkpoints.to_string_lossy().into_owned();
    let r = run_runner(
        &job(RunnerPhase::Apply, "job-x"),
        &[
            (IDEMPOTENCY_KEY_ENV, "saga-8:apply:1"),
            (CHECKPOINT_DIR_ENV, &checkpoints),
            ("PTYKTOS_AGENT_RUNNER_SUBSTRATE", "production"),
        ],
    );
    assert_eq!(r.code, 2);
}

#[test]
fn missing_idempotency_key_exits_2() {
    let env = Env::new();
    let checkpoints = env.checkpoints.to_string_lossy().into_owned();
    let r = run_runner(
        &job(RunnerPhase::Apply, "job-nokey"),
        &[
            (CHECKPOINT_DIR_ENV, &checkpoints),
            ("PTYKTOS_AGENT_RUNNER_SUBSTRATE", "local-test"),
            (
                "PTYKTOS_AGENT_RUNNER_TEST_ROOT",
                &env.substrate_root.to_string_lossy(),
            ),
        ],
    );
    assert_eq!(r.code, 2);
    assert!(r.last_data()["reason"]
        .as_str()
        .unwrap()
        .contains(IDEMPOTENCY_KEY_ENV));
}

#[test]
fn corrupt_checkpoint_is_corrupt_state_not_a_silent_restart() {
    let env = Env::new();
    std::fs::create_dir_all(&env.checkpoints).unwrap();
    let path = checkpoint_path(&env.checkpoints, "saga-9:apply:1");
    std::fs::write(&path, b"{ not json").unwrap();
    let r = phase(&env, "saga-9:apply:1", RunnerPhase::Apply, "job-a9");
    assert_eq!(r.code, 1);
    assert_eq!(
        r.last_data()["category"],
        FailureCategory::CorruptState.as_str()
    );
    assert!(
        env.log().is_empty(),
        "no mount may happen on a corrupt checkpoint"
    );
}

#[test]
fn checkpoint_round_trips_through_the_file() {
    let env = Env::new();
    assert_eq!(
        phase(&env, "saga-10:apply:1", RunnerPhase::Apply, "j10").code,
        0
    );
    let path = checkpoint_path(&env.checkpoints, "saga-10:apply:1");
    let state = RunnerState::open(&path).unwrap();
    assert!(state.workspace_id.is_some());
    assert!(state.snapshot_id.is_some());
    assert_eq!(
        state.agent_output.unwrap().branch.as_deref(),
        Some("feat/phase0")
    );
}
