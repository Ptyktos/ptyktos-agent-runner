//! Process-level tests of the `ptyktos-agent-runner` binary.
//!
//! Each call starts a fresh process, the way the saga does. State is shared
//! only through the checkpoint directory and the local test substrate root.
//! The substrate is the test double (`test-substrate` feature), so nothing
//! here touches a real workspace, review or gate.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use ptyktos_agent_runner::contract::{
    FailureCategory, RunnerDestination, RunnerJob, RunnerPhase, RunnerRecipeRef,
};
use ptyktos_agent_runner::{
    checkpoint_path, phase_name, RunnerState, CHECKPOINT_DIR_ENV, IDEMPOTENCY_KEY_ENV,
};
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

/// Start the runner binary with `envs` set and `job` on stdin (then closed).
fn spawn_runner(job: &RunnerJob, envs: &[(&str, &str)]) -> Child {
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
    child
}

/// Run the runner binary to completion and collect its events.
fn run_runner(job: &RunnerJob, envs: &[(&str, &str)]) -> Ran {
    let out = spawn_runner(job, envs).wait_with_output().unwrap();
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

/// The full idempotency key for one attempt of one phase of a saga.
fn key(saga: &str, phase: RunnerPhase, ordinal: u32) -> String {
    format!("{saga}:{}:{ordinal}", phase_name(phase))
}

/// Run a phase of saga `saga` (attempt `ordinal`) with the local test substrate.
fn phase(env: &Env, saga: &str, phase: RunnerPhase, ordinal: u32, job_id: &str) -> Ran {
    phase_with_key(env, &key(saga, phase, ordinal), phase, job_id)
}

/// Run a job of phase `phase` with an explicit idempotency key.
fn phase_with_key(env: &Env, key: &str, phase: RunnerPhase, job_id: &str) -> Ran {
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
    let apply = phase(&env, "saga-1", RunnerPhase::Apply, 1, "job-a1");
    assert_eq!(apply.code, 0, "{:?}", apply.events);
    assert_eq!(apply.last_kind(), "succeeded");
    assert_eq!(
        apply.last_data()["branch"],
        "feat/phase0",
        "apply emits its outputs"
    );

    // A different process and a different job_id, same idempotency key.
    let verify = phase(&env, "saga-1", RunnerPhase::Verify, 1, "job-v1");
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
    let r = phase(&env, "saga-unknown", RunnerPhase::Verify, 1, "job-v");
    assert_eq!(r.code, 2);
    assert_eq!(r.last_kind(), "aborted");
    assert_eq!(r.last_data()["category"], "precondition");
}

#[test]
fn gate_denial_yields_needs_compensation() {
    let env = Env::new();
    assert_eq!(
        phase(&env, "saga-2", RunnerPhase::Apply, 1, "job-a2").code,
        0
    );
    env.set_flag("gate", "deny");

    let verify = phase(&env, "saga-2", RunnerPhase::Verify, 1, "job-v2");
    assert_eq!(verify.code, 1, "{:?}", verify.events);
    assert_eq!(verify.last_kind(), "needs_compensation");
    assert_eq!(verify.last_data()["category"], "policy");
    assert!(verify.last_data()["reason"]
        .as_str()
        .unwrap()
        .contains("denied"));

    // The saga then runs compensate from the same checkpoint and it restores.
    let comp = phase(&env, "saga-2", RunnerPhase::Compensate, 1, "job-c2");
    assert_eq!(comp.code, 0, "{:?}", comp.events);
    assert!(env.log().contains("restore "));
}

#[test]
fn review_rejection_also_yields_needs_compensation() {
    let env = Env::new();
    assert_eq!(
        phase(&env, "saga-3", RunnerPhase::Apply, 1, "job-a3").code,
        0
    );
    env.set_flag("review", "fail");
    let verify = phase(&env, "saga-3", RunnerPhase::Verify, 1, "job-v3");
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
    let first = phase(&env, "saga-4", RunnerPhase::Apply, 1, "job-a4-attempt1");
    assert_eq!(first.code, 0);
    // The saga retries with a new job_id after the first attempt's exit.
    let retry = phase(&env, "saga-4", RunnerPhase::Apply, 1, "job-a4-attempt2");
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
    let first = phase(&env, "saga-4b", RunnerPhase::Apply, 1, "job-4b-1");
    assert_eq!(first.code, 1, "{:?}", first.events);
    assert_eq!(first.last_kind(), "failed");
    assert_eq!(first.last_data()["category"], "transient");

    env.set_flag("snapshot", "ok");
    let retry = phase(&env, "saga-4b", RunnerPhase::Apply, 1, "job-4b-2");
    assert_eq!(retry.code, 0, "{:?}", retry.events);
    let mounts = env
        .log()
        .lines()
        .filter(|l| l.starts_with("mount "))
        .count();
    assert_eq!(mounts, 1, "log:\n{}", env.log());
}

#[test]
fn a_different_saga_gets_its_own_workspace() {
    let env = Env::new();
    assert_eq!(phase(&env, "saga-5a", RunnerPhase::Apply, 1, "j1").code, 0);
    assert_eq!(phase(&env, "saga-5b", RunnerPhase::Apply, 1, "j2").code, 0);
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
    assert_eq!(phase(&env, "saga-6", RunnerPhase::Apply, 1, "j-a6").code, 0);
    env.set_flag("review", "pending:2");
    let verify = phase(&env, "saga-6", RunnerPhase::Verify, 1, "j-v6");
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
    assert!(!checkpoint_path(Path::new(&checkpoints), "saga-7").exists());
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
    let path = checkpoint_path(&env.checkpoints, "saga-9");
    std::fs::write(&path, b"{ not json").unwrap();
    let r = phase(&env, "saga-9", RunnerPhase::Apply, 1, "job-a9");
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
    assert_eq!(phase(&env, "saga-10", RunnerPhase::Apply, 1, "j10").code, 0);
    let path = checkpoint_path(&env.checkpoints, "saga-10");
    let state = RunnerState::open(&path).unwrap();
    assert!(state.workspace_id.is_some());
    assert!(state.snapshot_id.is_some());
    assert_eq!(
        state.agent_output.unwrap().branch.as_deref(),
        Some("feat/phase0")
    );
}

#[test]
fn four_phases_with_distinct_keys_share_one_saga_checkpoint() {
    // apply, verify, compensate and destroy each get their own full key (own
    // phase and ordinal), but all belong to one saga and so one checkpoint.
    let env = Env::new();
    let saga = "saga-P1";

    let apply = phase(&env, saga, RunnerPhase::Apply, 1, "job-apply");
    assert_eq!(apply.code, 0, "{:?}", apply.events);
    let ws = {
        let state = RunnerState::open(&checkpoint_path(&env.checkpoints, saga)).unwrap();
        state
            .workspace_id
            .clone()
            .expect("apply recorded the workspace")
    };
    assert!(env.substrate_root.join("workspaces").join(&ws).is_dir());

    let verify = phase(&env, saga, RunnerPhase::Verify, 1, "job-verify");
    assert_eq!(
        verify.code, 0,
        "verify must see apply's output: {:?}",
        verify.events
    );
    assert!(verify.last_data()["review_id"]
        .as_str()
        .unwrap()
        .starts_with("rev-"));

    let comp = phase(&env, saga, RunnerPhase::Compensate, 1, "job-comp");
    assert_eq!(
        comp.code, 0,
        "compensate must find apply's snapshot: {:?}",
        comp.events
    );
    assert!(
        env.log().contains(&format!("restore {ws} snap-{ws}")),
        "log:\n{}",
        env.log()
    );

    let destroy = phase(&env, saga, RunnerPhase::Destroy, 1, "job-destroy");
    assert_eq!(destroy.code, 0, "{:?}", destroy.events);
    assert!(
        !env.substrate_root.join("workspaces").join(&ws).exists(),
        "destroy must remove the workspace apply mounted"
    );
    assert!(env.log().contains(&format!("destroy {ws}")));

    // One checkpoint file for the whole saga.
    let json_files = std::fs::read_dir(&env.checkpoints)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|x| x == "json")
        })
        .count();
    assert_eq!(json_files, 1);
}

#[test]
fn a_key_naming_another_phase_is_refused() {
    let env = Env::new();
    // The job is apply, but the key says verify.
    let r = phase_with_key(&env, "saga-mm:verify:1", RunnerPhase::Apply, "job-mm");
    assert_eq!(r.code, 2, "{:?}", r.events);
    assert_eq!(r.last_kind(), "aborted");
    assert!(r.last_data()["reason"].as_str().unwrap().contains("verify"));
    assert!(env.log().is_empty(), "nothing may run for a mismatched key");
}

#[test]
fn a_key_without_the_three_part_shape_is_refused() {
    let env = Env::new();
    for bad in [
        "saga-no-colons",
        "saga:apply",
        "saga::1",
        ":apply:1",
        "a:b:c:d",
    ] {
        let r = phase_with_key(&env, bad, RunnerPhase::Apply, "job-bad");
        assert_eq!(r.code, 2, "key {bad:?}: {:?}", r.events);
        assert_eq!(r.last_data()["category"], "precondition", "key {bad:?}");
    }
    assert!(env.log().is_empty());
}

#[test]
fn sigterm_while_waiting_for_the_checkpoint_lock_compensates_and_exits_143() {
    // Another runner holds this saga's checkpoint lock, as a long apply would.
    // The second attempt waits on it. SIGTERM (a lease expiring) must still
    // produce the contract's needs_compensation event and exit 143.
    let env = Env::new();
    std::fs::create_dir_all(&env.checkpoints).unwrap();
    let lock_file = checkpoint_path(&env.checkpoints, "saga-lock").with_extension("lock");
    let holder = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_file)
        .unwrap();
    holder.lock().unwrap();

    let checkpoints = env.checkpoints.to_string_lossy().into_owned();
    let root = env.substrate_root.to_string_lossy().into_owned();
    let mut child = spawn_runner(
        &job(RunnerPhase::Apply, "job-lock"),
        &[
            (IDEMPOTENCY_KEY_ENV, "saga-lock:apply:1"),
            (CHECKPOINT_DIR_ENV, &checkpoints),
            ("PTYKTOS_AGENT_RUNNER_SUBSTRATE", "local-test"),
            ("PTYKTOS_AGENT_RUNNER_TEST_ROOT", &root),
            ("PTYKTOS_AGENT_RUNNER_POLL_MS", "1"),
        ],
    );

    // Allow time to reach the lock wait. The holder keeps the lock, so the
    // attempt cannot finish and must still be running.
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        child.try_wait().unwrap().is_none(),
        "the attempt should still be waiting on the lock"
    );

    let kill = Command::new("sh")
        .arg("-c")
        .arg(format!("kill -TERM {}", child.id()))
        .status()
        .unwrap();
    assert!(kill.success());
    let out = child.wait_with_output().unwrap();
    drop(holder);

    assert_eq!(
        out.status.code(),
        Some(143),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let events: Vec<Value> = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).expect("each stdout line is a RunnerEvent"))
        .collect();
    let last = events.last().expect("a terminal event");
    assert_eq!(last["kind"], "needs_compensation");
    assert!(last["data"]["reason"]
        .as_str()
        .unwrap()
        .contains("checkpoint lock"));
    assert!(env.log().is_empty(), "no substrate action may run");
}

#[test]
fn sigterm_during_a_running_phase_compensates_and_exits_143() {
    // apply finishes. verify then waits on a review that never decides, and
    // SIGTERM arrives while the phase is running.
    let env = Env::new();
    let apply = phase(&env, "saga-run", RunnerPhase::Apply, 1, "job-run-a");
    assert_eq!(apply.code, 0, "{:?}", apply.events);
    env.set_flag("review", "pending:1000000");

    let checkpoints = env.checkpoints.to_string_lossy().into_owned();
    let root = env.substrate_root.to_string_lossy().into_owned();
    let mut child = spawn_runner(
        &job(RunnerPhase::Verify, "job-run-v"),
        &[
            (IDEMPOTENCY_KEY_ENV, "saga-run:verify:1"),
            (CHECKPOINT_DIR_ENV, &checkpoints),
            ("PTYKTOS_AGENT_RUNNER_SUBSTRATE", "local-test"),
            ("PTYKTOS_AGENT_RUNNER_TEST_ROOT", &root),
            // 60 checks at one per second: verify stays in progress for a minute.
            ("PTYKTOS_AGENT_RUNNER_POLL_MS", "1000"),
        ],
    );
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        child.try_wait().unwrap().is_none(),
        "verify should still be polling the review"
    );

    let kill = Command::new("sh")
        .arg("-c")
        .arg(format!("kill -TERM {}", child.id()))
        .status()
        .unwrap();
    assert!(kill.success());
    let out = child.wait_with_output().unwrap();

    assert_eq!(
        out.status.code(),
        Some(143),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let events: Vec<Value> = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).expect("each stdout line is a RunnerEvent"))
        .collect();
    let last = events.last().expect("a terminal event");
    assert_eq!(last["kind"], "needs_compensation");
    assert!(last["data"]["reason"]
        .as_str()
        .unwrap()
        .contains("during phase"));
}
