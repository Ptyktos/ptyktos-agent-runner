//! Contract tests for the `ptyktos-agent-runner` runner.
//!
//! These tests verify the runner contract — the wire shape that the
//! `saga-orchestrator` and `workflow-worker` consume — without
//! requiring the full Ptyktos substrate to be present. The substrate
//! is the no-op default; the runner still reads a real
//! [`RunnerJob`], runs the 5 phases, and emits real [`RunnerEvent`]s.

use std::sync::Arc;

use async_trait::async_trait;
use ptyktos_agent_runner::contract::{
    FailureCategory, RunnerDestination, RunnerJob, RunnerOutcome, RunnerPhase, RunnerRecipeRef,
};
use ptyktos_agent_runner::substrate::{
    AgentOutput, AgentRuntime, AgentTask, GateDecision, GovernanceGate, PtyktosSubstrate,
    ReviewVerdict, SubstrateError, SubstrateResult,
};
use ptyktos_agent_runner::testing::{noop, NoopSubstrate};
use ptyktos_agent_runner::{run, RenderedTemplate, RunnerState};
use serde_json::json;

fn make_job(recipe_template: serde_json::Value, phase: RunnerPhase) -> RunnerJob {
    RunnerJob {
        version: "1".to_string(),
        job_id: "test-job-1".to_string(),
        tenant: "test-tenant".to_string(),
        deployment_ref: "test-deployment-ref".to_string(),
        phase,
        authz: None,
        destination: Some(RunnerDestination {
            name: "test-dest".to_string(),
            endpoint: "https://example.com".to_string(),
            credentials_ref: "vault://test/cred".to_string(),
        }),
        recipe: Some(RunnerRecipeRef {
            id: "ptyktos-agent-deploy@0.1.0".to_string(),
            version: "0.1.0".to_string(),
            rendered_template: recipe_template,
        }),
        requires: std::collections::BTreeMap::new(),
        previous_attempt: None,
    }
}

fn good_template() -> serde_json::Value {
    json!({
        "agent": {
            "repo": "ptyktos/memoryfold",
            "branch": "feat/test",
            "base_sha": "abc123",
            "task": "implement the trace ontology"
        }
    })
}

// ─── rendered template parsing ──────────────────────────────────────

#[test]
fn rendered_template_parses_correct_shape() {
    let t: RenderedTemplate = serde_json::from_value(good_template()).unwrap();
    assert_eq!(t.agent.repo, "ptyktos/memoryfold");
    assert_eq!(t.agent.branch, "feat/test");
    assert_eq!(t.agent.base_sha.as_deref(), Some("abc123"));
    assert_eq!(t.agent.task, "implement the trace ontology");
}

#[test]
fn rendered_template_rejects_wrong_shape_with_precondition() {
    let bad = json!({ "not_the_right_key": 42 });
    let r = RenderedTemplate::from_recipe(&RunnerRecipeRef {
        id: "x@0.1.0".to_string(),
        version: "0.1.0".to_string(),
        rendered_template: bad,
    });
    assert!(matches!(r, Err(SubstrateError::Precondition(_))));
}

// ─── no-op substrate runs the 5 phases end-to-end ───────────────────

#[tokio::test]
async fn noop_substrate_runs_all_5_phases() {
    let substrate = noop();
    let mut state = RunnerState::default();

    for phase in [
        RunnerPhase::Plan,
        RunnerPhase::Apply,
        RunnerPhase::Verify,
        RunnerPhase::Compensate,
        RunnerPhase::Destroy,
    ] {
        let job = make_job(good_template(), phase);
        let outcome = run(&job, &substrate, &mut state).await;
        // Every phase on the no-op substrate succeeds. (Verify's
        // pass through the no-op review + no-op gate is the contract
        // guarantee for the minimal build.)
        assert!(
            outcome.exit_code() == 0,
            "phase {phase:?} expected exit 0, got {outcome:?}"
        );
    }
}

#[tokio::test]
async fn apply_phase_populates_state() {
    let substrate = noop();
    let mut state = RunnerState::default();

    // plan first (does not mutate state)
    let plan_job = make_job(good_template(), RunnerPhase::Plan);
    let _ = run(&plan_job, &substrate, &mut state).await;
    assert!(state.workspace_id.is_none());
    assert!(state.snapshot_id.is_none());

    // apply populates state
    let apply_job = make_job(good_template(), RunnerPhase::Apply);
    let outcome = run(&apply_job, &substrate, &mut state).await;
    assert_eq!(outcome.exit_code(), 0);
    assert!(state.workspace_id.is_some());
    assert!(state.snapshot_id.is_some());
    assert!(state.agent_output.is_some());
}

#[tokio::test]
async fn verify_without_apply_is_precondition() {
    let substrate = noop();
    let mut state = RunnerState::default();

    let job = make_job(good_template(), RunnerPhase::Verify);
    let outcome = run(&job, &substrate, &mut state).await;
    // The runner should fail with Precondition because there's no
    // agent_output from the apply phase.
    assert_eq!(outcome.exit_code(), 2); // Precondition = exit 2
}

#[tokio::test]
async fn missing_recipe_aborts_with_precondition() {
    let substrate = noop();
    let mut state = RunnerState::default();
    let mut job = make_job(good_template(), RunnerPhase::Plan);
    job.recipe = None;
    let outcome = run(&job, &substrate, &mut state).await;
    assert_eq!(outcome.exit_code(), 2);
}

// ─── a custom substrate that denies at the gate ──────────────────────

struct DenyingGate;

#[async_trait]
impl GovernanceGate for DenyingGate {
    async fn gate(
        &self,
        _j: &RunnerJob,
        _r: &ReviewVerdict,
        _o: &AgentOutput,
    ) -> SubstrateResult<GateDecision> {
        Ok(GateDecision {
            allowed: false,
            reason: "policy X denied".to_string(),
        })
    }
}

#[tokio::test]
async fn gate_denial_fails_verify() {
    // Build a substrate where the gate denies.
    let n = Arc::new(NoopSubstrate);
    let substrate =
        PtyktosSubstrate::new(n.clone(), n.clone(), n.clone(), n, Arc::new(DenyingGate));
    let mut state = RunnerState::default();

    // First run apply to populate state
    let apply_job = make_job(good_template(), RunnerPhase::Apply);
    let _ = run(&apply_job, &substrate, &mut state).await;

    // Now verify — the gate should deny
    let verify_job = make_job(good_template(), RunnerPhase::Verify);
    let outcome = run(&verify_job, &substrate, &mut state).await;
    // A gate denial after apply means the saga must compensate: a
    // NeedsCompensation outcome (exit 1), not a plain policy abort.
    assert!(
        matches!(
            outcome,
            ptyktos_agent_runner::contract::RunnerOutcome::NeedsCompensation {
                category: FailureCategory::Policy,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(outcome.exit_code(), 1);
}

// ─── a custom substrate that errors at the agent runtime ─────────────

struct FailingRuntime;

#[async_trait]
impl AgentRuntime for FailingRuntime {
    async fn run_phase(
        &self,
        _w: &str,
        _t: &AgentTask,
        _p: RunnerPhase,
    ) -> SubstrateResult<AgentOutput> {
        Err(SubstrateError::AgentInvocation("agent crashed".to_string()))
    }
}

#[tokio::test]
async fn runtime_error_fails_apply() {
    let noop = Arc::new(NoopSubstrate);
    let substrate = PtyktosSubstrate::new(
        noop.clone(),
        Arc::new(FailingRuntime),
        noop.clone(),
        noop.clone(),
        noop,
    );
    let mut state = RunnerState::default();
    let job = make_job(good_template(), RunnerPhase::Apply);
    let outcome = run(&job, &substrate, &mut state).await;
    // ProviderInternal = exit 1
    assert_eq!(outcome.exit_code(), 1);
}

// ─── contract: RunnerEventKind::is_terminal covers the right set ────

#[test]
fn runner_event_kind_terminal_set() {
    use ptyktos_agent_runner::contract::RunnerEventKind;
    let terminals = [
        RunnerEventKind::Succeeded,
        RunnerEventKind::Failed,
        RunnerEventKind::Aborted,
        RunnerEventKind::NeedsCompensation,
    ];
    let non_terminals = [
        RunnerEventKind::Started,
        RunnerEventKind::Progress,
        RunnerEventKind::ProviderCall,
        RunnerEventKind::Plan,
        RunnerEventKind::Verified,
    ];
    for k in terminals {
        assert!(k.is_terminal(), "{k:?} should be terminal");
    }
    for k in non_terminals {
        assert!(!k.is_terminal(), "{k:?} should not be terminal");
    }
}

// ─── contract: RunnerOutcome exit codes match the contract table ─────

#[test]
fn runner_outcome_exit_codes() {
    let s = RunnerOutcome::succeeded();
    assert_eq!(s.exit_code(), 0);
    let f = RunnerOutcome::failed("transient", FailureCategory::Transient);
    assert_eq!(f.exit_code(), 1);
    let p = RunnerOutcome::aborted("precondition", FailureCategory::Precondition);
    assert_eq!(p.exit_code(), 2);
    let pol = RunnerOutcome::aborted("policy", FailureCategory::Policy);
    assert_eq!(pol.exit_code(), 3);
}
