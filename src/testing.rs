//! Test doubles. Compiled only with the `test-substrate` feature, which is
//! off by default, so a production build cannot select them.
//!
//! * [`noop`] allows every gate and reports every review as passed. It is
//!   fail-open by design and only for unit tests that do not care.
//! * [`local_substrate`] fakes all five adapters on local disk under one
//!   root directory. Tests steer it with small flag files:
//!   - `review`: `fail` rejects the review; `pending:N` answers `Pending`
//!     N times, then passes. Missing or anything else passes.
//!   - `gate`: `deny` denies the gate. Missing or anything else allows.
//!   - `snapshot`: `fail` makes the snapshot step fail transiently (after
//!     the workspace is mounted), to test retry from a checkpoint.
//!
//! Every adapter call is recorded in `log.txt`, so tests can count mounts.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;

use crate::contract::{RunnerJob, RunnerPhase};
use crate::substrate::{
    AgentOutput, AgentRuntime, AgentTask, AgentWorkspace, GateDecision, GovernanceGate,
    PtyktosSubstrate, ReviewStatus, ReviewSurface, ReviewVerdict, SubstrateError, SubstrateResult,
    TraceSubstrate,
};

// ─── the fail-open no-op ─────────────────────────────────────────────────

/// Allows everything. Unit tests only.
#[derive(Debug, Default, Clone)]
pub struct NoopSubstrate;

#[async_trait]
impl AgentWorkspace for NoopSubstrate {
    async fn mount(&self, _job: &RunnerJob) -> SubstrateResult<String> {
        Ok("noop-workspace".to_string())
    }
    async fn snapshot(&self, _id: &str) -> SubstrateResult<String> {
        Ok("noop-snapshot".to_string())
    }
    async fn restore(&self, _id: &str, _s: &str) -> SubstrateResult<()> {
        Ok(())
    }
    async fn destroy(&self, _id: &str) -> SubstrateResult<()> {
        Ok(())
    }
    async fn read(&self, _id: &str, _p: &str) -> SubstrateResult<String> {
        Ok(String::new())
    }
}

#[async_trait]
impl AgentRuntime for NoopSubstrate {
    async fn run_phase(
        &self,
        _w: &str,
        _t: &AgentTask,
        _p: RunnerPhase,
    ) -> SubstrateResult<AgentOutput> {
        Ok(AgentOutput {
            branch: Some("noop-branch".to_string()),
            pr_url: None,
            trace_ids: vec![],
            bench_summary: None,
            agent_note: Some("noop".to_string()),
        })
    }
}

#[async_trait]
impl TraceSubstrate for NoopSubstrate {
    async fn record(&self, _j: &RunnerJob, _k: &str, _n: &str) -> SubstrateResult<String> {
        Ok("noop-trace".to_string())
    }
    async fn list_for_job(&self, _id: &str) -> SubstrateResult<Vec<String>> {
        Ok(vec![])
    }
}

#[async_trait]
impl ReviewSurface for NoopSubstrate {
    async fn submit_for_review(&self, _j: &RunnerJob, _b: &str) -> SubstrateResult<String> {
        Ok("noop-review".to_string())
    }
    async fn check_review(&self, _id: &str) -> SubstrateResult<ReviewStatus> {
        Ok(ReviewStatus::Decided(ReviewVerdict {
            passed: true,
            rule: None,
            reason: "noop".to_string(),
        }))
    }
}

#[async_trait]
impl GovernanceGate for NoopSubstrate {
    async fn gate(
        &self,
        _j: &RunnerJob,
        _r: &ReviewVerdict,
        _o: &AgentOutput,
    ) -> SubstrateResult<GateDecision> {
        Ok(GateDecision {
            allowed: true,
            reason: "noop".to_string(),
        })
    }
}

/// A substrate that allows everything. Unit tests only.
pub fn noop() -> PtyktosSubstrate {
    let n = Arc::new(NoopSubstrate);
    PtyktosSubstrate::new(n.clone(), n.clone(), n.clone(), n.clone(), n)
}

// ─── the local test substrate ────────────────────────────────────────────

/// Fake adapters that keep their state under one directory.
#[derive(Debug, Clone)]
pub struct LocalTestSubstrate {
    root: PathBuf,
}

impl LocalTestSubstrate {
    /// Use `root` for all state. Created on first use.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn flag(&self, name: &str) -> Option<String> {
        std::fs::read_to_string(self.root.join(name))
            .ok()
            .map(|s| s.trim().to_string())
    }

    fn log(&self, line: &str) -> SubstrateResult<()> {
        use std::io::Write;
        std::fs::create_dir_all(&self.root)
            .map_err(|e| SubstrateError::Transient(format!("test root: {e}")))?;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("log.txt"))
            .map_err(|e| SubstrateError::Transient(format!("test log: {e}")))?;
        writeln!(f, "{line}").map_err(|e| SubstrateError::Transient(format!("test log: {e}")))
    }

    fn ws_dir(&self, workspace_id: &str) -> PathBuf {
        self.root.join("workspaces").join(workspace_id)
    }
}

/// Build a [`PtyktosSubstrate`] of local test adapters rooted at `root`.
pub fn local_substrate(root: impl Into<PathBuf>) -> PtyktosSubstrate {
    let s = Arc::new(LocalTestSubstrate::new(root));
    PtyktosSubstrate::new(s.clone(), s.clone(), s.clone(), s.clone(), s)
}

#[async_trait]
impl AgentWorkspace for LocalTestSubstrate {
    async fn mount(&self, job: &RunnerJob) -> SubstrateResult<String> {
        let id = format!("ws-{}", uuid::Uuid::new_v4());
        std::fs::create_dir_all(self.ws_dir(&id))
            .map_err(|e| SubstrateError::WorkspaceMount(e.to_string()))?;
        self.log(&format!("mount {id} job={}", job.job_id))?;
        Ok(id)
    }
    async fn snapshot(&self, workspace_id: &str) -> SubstrateResult<String> {
        if self.flag("snapshot").as_deref() == Some("fail") {
            return Err(SubstrateError::Transient(
                "snapshot unavailable (test flag)".into(),
            ));
        }
        self.log(&format!("snapshot {workspace_id}"))?;
        Ok(format!("snap-{workspace_id}"))
    }
    async fn restore(&self, workspace_id: &str, snapshot_id: &str) -> SubstrateResult<()> {
        self.log(&format!("restore {workspace_id} {snapshot_id}"))
    }
    async fn destroy(&self, workspace_id: &str) -> SubstrateResult<()> {
        self.log(&format!("destroy {workspace_id}"))?;
        let _ = std::fs::remove_dir_all(self.ws_dir(workspace_id));
        Ok(())
    }
    async fn read(&self, workspace_id: &str, path: &str) -> SubstrateResult<String> {
        std::fs::read_to_string(self.ws_dir(workspace_id).join(path))
            .map_err(|e| SubstrateError::Precondition(format!("read {path}: {e}")))
    }
}

#[async_trait]
impl AgentRuntime for LocalTestSubstrate {
    async fn run_phase(
        &self,
        workspace_id: &str,
        task: &AgentTask,
        phase: RunnerPhase,
    ) -> SubstrateResult<AgentOutput> {
        self.log(&format!(
            "agent {phase:?} {workspace_id} branch={}",
            task.branch
        ))?;
        Ok(AgentOutput {
            branch: Some(task.branch.clone()),
            pr_url: None,
            trace_ids: vec![],
            bench_summary: Some("local-test".to_string()),
            agent_note: None,
        })
    }
}

#[async_trait]
impl TraceSubstrate for LocalTestSubstrate {
    async fn record(&self, job: &RunnerJob, kind: &str, note: &str) -> SubstrateResult<String> {
        self.log(&format!("trace {kind} job={} {note}", job.job_id))?;
        Ok(format!("trace-{kind}"))
    }
    async fn list_for_job(&self, _job_id: &str) -> SubstrateResult<Vec<String>> {
        Ok(vec![])
    }
}

#[async_trait]
impl ReviewSurface for LocalTestSubstrate {
    async fn submit_for_review(&self, _job: &RunnerJob, branch: &str) -> SubstrateResult<String> {
        self.log(&format!("review submit branch={branch}"))?;
        Ok(format!("rev-{}", uuid::Uuid::new_v4()))
    }
    async fn check_review(&self, _review_id: &str) -> SubstrateResult<ReviewStatus> {
        let raw = self.flag("review").unwrap_or_default();
        if raw == "fail" {
            return Ok(ReviewStatus::Decided(ReviewVerdict {
                passed: false,
                rule: Some("test-rule".to_string()),
                reason: "flagged by the test review".to_string(),
            }));
        }
        if let Some(n) = raw
            .strip_prefix("pending:")
            .and_then(|n| n.parse::<u32>().ok())
        {
            if n > 0 {
                std::fs::write(self.root.join("review"), format!("pending:{}", n - 1))
                    .map_err(|e| SubstrateError::ReviewSurface(e.to_string()))?;
                return Ok(ReviewStatus::Pending);
            }
        }
        Ok(ReviewStatus::Decided(ReviewVerdict {
            passed: true,
            rule: None,
            reason: "test review passed".to_string(),
        }))
    }
}

#[async_trait]
impl GovernanceGate for LocalTestSubstrate {
    async fn gate(
        &self,
        job: &RunnerJob,
        _review: &ReviewVerdict,
        _output: &AgentOutput,
    ) -> SubstrateResult<GateDecision> {
        let deny = self.flag("gate").as_deref() == Some("deny");
        self.log(&format!("gate job={} allowed={}", job.job_id, !deny))?;
        Ok(GateDecision {
            allowed: !deny,
            reason: if deny {
                "test gate: deny".into()
            } else {
                "test gate: allow".into()
            },
        })
    }
}
