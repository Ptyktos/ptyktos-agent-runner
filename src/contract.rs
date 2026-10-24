//! The runner contract: the wire shape that `saga-orchestrator` and
//! `workflow-worker` consume. These types are a local copy of the
//! `hosting_core::task` runner types (`RunnerJob`, `RunnerEvent`,
//! `RunnerOutcome`, `RunnerPhase`, `RunnerEventKind`, `FailureCategory`,
//! ...). This crate must not depend on `hosting-core`: that crate is
//! AGPL and the runner is distributed as a separate MIT/Apache binary.
//!
//! The copy is kept in one file so a change to the canonical schema is a
//! one-file diff. Byte-equality with the canonical fixtures is NOT checked
//! here (the hosting-runtime workspace is not part of this repository).

// The variant and field names here are the wire schema; each maps to a
// documented row in README.md, so they carry no separate doc comments.
#![allow(missing_docs)]

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ─── RunnerPhase ──────────────────────────────────────────────────────

/// The lifecycle phase this invocation performs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunnerPhase {
    Plan,
    Apply,
    Verify,
    Compensate,
    Destroy,
}

// ─── RunnerEventKind ──────────────────────────────────────────────────

/// The `kind` field on a [`RunnerEvent`]. Serializes `snake_case`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunnerEventKind {
    Started,
    Progress,
    ProviderCall,
    Plan,
    Verified,
    Succeeded,
    Failed,
    Aborted,
    NeedsCompensation,
}

impl RunnerEventKind {
    /// `true` for the four terminal kinds.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Aborted | Self::NeedsCompensation
        )
    }
}

// ─── FailureCategory ──────────────────────────────────────────────────

/// A failure category the saga branches on. The wire string is the
/// `runner-event.schema.json` `category` enum value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureCategory {
    /// Retryable — transient infra / lock contention / unclassified.
    Transient,
    /// A precondition wasn't met (quota, already-exists, …) — don't
    /// blind-retry.
    Precondition,
    /// Authz / policy denial.
    Policy,
    /// The provider's own internal error (rpc error, inconsistent
    /// result).
    ProviderInternal,
    /// State is corrupt / unparseable / version-skewed — needs a human.
    CorruptState,
    /// A SIGTERM landed mid-apply.
    RestartDuringApply,
}

impl FailureCategory {
    /// The wire string.
    pub fn as_str(self) -> &'static str {
        match self {
            FailureCategory::Transient => "transient",
            FailureCategory::Precondition => "precondition",
            FailureCategory::Policy => "policy",
            FailureCategory::ProviderInternal => "provider_internal",
            FailureCategory::CorruptState => "corrupt_state",
            FailureCategory::RestartDuringApply => "restart_during_apply",
        }
    }
}

// ─── AuthzContext ─────────────────────────────────────────────────────

/// The authorisation decision that admitted a [`RunnerJob`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthzContext {
    /// Stable id of the decision; carried into every audit row.
    pub decision_id: String,
    /// Decision obligations the runner must honour (region pinning,
    /// tag reqs, …).
    #[serde(default)]
    pub obligations: Vec<serde_json::Value>,
}

// ─── RunnerDestination ───────────────────────────────────────────────

/// The resolved Destination a [`RunnerJob`] targets.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunnerDestination {
    pub name: String,
    /// Provider API endpoint (a URI).
    pub endpoint: String,
    /// A `vault://`-style reference resolved AFTER spawn through a
    /// parent-controlled FIFO — never an inline secret.
    pub credentials_ref: String,
}

// ─── RunnerRecipeRef ──────────────────────────────────────────────────

/// The verified, already-Tera-rendered recipe a [`RunnerJob`] applies.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunnerRecipeRef {
    pub id: String,
    /// Recipe semver.
    pub version: String,
    /// The rendered template payload (HCL/playbook/values) — opaque here.
    pub rendered_template: serde_json::Value,
}

// ─── RunnerRequiresBinding ───────────────────────────────────────────

/// One resolved composition binding (`requires.<name>` in the recipe).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunnerRequiresBinding {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    /// `vault://`-style reference, resolved after spawn. Never inline.
    pub credentials_ref: String,
}

// ─── RunnerPreviousAttempt ───────────────────────────────────────────

/// Set on a retry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunnerPreviousAttempt {
    pub attempt: u32,
    /// The `kind` of the last event the prior attempt emitted (or its
    /// `inflight` checkpoint marker).
    pub last_event: String,
    /// True if the prior tempdir is still on disk.
    pub tempdir_preserved: bool,
}

fn default_runner_schema_version() -> String {
    "1".to_string()
}

// ─── RunnerJob ────────────────────────────────────────────────────────

/// The single JSON document a runner process reads from stdin.
/// Matches `runner-job.schema.json` (the canonical contract).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunnerJob {
    #[serde(default = "default_runner_schema_version")]
    pub version: String,
    /// ULID, unique per saga step.
    pub job_id: String,
    /// Tenant id, for path scoping.
    pub tenant: String,
    /// Signed `external_id` from `DeploymentRef`. Opaque.
    pub deployment_ref: String,
    /// Which lifecycle phase this invocation performs.
    pub phase: RunnerPhase,
    /// The authz decision that admitted this job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authz: Option<AuthzContext>,
    /// The resolved Destination.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<RunnerDestination>,
    /// The verified, rendered recipe.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipe: Option<RunnerRecipeRef>,
    /// Resolved composition bindings, by binding name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub requires: BTreeMap<String, RunnerRequiresBinding>,
    /// Present only on retry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_attempt: Option<RunnerPreviousAttempt>,
}

// ─── RunnerEvent ─────────────────────────────────────────────────────

/// One line-delimited JSON event a runner writes to stdout.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunnerEvent {
    #[serde(default = "default_runner_schema_version")]
    pub version: String,
    pub ts: DateTime<Utc>,
    pub job_id: String,
    pub kind: RunnerEventKind,
    pub phase: RunnerPhase,
    /// Monotonic per `job_id`, dense from 0.
    pub seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_id: Option<String>,
    #[serde(default)]
    pub data: serde_json::Value,
}

// ─── RunnerOutcome ───────────────────────────────────────────────────

/// The result a runner returns to the saga. The runner maps every
/// outcome to a (kind, data) terminal event + an exit code.
#[derive(Clone, Debug)]
pub enum RunnerOutcome {
    /// The phase completed. `outputs` is the phase's result payload
    /// (e.g. the apply branch / PR), emitted on the terminal event.
    Succeeded { outputs: serde_json::Value },
    Failed {
        reason: String,
        category: FailureCategory,
    },
    Aborted {
        reason: String,
        category: FailureCategory,
    },
    NeedsCompensation {
        reason: String,
        category: FailureCategory,
    },
}

impl RunnerOutcome {
    pub fn succeeded() -> Self {
        Self::Succeeded {
            outputs: serde_json::json!({}),
        }
    }
    pub fn succeeded_with(outputs: serde_json::Value) -> Self {
        Self::Succeeded { outputs }
    }
    pub fn failed(reason: impl Into<String>, category: FailureCategory) -> Self {
        Self::Failed {
            reason: reason.into(),
            category,
        }
    }
    pub fn aborted(reason: impl Into<String>, category: FailureCategory) -> Self {
        Self::Aborted {
            reason: reason.into(),
            category,
        }
    }
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Succeeded { .. } => 0,
            Self::Failed { category, .. } => match category {
                FailureCategory::Transient
                | FailureCategory::CorruptState
                | FailureCategory::ProviderInternal
                | FailureCategory::RestartDuringApply => 1,
                FailureCategory::Precondition => 2,
                FailureCategory::Policy => 3,
            },
            Self::Aborted { category, .. } => match category {
                FailureCategory::Precondition => 2,
                _ => 3,
            },
            Self::NeedsCompensation { .. } => 1,
        }
    }
    /// The terminal [`RunnerEventKind`] and the data payload for
    /// the `succeeded` / `failed` / `aborted` event.
    pub fn terminal_event_data(&self) -> (RunnerEventKind, serde_json::Value) {
        match self {
            Self::Succeeded { outputs } => (RunnerEventKind::Succeeded, outputs.clone()),
            Self::Failed { reason, category } => (
                RunnerEventKind::Failed,
                serde_json::json!({ "reason": reason, "category": category.as_str() }),
            ),
            Self::Aborted { reason, category } => (
                RunnerEventKind::Aborted,
                serde_json::json!({ "reason": reason, "category": category.as_str() }),
            ),
            Self::NeedsCompensation { reason, category } => (
                RunnerEventKind::NeedsCompensation,
                serde_json::json!({ "reason": reason, "category": category.as_str() }),
            ),
        }
    }
}

impl From<RunnerOutcome> for (RunnerEventKind, serde_json::Value) {
    fn from(o: RunnerOutcome) -> Self {
        o.terminal_event_data()
    }
}
