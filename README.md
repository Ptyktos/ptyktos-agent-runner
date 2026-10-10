# ptyktos-agent-runner

The Ptyktos substrate as a saga runner. Implements the
[`hosting_core::task::RunnerJob`] / [`hosting_core::task::RunnerEvent`]
contract for the 5 lifecycle phases
(`plan` / `apply` / `verify` / `compensate` / `destroy`). Plugs into
the Tasmanian-Cloud/saga `runner` crate as a 6th binary alongside
`tofu-runner` / `ansible-runner` / `helm-runner` / `proxmox-runner` /
`talm-runner`.

## The contract

The runner reads one [`RunnerJob`] from stdin (JSON, schema version
`"1"`) and emits line-delimited [`RunnerEvent`]s on stdout. Exit
codes are the contract table:

| Outcome | Exit code | Terminal kind |
|---|---:|---|
| `Succeeded` | 0 | `Succeeded` |
| `Failed(Transient)` | 1 | `Failed` |
| `Aborted(Precondition)` | 2 | `Aborted` |
| `Aborted(Policy)` | 3 | `Aborted` |
| `Failed(CorruptState)` | 1 | `Failed` |
| `Failed(ProviderInternal)` | 1 | `Failed` |
| SIGTERM | 143 | `NeedsCompensation` |

Free-form logs go to stderr; stdout is events only.

## The 5 phases, mapped to the Ptyktos substrate

| Phase | Substrate touched | What the runner does |
|---|---|---|
| `plan` | none | Read the recipe; assemble the [`AgentTask`]; emit a `plan` event |
| `apply` | `agentvfs` + `intent` + `memoryfold` | Mount the workspace, snapshot for rollback, invoke the agent, capture the output (PR, branch, trace ids) |
| `verify` | `caretaker` + `telos` | Submit the PR for review, await the verdict, gate the `Done` event through `telos` |
| `compensate` | `agentvfs` | Rollback the workspace to the pre-apply snapshot |
| `destroy` | `agentvfs` + `memoryfold` | Tear down the workspace; seal the trace |

## The recipe shape

The runner expects the rendered template to be a JSON object with the
`agent` section:

```json
{
  "agent": {
    "repo": "ptyktos/memoryfold",
    "branch": "feat/trace-ontology-v2",
    "base_sha": "abc123",
    "task": "the natural-language task description"
  }
}
```

If the rendered template doesn't match this shape, the runner aborts
with a `Precondition` failure and the saga-orchestrator branches on
it.

## How to plug it into the saga

1. Add `ptyktos-agent-runner = { path = "../ptyktos-agent-runner" }` to
   the saga `runner` crate's `Cargo.toml`.
2. Add a new binary:

   ```rust
   // crates/runner/src/bin/ptyktos_agent_runner.rs
   fn main() -> std::process::ExitCode {
       let Some(job) = runner::read_job_from_stdin() else {
           return std::process::ExitCode::from(64);
       };
       let emitter = runner::EventEmitter::new(&job);
       let substrate = ptyktos_agent_runner::PtyktosSubstrate::ptyktos();
       let mut state = ptyktos_agent_runner::RunnerState::default();
       match runner::race_sigterm(ptyktos_agent_runner::run(&job, &substrate, &mut state)).await {
           runner::RaceOutcome::Completed(outcome) => runner::finish(&emitter, outcome),
           runner::RaceOutcome::SigTerm => runner::handle_sigterm(&job),
       }
   }
   ```

3. Register the new runner in the saga-planner's
   `RunnerKind::parse` so a recipe's `kind: ptyktos-agent` dispatches
   to this binary.

4. Author the `ptyktos-agent-deploy` recipe type in
   `recipe-composer` (alongside `tofu-deploy`, `ansible-deploy`,
   `helm-deploy`, `proxmox-deploy`, `talos-deploy`). The recipe's
   `template` is the agent-task JSON above; the `requires` are the
   substrate crates the agent needs mounted into its workspace
   (typically `memoryfold` for the trace context, `caretaker` for the
   review target, `telos` for the gate's policy file).

5. The saga-orchestrator handles the rest — phases, retries, lease
   boundaries, audit — exactly the same as for the existing runners.

## Build

```sh
# minimal (no substrate — uses the no-op adapters; the lib still builds)
cargo build --no-default-features --features minimal

# full (with the Ptyktos substrate — `ptyktos-coding-agent` and friends
# path-deps in the dev layout)
cargo build --features substrate

# test
cargo test
```

## Why a new runner and not a new saga?

The agent's work is a *single step* in a larger deploy flow. The
saga-orchestrator owns the multi-step flow (plan → apply → verify
→ compensate/destroy). The runner is the workhorse. Trying to model
agent work as its own saga would re-implement the runner contract
without a reason.

## Cross-references

- The runner contract: `hosting_core::task::{RunnerJob, RunnerEvent,
  RunnerOutcome, RunnerPhase, FailureCategory}`. The contract
  schema lives in `runtime/hosting-runtime/crates/runner-contract-tests/`.
- The saga-orchestrator: `Tasmanian-Cloud/saga` repo, the
  `saga-orchestrator` crate. The runner binaries are siblings in
  the `runner` crate.
- The Ptyktos substrate: `Ptyktos/{ptyktos-coding-agent, memoryfold,
  intent, caretaker, conduit, telos, workfold, hashtrinity,
  modelfold, sangdb, agentvfs}` on GitHub.
- The factory this runner plugs into:
  `runtime/hosting-runtime` (workflow + reconciliation services).
