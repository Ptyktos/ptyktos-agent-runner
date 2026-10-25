# ptyktos-agent-runner

A saga runner that drives the Ptyktos substrate through the runner
contract: one `RunnerJob` on stdin, line-delimited `RunnerEvent`s on
stdout, exit code from the contract table.

**Status: the runner is wired, but no production substrate exists yet.**
The binary refuses to run without a substrate (exit 2), so a default build
cannot report a fake success. The only substrate in the crate is a local test
double, behind the `test-substrate` cargo feature (off by default).

## The contract

| Outcome | Exit code | Terminal kind |
|---|---:|---|
| `Succeeded` (with `outputs`) | 0 | `succeeded` |
| `Failed(Transient)` / `ProviderInternal` / `CorruptState` | 1 | `failed` |
| `NeedsCompensation(Policy)`: gate denial, review rejection | 1 | `needs_compensation` |
| `Aborted(Precondition)`: bad job, no substrate, no checkpoint key | 2 | `aborted` |
| `Aborted(Policy)` | 3 | `aborted` |
| unreadable job on stdin | 64 | (no event) |
| SIGTERM during a phase | 143 | `needs_compensation` |

Logs go to stderr; stdout is events only. The wire types are a local copy
of `hosting_core::task` (`src/contract.rs`). The copy is not checked against
the canonical fixtures here.

## Environment

| Variable | Meaning |
|---|---|
| `TAS_RUNNER_IDEMPOTENCY_KEY` | Required. `<lifecycle>:<phase>:<ordinal>`, e.g. `saga-42:apply:1`. The lifecycle id (no `:` inside) names the checkpoint, so all phases of one saga share it. The phase segment must match the job's phase, or the runner exits 2. The full key identifies one attempt of one phase. |
| `TAS_RUNNER_CHECKPOINT_DIR` | Required. Directory for checkpoint files: one `<hex lifecycle>.json` per saga, plus a `.lock` beside each. |
| `PTYKTOS_AGENT_RUNNER_SUBSTRATE` | Selects a substrate. Only `local-test`, and only in a build with `--features test-substrate`. Anything else (or unset) exits 2. |

## Checkpoint semantics

- The checkpoint is held under an exclusive file lock from load to the end
  of the run, so two overlapping runners on one saga serialise. A runner
  waiting for the lock polls it, so SIGTERM during that wait still ends with
  `needs_compensation` and exit 143.
- Every write goes to a unique temp file (`create_new`, pid, counter, clock),
  is fsynced, then renamed over the checkpoint. The directory is fsynced
  after the rename, before the write reports success.
- A phase that completed under a given full key records that key. The same
  key again replays its outcome. A different ordinal re-runs the phase
  idempotently: apply does not mount or run the agent again if it already
  recorded its output.

## The phases

| Phase | What it does | Checkpoint |
|---|---|---|
| `plan` | Parses the recipe; no side effects. | none |
| `apply` | Mounts the workspace once, snapshots it, runs the agent, records the output. A retry with the same key returns the recorded output without mounting or running again. | workspace, snapshot, agent output |
| `verify` | Submits the branch once, polls the review until it decides (pending is polled again), then consults the gate. A rejection or a denial is `needs_compensation`. | review id |
| `compensate` | Restores the pre-apply snapshot. | reads apply's |
| `destroy` | Removes the workspace. | reads apply's |

Successful terminal events carry `outputs`: for `apply` the agent output
(branch, PR, trace ids), for `verify` the review id.

## Recipe shape

The rendered template must be an object with an `agent` section:

```json
{ "agent": { "repo": "ptyktos/memoryfold", "branch": "feat/x", "base_sha": "abc123", "task": "..." } }
```

Anything else is a `precondition` failure.

## Build and test

```sh
cargo build                       # production shape: no substrate, exits 2
cargo test                        # dev-dependency turns on test-substrate
cargo clippy --all-targets -- -D warnings
cargo deny check
```

The process tests (`tests/process.rs`) start the real binary, run apply and
verify as separate processes sharing one checkpoint directory, and check:
denial yields compensation, a retried apply does not mount a second
workspace (including after a failure between mount and snapshot), a pending
review is polled, a missing substrate exits 2, and a corrupt checkpoint is
`corrupt_state`.

## Not done

- No production adapters for `agentvfs`, `intent`, `memoryfold`,
  `caretaker`, or `telos`. Their APIs were not available to verify. They
  belong in a composition root outside this crate.
- The wire types are not checked for byte-equality against
  `runtime/hosting-runtime/crates/runner-contract-tests`, which is not in
  this repository.
- A crash between mounting a workspace and writing its checkpoint can still
  leave an orphan workspace. The checkpoint is written right after the mount,
  but nothing reconciles a workspace that was never recorded.
