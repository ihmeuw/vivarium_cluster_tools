# TDD Plan: Slack Notification on Workflow Completion

## Overview

Add Slack notifications to psimulate so that users are notified when any
workflow completes — whether launched via `psimulate run` or
`psimulate workflow`. The notification fires as post-`workflow.run()` Python
code in the launcher process (not as a Jobmon task), ensuring it triggers on
both success and failure.

Configuration is via a single environment variable
`PSIMULATE_SLACK_WEBHOOK`. If the webhook is unset or the HTTP POST fails,
the notification is skipped gracefully (logged, never raised).

### Slack Delivery Mechanism: Workflow Webhook

The notification uses a **Slack Workflow with a webhook trigger**. This
approach requires no admin privileges — any workspace member can create one.

**Setup (one-time, per user):**

1. In Slack → **Automations** → **New Workflow** → trigger: **Webhook**.
2. Define webhook variables: `workflow_name` (text), `status` (text),
   `monitoring_url` (text), `results_dir` (text), `user` (text).
3. Add a **Send a message** step composing the notification from those
   variables (to yourself, a channel, or another person).
4. **Publish** — Slack provides a webhook URL.
5. Set the env var on the cluster:
   ```bash
   export PSIMULATE_SLACK_WEBHOOK="https://hooks.slack.com/workflows/T.../A.../..."
   ```

The psimulate code simply POSTs JSON with the five variables to that URL.
The Slack Workflow handles all routing (DM, channel, mention format, etc.)
— psimulate does not need a bot token, OAuth scopes, or email lookup.

### Why not a Bot App?

A bot app (`PSIMULATE_SLACK_BOT_TOKEN`) would allow richer programmatic
control (DM via `chat.postMessage`, user lookup via `users.lookupByEmail`,
channel mentions with `<@user_id>`). However, it requires a workspace admin
to install the app. The webhook approach is chosen because:

- **No admin needed** — any user can create a Slack Workflow.
- **Simpler code** — one `requests.post` call, no multi-step API flow.
- **User-controlled routing** — each user configures their own Slack
  Workflow to deliver notifications however they prefer.

If a bot app becomes available in the future, the implementation can be
extended behind the same `send_slack_notification()` interface without
changing the caller code in `runner.py`.

## Codebase Context

- **Test framework**: pytest (`pyproject.toml` → `testpaths = ["tests/"]`)
- **Test style**: classes grouping related tests, `from __future__ import annotations`,
  `unittest.mock` / `pytest_mock.MockerFixture`, `monkeypatch` for env vars
- **Relevant modules**:
  - `src/vivarium_cluster_tools/psimulate/runner.py` — `main()` (psimulate run)
    and `workflow_main()` (psimulate workflow) are the two integration points
  - New module: `src/vivarium_cluster_tools/psimulate/notifications.py`
- **Key patterns observed**:
  - Tests mirror source layout: `tests/psimulate/` ↔ `src/vivarium_cluster_tools/psimulate/`
  - Fixtures and helpers live in `conftest.py` / `utilities.py` per sub-package
  - HTTP/external calls are mocked (no real network in unit tests)
  - `requests` is already an install dependency (via `vivarium_dependencies`)
- **Conda environment**: `vct`

## Prerequisites

- No new dependencies required (`requests` is already installed via
  `vivarium_dependencies`).
- A Slack Workflow with a webhook trigger (see setup steps above).
- The webhook URL stored in `PSIMULATE_SLACK_WEBHOOK` on the cluster.
- PR #302 (Jobmon refactor) must be merged (it already is on this branch).

---

## Phase 1: Tests and Stubs

Write both tests and the minimal stub needed for imports before any real
implementation exists. Both tests are marked `@pytest.mark.xfail`.

### Module: `notifications`

**Test file**: `tests/psimulate/test_notifications.py`

The webhook POST is mocked via `unittest.mock.patch` on `requests.post`.
The SLURM username is controlled by monkeypatching `$USER`. The webhook URL
is set via monkeypatching `$PSIMULATE_SLACK_WEBHOOK`.

| # | Test | Asserts | xfail? |
|---|------|---------|--------|
| 1 | `test_notification_on_workflow_success` | When a workflow finishes with status `"D"`: (a) `requests.post` is called once with the webhook URL, (b) the JSON payload contains `workflow_name`, `status` = `"DONE"`, `monitoring_url`, `results_dir`, and `user` = `$USER` | Yes |
| 2 | `test_notification_on_workflow_failure` | When a workflow finishes with status `"F"`: (a) `requests.post` is called once with the webhook URL, (b) the JSON payload contains `workflow_name`, `status` = `"ERROR"`, `monitoring_url`, `results_dir`, and `user` = `$USER` | Yes |

Each test:
1. Sets `PSIMULATE_SLACK_WEBHOOK=https://hooks.slack.com/workflows/test`
   via `monkeypatch.setenv`.
2. Sets `USER=testuser` via `monkeypatch.setenv`.
3. Mocks `requests.post` to return a `MagicMock` with `status_code=200`.
4. Calls `send_slack_notification(workflow_name="my_pipeline", status=...,
   monitoring_url="https://jobmon.example.com/#/workflow/123",
   results_dir="/tmp/results")`.
5. Asserts on the `requests.post` call:
   - First arg is the webhook URL.
   - `json=` kwarg is a dict with keys `workflow_name`, `status`,
     `monitoring_url`, `results_dir`, `user`.
   - `status` value is `"DONE"` for test 1, `"ERROR"` for test 2.
   - `user` value is `"testuser"`.

**Stubs needed**:

- `src/vivarium_cluster_tools/psimulate/notifications.py`:

```python
"""Slack notification support for psimulate workflows."""

from __future__ import annotations


def send_slack_notification(
    workflow_name: str,
    status: str,
    monitoring_url: str | None = None,
    results_dir: str | None = None,
) -> None:
    """Send a Slack notification after a workflow completes.

    POSTs workflow details to a Slack Workflow webhook URL read from
    ``PSIMULATE_SLACK_WEBHOOK``. The SLURM ``$USER`` is included in
    the payload so the Slack Workflow can route/mention appropriately.

    If the webhook URL is unset or the POST fails, logs a warning and
    returns without raising.
    """
    raise NotImplementedError
```

---

## Phase 2: Implementation

After this phase, the xfail-marked tests should report unexpected passes
(the underlying assertions now pass).

### Summary of Changes

| File | Change Description |
|------|--------------------|
| `src/vivarium_cluster_tools/psimulate/notifications.py` | Implement `send_slack_notification()`: read env vars, format payload, POST to webhook, handle errors gracefully |
| `src/vivarium_cluster_tools/psimulate/runner.py` | Call `send_slack_notification()` in `workflow_main()` and `main()` after `_bind_and_run_workflow()` returns; refactor `_bind_and_run_workflow` to return `monitoring_url` alongside the status |

### Implementation Details

#### `notifications.py`

- **File**: `src/vivarium_cluster_tools/psimulate/notifications.py`
- **Changes**:
  1. Read `PSIMULATE_SLACK_WEBHOOK` from `os.environ`. If unset, log a
     debug message via `loguru.logger` and return early.
  2. Get the SLURM username from `os.environ.get("USER", "unknown")`.
  3. Map the Jobmon status code to a human-readable string:
     `"DONE"` if `status == "D"` else `"ERROR"`.
  4. Build the JSON payload:
     ```python
     {
         "workflow_name": workflow_name,
         "status": status_text,       # "DONE" or "ERROR"
         "monitoring_url": monitoring_url or "",
         "results_dir": results_dir or "",
         "user": username,
     }
     ```
  5. POST to the webhook URL via `requests.post(webhook_url, json=payload)`.
  6. Wrap the entire function body in a `try/except Exception` block. On
     failure, log a warning via `loguru.logger` and return (never raise).

#### `runner.py`

- **File**: `src/vivarium_cluster_tools/psimulate/runner.py`
- **Changes**:
  1. Import `send_slack_notification` from
     `vivarium_cluster_tools.psimulate.notifications`.
  2. Refactor `_bind_and_run_workflow()` to return a
     `tuple[str, str]` — `(wf_status, monitoring_url)` — instead of just
     `wf_status`, so callers have the monitoring URL.
  3. In `workflow_main()`, after `_bind_and_run_workflow()`, call:
     ```python
     send_slack_notification(
         workflow_name=workflow_config.name,
         status=wf_status,
         monitoring_url=monitoring_url,
         results_dir=str(output_root),
     )
     ```
  4. In `main()`, after `_bind_and_run_workflow()`, call:
     ```python
     send_slack_notification(
         workflow_name=workflow_name,
         status=wf_status,
         monitoring_url=monitoring_url,
         results_dir=str(output_paths.root),
     )
     ```
  5. Both call sites are already protected because `send_slack_notification`
     catches all exceptions internally. No extra try/except needed in
     `runner.py`.

---

## Phase 3: Remove xfail Markers and Verify

Remove all `@pytest.mark.xfail` decorators from the tests written in Phase 1.
All tests should now pass.

Run a full local build to verify everything is green:

```bash
conda activate vct
make check
```

### Checklist

- [ ] All `@pytest.mark.xfail` markers removed
- [ ] All tests pass locally
- [ ] `make check` passes
- [ ] Notification is skipped gracefully when `PSIMULATE_SLACK_WEBHOOK` is unset (no test failures in CI)
- [ ] HTTP failures are caught and logged, never raised
- [ ] Both `psimulate run` and `psimulate workflow` trigger notifications
