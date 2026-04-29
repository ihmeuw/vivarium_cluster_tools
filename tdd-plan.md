# TDD Plan: Slack Notification on Workflow Completion

## Overview

Add Slack notifications to psimulate so that users are notified when any
workflow completes — whether launched via `psimulate run` or
`psimulate workflow`. The notification fires as post-`workflow.run()` Python
code in the launcher process (not as a Jobmon task), ensuring it triggers on
both success and failure.

Configuration is via a single environment variable
`PSIMULATE_SLACK_BOT_TOKEN` (a Slack bot token). The SLURM username
(`$USER`, e.g. `albrja`) is resolved to a Slack user via the
`{username}@uw.edu` email convention using the Slack API. If the token is
unset or any Slack API call fails, the notification is skipped gracefully
(logged, never raised).

### Slack Delivery Mechanism: Bot App

The notification uses a **Slack Bot App** installed in the workspace.

**Setup (one-time, requires workspace admin):**

1. Go to https://api.slack.com/apps → **Create New App** → **From scratch**.
2. Name it (e.g. `psimulate-notifications`), select your workspace.
3. Go to **OAuth & Permissions** → **Bot Token Scopes**, add:
   - `users:read.email` — look up users by email
   - `chat:write` — send messages
   - `im:write` — open DM conversations
4. Click **Install to Workspace** (requires admin approval) → Approve.
5. Copy the **Bot User OAuth Token** (`xoxb-...`).
6. On the cluster, add to a shared profile or `~/.bashrc`:
   ```bash
   export PSIMULATE_SLACK_BOT_TOKEN="xoxb-your-token-here"
   ```
7. Optionally, to post to a channel instead of DM:
   - Invite the bot to the channel (`/invite @psimulate-notifications`).
   - Set `export PSIMULATE_SLACK_CHANNEL="C0123ABCDEF"` (the channel ID).

**How it works in code:**

1. Read `PSIMULATE_SLACK_BOT_TOKEN` from `os.environ`. If unset, skip.
2. Get the SLURM username from `$USER` (e.g. `albrja`).
3. Call Slack `users.lookupByEmail` with `{username}@uw.edu` to get the
   Slack user ID.
4. If `PSIMULATE_SLACK_CHANNEL` is set, post to that channel with an
   `<@user_id>` mention. Otherwise, call `conversations.open` to get a
   DM channel ID, then post there via `chat.postMessage`.

### Bot Token Scopes

| Scope | Required for |
|-------|-------------|
| `users:read.email` | `users.lookupByEmail` — resolve `$USER@uw.edu` → Slack user ID |
| `chat:write` | `chat.postMessage` — send the notification |
| `im:write` | `conversations.open` — open a DM conversation (DM mode only) |

For **channel mode**, the bot must also be invited to the target channel.

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
- A Slack Bot App with the scopes listed above, installed to the workspace.
- The bot token stored in `PSIMULATE_SLACK_BOT_TOKEN` on the cluster.
- PR #302 (Jobmon refactor) must be merged (it already is on this branch).

## Token Deployment

The bot token is distributed to all team members automatically via
`vivarium_build_utils`. No per-user setup is required.

### How it works

1. **One-time setup**: A team member creates a shared token file on the
   cluster filesystem:
   ```bash
   mkdir -p /mnt/team/simulation_science/priv/engineering/config
   cat > /mnt/team/simulation_science/priv/engineering/config/slack_bot_config.sh << 'EOF'
   export PSIMULATE_SLACK_BOT_TOKEN="xoxb-your-actual-token-here"
   EOF
   chmod 640 /mnt/team/simulation_science/priv/engineering/config/slack_bot_config.sh
   ```

2. **`vivarium_build_utils` change**: The `install` target in
   `resources/makefiles/base.mk` calls a new `setup-slack` target that
   copies the token file into the conda env's activation directory
   (`$CONDA_PREFIX/etc/conda/activate.d/`). A corresponding deactivation
   script unsets the variable on `conda deactivate`.

3. **Result**: Any team member running `make build-env` on any vivarium repo
   gets the token automatically. `conda activate <env>` sets
   `PSIMULATE_SLACK_BOT_TOKEN`; `conda deactivate` unsets it. If the shared
   file doesn't exist, `make install` prints a note and continues — Slack
   notifications are simply disabled.

---

## Phase 1: Tests and Stubs

Write both tests and the minimal stub needed for imports before any real
implementation exists. Both tests are marked `@pytest.mark.xfail`.

### Module: `notifications`

**Test file**: `tests/psimulate/test_notifications.py`

All Slack API calls (`users.lookupByEmail`, `conversations.open`,
`chat.postMessage`) are mocked via `unittest.mock.patch` on `requests.post`.
The SLURM username is controlled by monkeypatching `$USER`. The bot token
is set via monkeypatching `$PSIMULATE_SLACK_BOT_TOKEN`.

| # | Test | Asserts | xfail? |
|---|------|---------|--------|
| 1 | `test_notification_on_workflow_success` | When a workflow finishes with status `"D"`: (a) `users.lookupByEmail` is called with `{$USER}@uw.edu`, (b) `conversations.open` is called with the resolved user ID, (c) `chat.postMessage` is called with a message containing `"DONE"`, the workflow name, monitoring URL, and results directory | Yes |
| 2 | `test_notification_on_workflow_failure` | When a workflow finishes with status `"F"`: (a) `users.lookupByEmail` is called with `{$USER}@uw.edu`, (b) `conversations.open` is called with the resolved user ID, (c) `chat.postMessage` is called with a message containing `"ERROR"`, the workflow name, monitoring URL, and results directory | Yes |

Each test:
1. Sets `PSIMULATE_SLACK_BOT_TOKEN=xoxb-test-token` via `monkeypatch.setenv`.
2. Sets `USER=testuser` via `monkeypatch.setenv`.
3. Ensures `PSIMULATE_SLACK_CHANNEL` is unset (DM mode).
4. Mocks `requests.post` to return canned Slack API responses for each
   sequential call:
   - Call 1 (`users.lookupByEmail`): `{"ok": true, "user": {"id": "U12345"}}`
   - Call 2 (`conversations.open`): `{"ok": true, "channel": {"id": "D67890"}}`
   - Call 3 (`chat.postMessage`): `{"ok": true}`
5. Calls `send_slack_notification(workflow_name="my_pipeline", status=...,
   monitoring_url="https://jobmon.example.com/#/workflow/123",
   results_dir="/tmp/results")`.
6. Asserts on the three `requests.post` calls:
   - Call 1: URL ends with `users.lookupByEmail`, body contains `testuser@uw.edu`.
   - Call 2: URL ends with `conversations.open`, body contains user ID `U12345`.
   - Call 3: URL ends with `chat.postMessage`, body contains channel `D67890`
     and a `text` with `"DONE"` / `"ERROR"`, the workflow name, monitoring URL,
     and results dir.

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

    Resolves the SLURM ``$USER`` to a Slack user via ``{user}@uw.edu``
    email lookup using the Slack API. Reads ``PSIMULATE_SLACK_BOT_TOKEN``
    from the environment. If the token is unset or any API call fails,
    logs a warning and returns without raising.
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
| `src/vivarium_cluster_tools/psimulate/notifications.py` | Implement `send_slack_notification()`: resolve user via Slack email lookup, format message, DM or channel post via `chat.postMessage`, handle errors gracefully |
| `src/vivarium_cluster_tools/psimulate/runner.py` | Call `send_slack_notification()` in `workflow_main()` and `main()` after `_bind_and_run_workflow()` returns; refactor `_bind_and_run_workflow` to return `monitoring_url` alongside the status |

### Implementation Details

#### `notifications.py`

- **File**: `src/vivarium_cluster_tools/psimulate/notifications.py`
- **Changes**:
  1. Read `PSIMULATE_SLACK_BOT_TOKEN` from `os.environ`. If unset, log a
     debug message via `loguru.logger` and return early.
  2. Get the SLURM username from `os.environ.get("USER", "unknown")`.
  3. Build the auth headers: `{"Authorization": f"Bearer {token}"}`.
  4. Call Slack `users.lookupByEmail` API via `requests.post`:
     ```
     POST https://slack.com/api/users.lookupByEmail
     Authorization: Bearer {token}
     Content-Type: application/x-www-form-urlencoded
     Body: email={username}@uw.edu
     ```
     Extract `user["id"]` from the response JSON.
  5. Determine the channel to post to:
     - If `PSIMULATE_SLACK_CHANNEL` is set, use that channel ID and
       mention the user with `<@{user_id}>` in the message text.
     - Otherwise (DM mode), call `conversations.open`:
       ```
       POST https://slack.com/api/conversations.open
       Authorization: Bearer {token}
       Content-Type: application/json
       Body: {"users": "{user_id}"}
       ```
       Extract `channel["id"]` from the response.
  6. Build the message string:
     - Status text: `"DONE"` if `status == "D"` else `"ERROR"`.
     - Emoji: `✅` if done, `❌` if error.
     - Line 1: `{emoji} psimulate workflow {status_text}: {workflow_name}`
     - Line 2 (conditional): `Monitor: {monitoring_url}` (only if not None)
     - Line 3 (conditional): `Results: {results_dir}` (only if not None)
     - In channel mode, prepend `<@{user_id}>` to the message.
  7. Call Slack `chat.postMessage` API via `requests.post`:
     ```
     POST https://slack.com/api/chat.postMessage
     Authorization: Bearer {token}
     Content-Type: application/json
     Body: {"channel": "{channel_id}", "text": "{message}"}
     ```
  8. Wrap the entire function body in a `try/except Exception` block. On
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
- [ ] Notification is skipped gracefully when `PSIMULATE_SLACK_BOT_TOKEN` is unset (no test failures in CI)
- [ ] Slack API failures are caught and logged, never raised
- [ ] Both `psimulate run` and `psimulate workflow` trigger notifications
- [ ] DM mode works when `PSIMULATE_SLACK_CHANNEL` is unset
- [ ] Channel mention mode works when `PSIMULATE_SLACK_CHANNEL` is set
