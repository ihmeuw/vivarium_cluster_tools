"""Slack notification support for psimulate workflows."""

# mypy: ignore-errors

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
