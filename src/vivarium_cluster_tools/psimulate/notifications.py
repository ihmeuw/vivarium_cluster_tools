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

    Resolves the SLURM ``$USER`` to a Slack user via ``{user}@uw.edu``
    email lookup using the Slack API. Reads ``PSIMULATE_SLACK_BOT_TOKEN``
    from the environment. If the token is unset or any API call fails,
    logs a warning and returns without raising.
    """
    raise NotImplementedError
