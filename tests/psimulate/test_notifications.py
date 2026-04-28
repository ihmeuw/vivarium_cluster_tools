"""Unit tests for Slack workflow notifications."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from vivarium_cluster_tools.psimulate.notifications import send_slack_notification

WEBHOOK_URL = "https://hooks.slack.com/workflows/T000/A000/test"
MONITORING_URL = "https://jobmon.example.com/#/workflow/123"
RESULTS_DIR = "/tmp/results"
WORKFLOW_NAME = "my_pipeline"


@pytest.mark.xfail(reason="send_slack_notification not yet implemented", strict=True)
def test_notification_on_workflow_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """A successful workflow POSTs a DONE payload to the Slack webhook."""
    monkeypatch.setenv("PSIMULATE_SLACK_WEBHOOK", WEBHOOK_URL)
    monkeypatch.setenv("USER", "testuser")

    with patch("vivarium_cluster_tools.psimulate.notifications.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=200)

        send_slack_notification(
            workflow_name=WORKFLOW_NAME,
            status="D",
            monitoring_url=MONITORING_URL,
            results_dir=RESULTS_DIR,
        )

        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert call_args[0][0] == WEBHOOK_URL

        payload = call_args[1]["json"]
        assert payload["workflow_name"] == WORKFLOW_NAME
        assert payload["status"] == "DONE"
        assert payload["monitoring_url"] == MONITORING_URL
        assert payload["results_dir"] == RESULTS_DIR
        assert payload["user"] == "testuser"


@pytest.mark.xfail(reason="send_slack_notification not yet implemented", strict=True)
def test_notification_on_workflow_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed workflow POSTs an ERROR payload to the Slack webhook."""
    monkeypatch.setenv("PSIMULATE_SLACK_WEBHOOK", WEBHOOK_URL)
    monkeypatch.setenv("USER", "testuser")

    with patch("vivarium_cluster_tools.psimulate.notifications.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=200)

        send_slack_notification(
            workflow_name=WORKFLOW_NAME,
            status="F",
            monitoring_url=MONITORING_URL,
            results_dir=RESULTS_DIR,
        )

        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert call_args[0][0] == WEBHOOK_URL

        payload = call_args[1]["json"]
        assert payload["workflow_name"] == WORKFLOW_NAME
        assert payload["status"] == "ERROR"
        assert payload["monitoring_url"] == MONITORING_URL
        assert payload["results_dir"] == RESULTS_DIR
        assert payload["user"] == "testuser"
