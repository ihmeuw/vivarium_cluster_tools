"""Tests for the vipin performance reporting module."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from vivarium_cluster_tools.vipin.perf_report import (
    BASE_PERF_INDEX_COLS,
    PerformanceSummary,
    add_jobmon_resource_data,
    print_stat_report,
    report_performance,
    set_index_scenario_cols,
)


@pytest.fixture
def sample_perf_df() -> pd.DataFrame:
    """A minimal perf DataFrame mimicking what PerformanceSummary.to_df() returns."""
    return pd.DataFrame(
        {
            "host": ["node1", "node2"],
            "jobmon_task_id": [111, 222],
            "job_number": ["100", "100"],
            "task_number": ["1", "2"],
            "run_id": ["abcdef123456-7890-full-uuid", "fedcba654321-0987-full-uuid"],
            "draw": [0, 0],
            "seed": [1, 2],
            "scenario_treatment": ["baseline", "intervention"],
            "event_start": [1700000000.0, 1700000000.0],
            "event_end": [1700003600.0, 1700003600.0],
            "exec_time_setup_minutes": [1.0, 1.2],
            "exec_time_total_minutes": [60.0, 60.0],
        }
    )


@pytest.fixture
def workflow_tasks_df() -> pd.DataFrame:
    """Fake result from jobmon workflow_tasks()."""
    return pd.DataFrame(
        {
            "TASK_ID": [111, 222],
            "TASK_NAME": [
                "psimulate_task_id-abcdef123456",
                "psimulate_task_id-fedcba654321",
            ],
            "STATUS": ["DONE", "DONE"],
            "RETRIES": [0, 1],
        }
    )


def _make_send_request(resource_map: dict):
    """Helper: return a mock send_request that responds to /task_resource_usage."""

    def _send_request(app_route, message, request_type):
        task_id = message.get("task_id")
        if app_route == "/task_resource_usage" and task_id in resource_map:
            usage = resource_map[task_id]
            return 200, {
                "resource_usage": (
                    usage["num_attempts"],
                    usage["nodename"],
                    usage["runtime"],
                    usage["memory"],
                )
            }
        return 404, {}

    return _send_request


class TestAddJobmonResourceData:
    def test_merges_resource_data(
        self, sample_perf_df: pd.DataFrame, workflow_tasks_df: pd.DataFrame
    ) -> None:
        """Jobmon resource columns are merged in by matching jobmon_task_id."""
        resource_map = {
            111: {
                "num_attempts": 1,
                "nodename": "node1",
                "runtime": "3600",
                "memory": "4000000000",
            },
            222: {
                "num_attempts": 2,
                "nodename": "node2",
                "runtime": "3600",
                "memory": "5000000000",
            },
        }
        mock_requester = MagicMock()
        mock_requester.send_request.side_effect = _make_send_request(resource_map)

        with (
            patch(
                "vivarium_cluster_tools.vipin.perf_report.workflow_tasks",
                return_value=workflow_tasks_df,
            ),
            patch(
                "vivarium_cluster_tools.vipin.perf_report.Requester.from_defaults",
                return_value=mock_requester,
            ),
        ):
            result = add_jobmon_resource_data(sample_perf_df, workflow_id=42)

        assert "jobmon_num_attempts" in result.columns
        assert "jobmon_runtime" in result.columns
        assert "jobmon_memory" in result.columns
        # nodename, task_name, task_status should NOT be included
        assert "jobmon_nodename" not in result.columns
        assert "jobmon_task_name" not in result.columns
        assert "jobmon_task_status" not in result.columns

        assert result.loc[0, "jobmon_memory"] == "4000000000"
        assert result.loc[1, "jobmon_memory"] == "5000000000"
        assert result.loc[1, "jobmon_num_attempts"] == 2

    def test_handles_api_failure_gracefully(self, sample_perf_df: pd.DataFrame) -> None:
        """If the Jobmon API call fails, the original dataframe is returned unchanged."""
        with patch(
            "vivarium_cluster_tools.vipin.perf_report.workflow_tasks",
            side_effect=Exception("connection refused"),
        ):
            result = add_jobmon_resource_data(sample_perf_df, workflow_id=42)

        assert "jobmon_runtime" not in result.columns
        assert len(result) == len(sample_perf_df)

    def test_handles_empty_workflow_tasks(self, sample_perf_df: pd.DataFrame) -> None:
        """If workflow_tasks returns no tasks, the original dataframe is returned."""
        with patch(
            "vivarium_cluster_tools.vipin.perf_report.workflow_tasks",
            return_value=pd.DataFrame(),
        ):
            result = add_jobmon_resource_data(sample_perf_df, workflow_id=42)

        assert "jobmon_runtime" not in result.columns
        assert len(result) == len(sample_perf_df)

    def test_partial_match(
        self, sample_perf_df: pd.DataFrame, workflow_tasks_df: pd.DataFrame
    ) -> None:
        """If only some tasks have resource data, unmatched rows get NaN."""
        # Only return data for the first task
        resource_map = {
            111: {
                "num_attempts": 1,
                "nodename": "node1",
                "runtime": "3600",
                "memory": "4000000000",
            },
        }
        mock_requester = MagicMock()
        mock_requester.send_request.side_effect = _make_send_request(resource_map)

        with (
            patch(
                "vivarium_cluster_tools.vipin.perf_report.workflow_tasks",
                return_value=workflow_tasks_df,
            ),
            patch(
                "vivarium_cluster_tools.vipin.perf_report.Requester.from_defaults",
                return_value=mock_requester,
            ),
        ):
            result = add_jobmon_resource_data(sample_perf_df, workflow_id=42)

        assert result.loc[0, "jobmon_memory"] == "4000000000"
        assert pd.isna(result.loc[1, "jobmon_memory"])

    def test_retries_for_blank_resource_data(
        self, sample_perf_df: pd.DataFrame, workflow_tasks_df: pd.DataFrame
    ) -> None:
        """Tasks with blank runtime/memory are retried until data appears."""
        call_count = {111: 0, 222: 0}

        def _send_request_with_delay(app_route, message, request_type):
            task_id = message.get("task_id")
            if app_route == "/task_resource_usage" and task_id in call_count:
                call_count[task_id] += 1
                # Task 222 returns blank data on first call, valid on second
                if task_id == 222 and call_count[task_id] < 2:
                    return 200, {"resource_usage": (1, "node2", None, None)}
                return 200, {"resource_usage": (1, "node1", "3600", "4000000000")}
            return 404, {}

        mock_requester = MagicMock()
        mock_requester.send_request.side_effect = _send_request_with_delay

        with (
            patch(
                "vivarium_cluster_tools.vipin.perf_report.workflow_tasks",
                return_value=workflow_tasks_df,
            ),
            patch(
                "vivarium_cluster_tools.vipin.perf_report.Requester.from_defaults",
                return_value=mock_requester,
            ),
            patch("vivarium_cluster_tools.vipin.perf_report.time.sleep"),
        ):
            result = add_jobmon_resource_data(
                sample_perf_df, workflow_id=42, max_retries=2, retry_delay=0
            )

        # Both tasks should now have data
        assert result.loc[0, "jobmon_memory"] == "4000000000"
        assert result.loc[1, "jobmon_memory"] == "4000000000"
        # Task 222 should have been called twice
        assert call_count[222] == 2


class TestReportPerformance:
    def test_workflow_id_none_skips_jobmon(self, tmp_path: pd.DataFrame) -> None:
        """When workflow_id is None, add_jobmon_resource_data is not called."""
        with patch.object(PerformanceSummary, "to_df", return_value=pd.DataFrame()):
            result = report_performance(
                input_directory=tmp_path,
                output_directory=tmp_path,
                output_hdf=False,
                verbose=0,
                workflow_id=None,
            )
        assert result is None  # no data found


class TestPrintStatReport:
    def test_no_scenario_cols(self) -> None:
        """print_stat_report should not crash when scenario_cols is empty."""
        perf_df = pd.DataFrame(
            {
                "host": ["node1", "node2"],
                "jobmon_task_id": [1, 2],
                "workflow_run_id": [10, 10],
                "job_hash": ["a", "b"],
                "draw": [0, 0],
                "seed": [1, 2],
                "exec_time_setup_minutes": [1.0, 1.2],
                "exec_time_total_minutes": [60.0, 55.0],
            }
        )
        perf_df, scenario_cols = set_index_scenario_cols(perf_df)
        assert scenario_cols == []
        # Should not raise
        print_stat_report(perf_df, scenario_cols)


class TestSetIndexScenarioCols:
    def test_does_not_mutate_base_index_cols(self) -> None:
        """set_index_scenario_cols must not mutate the module-level constant."""
        original = list(BASE_PERF_INDEX_COLS)
        df = pd.DataFrame(
            {
                "host": ["n1"],
                "jobmon_task_id": [1],
                "workflow_run_id": [10],
                "job_hash": ["a"],
                "draw": [0],
                "seed": [1],
                "scenario_x": ["foo"],
            }
        )
        set_index_scenario_cols(df)
        assert BASE_PERF_INDEX_COLS == original
