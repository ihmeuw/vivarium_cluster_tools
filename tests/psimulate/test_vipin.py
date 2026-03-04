"""Tests for the vipin performance reporting module."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from vivarium_cluster_tools.vipin.perf_report import (
    PerformanceSummary,
    add_jobmon_resource_data,
    report_performance,
    set_index_scenario_cols,
)


@pytest.fixture
def sample_perf_df() -> pd.DataFrame:
    """A minimal perf DataFrame mimicking what PerformanceSummary.to_df() returns."""
    return pd.DataFrame(
        {
            "host": ["node1", "node2"],
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
def jobmon_resource_response() -> list[dict]:
    """Fake Jobmon wf_resource_usage API response."""
    return [
        {
            "task_id": 111,
            "task_name": "psim_abcdef123456",
            "task_status": "D",
            "task_num_attempts": 1,
            "task_instance_id": 9001,
            "ti_usage_str": "wallclock=3600 cpu=1, mem=4 GBs, io=2 GB",
            "ti_wallclock": 3600,
            "ti_maxrss": 4,
            "ti_maxpss": 3,
            "ti_cpu": 1,
            "ti_io": 2,
            "ti_status": "D",
        },
        {
            "task_id": 222,
            "task_name": "psim_fedcba654321",
            "task_status": "D",
            "task_num_attempts": 2,
            "task_instance_id": 9002,
            "ti_usage_str": "wallclock=3600 cpu=1, mem=5 GBs, io=3 GB",
            "ti_wallclock": 3600,
            "ti_maxrss": 5,
            "ti_maxpss": 4,
            "ti_cpu": 1,
            "ti_io": 3,
            "ti_status": "D",
        },
    ]


class TestAddJobmonResourceData:
    def test_merges_resource_data(
        self, sample_perf_df: pd.DataFrame, jobmon_resource_response: list[dict]
    ) -> None:
        """Jobmon resource columns are merged in by matching run_id prefix to task_name."""
        mock_response = MagicMock()
        mock_response.json.return_value = jobmon_resource_response
        mock_response.raise_for_status.return_value = None

        with (
            patch(
                "vivarium_cluster_tools.vipin.perf_report.requests.get",
                return_value=mock_response,
            ),
            patch("vivarium_cluster_tools.vipin.perf_report.JobmonConfig") as mock_config_cls,
        ):
            mock_config_cls.return_value.get.return_value = "https://jobmon.example.com"
            result = add_jobmon_resource_data(sample_perf_df, workflow_id=42)

        # Verify jobmon columns were added
        assert "jobmon_ti_wallclock" in result.columns
        assert "jobmon_ti_maxrss" in result.columns
        assert "jobmon_ti_cpu" in result.columns
        assert "jobmon_ti_io" in result.columns
        assert "jobmon_task_status" in result.columns
        assert "jobmon_task_num_attempts" in result.columns

        # Verify values merged correctly
        assert result.loc[0, "jobmon_ti_maxrss"] == 4
        assert result.loc[1, "jobmon_ti_maxrss"] == 5
        assert result.loc[1, "jobmon_task_num_attempts"] == 2

        # Verify temp column was cleaned up
        assert "run_id_prefix" not in result.columns

    def test_handles_api_failure_gracefully(self, sample_perf_df: pd.DataFrame) -> None:
        """If the Jobmon API call fails, the original dataframe is returned unchanged."""
        with (
            patch(
                "vivarium_cluster_tools.vipin.perf_report.requests.get",
                side_effect=Exception("connection refused"),
            ),
            patch("vivarium_cluster_tools.vipin.perf_report.JobmonConfig") as mock_config_cls,
        ):
            mock_config_cls.return_value.get.return_value = "https://jobmon.example.com"
            result = add_jobmon_resource_data(sample_perf_df, workflow_id=42)

        # DataFrame should be returned as-is, no jobmon columns
        assert "jobmon_ti_wallclock" not in result.columns
        assert "run_id_prefix" not in result.columns
        assert len(result) == len(sample_perf_df)

    def test_handles_empty_api_response(self, sample_perf_df: pd.DataFrame) -> None:
        """If the API returns no data, the original dataframe is returned unchanged."""
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None

        with (
            patch(
                "vivarium_cluster_tools.vipin.perf_report.requests.get",
                return_value=mock_response,
            ),
            patch("vivarium_cluster_tools.vipin.perf_report.JobmonConfig") as mock_config_cls,
        ):
            mock_config_cls.return_value.get.return_value = "https://jobmon.example.com"
            result = add_jobmon_resource_data(sample_perf_df, workflow_id=42)

        assert "jobmon_ti_wallclock" not in result.columns
        assert len(result) == len(sample_perf_df)

    def test_partial_match(
        self, sample_perf_df: pd.DataFrame, jobmon_resource_response: list[dict]
    ) -> None:
        """If only some tasks match, unmatched rows get NaN for jobmon columns."""
        # Only return data for the first task
        partial_response = [jobmon_resource_response[0]]

        mock_response = MagicMock()
        mock_response.json.return_value = partial_response
        mock_response.raise_for_status.return_value = None

        with (
            patch(
                "vivarium_cluster_tools.vipin.perf_report.requests.get",
                return_value=mock_response,
            ),
            patch("vivarium_cluster_tools.vipin.perf_report.JobmonConfig") as mock_config_cls,
        ):
            mock_config_cls.return_value.get.return_value = "https://jobmon.example.com"
            result = add_jobmon_resource_data(sample_perf_df, workflow_id=42)

        assert result.loc[0, "jobmon_ti_maxrss"] == 4
        assert pd.isna(result.loc[1, "jobmon_ti_maxrss"])


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
