"""Tests for the results writing module."""

import json
from pathlib import Path

import pandas as pd
import pytest

from vivarium_cluster_tools.psimulate.jobs import JobParameters
from vivarium_cluster_tools.psimulate.results.writing import (
    collect_metadata,
    count_completed_tasks,
    write_task_results,
    write_metadata,
)


def _make_job_parameters(
    input_draw: int = 1, random_seed: int = 42, branch_configuration: dict | None = None
) -> JobParameters:
    return JobParameters(
        model_specification="test_model_spec.yaml",
        branch_configuration=branch_configuration or {},
        input_draw=input_draw,
        random_seed=random_seed,
        results_path="~/tmp",
        backup_configuration={},
        extras={},
    )


@pytest.fixture
def results_dir(tmp_path: Path) -> Path:
    d = tmp_path / "results"
    d.mkdir()
    return d


@pytest.fixture
def metadata_dir(tmp_path: Path) -> Path:
    d = tmp_path / "metadata"
    d.mkdir()
    return d


class TestWriteTaskResults:
    def test_writes_metric_parquets(self, results_dir: Path) -> None:
        """Write task results and verify parquet files are created correctly."""
        task_id = "abc123"
        job_params = _make_job_parameters()
        results_dict = {
            "deaths": pd.DataFrame({"value": [10, 20], "year": [2020, 2021]}),
            "ylls": pd.DataFrame({"value": [100], "year": [2020]}),
        }

        write_task_results(results_dir, task_id, job_params, results_dict)

        # Check metric parquets written
        assert (results_dir / "deaths" / f"{task_id}.parquet").exists()
        assert (results_dir / "ylls" / f"{task_id}.parquet").exists()
        pd.testing.assert_frame_equal(
            pd.read_parquet(results_dir / "deaths" / f"{task_id}.parquet"),
            results_dict["deaths"],
        )
        pd.testing.assert_frame_equal(
            pd.read_parquet(results_dir / "ylls" / f"{task_id}.parquet"),
            results_dict["ylls"],
        )

    def test_no_metadata_written(self, results_dir: Path) -> None:
        """write_task_results should NOT write metadata JSON (that's the workflow builder's job)."""
        task_id = "no_meta"
        job_params = _make_job_parameters()
        write_task_results(results_dir, task_id, job_params, {"m": pd.DataFrame({"x": [1]})})

        # No metadata subdirectory should exist
        assert not (results_dir / "metadata").exists()

    def test_empty_results(self, results_dir: Path) -> None:
        """Write task results with no metric DataFrames."""
        task_id = "empty_task"
        job_params = _make_job_parameters()
        results_dict: dict[str, pd.DataFrame] = {}

        write_task_results(results_dir, task_id, job_params, results_dict)

        # No directories created
        dirs = [d for d in results_dir.iterdir() if d.is_dir()]
        assert dirs == []

    def test_multiple_tasks_same_metric(self, results_dir: Path) -> None:
        """Multiple tasks writing to the same metric directory."""
        for i, task_id in enumerate(["task_a", "task_b", "task_c"]):
            job_params = _make_job_parameters(input_draw=i, random_seed=i * 10)
            results_dict = {
                "deaths": pd.DataFrame({"value": [i * 100], "year": [2020]}),
            }
            write_task_results(results_dir, task_id, job_params, results_dict)

        # pd.read_parquet on the directory combines all files
        all_deaths = pd.read_parquet(results_dir / "deaths")
        assert len(all_deaths) == 3
        assert set(all_deaths["value"].tolist()) == {0, 100, 200}


class TestCollectMetadata:
    def test_no_results(self, metadata_dir: Path, results_dir: Path) -> None:
        """No completed tasks means empty DataFrame."""
        result = collect_metadata(metadata_dir, results_dir)
        assert result.empty

    def test_collects_metadata_for_completed_tasks(
        self, metadata_dir: Path, results_dir: Path
    ) -> None:
        """collect_metadata reads metadata JSONs only for tasks with result parquets."""
        # Create metadata JSONs for 3 tasks
        for i, tid in enumerate(["task_a", "task_b", "task_c"]):
            params = _make_job_parameters(input_draw=i, random_seed=i * 10)
            write_metadata(metadata_dir, tid, params)

        # Only task_a and task_c have result parquets
        metric_dir = results_dir / "deaths"
        metric_dir.mkdir(parents=True)
        pd.DataFrame({"v": [1]}).to_parquet(metric_dir / "task_a.parquet")
        pd.DataFrame({"v": [2]}).to_parquet(metric_dir / "task_c.parquet")

        result = collect_metadata(metadata_dir, results_dir)
        assert len(result) == 2
        assert set(result["input_draw"].tolist()) == {0, 2}

    def test_includes_branch_config(self, metadata_dir: Path, results_dir: Path) -> None:
        """Metadata DataFrame includes flattened branch configuration keys."""
        params = _make_job_parameters(
            input_draw=5,
            random_seed=99,
            branch_configuration={"scenario": {"treatment": "A"}},
        )
        write_metadata(metadata_dir, "branch_task", params)

        metric_dir = results_dir / "metric"
        metric_dir.mkdir(parents=True)
        pd.DataFrame({"v": [1]}).to_parquet(metric_dir / "branch_task.parquet")

        result = collect_metadata(metadata_dir, results_dir)
        assert len(result) == 1
        assert result["input_draw"].iloc[0] == 5
        assert result["random_seed"].iloc[0] == 99
        assert result["scenario.treatment"].iloc[0] == "A"

    def test_ignores_tasks_without_metadata(
        self, metadata_dir: Path, results_dir: Path
    ) -> None:
        """Tasks with parquets but no metadata JSON are silently skipped."""
        metric_dir = results_dir / "metric"
        metric_dir.mkdir(parents=True)
        pd.DataFrame({"v": [1]}).to_parquet(metric_dir / "orphan_task.parquet")

        result = collect_metadata(metadata_dir, results_dir)
        assert result.empty
