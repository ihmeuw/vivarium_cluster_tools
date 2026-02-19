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


class TestWriteTaskResults:
    def test_writes_metadata_and_metrics(self, results_dir: Path) -> None:
        """Write task results and verify files are created correctly."""
        task_id = "abc123"
        job_params = _make_job_parameters()
        results_dict = {
            "deaths": pd.DataFrame({"value": [10, 20], "year": [2020, 2021]}),
            "ylls": pd.DataFrame({"value": [100], "year": [2020]}),
        }

        write_task_results(results_dir, task_id, job_params, results_dict)

        # Check metadata written as JSON
        metadata_path = results_dir / "metadata" / f"{task_id}_metadata.json"
        assert metadata_path.exists()
        with open(metadata_path) as f:
            read_metadata = json.load(f)
        assert read_metadata == {"input_draw": 1, "random_seed": 42}

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

    def test_metadata_includes_branch_config(self, results_dir: Path) -> None:
        """Metadata JSON includes flattened branch configuration keys."""
        task_id = "branch_task"
        job_params = _make_job_parameters(
            input_draw=5,
            random_seed=99,
            branch_configuration={"scenario": {"treatment": "A"}},
        )
        write_task_results(results_dir, task_id, job_params, {})

        with open(results_dir / "metadata" / f"{task_id}_metadata.json") as f:
            metadata = json.load(f)
        assert metadata["input_draw"] == 5
        assert metadata["random_seed"] == 99
        assert metadata["scenario.treatment"] == "A"

    def test_empty_results(self, results_dir: Path) -> None:
        """Write task results with no metric DataFrames."""
        task_id = "empty_task"
        job_params = _make_job_parameters()
        results_dict: dict[str, pd.DataFrame] = {}

        write_task_results(results_dir, task_id, job_params, results_dict)

        metadata_path = results_dir / "metadata" / f"{task_id}_metadata.json"
        assert metadata_path.exists()
        # No metric directories created
        dirs = [d for d in results_dir.iterdir() if d.is_dir() and d.name != "metadata"]
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
    def test_no_metadata(self, results_dir: Path) -> None:
        result = collect_metadata(results_dir)
        assert result.empty

    def test_collects_all_metadata(self, results_dir: Path) -> None:
        metadata_dir = results_dir / "metadata"
        metadata_dir.mkdir()

        for name, data in [
            ("task_a_metadata.json", {"input_draw": 0, "random_seed": 10}),
            ("task_b_metadata.json", {"input_draw": 1, "random_seed": 20}),
        ]:
            with open(metadata_dir / name, "w") as f:
                json.dump(data, f)

        result = collect_metadata(results_dir)
        assert len(result) == 2
        assert set(result["input_draw"].tolist()) == {0, 1}


class TestCountCompletedTasks:
    def test_no_tasks(self, results_dir: Path) -> None:
        assert count_completed_tasks(results_dir) == 0

    def test_counts_correctly(self, results_dir: Path) -> None:
        metadata_dir = results_dir / "metadata"
        metadata_dir.mkdir()
        (metadata_dir / "task_a_metadata.json").touch()
        (metadata_dir / "task_b_metadata.json").touch()
        (metadata_dir / "task_c_metadata.json").touch()
        assert count_completed_tasks(results_dir) == 3
