"""Tests for the results writing module."""

from pathlib import Path

import pandas as pd
import pytest

from vivarium_cluster_tools.psimulate.results.writing import (
    collect_metadata,
    count_completed_tasks,
    write_task_results,
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
        metadata_df = pd.DataFrame({"input_draw": [1], "random_seed": [42]})
        results_dict = {
            "deaths": pd.DataFrame({"value": [10, 20], "year": [2020, 2021]}),
            "ylls": pd.DataFrame({"value": [100], "year": [2020]}),
        }

        write_task_results(results_dir, task_id, metadata_df, results_dict)

        # Check metadata written
        metadata_path = results_dir / "metadata" / f"{task_id}_metadata.csv"
        assert metadata_path.exists()
        read_metadata = pd.read_csv(metadata_path)
        pd.testing.assert_frame_equal(read_metadata, metadata_df)

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

    def test_empty_results(self, results_dir: Path) -> None:
        """Write task results with no metric DataFrames."""
        task_id = "empty_task"
        metadata_df = pd.DataFrame({"input_draw": [1], "random_seed": [42]})
        results_dict: dict[str, pd.DataFrame] = {}

        write_task_results(results_dir, task_id, metadata_df, results_dict)

        metadata_path = results_dir / "metadata" / f"{task_id}_metadata.csv"
        assert metadata_path.exists()
        # No metric directories created
        dirs = [d for d in results_dir.iterdir() if d.is_dir() and d.name != "metadata"]
        assert dirs == []

    def test_multiple_tasks_same_metric(self, results_dir: Path) -> None:
        """Multiple tasks writing to the same metric directory."""
        for i, task_id in enumerate(["task_a", "task_b", "task_c"]):
            metadata_df = pd.DataFrame({"input_draw": [i], "random_seed": [i * 10]})
            results_dict = {
                "deaths": pd.DataFrame({"value": [i * 100], "year": [2020]}),
            }
            write_task_results(results_dir, task_id, metadata_df, results_dict)

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

        pd.DataFrame({"input_draw": [0], "random_seed": [10]}).to_csv(
            metadata_dir / "task_a_metadata.csv", index=False
        )
        pd.DataFrame({"input_draw": [1], "random_seed": [20]}).to_csv(
            metadata_dir / "task_b_metadata.csv", index=False
        )

        result = collect_metadata(results_dir)
        assert len(result) == 2
        assert set(result["input_draw"].tolist()) == {0, 1}


class TestCountCompletedTasks:
    def test_no_tasks(self, results_dir: Path) -> None:
        assert count_completed_tasks(results_dir) == 0

    def test_counts_correctly(self, results_dir: Path) -> None:
        metadata_dir = results_dir / "metadata"
        metadata_dir.mkdir()
        (metadata_dir / "task_a_metadata.csv").touch()
        (metadata_dir / "task_b_metadata.csv").touch()
        (metadata_dir / "task_c_metadata.csv").touch()
        assert count_completed_tasks(results_dir) == 3
