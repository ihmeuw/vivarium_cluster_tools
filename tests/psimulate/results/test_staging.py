"""Tests for the results staging module."""

from pathlib import Path

import pandas as pd
import pytest

from vivarium_cluster_tools.psimulate.results.staging import (
    cleanup_staging_files,
    read_staging_result,
    scan_done_markers,
    write_staging_results,
)


@pytest.fixture
def staging_dir(tmp_path: Path) -> Path:
    d = tmp_path / "staging"
    d.mkdir()
    return d


class TestWriteAndReadStagingResults:
    def test_round_trip(self, staging_dir: Path) -> None:
        """Write staging results and read them back."""
        task_id = "abc123"
        metadata_df = pd.DataFrame({"input_draw": [1], "random_seed": [42]})
        results_dict = {
            "deaths": pd.DataFrame({"value": [10, 20], "year": [2020, 2021]}),
            "ylls": pd.DataFrame({"value": [100], "year": [2020]}),
        }

        write_staging_results(staging_dir, task_id, metadata_df, results_dict)

        # Check files were created
        assert (staging_dir / f"{task_id}_metadata.csv").exists()
        assert (staging_dir / f"{task_id}_deaths.parquet").exists()
        assert (staging_dir / f"{task_id}_ylls.parquet").exists()
        assert (staging_dir / f"{task_id}.done").exists()

        # Read them back
        read_metadata, read_results = read_staging_result(staging_dir, task_id)
        pd.testing.assert_frame_equal(read_metadata, metadata_df)
        assert set(read_results.keys()) == {"deaths", "ylls"}
        pd.testing.assert_frame_equal(read_results["deaths"], results_dict["deaths"])
        pd.testing.assert_frame_equal(read_results["ylls"], results_dict["ylls"])

    def test_empty_results(self, staging_dir: Path) -> None:
        """Write staging results with no metric DataFrames."""
        task_id = "empty_task"
        metadata_df = pd.DataFrame({"input_draw": [1], "random_seed": [42]})
        results_dict: dict[str, pd.DataFrame] = {}

        write_staging_results(staging_dir, task_id, metadata_df, results_dict)
        read_metadata, read_results = read_staging_result(staging_dir, task_id)
        pd.testing.assert_frame_equal(read_metadata, metadata_df)
        assert read_results == {}


class TestScanDoneMarkers:
    def test_scan_empty(self, staging_dir: Path) -> None:
        assert scan_done_markers(staging_dir, set()) == []

    def test_scan_finds_new(self, staging_dir: Path) -> None:
        (staging_dir / "task_a.done").touch()
        (staging_dir / "task_b.done").touch()
        result = scan_done_markers(staging_dir, set())
        assert set(result) == {"task_a", "task_b"}

    def test_scan_excludes_processed(self, staging_dir: Path) -> None:
        (staging_dir / "task_a.done").touch()
        (staging_dir / "task_b.done").touch()
        result = scan_done_markers(staging_dir, {"task_a"})
        assert result == ["task_b"]


class TestCleanupStagingFiles:
    def test_cleanup(self, staging_dir: Path) -> None:
        task_id = "cleanup_me"
        (staging_dir / f"{task_id}_metadata.csv").touch()
        (staging_dir / f"{task_id}_deaths.parquet").touch()
        (staging_dir / f"{task_id}.done").touch()
        # Also a file from another task
        (staging_dir / "other_task.done").touch()

        cleanup_staging_files(staging_dir, task_id)

        assert not (staging_dir / f"{task_id}_metadata.csv").exists()
        assert not (staging_dir / f"{task_id}_deaths.parquet").exists()
        assert not (staging_dir / f"{task_id}.done").exists()
        # Other task's files should be untouched
        assert (staging_dir / "other_task.done").exists()
