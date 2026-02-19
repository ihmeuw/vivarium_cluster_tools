"""
End-to-end tests for psimulate run and restart.

These tests run against a real SLURM cluster and exercise the full psimulate
pipeline: job creation, SLURM submission, worker execution, result collection,
and output file generation.

They are gated behind the ``@pytest.mark.cluster`` marker and require:
1. Execution on a SLURM cluster (automatically detected)
2. The ``--runslow`` pytest option
3. Running on the designated slow test day (default: Sunday)

A SLURM project can be specified with ``--slurm-project`` (defaults to
``proj_simscience``).

These tests serve as a pre-refactor behavioral baseline: they capture the
current system's observable behavior so that we can verify the jobmon refactor
preserves it (red/green TDD).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
import pytest
import yaml

_DATA_DIR = Path(__file__).parent / "data"
_MODEL_SPEC = _DATA_DIR / "e2e_model_spec.yaml"
_BRANCHES = _DATA_DIR / "e2e_branches.yaml"

# Total jobs produced by the branch config (2 draws x 2 seeds x 1 branch)
_EXPECTED_TOTAL_JOBS = 4

# Generous timeout for SLURM scheduling + execution (10 minutes)
_TIMEOUT = 600

RESULTS_DIR = "/mnt/team/simulation_science/priv/engineering/tests/output/"

# Don't enforce weekly run requirement during development
# pytestmark = [pytest.mark.cluster, pytest.mark.slow, pytest.mark.weekly]
pytestmark = [pytest.mark.cluster, pytest.mark.slow]


@pytest.fixture
def shared_tmp_path() -> Iterator[Path]:
    """Temporary directory on a shared filesystem visible to all cluster nodes.

    pytest's ``tmp_path`` creates directories under ``/tmp`` which is
    node-local.  Workers scheduled on other nodes cannot access those
    paths, causing every job to fail immediately.  This fixture creates
    a temporary directory under the user's home directory (which lives
    on the shared ``/ihme`` filesystem) and cleans it up after the test.
    """
    results_dir = tempfile.mkdtemp(dir=RESULTS_DIR)
    # give the dir the same permissions as the parent directory so that cluster jobs
    # can write to it
    os.chmod(results_dir, os.stat(RESULTS_DIR).st_mode)
    results_dir = Path(results_dir)
    yield results_dir

    # Try 10 times to delete the dir.
    # NOTE: There seems to be times where the directory is not removed (even after
    # the several attempts with a rest between them). Typically the dir is empty.
    for _ in range(10):
        if not results_dir.exists():
            break  # the dir has been removed
        # Take a quick nap to ensure processes are finished with the directory
        time.sleep(1)
        shutil.rmtree(results_dir)


@pytest.fixture
def slurm_project(request: Any) -> str:
    """SLURM project for cluster tests, from --slurm-project CLI option."""
    return request.config.getoption("--slurm-project")


def _run_psimulate(
    args: list[str],
    timeout: int = _TIMEOUT,
) -> subprocess.CompletedProcess[str]:
    """Run a psimulate CLI command as a subprocess."""
    cmd = ["psimulate", *args]
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _find_output_dir(result_directory: Path) -> Path:
    """Discover the timestamped output directory created by ``psimulate run``.

    ``psimulate run`` creates ``result_directory / model_name / timestamp``.
    This helper navigates that structure to find the leaf directory.
    """
    # result_directory / <model_name> / <timestamp>
    model_dirs = [d for d in result_directory.iterdir() if d.is_dir()]
    assert len(model_dirs) == 1, (
        f"Expected exactly 1 model directory in {result_directory}, "
        f"found {len(model_dirs)}: {model_dirs}"
    )
    timestamp_dirs = [d for d in model_dirs[0].iterdir() if d.is_dir()]
    assert len(timestamp_dirs) == 1, (
        f"Expected exactly 1 timestamp directory in {model_dirs[0]}, "
        f"found {len(timestamp_dirs)}: {timestamp_dirs}"
    )
    return timestamp_dirs[0]


def _read_metadata(output_dir: Path) -> pd.DataFrame:
    """Read the finished simulation metadata CSV from an output directory."""
    metadata_path = output_dir / "finished_sim_metadata.csv"
    assert metadata_path.exists(), f"Metadata file not found at {metadata_path}"
    return pd.read_csv(metadata_path)


def _run_basic_simulation(
    tmp_path: Path,
    slurm_project: str,
    extra_args: list[str] | None = None,
) -> tuple[subprocess.CompletedProcess[str], Path]:
    """Run a basic psimulate simulation and return (process_result, output_dir).

    This is a shared helper for tests that need a completed simulation run
    as a prerequisite.
    """
    result_dir = tmp_path / "results"
    result_dir.mkdir()

    args = [
        "run",
        str(_MODEL_SPEC),
        str(_BRANCHES),
        "-o",
        str(result_dir),
        "-P",
        slurm_project,
        "-r",
        "00:30:00",
        "-m",
        "1",
        "-w",
        str(_EXPECTED_TOTAL_JOBS),
    ]
    if extra_args:
        args.extend(extra_args)

    proc = _run_psimulate(args)
    if proc.returncode != 0:
        # Include stderr in assertion for debugging
        pytest.fail(
            f"psimulate run failed with exit code {proc.returncode}.\n"
            f"STDOUT:\n{proc.stdout}\n"
            f"STDERR:\n{proc.stderr}"
        )

    output_dir = _find_output_dir(result_dir)
    return proc, output_dir


class TestPsimulateRun:
    """E2E tests for ``psimulate run``."""

    def test_basic_run(self, shared_tmp_path: Path, slurm_project: str) -> None:
        """Run a minimal simulation and verify output files are created."""
        proc, output_dir = _run_basic_simulation(shared_tmp_path, slurm_project)

        # Verify metadata file
        metadata = _read_metadata(output_dir)
        assert (
            len(metadata) == _EXPECTED_TOTAL_JOBS
        ), f"Expected {_EXPECTED_TOTAL_JOBS} rows in metadata, got {len(metadata)}"

        # Verify expected columns exist
        assert "input_draw" in metadata.columns
        assert "random_seed" in metadata.columns
        assert pd.api.types.is_numeric_dtype(metadata["input_draw"])
        assert pd.api.types.is_numeric_dtype(metadata["random_seed"])

        # Verify all draw/seed combinations are unique
        draw_seed_pairs = metadata[["input_draw", "random_seed"]].drop_duplicates()
        assert len(draw_seed_pairs) == _EXPECTED_TOTAL_JOBS

        # Verify supporting output files exist
        assert (output_dir / "model_specification.yaml").exists()
        assert (output_dir / "keyspace.yaml").exists()
        assert (output_dir / "branches.yaml").exists()
        assert (output_dir / "requirements.txt").exists()

        # Verify keyspace YAML content
        keyspace = yaml.safe_load((output_dir / "keyspace.yaml").read_text())
        assert "input_draw" in keyspace
        assert "random_seed" in keyspace
        assert len(keyspace["input_draw"]) == 2  # input_draw_count: 2
        assert len(keyspace["random_seed"]) == 2  # random_seed_count: 2

        # Verify log directories were created
        log_dirs = list((output_dir / "logs").iterdir())
        assert len(log_dirs) >= 1, "Expected at least one log directory"

    def test_run_with_max_workers(self, shared_tmp_path: Path, slurm_project: str) -> None:
        """Verify that --max-workers is accepted and all jobs still complete."""
        result_dir = shared_tmp_path / "results"
        result_dir.mkdir()

        proc = _run_psimulate(
            [
                "run",
                str(_MODEL_SPEC),
                str(_BRANCHES),
                "-o",
                str(result_dir),
                "-P",
                slurm_project,
                "-r",
                "00:30:00",
                "-m",
                "1",
                "-w",
                "2",  # Limit to 2 concurrent workers for 4 jobs
            ]
        )
        assert proc.returncode == 0, (
            f"psimulate run with --max-workers 2 failed.\n" f"STDERR:\n{proc.stderr}"
        )

        output_dir = _find_output_dir(result_dir)
        metadata = _read_metadata(output_dir)
        assert len(metadata) == _EXPECTED_TOTAL_JOBS


class TestPsimulateRestart:
    """E2E tests for ``psimulate restart``."""

    def test_restart_completes_remaining(
        self, shared_tmp_path: Path, slurm_project: str
    ) -> None:
        """Delete partial outputs, restart, and verify only missing jobs re-run."""
        _, output_dir = _run_basic_simulation(shared_tmp_path, slurm_project)

        # Verify initial completion
        metadata = _read_metadata(output_dir)
        assert len(metadata) == _EXPECTED_TOTAL_JOBS

        # Simulate a partial run by removing SOME outputs.
        # Delete metadata file and half of the result files.
        metadata_path = output_dir / "finished_sim_metadata.csv"
        metadata_path.unlink()

        results_dir = output_dir / "results"
        if results_dir.exists():
            result_files = list(results_dir.iterdir())
            # Delete half of the result files to simulate partial completion
            files_to_delete = result_files[: len(result_files) // 2]
            for f in files_to_delete:
                f.unlink()

        # Restart -- should re-run only the missing jobs
        proc = _run_psimulate(
            [
                "restart",
                str(output_dir),
                "-P",
                slurm_project,
                "-r",
                "00:30:00",
                "-m",
                "1",
                "-w",
                str(_EXPECTED_TOTAL_JOBS),
            ]
        )
        assert proc.returncode == 0, f"psimulate restart failed.\nSTDERR:\n{proc.stderr}"

        # Verify all jobs completed (both preserved and re-run)
        metadata = _read_metadata(output_dir)
        assert len(metadata) == _EXPECTED_TOTAL_JOBS

        # Verify draw/seed combinations are correct
        draw_seed_pairs = metadata[["input_draw", "random_seed"]].drop_duplicates()
        assert len(draw_seed_pairs) == _EXPECTED_TOTAL_JOBS


class TestPsimulateExpand:
    """E2E tests for ``psimulate expand``."""

    def test_expand_adds_draws_and_seeds(
        self, shared_tmp_path: Path, slurm_project: str
    ) -> None:
        """Expand a completed run by adding draws and seeds, verify new jobs complete."""
        _, output_dir = _run_basic_simulation(shared_tmp_path, slurm_project)

        # Verify initial completion: 2 draws x 2 seeds = 4 jobs
        metadata = _read_metadata(output_dir)
        assert len(metadata) == _EXPECTED_TOTAL_JOBS
        initial_draws = set(metadata["input_draw"])
        initial_seeds = set(metadata["random_seed"])

        # Expand by adding 1 draw and 1 seed.
        # New jobs: (1 new draw x 2 old seeds) + (3 total draws x 1 new seed) = 2 + 3 = 5
        # Total: 4 original + 5 new = 9
        proc = _run_psimulate(
            [
                "expand",
                str(output_dir),
                "--add-draws",
                "1",
                "--add-seeds",
                "1",
                "-P",
                slurm_project,
                "-r",
                "00:30:00",
                "-m",
                "1",
                "-w",
                str(_EXPECTED_TOTAL_JOBS),
            ]
        )
        assert (
            proc.returncode == 0
        ), f"psimulate expand failed.\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"

        metadata = _read_metadata(output_dir)
        expected_total = 9  # 3 draws x 3 seeds
        assert (
            len(metadata) == expected_total
        ), f"Expected {expected_total} rows after expand, got {len(metadata)}"

        # Verify we have 3 distinct draws (original 2 + 1 new)
        expanded_draws = set(metadata["input_draw"])
        assert (
            len(expanded_draws) == 3
        ), f"Expected 3 distinct draws, got {len(expanded_draws)}: {expanded_draws}"
        new_draws = expanded_draws - initial_draws
        assert len(new_draws) == 1, f"Expected 1 new draw, got {new_draws}"

        # Verify we have 3 distinct seeds (original 2 + 1 new)
        expanded_seeds = set(metadata["random_seed"])
        assert (
            len(expanded_seeds) == 3
        ), f"Expected 3 distinct seeds, got {len(expanded_seeds)}: {expanded_seeds}"
        new_seeds = expanded_seeds - initial_seeds
        assert len(new_seeds) == 1, f"Expected 1 new seed, got {new_seeds}"

        # Every draw/seed pair should be unique
        draw_seed_pairs = metadata[["input_draw", "random_seed"]].drop_duplicates()
        assert len(draw_seed_pairs) == expected_total


class TestPsimulateLoadTest:
    """E2E tests for ``psimulate test``."""

    # Number of workers to use for the load test (keep small for speed)
    _NUM_WORKERS = 2
    # large_results_test sleeps for 30s per worker, so allow generous timeout
    _LOAD_TEST_TIMEOUT = _TIMEOUT

    @pytest.mark.xfail(reason="large_results load test currently failing")
    def test_large_results(self, shared_tmp_path: Path, slurm_project: str) -> None:
        """Run the large_results load test and verify outputs are produced."""
        result_dir = shared_tmp_path / "load_test_results"
        result_dir.mkdir()

        proc = _run_psimulate(
            [
                "test",
                "large_results",
                "-n",
                str(self._NUM_WORKERS),
                "-o",
                str(result_dir),
                "-P",
                slurm_project,
                "-w",
                str(self._NUM_WORKERS),
            ],
            timeout=self._LOAD_TEST_TIMEOUT,
        )
        assert proc.returncode == 0, (
            f"psimulate test large_results failed.\n"
            f"STDOUT:\n{proc.stdout}\n"
            f"STDERR:\n{proc.stderr}"
        )

        # Verify that the output directory was created with results
        output_dir = _find_output_dir(result_dir)
        metadata = _read_metadata(output_dir)
        assert (
            len(metadata) == self._NUM_WORKERS
        ), f"Expected {self._NUM_WORKERS} rows in metadata, got {len(metadata)}"
