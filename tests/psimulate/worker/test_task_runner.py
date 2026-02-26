"""Tests for the task_runner module (Jobmon task CLI entry point).

These tests verify the orchestration logic in task_runner.main():
argument parsing, metadata loading, command dispatch routing, result
plumbing, and logging setup.  The actual work horses and result writing
are mocked — they have their own dedicated test suites.
"""

import json
import os
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tests.psimulate.conftest import make_job_parameters
from vivarium_cluster_tools.psimulate import COMMANDS
from vivarium_cluster_tools.psimulate.jobs import JobParameters
from vivarium_cluster_tools.psimulate.worker.task_runner import main, parse_args


_TASK_ID = "test_task_0001"

_JOB_PARAMS = make_job_parameters(input_draw=1, random_seed=42)
_JOB_PARAMS_DICT = _JOB_PARAMS.to_dict()


def _write_metadata(
    metadata_dir: Path,
    task_id: str,
    job_params_dict: dict[str, Any] | None = None,
) -> Path:
    """Write a task-metadata JSON file and return its path."""
    metadata_dir.mkdir(parents=True, exist_ok=True)
    path = metadata_dir / f"{task_id}.json"
    path.write_text(json.dumps(job_params_dict or _JOB_PARAMS_DICT))
    return path


def _build_argv(
    metadata_dir: Path,
    results_dir: Path,
    worker_log_dir: Path,
    command: str,
    task_id: str = _TASK_ID,
) -> list[str]:
    """Build a CLI argv list for ``main()``."""
    return [
        "--metadata-dir",
        str(metadata_dir),
        "--task-id",
        task_id,
        "--results-dir",
        str(results_dir),
        "--worker-log-dir",
        str(worker_log_dir),
        "--command",
        command,
    ]


@pytest.fixture()
def dirs(tmp_path: Path) -> dict[str, Path]:
    """Create and return the three directories used by task_runner."""
    d = {
        "metadata": tmp_path / "metadata",
        "results": tmp_path / "results",
        "worker_logs": tmp_path / "worker_logs",
    }
    for p in d.values():
        p.mkdir()
    return d


class TestParseArgs:
    def test_valid_args(self, tmp_path: Path) -> None:
        argv = [
            "--metadata-dir",
            str(tmp_path / "meta"),
            "--task-id",
            "abc123",
            "--results-dir",
            str(tmp_path / "res"),
            "--worker-log-dir",
            str(tmp_path / "logs"),
            "--command",
            "run",
        ]
        ns = parse_args(argv)
        assert ns.metadata_dir == tmp_path / "meta"
        assert ns.task_id == "abc123"
        assert ns.results_dir == tmp_path / "res"
        assert ns.worker_log_dir == tmp_path / "logs"
        assert ns.command == "run"
        assert isinstance(ns.metadata_dir, Path)
        assert isinstance(ns.task_id, str)

    def test_missing_required_arg_raises_system_exit(self, tmp_path: Path) -> None:
        """Omitting any required argument must trigger SystemExit (argparse)."""
        with pytest.raises(SystemExit):
            parse_args(["--metadata-dir", str(tmp_path)])

    def test_unknown_arg_raises_system_exit(self, tmp_path: Path) -> None:
        argv = _build_argv(tmp_path, tmp_path, tmp_path, command="run") + ["--bogus"]
        with pytest.raises(SystemExit):
            parse_args(argv)


# ---------------------------------------------------------------------------
# main – command dispatch
# ---------------------------------------------------------------------------

# Patch targets are the names as imported into task_runner.
_WORK_HORSE = "vivarium_cluster_tools.psimulate.worker.task_runner.work_horse"
_LOAD_TEST_WORK_HORSE = (
    "vivarium_cluster_tools.psimulate.worker.task_runner.load_test_work_horse"
)
_WRITE_TASK_RESULTS = "vivarium_cluster_tools.psimulate.worker.task_runner.write_task_results"


class TestMainDispatch:
    """main() routes to the correct work horse based on the command field."""

    @pytest.mark.parametrize("command", [COMMANDS.run, COMMANDS.restart, COMMANDS.expand])
    def test_vivarium_commands_call_work_horse(
        self, dirs: dict[str, Path], command: str
    ) -> None:
        _write_metadata(dirs["metadata"], _TASK_ID)
        mock_results = {"some_metric": pd.DataFrame({"a": [1]})}

        with (
            patch(_WORK_HORSE, return_value=mock_results) as wh,
            patch(_LOAD_TEST_WORK_HORSE) as lt_wh,
            patch(_WRITE_TASK_RESULTS) as write,
        ):
            main(_build_argv(dirs["metadata"], dirs["results"], dirs["worker_logs"], command=command))

            wh.assert_called_once()
            lt_wh.assert_not_called()

            # Verify the JobParameters passed to work_horse
            args, kwargs = wh.call_args
            assert isinstance(args[0], JobParameters)
            assert args[0].input_draw == _JOB_PARAMS_DICT["input_draw"]
            assert args[0].random_seed == _JOB_PARAMS_DICT["random_seed"]
            assert kwargs["task_id"] == _TASK_ID

            # Verify write_task_results receives the work_horse return value
            write.assert_called_once_with(
                results_dir=dirs["results"],
                task_id=_TASK_ID,
                job_parameters=args[0],
                results_dict=mock_results,
            )

    def test_load_test_calls_load_test_work_horse(self, dirs: dict[str, Path]) -> None:
        _write_metadata(dirs["metadata"], _TASK_ID)
        mock_df = pd.DataFrame({"x": [1, 2, 3]})

        with (
            patch(_WORK_HORSE) as wh,
            patch(_LOAD_TEST_WORK_HORSE, return_value=mock_df) as lt_wh,
            patch(_WRITE_TASK_RESULTS) as write,
        ):
            main(_build_argv(dirs["metadata"], dirs["results"], dirs["worker_logs"], command=COMMANDS.load_test))

            lt_wh.assert_called_once()
            wh.assert_not_called()

            args, kwargs = lt_wh.call_args
            assert isinstance(args[0], JobParameters)
            assert kwargs["task_id"] == _TASK_ID

    def test_unknown_command_raises_value_error(self, dirs: dict[str, Path]) -> None:
        _write_metadata(dirs["metadata"], _TASK_ID)

        with (
            patch(_WORK_HORSE),
            patch(_LOAD_TEST_WORK_HORSE),
            patch(_WRITE_TASK_RESULTS),
        ):
            with pytest.raises(ValueError, match="bogus_command"):
                main(_build_argv(dirs["metadata"], dirs["results"], dirs["worker_logs"], command="bogus_command"))


class TestLoadTestResultWrapping:
    """load_test_work_horse returns a single DataFrame; main() must wrap it
    as ``{"load_test": df}`` before passing to write_task_results."""

    def test_load_test_result_wrapped_in_dict(self, dirs: dict[str, Path]) -> None:
        _write_metadata(dirs["metadata"], _TASK_ID)
        mock_df = pd.DataFrame({"col": [10, 20]})

        with (
            patch(_WORK_HORSE),
            patch(_LOAD_TEST_WORK_HORSE, return_value=mock_df),
            patch(_WRITE_TASK_RESULTS) as write,
        ):
            main(_build_argv(dirs["metadata"], dirs["results"], dirs["worker_logs"], command=COMMANDS.load_test))

            _, call_kwargs = write.call_args
            results_dict = call_kwargs["results_dict"]
            assert list(results_dict.keys()) == ["load_test"]
            pd.testing.assert_frame_equal(results_dict["load_test"], mock_df)


class TestMainLoggingSetup:
    def test_env_var_is_set(self, dirs: dict[str, Path]) -> None:
        """VIVARIUM_LOGGING_DIRECTORY must be set to the worker-log dir."""
        _write_metadata(dirs["metadata"], _TASK_ID)

        with (
            patch(_WORK_HORSE, return_value={}),
            patch(_WRITE_TASK_RESULTS),
        ):
            main(_build_argv(dirs["metadata"], dirs["results"], dirs["worker_logs"], command=COMMANDS.run))

        assert os.environ["VIVARIUM_LOGGING_DIRECTORY"] == str(dirs["worker_logs"])

    def test_per_task_log_file_created(self, dirs: dict[str, Path]) -> None:
        """Loguru should create a per-task log file in worker_log_dir."""
        _write_metadata(dirs["metadata"], _TASK_ID)

        with (
            patch(_WORK_HORSE, return_value={}),
            patch(_WRITE_TASK_RESULTS),
        ):
            main(_build_argv(dirs["metadata"], dirs["results"], dirs["worker_logs"], command=COMMANDS.run))

        log_file = dirs["worker_logs"] / f"{_TASK_ID}.log"
        assert log_file.exists()


class TestMainMissingMetadata:
    def test_missing_metadata_file_raises(self, dirs: dict[str, Path]) -> None:
        """If the metadata JSON does not exist, main() should raise."""
        with pytest.raises(FileNotFoundError):
            main(
                _build_argv(
                    dirs["metadata"],
                    dirs["results"],
                    dirs["worker_logs"],
                    command=COMMANDS.run,
                    task_id="nonexistent",
                )
            )
