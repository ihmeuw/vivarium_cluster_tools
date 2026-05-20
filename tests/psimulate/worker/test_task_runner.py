"""Tests for the task_runner module (Jobmon task CLI entry point).

These tests verify the orchestration logic in task_runner.main():
argument parsing, metadata loading, command dispatch routing, result
plumbing, and logging setup.  The actual work horses and result writing
are mocked — they have their own dedicated test suites.
"""

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from loguru import logger

from tests.psimulate.conftest import make_job_parameters
from vivarium_cluster_tools.psimulate import COMMANDS
from vivarium_cluster_tools.psimulate.jobs import JobParameters
from vivarium_cluster_tools.psimulate.results.writing import write_metadata
from vivarium_cluster_tools.psimulate.worker import task_runner
from vivarium_cluster_tools.psimulate.worker.task_runner import (
    _configure_dual_sink,
    main,
    parse_args,
)

PY = sys.executable

# Patch targets are the names as imported into task_runner.
_WORK_HORSE = "vivarium_cluster_tools.psimulate.worker.task_runner.work_horse"
_LOAD_TEST_WORK_HORSE = (
    "vivarium_cluster_tools.psimulate.worker.task_runner.load_test_work_horse"
)
_WRITE_TASK_RESULTS = "vivarium_cluster_tools.psimulate.worker.task_runner.write_task_results"


@pytest.fixture(scope="module")
def job_params() -> JobParameters:
    return make_job_parameters(input_draw=1, random_seed=42)


def _build_argv(
    metadata_dir: Path,
    results_dir: Path,
    command: str,
    task_id: str,
) -> list[str]:
    """Build a CLI argv list for ``main()`` in simulation mode."""
    return [
        "simulation",
        "--metadata-dir",
        str(metadata_dir),
        "--task-id",
        task_id,
        "--results-dir",
        str(results_dir),
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
            "simulation",
            "--metadata-dir",
            str(tmp_path / "meta"),
            "--task-id",
            "abc123",
            "--results-dir",
            str(tmp_path / "res"),
            "--command",
            "run",
        ]
        ns = parse_args(argv)
        assert ns.mode == "simulation"
        assert ns.metadata_dir == tmp_path / "meta"
        assert ns.task_id == "abc123"
        assert ns.results_dir == tmp_path / "res"
        assert ns.command == "run"
        assert isinstance(ns.metadata_dir, Path)
        assert isinstance(ns.task_id, str)

    def test_missing_required_arg_raises_system_exit(self, tmp_path: Path) -> None:
        """Omitting any required argument must trigger SystemExit (argparse)."""
        with pytest.raises(SystemExit):
            parse_args(["simulation", "--metadata-dir", str(tmp_path)])

    def test_unknown_arg_raises_system_exit(self, tmp_path: Path) -> None:
        argv = _build_argv(tmp_path, tmp_path, command="run", task_id="x") + ["--bogus"]
        with pytest.raises(SystemExit):
            parse_args(argv)


class TestMainDispatch:
    """main() routes to the correct work horse based on the command field."""

    @pytest.mark.parametrize("command", [COMMANDS.run, COMMANDS.restart, COMMANDS.expand])
    def test_vivarium_commands_call_work_horse(
        self, dirs: dict[str, Path], job_params: JobParameters, command: str
    ) -> None:
        write_metadata(dirs["metadata"], job_params)
        mock_results = {"some_metric": pd.DataFrame({"a": [1]})}

        with (
            patch(_WORK_HORSE, return_value=mock_results) as work_horse,
            patch(_LOAD_TEST_WORK_HORSE) as load_test_work_horse,
            patch(_WRITE_TASK_RESULTS) as write,
        ):
            main(
                _build_argv(
                    dirs["metadata"],
                    dirs["results"],
                    command=command,
                    task_id=job_params.task_id,
                )
            )

            work_horse.assert_called_once()
            load_test_work_horse.assert_not_called()

            # Verify the JobParameters passed to work_horse
            args, kwargs = work_horse.call_args
            assert isinstance(args[0], JobParameters)
            assert args[0].input_draw == job_params.input_draw
            assert args[0].random_seed == job_params.random_seed

            # Verify write_task_results receives the work_horse return value
            write.assert_called_once_with(
                results_dir=dirs["results"],
                job_parameters=args[0],
                results_dict=mock_results,
            )

    def test_load_test_calls_load_test_work_horse(
        self, dirs: dict[str, Path], job_params: JobParameters
    ) -> None:
        write_metadata(dirs["metadata"], job_params)
        mock_df = pd.DataFrame({"x": [1, 2, 3]})

        with (
            patch(_WORK_HORSE) as work_horse,
            patch(_LOAD_TEST_WORK_HORSE, return_value=mock_df) as load_test_work_horse,
            patch(_WRITE_TASK_RESULTS) as write,
        ):
            main(
                _build_argv(
                    dirs["metadata"],
                    dirs["results"],
                    command=COMMANDS.load_test,
                    task_id=job_params.task_id,
                )
            )

            load_test_work_horse.assert_called_once()
            work_horse.assert_not_called()

            args, kwargs = load_test_work_horse.call_args
            assert isinstance(args[0], JobParameters)

    def test_unknown_command_raises_value_error(
        self, dirs: dict[str, Path], job_params: JobParameters
    ) -> None:
        write_metadata(dirs["metadata"], job_params)

        with (
            patch(_WORK_HORSE),
            patch(_LOAD_TEST_WORK_HORSE),
            patch(_WRITE_TASK_RESULTS),
        ):
            with pytest.raises(ValueError, match="bogus_command"):
                main(
                    _build_argv(
                        dirs["metadata"],
                        dirs["results"],
                        command="bogus_command",
                        task_id=job_params.task_id,
                    )
                )


class TestMainMissingMetadata:
    def test_missing_metadata_file_raises(self, dirs: dict[str, Path]) -> None:
        """If the metadata JSON does not exist, main() should raise."""
        with pytest.raises(FileNotFoundError):
            main(
                _build_argv(
                    dirs["metadata"],
                    dirs["results"],
                    command=COMMANDS.run,
                    task_id="nonexistent",
                )
            )


class TestSubprocessMode:
    """Tests for ``task_runner subprocess <argv>`` — the dual-stream wrapper
    used by typed workflow steps (pytest, python, notebook, raw command).
    All tests use real Python subprocesses (no Popen mocking)."""

    def test_success_writes_subprocess_output_to_stdout(
        self, capfd: pytest.CaptureFixture[str]
    ) -> None:
        """On exit 0, the subprocess's stdout is mirrored to our stdout."""
        main(["subprocess", PY, "-c", "print('hello')"])
        out, _ = capfd.readouterr()
        assert "hello" in out

    def test_success_does_not_replay_to_stderr(
        self, capfd: pytest.CaptureFixture[str]
    ) -> None:
        """On exit 0, captured output must NOT be replayed to stderr."""
        main(["subprocess", PY, "-c", "print('alpha'); print('beta')"])
        _, err = capfd.readouterr()
        assert "alpha" not in err
        assert "beta" not in err

    def test_failure_replays_buffered_output_to_stderr(
        self, capfd: pytest.CaptureFixture[str]
    ) -> None:
        """On non-zero exit, captured stdout is replayed to stderr so the
        Jobmon GUI (which reads the SLURM stderr file) sees the failure
        output. Also asserts the runner's own ERROR line — the only
        observable signal that ``_configure_dual_sink``'s WARNING+ sink
        is wired correctly through loguru."""
        main(["subprocess", PY, "-c", "print('boom'); import sys; sys.exit(2)"])
        out, err = capfd.readouterr()
        assert "boom" in out
        assert "boom" in err
        assert "Subprocess exited with code 2" in err
        assert "replaying output to stderr" in err

    def test_exit_code_propagates_on_success(self) -> None:
        assert main(["subprocess", PY, "-c", "pass"]) == 0

    def test_exit_code_propagates_on_failure(self) -> None:
        assert main(["subprocess", PY, "-c", "import sys; sys.exit(7)"]) == 7

    def test_inner_argv_is_executed(self, tmp_path: Path) -> None:
        """Whatever follows ``subprocess`` is what gets executed — prove it
        by having the child write to a tmp file we can read back."""
        marker = tmp_path / "ran.txt"
        main(
            [
                "subprocess",
                PY,
                "-c",
                f"open({str(marker)!r}, 'w').write('here')",
            ]
        )
        assert marker.read_text() == "here"

    def test_buffer_cap_does_not_crash_on_large_output(
        self,
        capfd: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """When output exceeds BUFFER_MAXLEN, the runner still completes and
        the tail of the output is preserved in the stderr replay."""
        monkeypatch.setattr(task_runner, "BUFFER_MAXLEN", 5)
        code = main(
            [
                "subprocess",
                PY,
                "-c",
                "import sys\nfor i in range(100): print(f'line-{i}')\nsys.exit(1)",
            ]
        )
        _, err = capfd.readouterr()
        assert code == 1
        assert "line-99" in err  # tail preserved
        assert "line-0" not in err  # head dropped (capped at 5)

    def test_missing_inner_argv_raises(self) -> None:
        """``subprocess`` mode with no argv to execute must raise."""
        with pytest.raises(ValueError, match="requires argv to execute"):
            main(["subprocess"])


class TestConfigureDualSink:
    """Tests for ``_configure_dual_sink`` — the loguru routing setup shared
    by both worker entry points. Asserts the level-routing contract
    directly rather than via the subprocess code path."""

    def test_routes_levels_to_correct_streams(
        self, capfd: pytest.CaptureFixture[str]
    ) -> None:
        """INFO+ records go to stdout; WARNING+ records also go to stderr;
        INFO records must NOT leak to stderr."""
        _configure_dual_sink()
        logger.info("info-msg")
        logger.warning("warn-msg")
        out, err = capfd.readouterr()
        assert "info-msg" in out
        assert "warn-msg" in out
        assert "warn-msg" in err
        assert "info-msg" not in err
