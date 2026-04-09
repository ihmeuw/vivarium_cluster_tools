"""Tests for the deprecation of positional arguments in favor of keyword options.

The following positional arguments are deprecated:
- ``model_specification`` → ``--model-specification`` / ``-M``
- ``branch_configuration`` → ``--branch-configuration`` / ``-B``
- ``results_root`` → ``--results-root`` / ``-R``

These tests verify that:
- Keyword options work correctly
- Positional arguments still work but emit a deprecation warning
- Providing both positional and keyword for the same param raises an error
- A time-bomb test fails after October 2026 to prompt full removal
"""

import datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from vivarium_cluster_tools.psimulate.cli import psimulate

_RUNNER_MAIN = "vivarium_cluster_tools.psimulate.runner.main"

# The date after which positional argument support should be removed.
_DEPRECATION_DEADLINE = datetime.date(2026, 10, 9)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_yaml(tmp_path: Path, data: dict[str, Any], name: str = "config.yaml") -> Path:
    p = tmp_path / name
    p.write_text(yaml.dump(data))
    return p


@pytest.fixture()
def model_spec(tmp_path: Path) -> Path:
    p = tmp_path / "model_spec.yaml"
    p.write_text("components: {}\n")
    return p


@pytest.fixture()
def branch_config(tmp_path: Path) -> Path:
    p = tmp_path / "branches.yaml"
    p.write_text("input_draw_count: 1\nrandom_seed_count: 1\n")
    return p


@pytest.fixture()
def result_dir(tmp_path: Path) -> Path:
    d = tmp_path / "results"
    d.mkdir()
    return d


@pytest.fixture()
def results_root(tmp_path: Path) -> Path:
    d = tmp_path / "previous_run"
    d.mkdir()
    return d


# ===================================================================
# Keyword argument tests (the new, preferred way)
# ===================================================================


class TestKeywordArgs:
    """Using the new keyword options works without deprecation warnings."""

    def test_run_keyword_args(
        self, tmp_path: Path, model_spec: Path, branch_config: Path, result_dir: Path
    ) -> None:
        """model_specification and branch_configuration via --flags."""
        cli_runner = CliRunner()
        with patch(_RUNNER_MAIN) as mock_main:
            result = cli_runner.invoke(
                psimulate,
                [
                    "run",
                    "--model-specification",
                    str(model_spec),
                    "--branch-configuration",
                    str(branch_config),
                    "--result-directory",
                    str(result_dir),
                    "--project",
                    "proj_simscience",
                ],
            )

        assert result.exit_code == 0, result.output
        assert "deprecated" not in result.output.lower()
        mock_main.assert_called_once()
        call_kwargs = mock_main.call_args.kwargs
        assert call_kwargs["input_paths"].model_specification == model_spec.resolve()
        assert call_kwargs["input_paths"].branch_configuration == branch_config.resolve()

    def test_run_short_flags(
        self, tmp_path: Path, model_spec: Path, branch_config: Path, result_dir: Path
    ) -> None:
        """Short flags -f and -b work for model spec and branches."""
        cli_runner = CliRunner()
        with patch(_RUNNER_MAIN) as mock_main:
            result = cli_runner.invoke(
                psimulate,
                [
                    "run",
                    "-M",
                    str(model_spec),
                    "-B",
                    str(branch_config),
                    "-o",
                    str(result_dir),
                    "-P",
                    "proj_simscience",
                ],
            )

        assert result.exit_code == 0, result.output
        assert "deprecated" not in result.output.lower()
        mock_main.assert_called_once()

    def test_restart_keyword_arg(self, tmp_path: Path, results_root: Path) -> None:
        """results_root via --results-root flag."""
        cli_runner = CliRunner()
        with patch(_RUNNER_MAIN) as mock_main:
            result = cli_runner.invoke(
                psimulate,
                [
                    "restart",
                    "--results-root",
                    str(results_root),
                    "--project",
                    "proj_simscience",
                ],
            )

        assert result.exit_code == 0, result.output
        assert "deprecated" not in result.output.lower()
        mock_main.assert_called_once()
        call_kwargs = mock_main.call_args.kwargs
        assert call_kwargs["input_paths"].result_directory == results_root.resolve()

    def test_expand_keyword_arg(self, tmp_path: Path, results_root: Path) -> None:
        """results_root via --results-root flag on expand."""
        cli_runner = CliRunner()
        with patch(_RUNNER_MAIN) as mock_main:
            result = cli_runner.invoke(
                psimulate,
                [
                    "expand",
                    "--results-root",
                    str(results_root),
                    "--project",
                    "proj_simscience",
                ],
            )

        assert result.exit_code == 0, result.output
        assert "deprecated" not in result.output.lower()
        mock_main.assert_called_once()


# ===================================================================
# Positional argument deprecation warning tests
# ===================================================================


class TestPositionalArgDeprecationWarning:
    """Using the old positional arguments still works but emits a warning."""

    def test_run_positional_args_emit_warning(
        self, tmp_path: Path, model_spec: Path, branch_config: Path, result_dir: Path
    ) -> None:
        cli_runner = CliRunner()
        with pytest.warns(FutureWarning, match="deprecated"):
            with patch(_RUNNER_MAIN) as mock_main:
                result = cli_runner.invoke(
                    psimulate,
                    [
                        "run",
                        str(model_spec),
                        str(branch_config),
                        "--result-directory",
                        str(result_dir),
                        "--project",
                        "proj_simscience",
                    ],
                )

        assert result.exit_code == 0, result.output
        mock_main.assert_called_once()
        call_kwargs = mock_main.call_args.kwargs
        assert call_kwargs["input_paths"].model_specification == model_spec.resolve()
        assert call_kwargs["input_paths"].branch_configuration == branch_config.resolve()

    def test_restart_positional_arg_emits_warning(
        self, tmp_path: Path, results_root: Path
    ) -> None:
        cli_runner = CliRunner()
        with pytest.warns(FutureWarning, match="deprecated"):
            with patch(_RUNNER_MAIN) as mock_main:
                result = cli_runner.invoke(
                    psimulate,
                    [
                        "restart",
                        str(results_root),
                        "--project",
                        "proj_simscience",
                    ],
                )

        assert result.exit_code == 0, result.output
        mock_main.assert_called_once()

    def test_expand_positional_arg_emits_warning(
        self, tmp_path: Path, results_root: Path
    ) -> None:
        cli_runner = CliRunner()
        with pytest.warns(FutureWarning, match="deprecated"):
            with patch(_RUNNER_MAIN) as mock_main:
                result = cli_runner.invoke(
                    psimulate,
                    [
                        "expand",
                        str(results_root),
                        "--project",
                        "proj_simscience",
                    ],
                )

        assert result.exit_code == 0, result.output
        mock_main.assert_called_once()


# ===================================================================
# Conflict tests — cannot provide both positional and keyword
# ===================================================================


class TestPositionalKeywordConflict:
    """Providing the same file path as both a positional arg and a keyword option
    raises an error."""

    def test_run_model_spec_conflict(
        self, tmp_path: Path, model_spec: Path, branch_config: Path, result_dir: Path
    ) -> None:
        cli_runner = CliRunner()
        result = cli_runner.invoke(
            psimulate,
            [
                "run",
                str(model_spec),  # positional
                "--model-specification",
                str(model_spec),  # keyword
                "--branch-configuration",
                str(branch_config),
                "--result-directory",
                str(result_dir),
                "--project",
                "proj_simscience",
            ],
        )
        assert result.exit_code != 0
        assert (
            "model_specification" in result.output.lower()
            or "conflict" in result.output.lower()
        )

    def test_restart_results_root_conflict(self, tmp_path: Path, results_root: Path) -> None:
        cli_runner = CliRunner()
        result = cli_runner.invoke(
            psimulate,
            [
                "restart",
                str(results_root),  # positional
                "--results-root",
                str(results_root),  # keyword
                "--project",
                "proj_simscience",
            ],
        )
        assert result.exit_code != 0


# ===================================================================
# Time-bomb test — fail after deprecation deadline to prompt removal
# ===================================================================


class TestDeprecationDeadline:
    """This test fails after the deprecation deadline to remind us to remove
    positional argument support entirely."""

    def test_positional_args_deadline_not_reached(self) -> None:
        """If this test fails, it is time to remove positional argument support
        for model_specification, branch_configuration, and results_root.
        Replace them entirely with the keyword options --model-specification/-M,
        --branch-configuration/-B, and --results-root/-R."""
        today = datetime.date.today()
        assert today < _DEPRECATION_DEADLINE, (
            f"Positional argument deprecation deadline ({_DEPRECATION_DEADLINE}) has passed. "
            f"Remove positional argument support for model_specification, "
            f"branch_configuration, and results_root. Use the keyword options "
            f"--model-specification/-M, --branch-configuration/-B, and "
            f"--results-root/-R exclusively."
        )
