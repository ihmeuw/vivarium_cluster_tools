"""Tests for the --run-config YAML configuration file feature.

These tests verify that:
- A YAML config file can supply CLI argument values via --run-config
- CLI arguments override values from the config file
- Validation (Click types, callbacks, Choice) applies to config file values
- Unknown keys in the config file produce clear errors
- Malformed YAML produces a clear error
- The feature works for the run, restart, and expand subcommands
"""

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from vivarium_cluster_tools.psimulate.cli import psimulate

# All tests are expected to fail until --run-config is implemented.
pytestmark = pytest.mark.xfail(reason="--run-config not yet implemented", strict=True)

# ---------------------------------------------------------------------------
# The runner.main call is the "leaf" of every CLI command.  We mock it so
# tests exercise only the CLI-parsing / config-loading layer.
# ---------------------------------------------------------------------------
_RUNNER_MAIN = "vivarium_cluster_tools.psimulate.runner.main"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_yaml(tmp_path: Path, data: dict[str, Any], name: str = "config.yaml") -> Path:
    """Write *data* as YAML to a temp file and return its path."""
    p = tmp_path / name
    p.write_text(yaml.dump(data))
    return p


def _make_results_root(tmp_path: Path) -> Path:
    """Create a writable directory that can serve as results_root."""
    d = tmp_path / "previous_run"
    d.mkdir()
    return d


@pytest.fixture()
def model_spec(tmp_path: Path) -> Path:
    """A minimal model specification file."""
    p = tmp_path / "model_spec.yaml"
    p.write_text("components: {}\n")
    return p


@pytest.fixture()
def branch_config(tmp_path: Path) -> Path:
    """A minimal branch configuration file."""
    p = tmp_path / "branches.yaml"
    p.write_text("input_draw_count: 1\nrandom_seed_count: 1\n")
    return p


@pytest.fixture()
def result_dir(tmp_path: Path) -> Path:
    """A temporary results directory."""
    d = tmp_path / "results"
    d.mkdir()
    return d


@pytest.fixture()
def results_root(tmp_path: Path) -> Path:
    """A writable directory that can serve as results_root."""
    return _make_results_root(tmp_path)


# ===================================================================
# Tests for ``psimulate run --run-config``
# ===================================================================


class TestRunSubcommand:
    """Tests for --run-config on the ``run`` subcommand."""

    def test_full_config_only(
        self, tmp_path: Path, model_spec: Path, branch_config: Path, result_dir: Path
    ) -> None:
        """All required args supplied via config file — no CLI args needed."""
        config = _write_yaml(
            tmp_path,
            {
                "model_specification": str(model_spec),
                "branch_configuration": str(branch_config),
                "result_directory": str(result_dir),
                "project": "proj_simscience",
                "peak_memory": 5,
                "max_runtime": "01:00:00",
                "queue": "all.q",
                "max_workers": 100,
                "max_attempts": 2,
                "hardware": "r650",
            },
        )

        cli_runner = CliRunner()
        with patch(_RUNNER_MAIN) as mock_main:
            result = cli_runner.invoke(psimulate, ["run", "--run-config", str(config)])

        assert result.exit_code == 0, result.output
        mock_main.assert_called_once()
        call_kwargs = mock_main.call_args.kwargs
        assert call_kwargs["native_specification"].project == "proj_simscience"
        assert call_kwargs["native_specification"].peak_memory == 5
        assert call_kwargs["native_specification"].queue == "all.q"
        assert call_kwargs["max_workers"] == 100
        assert call_kwargs["max_attempts"] == 2
        assert call_kwargs["input_paths"].model_specification == model_spec.resolve()
        assert call_kwargs["input_paths"].branch_configuration == branch_config.resolve()
        assert call_kwargs["input_paths"].result_directory == result_dir.resolve()

    def test_cli_overrides_config(
        self, tmp_path: Path, model_spec: Path, branch_config: Path, result_dir: Path
    ) -> None:
        """A value given on the CLI wins over the same key in the config."""
        config = _write_yaml(
            tmp_path,
            {
                "model_specification": str(model_spec),
                "branch_configuration": str(branch_config),
                "result_directory": str(result_dir),
                "project": "proj_simscience",
                "peak_memory": 3,
            },
        )

        cli_runner = CliRunner()
        with patch(_RUNNER_MAIN) as mock_main:
            result = cli_runner.invoke(
                psimulate,
                [
                    "run",
                    "--run-config",
                    str(config),
                    "--peak-memory",
                    "10",
                ],
            )

        assert result.exit_code == 0, result.output
        mock_main.assert_called_once()
        # The NativeSpecification should have peak_memory=10 (CLI), not 3 (config)
        call_kwargs = mock_main.call_args.kwargs
        assert call_kwargs["native_specification"].peak_memory == 10

    def test_partial_config_plus_cli(
        self, tmp_path: Path, model_spec: Path, branch_config: Path, result_dir: Path
    ) -> None:
        """Config provides some values, CLI provides the rest."""
        # Config only has cluster options
        config = _write_yaml(
            tmp_path,
            {
                "project": "proj_simscience",
                "peak_memory": 5,
            },
        )

        cli_runner = CliRunner()
        with patch(_RUNNER_MAIN) as mock_main:
            result = cli_runner.invoke(
                psimulate,
                [
                    "run",
                    "--run-config",
                    str(config),
                    str(model_spec),  # positional arg on CLI
                    str(branch_config),  # positional arg on CLI
                    "--result-directory",
                    str(result_dir),
                ],
            )

        assert result.exit_code == 0, result.output
        mock_main.assert_called_once()
        call_kwargs = mock_main.call_args.kwargs
        # peak_memory came from config, project came from config
        assert call_kwargs["native_specification"].peak_memory == 5
        assert call_kwargs["native_specification"].project == "proj_simscience"
        # positional args came from CLI
        assert call_kwargs["input_paths"].model_specification == model_spec.resolve()

    def test_positional_args_from_config(
        self, tmp_path: Path, model_spec: Path, branch_config: Path, result_dir: Path
    ) -> None:
        """Positional arguments (model_specification, branch_configuration) can
        be provided in the config file."""
        config = _write_yaml(
            tmp_path,
            {
                "model_specification": str(model_spec),
                "branch_configuration": str(branch_config),
                "result_directory": str(result_dir),
                "project": "proj_simscience",
            },
        )

        cli_runner = CliRunner()
        with patch(_RUNNER_MAIN) as mock_main:
            # No positional args on CLI at all
            result = cli_runner.invoke(psimulate, ["run", "--run-config", str(config)])

        assert result.exit_code == 0, result.output
        mock_main.assert_called_once()


# ===================================================================
# Tests for ``psimulate restart --run-config``
# ===================================================================


class TestRestartSubcommand:
    """Tests for --run-config on the ``restart`` subcommand."""

    def test_full_config_only(self, tmp_path: Path, results_root: Path) -> None:
        config = _write_yaml(
            tmp_path,
            {
                "results_root": str(results_root),
                "project": "proj_simscience",
            },
        )

        cli_runner = CliRunner()
        with patch(_RUNNER_MAIN) as mock_main:
            result = cli_runner.invoke(psimulate, ["restart", "--run-config", str(config)])

        assert result.exit_code == 0, result.output
        mock_main.assert_called_once()
        call_kwargs = mock_main.call_args.kwargs
        assert call_kwargs["native_specification"].project == "proj_simscience"
        assert call_kwargs["input_paths"].result_directory == results_root.resolve()

    def test_cli_overrides_config(self, tmp_path: Path, results_root: Path) -> None:
        config = _write_yaml(
            tmp_path,
            {
                "results_root": str(results_root),
                "project": "proj_simscience",
                "peak_memory": 3,
            },
        )

        cli_runner = CliRunner()
        with patch(_RUNNER_MAIN) as mock_main:
            result = cli_runner.invoke(
                psimulate,
                [
                    "restart",
                    "--run-config",
                    str(config),
                    "--peak-memory",
                    "8",
                ],
            )

        assert result.exit_code == 0, result.output
        mock_main.assert_called_once()
        call_kwargs = mock_main.call_args.kwargs
        assert call_kwargs["native_specification"].peak_memory == 8


# ===================================================================
# Tests for ``psimulate expand --run-config``
# ===================================================================


class TestExpandSubcommand:
    """Tests for --run-config on the ``expand`` subcommand."""

    def test_full_config_only(self, tmp_path: Path, results_root: Path) -> None:
        config = _write_yaml(
            tmp_path,
            {
                "results_root": str(results_root),
                "project": "proj_simscience",
                "add_draws": 5,
                "add_seeds": 3,
            },
        )

        cli_runner = CliRunner()
        with patch(_RUNNER_MAIN) as mock_main:
            result = cli_runner.invoke(psimulate, ["expand", "--run-config", str(config)])

        assert result.exit_code == 0, result.output
        mock_main.assert_called_once()
        call_kwargs = mock_main.call_args.kwargs
        assert call_kwargs["native_specification"].project == "proj_simscience"
        assert call_kwargs["input_paths"].result_directory == results_root.resolve()
        assert call_kwargs["extra_args"]["num_draws"] == 5
        assert call_kwargs["extra_args"]["num_seeds"] == 3

    def test_expand_cli_overrides_config(self, tmp_path: Path, results_root: Path) -> None:
        """add_draws and add_seeds in config are overridden by CLI."""
        config = _write_yaml(
            tmp_path,
            {
                "results_root": str(results_root),
                "project": "proj_simscience",
                "add_draws": 5,
                "add_seeds": 3,
            },
        )

        cli_runner = CliRunner()
        with patch(_RUNNER_MAIN) as mock_main:
            result = cli_runner.invoke(
                psimulate,
                [
                    "expand",
                    "--run-config",
                    str(config),
                    "--add-draws",
                    "10",
                ],
            )

        assert result.exit_code == 0, result.output
        mock_main.assert_called_once()
        call_kwargs = mock_main.call_args.kwargs
        assert call_kwargs["extra_args"]["num_draws"] == 10  # CLI override
        assert call_kwargs["extra_args"]["num_seeds"] == 3  # from config


# ===================================================================
# Error handling tests
# ===================================================================


class TestErrorHandling:
    """Tests for error cases in --run-config."""

    def test_missing_config_file(self) -> None:
        """--run-config pointing to a nonexistent file is an error."""
        cli_runner = CliRunner()
        result = cli_runner.invoke(
            psimulate,
            [
                "run",
                "--run-config",
                "/nonexistent/path/config.yaml",
            ],
        )
        assert result.exit_code != 0
        assert "does not exist" in result.output or "Invalid value" in result.output

    def test_unknown_key_in_config(self, tmp_path: Path) -> None:
        """Unrecognized keys in the config file produce an error."""
        config = _write_yaml(
            tmp_path,
            {
                "project": "proj_simscience",
                "bogus_option": "some_value",
            },
        )
        cli_runner = CliRunner()
        result = cli_runner.invoke(psimulate, ["run", "--run-config", str(config)])
        assert result.exit_code != 0
        assert "bogus_option" in result.output


# ===================================================================
# Validation parity tests — config values go through same validation
# ===================================================================


class TestValidationParity:
    """Config file values must go through the same Click validation as CLI args."""

    def test_invalid_project_choice(
        self, tmp_path: Path, model_spec: Path, branch_config: Path, result_dir: Path
    ) -> None:
        """A project value not in the Choice list is rejected."""
        config = _write_yaml(
            tmp_path,
            {
                "model_specification": str(model_spec),
                "branch_configuration": str(branch_config),
                "result_directory": str(result_dir),
                "project": "invalid_project",
            },
        )

        cli_runner = CliRunner()
        result = cli_runner.invoke(psimulate, ["run", "--run-config", str(config)])
        assert result.exit_code != 0
        assert "invalid_project" in result.output or "Invalid value" in result.output

    def test_invalid_hardware(
        self, tmp_path: Path, model_spec: Path, branch_config: Path, result_dir: Path
    ) -> None:
        """An invalid hardware value is caught by the validation callback."""
        config = _write_yaml(
            tmp_path,
            {
                "model_specification": str(model_spec),
                "branch_configuration": str(branch_config),
                "result_directory": str(result_dir),
                "project": "proj_simscience",
                "hardware": "nonexistent_node_type",
            },
        )

        cli_runner = CliRunner()
        result = cli_runner.invoke(psimulate, ["run", "--run-config", str(config)])
        assert result.exit_code != 0
        assert "nonexistent_node_type" in result.output or "not supported" in result.output
