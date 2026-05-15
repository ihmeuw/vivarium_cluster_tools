"""Tests for the workflow_config Python API in ``interface.py``."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import pytest

from vivarium_cluster_tools.psimulate.workflow_config.config import (
    CommandStepConfig,
    NotebookStepConfig,
    PytestStepConfig,
    PythonStepConfig,
    ResourceConfig,
    SimulationStepConfig,
)
from vivarium_cluster_tools.psimulate.workflow_config.interface import (
    get_command_step,
    get_notebook_step,
    get_pytest_step,
    get_python_step,
    get_simulation_step,
)


def _resources() -> ResourceConfig:
    return ResourceConfig(memory_gb=4, project="proj_simscience", queue="all.q")


def test_get_command_step_returns_command_step_config() -> None:
    """API returns a CommandStepConfig with the supplied fields applied."""
    step = get_command_step(
        name="cmd",
        resources=_resources(),
        command="echo hi",
        output_directory=Path("/tmp/results"),
        environment="my_env",
    )
    assert isinstance(step, CommandStepConfig)
    assert step.name == "cmd"
    assert step.command == "echo hi"
    assert step.environment == "my_env"


def test_get_simulation_step_returns_simulation_step_config(
    valid_model_spec_file: Path,
    valid_branch_config_file: Path,
    valid_artifact_file: Path,
) -> None:
    """API returns a SimulationStepConfig with all supported args applied."""
    step = get_simulation_step(
        name="sim",
        resources=_resources(),
        output_directory=Path("/tmp/results"),
        model_specification=valid_model_spec_file,
        branch_configuration=valid_branch_config_file,
        artifact_path=valid_artifact_file,
        backup_freq=600.0,
        sim_verbosity=2,
        environment="sim_env",
    )
    assert isinstance(step, SimulationStepConfig)
    assert step.model_specification == valid_model_spec_file
    assert step.branch_configuration == valid_branch_config_file
    assert step.artifact_path == valid_artifact_file
    assert step.backup_freq == 600.0
    assert step.sim_verbosity == 2
    assert step.environment == "sim_env"


def test_get_pytest_step_returns_pytest_step_config(valid_pytest_path: str) -> None:
    """API returns a PytestStepConfig with all supported args applied."""
    step = get_pytest_step(
        name="tests",
        resources=_resources(),
        output_directory=Path("/tmp/results"),
        path=valid_pytest_path,
        k="test_foo",
        runslow=True,
    )
    assert isinstance(step, PytestStepConfig)
    assert step.path == valid_pytest_path
    assert step.k == "test_foo"
    assert step.runslow is True


def test_get_python_step_returns_python_step_config(valid_python_script: str) -> None:
    """API returns a PythonStepConfig; path/positional_args/keyword_args are
    bundled into the dataclass's ``args`` dict."""
    step = get_python_step(
        name="script",
        resources=_resources(),
        output_directory=Path("/tmp/results"),
        path=valid_python_script,
        positional_args=["foo", 42],
        keyword_args={"verbose": True, "out_dir": "/tmp/out"},
    )
    assert isinstance(step, PythonStepConfig)
    assert step.args["path"] == valid_python_script
    assert step.args["positional_args"] == ["foo", 42]
    assert step.args["keyword_args"] == {"verbose": True, "out_dir": "/tmp/out"}


def test_get_notebook_step_returns_notebook_step_config(valid_notebook_path: Path) -> None:
    """API returns a NotebookStepConfig with all supported args applied."""
    step = get_notebook_step(
        name="nb",
        resources=_resources(),
        output_directory=Path("/tmp/results"),
        path=valid_notebook_path,
        output_path=Path("/tmp/results/out.ipynb"),
        parameters={"year": 2020, "verbose": True},
        cwd=valid_notebook_path.parent,
    )
    assert isinstance(step, NotebookStepConfig)
    assert step.path == valid_notebook_path
    assert step.output_path == Path("/tmp/results/out.ipynb")
    assert step.parameters == {"year": 2020, "verbose": True}
    assert step.cwd == valid_notebook_path.parent


@pytest.mark.parametrize(
    "api_fn, extra_kwargs, expected_error",
    [
        # Empty command -> CommandStepConfig._validate raises ValueError.
        (get_command_step, {"command": ""}, ValueError),
        # Nonexistent paths -> SimulationStepConfig._validate_required_paths
        # raises FileNotFoundError.
        (
            get_simulation_step,
            {
                "model_specification": Path("/nonexistent/model.yaml"),
                "branch_configuration": Path("/nonexistent/branches.yaml"),
            },
            FileNotFoundError,
        ),
        # Neither path nor k -> PytestStepConfig._validate raises ValueError.
        (get_pytest_step, {}, ValueError),
        # Nonexistent script -> PythonStepConfig._validate_required_paths
        # raises FileNotFoundError.
        (get_python_step, {"path": "/nonexistent/script.py"}, FileNotFoundError),
        # Bad notebook extension -> NotebookStepConfig._validate raises ValueError.
        (
            get_notebook_step,
            {
                "path": Path("/tmp/not_a_notebook.txt"),
                "output_path": Path("/tmp/out.ipynb"),
            },
            ValueError,
        ),
    ],
    ids=["command", "simulation", "pytest", "python", "notebook"],
)
def test_validation_propagates_through_api(
    api_fn: Callable[..., Any],
    extra_kwargs: dict[str, Any],
    expected_error: type[Exception],
) -> None:
    """Dataclass __post_init__ validation must fire when constructing via the API."""
    common: dict[str, Any] = {
        "name": "bad",
        "resources": _resources(),
        "output_directory": Path("/tmp/results"),
    }
    with pytest.raises(expected_error):
        api_fn(**{**common, **extra_kwargs})
