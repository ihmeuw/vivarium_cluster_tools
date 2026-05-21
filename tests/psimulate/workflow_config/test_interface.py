"""Tests for the workflow_config Python API in ``interface.py``."""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any, Callable
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from vivarium_cluster_tools.psimulate.workflow_config.builder import STEP_TYPE_API_FNS
from vivarium_cluster_tools.psimulate.workflow_config.config import ResourceConfig
from vivarium_cluster_tools.psimulate.workflow_config.interface import (
    get_command_step_tasks,
    get_notebook_step_tasks,
    get_pytest_step_tasks,
    get_python_step_tasks,
    get_simulation_step_tasks,
)
from vivarium_cluster_tools.psimulate.workflow_config.parsing import STEP_TYPE_YAML_PARSERS
from vivarium_cluster_tools.psimulate.workflow_config.utilities import (
    BUILD_TIMESTAMP_FILENAME,
    get_or_create_build_timestamp,
    resolve_step_env_prefix,
)
from vivarium_cluster_tools.psimulate.workflow_config.validation import (
    validate_command_step,
    validate_notebook_step,
    validate_pytest_step,
    validate_python_step,
    validate_simulation_step,
)


def _resources() -> ResourceConfig:
    return ResourceConfig(memory_gb=4, project="proj_simscience", queue="all.q")


_TOOL: Any = "tool-sentinel"
_BUILD_TIMESTAMP = "2026_05_18_10_00_00"
_TASKS = ["task-sentinel"]


_BUILDER_PATHS: dict[str, str] = {
    "command": "build_command_step_tasks",
    "simulation": "build_simulation_step_tasks",
    "pytest": "build_pytest_step_tasks",
    "python": "build_python_step_tasks",
    "notebook": "build_notebook_step_tasks",
}


@pytest.fixture()
def patch_task_builders(mocker: MockerFixture) -> dict[str, MagicMock]:
    """Patch each ``build_<type>_step_tasks`` at its interface.py import site.

    Returns a mapping from step_type to the patched builder mock so
    individual tests can assert on the call.
    """
    return {
        step_type: mocker.patch(
            f"vivarium_cluster_tools.psimulate.workflow_config.interface.{builder_name}",
            return_value=_TASKS,
        )
        for step_type, builder_name in _BUILDER_PATHS.items()
    }


@pytest.fixture()
def patch_resolve_env_prefix(mocker: MockerFixture) -> MagicMock:
    """Stub the conda lookup so API tests can assert on the resolved prefix
    without invoking ``conda env list``."""
    return mocker.patch(
        "vivarium_cluster_tools.psimulate.workflow_config.utilities.resolve_env_prefix",
        side_effect=lambda env: f"/envs/{env}",
    )


@pytest.fixture()
def patch_build_timestamp(mocker: MockerFixture) -> MagicMock:
    """Stub the build-timestamp helper so API tests don't touch the
    filesystem and can assert on the timestamp passed to the builder."""
    return mocker.patch(
        "vivarium_cluster_tools.psimulate.workflow_config.interface.get_or_create_build_timestamp",
        return_value=_BUILD_TIMESTAMP,
    )


def _builder_kwargs(mock: MagicMock) -> dict[str, Any]:
    """Return the kwargs passed to a patched builder on its single call."""
    mock.assert_called_once()
    _, kwargs = mock.call_args
    return dict(kwargs)


def test_get_command_step_returns_tasks(
    patch_task_builders: dict[str, MagicMock],
    patch_resolve_env_prefix: MagicMock,
    patch_build_timestamp: MagicMock,
) -> None:
    """API validates kwargs and forwards them to ``build_command_step_tasks``."""
    output_directory = Path("/tmp/results")
    tasks = get_command_step_tasks(
        name="cmd",
        resources=_resources(),
        command="echo hi",
        output_directory=output_directory,
        environment="my_env",
        tool=_TOOL,
    )
    assert tasks is _TASKS
    patch_build_timestamp.assert_called_once_with(output_directory)
    kwargs = _builder_kwargs(patch_task_builders["command"])
    assert kwargs["name"] == "cmd"
    assert kwargs["command"] == "echo hi"
    assert kwargs["environment"] == "my_env"
    assert kwargs["env_prefix"] == "/envs/my_env"
    assert kwargs["build_timestamp"] == _BUILD_TIMESTAMP
    assert kwargs["is_resume"] is False
    assert kwargs["tool"] is _TOOL


def test_get_simulation_step_returns_tasks(
    patch_task_builders: dict[str, MagicMock],
    patch_resolve_env_prefix: MagicMock,
    patch_build_timestamp: MagicMock,
    valid_model_spec_file: Path,
    valid_branch_config_file: Path,
    valid_artifact_file: Path,
) -> None:
    """API validates kwargs and forwards them to ``build_simulation_step_tasks``."""
    output_directory = Path("/tmp/results")
    tasks = get_simulation_step_tasks(
        name="sim",
        resources=_resources(),
        output_directory=output_directory,
        model_specification=valid_model_spec_file,
        branch_configuration=valid_branch_config_file,
        artifact_path=valid_artifact_file,
        backup_freq=600.0,
        sim_verbosity=2,
        environment="sim_env",
        tool=_TOOL,
        is_resume=True,
    )
    assert tasks is _TASKS
    patch_build_timestamp.assert_called_once_with(output_directory)
    kwargs = _builder_kwargs(patch_task_builders["simulation"])
    assert kwargs["name"] == "sim"
    assert kwargs["model_specification"] == valid_model_spec_file
    assert kwargs["branch_configuration"] == valid_branch_config_file
    assert kwargs["artifact_path"] == valid_artifact_file
    assert kwargs["backup_freq"] == 600.0
    assert kwargs["sim_verbosity"] == 2
    assert kwargs["environment"] == "sim_env"
    assert kwargs["env_prefix"] == "/envs/sim_env"
    assert kwargs["is_resume"] is True


def test_get_pytest_step_returns_tasks(
    patch_task_builders: dict[str, MagicMock],
    patch_resolve_env_prefix: MagicMock,
    patch_build_timestamp: MagicMock,
    valid_pytest_path: str,
) -> None:
    """API validates kwargs and forwards them to ``build_pytest_step_tasks``."""
    tasks = get_pytest_step_tasks(
        name="tests",
        resources=_resources(),
        output_directory=Path("/tmp/results"),
        path=valid_pytest_path,
        k="test_foo",
        runslow=True,
        environment="test_env",
        tool=_TOOL,
    )
    assert tasks is _TASKS
    kwargs = _builder_kwargs(patch_task_builders["pytest"])
    assert kwargs["path"] == valid_pytest_path
    assert kwargs["k"] == "test_foo"
    assert kwargs["runslow"] is True
    assert kwargs["env_prefix"] == "/envs/test_env"


def test_get_python_step_returns_tasks(
    patch_task_builders: dict[str, MagicMock],
    patch_resolve_env_prefix: MagicMock,
    patch_build_timestamp: MagicMock,
    valid_python_script: str,
) -> None:
    """API validates kwargs and forwards them to ``build_python_step_tasks``."""
    tasks = get_python_step_tasks(
        name="script",
        resources=_resources(),
        output_directory=Path("/tmp/results"),
        path=valid_python_script,
        positional_args=["foo", 42],
        keyword_args={"verbose": True, "out_dir": "/tmp/out"},
        environment="py_env",
        tool=_TOOL,
    )
    assert tasks is _TASKS
    kwargs = _builder_kwargs(patch_task_builders["python"])
    assert kwargs["path"] == valid_python_script
    assert kwargs["positional_args"] == ["foo", 42]
    assert kwargs["keyword_args"] == {"verbose": True, "out_dir": "/tmp/out"}
    assert kwargs["env_prefix"] == "/envs/py_env"


def test_get_notebook_step_returns_tasks(
    patch_task_builders: dict[str, MagicMock],
    patch_resolve_env_prefix: MagicMock,
    patch_build_timestamp: MagicMock,
    valid_notebook_path: Path,
) -> None:
    """API validates kwargs and forwards them to ``build_notebook_step_tasks``."""
    tasks = get_notebook_step_tasks(
        name="nb",
        resources=_resources(),
        output_directory=Path("/tmp/results"),
        path=valid_notebook_path,
        output_path=Path("/tmp/results/out.ipynb"),
        parameters={"year": 2020, "verbose": True},
        cwd=valid_notebook_path.parent,
        environment="nb_env",
        tool=_TOOL,
    )
    assert tasks is _TASKS
    kwargs = _builder_kwargs(patch_task_builders["notebook"])
    assert kwargs["path"] == valid_notebook_path
    assert kwargs["output_path"] == Path("/tmp/results/out.ipynb")
    assert kwargs["parameters"] == {"year": 2020, "verbose": True}
    assert kwargs["cwd"] == valid_notebook_path.parent
    assert kwargs["env_prefix"] == "/envs/nb_env"


def test_step_env_falls_back_to_conda_default_env(
    patch_task_builders: dict[str, MagicMock],
    patch_resolve_env_prefix: MagicMock,
    patch_build_timestamp: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``environment`` is omitted, env_prefix resolves from the runner's
    active ``CONDA_DEFAULT_ENV``."""
    monkeypatch.setenv("CONDA_DEFAULT_ENV", "runner_env")
    get_command_step_tasks(
        name="cmd",
        resources=_resources(),
        command="echo hi",
        output_directory=Path("/tmp/results"),
        tool=_TOOL,
    )
    kwargs = _builder_kwargs(patch_task_builders["command"])
    assert kwargs["env_prefix"] == "/envs/runner_env"


@pytest.mark.parametrize(
    "api_fn, extra_kwargs, expected_error",
    [
        # Empty command -> validate_command_step raises ValueError.
        (get_command_step_tasks, {"command": ""}, ValueError),
        # Nonexistent paths -> validate_simulation_step raises FileNotFoundError.
        (
            get_simulation_step_tasks,
            {
                "model_specification": Path("/nonexistent/model.yaml"),
                "branch_configuration": Path("/nonexistent/branches.yaml"),
            },
            FileNotFoundError,
        ),
        # Neither path nor k -> validate_pytest_step raises ValueError.
        (get_pytest_step_tasks, {}, ValueError),
        # Nonexistent script -> validate_python_step raises FileNotFoundError.
        (get_python_step_tasks, {"path": "/nonexistent/script.py"}, FileNotFoundError),
        # Bad notebook extension -> validate_notebook_step raises ValueError.
        (
            get_notebook_step_tasks,
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
    """Each API function calls ``validate_<type>_step(**kwargs)`` before
    dispatching, so bad kwargs raise without ever reaching the builder."""
    common: dict[str, Any] = {
        "name": "bad",
        "resources": _resources(),
        "output_directory": Path("/tmp/results"),
        "tool": _TOOL,
    }
    with pytest.raises(expected_error):
        api_fn(**{**common, **extra_kwargs})


class TestResolveStepEnvPrefix:
    """Verify the precedence and validation in ``resolve_step_env_prefix``."""

    @pytest.fixture(autouse=True)
    def patch_resolve_env_prefix(self, mocker: MockerFixture) -> None:
        """Stub out the conda lookup so tests exercise only the precedence chain."""
        mocker.patch(
            "vivarium_cluster_tools.psimulate.workflow_config.utilities.resolve_env_prefix",
            side_effect=lambda env: f"/envs/{env}",
        )

    def test_step_environment_takes_priority(self) -> None:
        assert (
            resolve_step_env_prefix(
                name="s", environment="step_env", default_environment="workflow_env"
            )
            == "/envs/step_env"
        )

    def test_default_environment_used_when_step_unset(self) -> None:
        assert (
            resolve_step_env_prefix(
                name="s", environment=None, default_environment="workflow_env"
            )
            == "/envs/workflow_env"
        )

    def test_conda_default_env_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CONDA_DEFAULT_ENV", "conda_env")
        assert resolve_step_env_prefix(name="s", environment=None) == "/envs/conda_env"

    def test_rejects_base_environment(self) -> None:
        with pytest.raises(ValueError, match="non-base conda environment is required"):
            resolve_step_env_prefix(name="s", environment="base")

    def test_raises_when_nothing_resolves(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CONDA_DEFAULT_ENV", raising=False)
        with pytest.raises(ValueError, match="non-base conda environment is required"):
            resolve_step_env_prefix(name="s", environment=None)


class TestGetOrCreateBuildTimestamp:
    """Verify the persisted-vs-fresh behavior of the timestamp helper."""

    def test_creates_and_persists_timestamp_on_first_call(self, tmp_path: Path) -> None:
        ts = get_or_create_build_timestamp(tmp_path)
        assert (tmp_path / BUILD_TIMESTAMP_FILENAME).read_text().strip() == ts

    def test_reuses_persisted_timestamp_on_subsequent_calls(self, tmp_path: Path) -> None:
        (tmp_path / BUILD_TIMESTAMP_FILENAME).write_text("2020_01_01_00_00_00")
        assert get_or_create_build_timestamp(tmp_path) == "2020_01_01_00_00_00"

    def test_creates_missing_output_directory(self, tmp_path: Path) -> None:
        target = tmp_path / "does" / "not" / "exist"
        ts = get_or_create_build_timestamp(target)
        assert (target / BUILD_TIMESTAMP_FILENAME).read_text().strip() == ts


def test_step_type_registries_match() -> None:
    """``STEP_TYPE_API_FNS`` and ``STEP_TYPE_YAML_PARSERS`` must have identical keysets.

    Catches keyset drift between the two registries: adding a step type to
    one without the other would otherwise produce a runtime error at
    workflow-build time.
    """
    assert STEP_TYPE_API_FNS.keys() == STEP_TYPE_YAML_PARSERS.keys(), (
        "Step-type registries disagree: "
        f"parsing.STEP_TYPE_YAML_PARSERS={sorted(STEP_TYPE_YAML_PARSERS)} vs "
        f"builder.STEP_TYPE_API_FNS={sorted(STEP_TYPE_API_FNS)}"
    )


_VALIDATORS: dict[str, Callable[..., None]] = {
    "command": validate_command_step,
    "simulation": validate_simulation_step,
    "pytest": validate_pytest_step,
    "python": validate_python_step,
    "notebook": validate_notebook_step,
}


@pytest.mark.parametrize("step_type", sorted(STEP_TYPE_API_FNS))
def test_validate_signature_matches_api_fn_signature(step_type: str) -> None:
    """Each per-type validator must take exactly the kwargs the matching API
    function accepts, minus ``tool`` and ``is_resume`` (supplied by the
    builder).

    Locks in the contract between each validator and its
    ``get_*_step_tasks`` partner so a kwarg rename / addition / removal on
    either side fails at test time instead of at workflow-build time.
    """
    validator = _VALIDATORS[step_type]
    api_fn = STEP_TYPE_API_FNS[step_type]
    validate_params = set(inspect.signature(validator).parameters)
    api_params = set(inspect.signature(api_fn).parameters)
    assert validate_params == api_params - {"tool", "is_resume"}
