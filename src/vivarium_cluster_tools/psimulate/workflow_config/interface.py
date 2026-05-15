"""
=========================
Workflow Config Interface
=========================

Python API for building workflow step configurations programmatically,
as an alternative to authoring a YAML workflow file. Each function
returns an instance of the corresponding step config class.

"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from vivarium_cluster_tools.psimulate.jobmon_config.workflow import resolve_env_prefix
from vivarium_cluster_tools.psimulate.workflow_config.config import (
    DEFAULT_BACKUP_FREQ_SECONDS,
    BaseStepConfig,
    CommandStepConfig,
    NotebookStepConfig,
    PytestStepConfig,
    PythonStepConfig,
    ResourceConfig,
    SimulationStepConfig,
)


def get_command_step(
    *,
    name: str,
    resources: ResourceConfig,
    command: str,
    output_directory: Path,
    environment: str | None = None,
) -> CommandStepConfig:
    """Build a command-based workflow step that runs an arbitrary shell command.

    Parameters
    ----------
    name
        Unique name for this step within the workflow.
    resources
        Compute resources for this step.
    command
        Shell command string to execute.
    output_directory
        Directory for this step's worker logs and step-level outputs.
    environment
        Optional conda environment name to use for this step. If omitted,
        falls back to the workflow-level default at build time.

    Returns
    -------
        A configured :class:`~vivarium_cluster_tools.psimulate.workflow_config.config.CommandStepConfig`.
    """
    return CommandStepConfig(
        name=name,
        resources=resources,
        command=command,
        output_directory=output_directory,
        environment=environment,
    )


def get_simulation_step(
    *,
    name: str,
    resources: ResourceConfig,
    output_directory: Path,
    model_specification: Path,
    branch_configuration: Path,
    environment: str | None = None,
    artifact_path: Path | None = None,
    backup_freq: float | None = DEFAULT_BACKUP_FREQ_SECONDS,
    sim_verbosity: int = 0,
) -> SimulationStepConfig:
    """Build a parallel-simulation workflow step.

    Produces one Jobmon task per ``(input_draw, random_seed, branch)``
    combination defined by the branch configuration. Uses the same task
    runner infrastructure as ``psimulate run``.

    Parameters
    ----------
    name
        Unique name for this step within the workflow.
    resources
        Compute resources for *each individual simulation task*
        (memory, runtime, cores).
    output_directory
        Directory for this step's outputs. The simulation step lays out
        ``model_name / timestamp / ...`` subdirectories beneath this.
    model_specification
        Path to the model specification YAML file. Both relative and
        absolute paths are accepted.
    branch_configuration
        Path to the branch configuration YAML file. Both relative and
        absolute paths are accepted.
    environment
        Optional conda environment name to use for this step.
    artifact_path
        Optional path to a data artifact file. Both relative and absolute
        paths are accepted.
    backup_freq
        Backup frequency in seconds, or ``None`` to disable backups.
        Defaults to 30 minutes.
    sim_verbosity
        Vivarium simulation logging verbosity level. Default is 0.

    Returns
    -------
        A configured :class:`~vivarium_cluster_tools.psimulate.workflow_config.config.SimulationStepConfig`.
    """
    return SimulationStepConfig(
        name=name,
        resources=resources,
        output_directory=output_directory,
        model_specification=model_specification,
        branch_configuration=branch_configuration,
        environment=environment,
        artifact_path=artifact_path,
        backup_freq=backup_freq,
        sim_verbosity=sim_verbosity,
    )


def get_pytest_step(
    *,
    name: str,
    resources: ResourceConfig,
    output_directory: Path,
    environment: str | None = None,
    path: str | list[str] | None = None,
    k: str | None = None,
    runslow: bool = False,
) -> PytestStepConfig:
    """Build a pytest-based workflow step.

    At least one of ``path`` or ``k`` must be provided. When the step's
    ``resources.cores`` is greater than 1, the command is run with
    ``--numprocesses <cores>`` (pytest-xdist).

    Parameters
    ----------
    name
        Unique name for this step within the workflow.
    resources
        Compute resources for this step.
    output_directory
        Directory for this step's worker logs and step-level outputs.
    environment
        Optional conda environment name to use for this step.
    path
        Test path(s) — a single file/directory or a list of them — passed
        to pytest as positional arguments. Both relative and absolute
        paths are accepted.
    k
        Pytest ``-k`` expression used to filter tests by name.
    runslow
        If ``True``, pass ``--runslow`` to pytest. Default is ``False``.

    Returns
    -------
        A configured :class:`~vivarium_cluster_tools.psimulate.workflow_config.config.PytestStepConfig`.
    """
    return PytestStepConfig(
        name=name,
        resources=resources,
        output_directory=output_directory,
        environment=environment,
        path=path,
        k=k,
        runslow=runslow,
    )


def get_python_step(
    *,
    name: str,
    resources: ResourceConfig,
    output_directory: Path,
    path: str,
    environment: str | None = None,
    positional_args: list[Any] | None = None,
    keyword_args: dict[str, Any] | None = None,
) -> PythonStepConfig:
    """Build a Python-script workflow step.

    Constructs a ``python <path> [positional_args...] [--key value...]``
    command. Positional arguments are appended in list order; keyword
    arguments are emitted sorted by key. Keyword values map to CLI flags
    as follows:

    - ``True`` or ``None`` -> bare ``--key`` flag
    - ``False`` -> omitted from the command
    - any other scalar -> ``--key value``

    Parameters
    ----------
    name
        Unique name for this step within the workflow.
    resources
        Compute resources for this step.
    output_directory
        Directory for this step's worker logs and step-level outputs.
    path
        Path to the Python script (must end with ``.py``). Both relative
        and absolute paths are accepted.
    environment
        Optional conda environment name to use for this step.
    positional_args
        Optional list of scalar values appended in order as positional
        CLI arguments.
    keyword_args
        Optional dict mapping identifier-style keys to scalar values,
        rendered as ``--key value`` flags (see flag rules above).

    Returns
    -------
        A configured :class:`~vivarium_cluster_tools.psimulate.workflow_config.config.PythonStepConfig`.
    """
    args: dict[str, Any] = {"path": path}
    if positional_args is not None:
        args["positional_args"] = positional_args
    if keyword_args is not None:
        args["keyword_args"] = keyword_args
    return PythonStepConfig(
        name=name,
        resources=resources,
        output_directory=output_directory,
        environment=environment,
        args=args,
    )


def get_notebook_step(
    *,
    name: str,
    resources: ResourceConfig,
    output_directory: Path,
    path: Path,
    output_path: Path,
    environment: str | None = None,
    parameters: dict[str, Any] | None = None,
    cwd: Path | None = None,
) -> NotebookStepConfig:
    """Build a notebook-based workflow step (executed via papermill).

    Parameter values map to papermill flags as follows:

    - ``str`` / ``int`` / ``float`` -> ``-p key value``
    - ``bool`` / ``None`` -> ``-y key {true,false,null}`` (YAML-typed)

    Parameter keys must be valid Python identifiers because papermill
    injects them as variable assignments in a notebook cell.

    Parameters
    ----------
    name
        Unique name for this step within the workflow.
    resources
        Compute resources for this step.
    output_directory
        Directory for this step's worker logs and step-level outputs.
    path
        Path to the input notebook (must end with ``.ipynb``). Both
        relative and absolute paths are accepted.
    output_path
        Path where the executed notebook will be written (must end with
        ``.ipynb``). Both relative and absolute paths are accepted.
    environment
        Optional conda environment name to use for this step.
    parameters
        Optional dict of scalar values injected as notebook parameters.
    cwd
        Optional working directory for notebook execution. If not
        provided, defaults to the parent directory of ``path``.

    Returns
    -------
        A configured :class:`~vivarium_cluster_tools.psimulate.workflow_config.config.NotebookStepConfig`.
    """
    return NotebookStepConfig(
        name=name,
        resources=resources,
        output_directory=output_directory,
        path=path,
        output_path=output_path,
        environment=environment,
        parameters=parameters if parameters is not None else {},
        cwd=cwd,
    )


def resolve_step_env_prefix(
    step: BaseStepConfig,
    *,
    default_environment: str | None = None,
) -> str:
    """Resolve a step's conda environment to an absolute filesystem prefix.

    Applies the standard precedence: ``step.environment`` →
    ``default_environment`` → the runner's active ``CONDA_DEFAULT_ENV``.
    The resolved env name must be a non-``"base"`` conda environment.

    Parameters
    ----------
    step
        The step config whose environment to resolve.
    default_environment
        Workflow-level fallback used when ``step.environment`` is unset.

    Returns
    -------
        The absolute filesystem prefix of the resolved conda environment,
        suitable for passing as ``env_prefix`` to Jobmon task builders.

    Raises
    ------
    ValueError
        If no non-base environment can be resolved.
    RuntimeError
        If the resolved env name has no matching filesystem prefix.
    """
    env = step.environment or default_environment or os.environ.get("CONDA_DEFAULT_ENV")
    if not env or env == "base":
        raise ValueError(
            f"Step '{step.name}': a non-base conda environment is required. "
            "Set 'environment' on the step, 'default_environment' on the workflow, "
            "or activate a conda environment before running."
        )
    return resolve_env_prefix(env)
