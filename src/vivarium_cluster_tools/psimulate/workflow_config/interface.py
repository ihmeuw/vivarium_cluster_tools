"""
=========================
Workflow Config Interface
=========================

Python API for building workflow step tasks programmatically, as an
alternative to authoring a YAML workflow file. Each function validates its
kwargs and dispatches to the matching task builder.

"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from vivarium_cluster_tools.psimulate.workflow_config.config import (
    DEFAULT_BACKUP_FREQ_SECONDS,
    ResourceConfig,
)
from vivarium_cluster_tools.psimulate.workflow_config.task_builders import (
    build_command_step_tasks,
    build_notebook_step_tasks,
    build_pytest_step_tasks,
    build_python_step_tasks,
    build_simulation_step_tasks,
)
from vivarium_cluster_tools.psimulate.workflow_config.utilities import (
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

if TYPE_CHECKING:
    from jobmon.client.api import Tool
    from jobmon.client.task import Task


def get_command_step_tasks(
    *,
    name: str,
    resources: ResourceConfig,
    command: str,
    output_directory: Path,
    tool: Tool,
    environment: str | None = None,
    is_resume: bool = False,
) -> list[Task]:
    """Build a command-based workflow step and return its Jobmon tasks.

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
    tool
        Jobmon Tool used to register task templates and create tasks.
    environment
        Optional conda environment name to use for this step. If unset,
        falls back to the runner's active ``CONDA_DEFAULT_ENV``.
    is_resume
        Whether this is a resumed workflow build.

    Returns
    -------
        The Jobmon tasks produced by the step.
    """
    validate_command_step(
        name=name,
        resources=resources,
        command=command,
        output_directory=output_directory,
        environment=environment,
    )
    return build_command_step_tasks(
        name=name,
        resources=resources,
        command=command,
        output_directory=output_directory,
        environment=environment,
        tool=tool,
        env_prefix=resolve_step_env_prefix(name=name, environment=environment),
        build_timestamp=get_or_create_build_timestamp(output_directory),
        is_resume=is_resume,
    )


def get_simulation_step_tasks(
    *,
    name: str,
    resources: ResourceConfig,
    output_directory: Path,
    model_specification: Path,
    branch_configuration: Path,
    tool: Tool,
    environment: str | None = None,
    artifact_path: Path | None = None,
    backup_freq: float | None = DEFAULT_BACKUP_FREQ_SECONDS,
    sim_verbosity: int = 0,
    is_resume: bool = False,
) -> list[Task]:
    """Build a parallel-simulation workflow step and return its Jobmon tasks.

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
    tool
        Jobmon Tool used to register task templates and create tasks.
    environment
        Optional conda environment name to use for this step. If unset,
        falls back to the runner's active ``CONDA_DEFAULT_ENV``.
    artifact_path
        Optional path to a data artifact file. Both relative and absolute
        paths are accepted.
    backup_freq
        Backup frequency in seconds, or ``None`` to disable backups.
        Defaults to 30 minutes.
    sim_verbosity
        Vivarium simulation logging verbosity level. Default is 0.
    is_resume
        Whether this is a resumed workflow build.

    Returns
    -------
        The Jobmon tasks produced by the step.
    """
    validate_simulation_step(
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
    return build_simulation_step_tasks(
        name=name,
        resources=resources,
        output_directory=output_directory,
        model_specification=model_specification,
        branch_configuration=branch_configuration,
        environment=environment,
        artifact_path=artifact_path,
        backup_freq=backup_freq,
        sim_verbosity=sim_verbosity,
        tool=tool,
        env_prefix=resolve_step_env_prefix(name=name, environment=environment),
        build_timestamp=get_or_create_build_timestamp(output_directory),
        is_resume=is_resume,
    )


def get_pytest_step_tasks(
    *,
    name: str,
    resources: ResourceConfig,
    output_directory: Path,
    tool: Tool,
    environment: str | None = None,
    path: str | list[str] | None = None,
    k: str | None = None,
    runslow: bool = False,
    is_resume: bool = False,
) -> list[Task]:
    """Build a pytest-based workflow step and return its Jobmon tasks.

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
    tool
        Jobmon Tool used to register task templates and create tasks.
    environment
        Optional conda environment name to use for this step. If unset,
        falls back to the runner's active ``CONDA_DEFAULT_ENV``.
    path
        Test path(s) — a single file/directory or a list of them — passed
        to pytest as positional arguments. Both relative and absolute
        paths are accepted.
    k
        Pytest ``-k`` expression used to filter tests by name.
    runslow
        If ``True``, pass ``--runslow`` to pytest. Default is ``False``.
    is_resume
        Whether this is a resumed workflow build.

    Returns
    -------
        The Jobmon tasks produced by the step.
    """
    validate_pytest_step(
        name=name,
        resources=resources,
        output_directory=output_directory,
        environment=environment,
        path=path,
        k=k,
        runslow=runslow,
    )
    return build_pytest_step_tasks(
        name=name,
        resources=resources,
        output_directory=output_directory,
        environment=environment,
        path=path,
        k=k,
        runslow=runslow,
        tool=tool,
        env_prefix=resolve_step_env_prefix(name=name, environment=environment),
        build_timestamp=get_or_create_build_timestamp(output_directory),
        is_resume=is_resume,
    )


def get_python_step_tasks(
    *,
    name: str,
    resources: ResourceConfig,
    output_directory: Path,
    path: str,
    tool: Tool,
    environment: str | None = None,
    positional_args: list[Any] | None = None,
    keyword_args: dict[str, Any] | None = None,
    is_resume: bool = False,
) -> list[Task]:
    """Build a Python-script workflow step and return its Jobmon tasks.

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
    tool
        Jobmon Tool used to register task templates and create tasks.
    environment
        Optional conda environment name to use for this step. If unset,
        falls back to the runner's active ``CONDA_DEFAULT_ENV``.
    positional_args
        Optional list of scalar values appended in order as positional
        CLI arguments.
    keyword_args
        Optional dict mapping identifier-style keys to scalar values,
        rendered as ``--key value`` flags (see flag rules above).
    is_resume
        Whether this is a resumed workflow build.

    Returns
    -------
        The Jobmon tasks produced by the step.
    """
    validate_python_step(
        name=name,
        resources=resources,
        output_directory=output_directory,
        path=path,
        environment=environment,
        positional_args=positional_args,
        keyword_args=keyword_args,
    )
    return build_python_step_tasks(
        name=name,
        resources=resources,
        output_directory=output_directory,
        path=path,
        environment=environment,
        positional_args=positional_args,
        keyword_args=keyword_args,
        tool=tool,
        env_prefix=resolve_step_env_prefix(name=name, environment=environment),
        build_timestamp=get_or_create_build_timestamp(output_directory),
        is_resume=is_resume,
    )


def get_notebook_step_tasks(
    *,
    name: str,
    resources: ResourceConfig,
    output_directory: Path,
    path: Path,
    output_path: Path,
    tool: Tool,
    environment: str | None = None,
    parameters: dict[str, Any] | None = None,
    cwd: Path | None = None,
    is_resume: bool = False,
) -> list[Task]:
    """Build a notebook-based workflow step and return its Jobmon tasks.

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
    tool
        Jobmon Tool used to register task templates and create tasks.
    environment
        Optional conda environment name to use for this step. If unset,
        falls back to the runner's active ``CONDA_DEFAULT_ENV``.
    parameters
        Optional dict of scalar values injected as notebook parameters.
    cwd
        Optional working directory for notebook execution. If not
        provided, defaults to the parent directory of ``path``.
    is_resume
        Whether this is a resumed workflow build.

    Returns
    -------
        The Jobmon tasks produced by the step.
    """
    validate_notebook_step(
        name=name,
        resources=resources,
        output_directory=output_directory,
        path=path,
        output_path=output_path,
        environment=environment,
        parameters=parameters,
        cwd=cwd,
    )
    return build_notebook_step_tasks(
        name=name,
        resources=resources,
        output_directory=output_directory,
        path=path,
        output_path=output_path,
        environment=environment,
        parameters=parameters,
        cwd=cwd,
        tool=tool,
        env_prefix=resolve_step_env_prefix(name=name, environment=environment),
        build_timestamp=get_or_create_build_timestamp(output_directory),
        is_resume=is_resume,
    )
