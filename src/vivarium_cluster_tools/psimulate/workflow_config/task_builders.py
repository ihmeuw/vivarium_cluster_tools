"""
===========================
Workflow Step Task Builders
===========================

Per-step-type Jobmon task construction. Each ``build_<type>_step_tasks``
function takes the kwargs of the matching API function (plus runtime
context: tool, env prefix, build timestamp, resume flag) and returns the
Jobmon ``Task`` objects for that step.

"""

from __future__ import annotations

import shlex
from pathlib import Path
from typing import TYPE_CHECKING, Any

from vivarium_cluster_tools.psimulate import COMMANDS, branches
from vivarium_cluster_tools.psimulate.jobmon_config.workflow import get_task_list
from vivarium_cluster_tools.psimulate.jobs import (
    BackupConfiguration,
    build_job_parameters_from_keyspace,
)
from vivarium_cluster_tools.psimulate.paths import OutputPaths
from vivarium_cluster_tools.psimulate.workflow_config.config import (
    DEFAULT_BACKUP_FREQ_SECONDS,
    ResourceConfig,
)

if TYPE_CHECKING:
    from jobmon.client.api import Tool
    from jobmon.client.task import Task

_NOTEBOOK_DEFAULT_KERNEL = "python3"
"""Jupyter kernel used for notebook execution. Not user-configurable."""


def build_command_step_tasks(
    *,
    name: str,
    resources: ResourceConfig,
    command: str,
    output_directory: Path,
    environment: str | None = None,
    tool: Tool,
    env_prefix: str,
    build_timestamp: str,
    is_resume: bool = False,
) -> list[Task]:
    """Create a single Jobmon Task for a command-based step."""
    return [
        _create_single_command_task(
            tool,
            name=name,
            resources=resources,
            output_directory=output_directory,
            env_prefix=env_prefix,
            command=command,
        )
    ]


def build_simulation_step_tasks(
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
    tool: Tool,
    env_prefix: str,
    build_timestamp: str,
    is_resume: bool = False,
) -> list[Task]:
    """Create parallel simulation Jobmon Tasks.

    Uses :class:`~vivarium_cluster_tools.psimulate.paths.OutputPaths` to
    create the same ``model_name / timestamp`` directory layout as
    ``psimulate run``. Parses the branch configuration into a keyspace,
    builds one :class:`~vivarium_cluster_tools.psimulate.jobs.JobParameters`
    per (draw, seed, branch) combination, writes per-task metadata, and
    returns the full list of Jobmon tasks.
    """
    output_paths = OutputPaths.from_entry_point_args(
        command=COMMANDS.run,
        input_artifact_path=artifact_path,
        result_directory=output_directory,
        input_model_spec_path=model_specification,
        launch_time=build_timestamp,
        is_resume=is_resume,
    )
    output_paths.touch()

    keyspace = branches.Keyspace.from_branch_configuration(branch_configuration)

    job_parameters = build_job_parameters_from_keyspace(
        keyspace,
        model_specification_path=model_specification,
        output_root=output_paths.root,
        worker_logging_root=output_paths.worker_logging_root,
        backup_configuration=BackupConfiguration(
            backup_dir=str(output_paths.backup_dir),
            backup_freq=backup_freq,
            backup_metadata_path=str(output_paths.backup_metadata_path),
        ),
        extras={
            "sim_verbosity": sim_verbosity,
        },
    )

    return get_task_list(
        tool=tool,
        command=COMMANDS.run,
        job_parameters_list=job_parameters,
        metadata_dir=output_paths.metadata_dir,
        results_dir=output_paths.results_dir,
        worker_logging_root=output_paths.worker_logging_root,
        native_specification=resources.to_native_specification(name),
        env_prefix=env_prefix,
        template_name=f"psimulate_{name}",
    )


def build_pytest_step_tasks(
    *,
    name: str,
    resources: ResourceConfig,
    output_directory: Path,
    environment: str | None = None,
    path: str | list[str] | None = None,
    k: str | None = None,
    runslow: bool = False,
    tool: Tool,
    env_prefix: str,
    build_timestamp: str,
    is_resume: bool = False,
) -> list[Task]:
    """Create a single Jobmon Task running ``pytest`` with the given filters."""
    command = _build_pytest_command(path=path, k=k, runslow=runslow, cores=resources.cores)
    return [
        _create_single_command_task(
            tool,
            name=name,
            resources=resources,
            output_directory=output_directory,
            env_prefix=env_prefix,
            command=command,
        )
    ]


def build_python_step_tasks(
    *,
    name: str,
    resources: ResourceConfig,
    output_directory: Path,
    path: str,
    environment: str | None = None,
    positional_args: list[Any] | None = None,
    keyword_args: dict[str, Any] | None = None,
    tool: Tool,
    env_prefix: str,
    build_timestamp: str,
    is_resume: bool = False,
) -> list[Task]:
    """Create a single Jobmon Task running a Python script."""
    command = _build_python_command(
        path=path,
        positional_args=positional_args,
        keyword_args=keyword_args,
    )
    return [
        _create_single_command_task(
            tool,
            name=name,
            resources=resources,
            output_directory=output_directory,
            env_prefix=env_prefix,
            command=command,
        )
    ]


def build_notebook_step_tasks(
    *,
    name: str,
    resources: ResourceConfig,
    output_directory: Path,
    path: Path,
    output_path: Path,
    environment: str | None = None,
    parameters: dict[str, Any] | None = None,
    cwd: Path | None = None,
    tool: Tool,
    env_prefix: str,
    build_timestamp: str,
    is_resume: bool = False,
) -> list[Task]:
    """Create a single Jobmon Task running ``papermill`` on a notebook."""
    command = _build_notebook_command(
        path=path,
        output_path=output_path,
        parameters=parameters or {},
        cwd=cwd,
    )
    return [
        _create_single_command_task(
            tool,
            name=name,
            resources=resources,
            output_directory=output_directory,
            env_prefix=env_prefix,
            command=command,
        )
    ]


def _create_single_command_task(
    tool: Tool,
    *,
    name: str,
    resources: ResourceConfig,
    output_directory: Path,
    env_prefix: str,
    command: str,
) -> Task:
    """Create a single Jobmon task that runs a command in a conda env."""
    task_template = tool.get_task_template(
        template_name="workflow_command_step",
        command_template="PATH={env_prefix}/bin:$PATH {command}",
        node_args=["command", "env_prefix"],
        task_args=[],
        op_args=[],
        default_cluster_name="slurm",
    )
    compute_resources = resources.to_native_specification(name).to_jobmon_spec(
        worker_logging_root=output_directory,
    )
    return task_template.create_task(
        name=name,
        compute_resources=compute_resources,
        env_prefix=env_prefix,
        command=command,
    )


def _build_pytest_command(
    *,
    path: str | list[str] | None,
    k: str | None,
    runslow: bool,
    cores: int,
) -> str:
    parts = ["pytest"]
    if path:
        if isinstance(path, list):
            parts.extend(shlex.quote(p) for p in path)
        else:
            parts.append(shlex.quote(path))
    if k:
        parts.append(f"-k {shlex.quote(k)}")
    if runslow:
        parts.append("--runslow")
    if cores > 1:
        parts.append(f"--numprocesses {cores}")
    return " ".join(parts)


def _build_python_command(
    *,
    path: str,
    positional_args: list[Any] | None,
    keyword_args: dict[str, Any] | None,
) -> str:
    """Build the python command string from the script path and args.

    Positional arguments are appended first (in the order provided), followed
    by keyword arguments (sorted alphabetically by key).
    """
    parts = ["python", shlex.quote(path)]
    for value in positional_args or []:
        parts.append(shlex.quote(str(value)))
    for key in sorted(keyword_args or {}):
        value = (keyword_args or {})[key]
        if value is True or value is None:
            parts.append(f"--{key}")
        elif value is False:
            continue
        else:
            parts.append(f"--{key} {shlex.quote(str(value))}")
    return " ".join(parts)


def _build_notebook_command(
    *,
    path: Path,
    output_path: Path,
    parameters: dict[str, Any],
    cwd: Path | None,
) -> str:
    effective_cwd = cwd if cwd is not None else path.parent
    parts = [
        f"mkdir -p {shlex.quote(str(output_path.parent))}",
        "&&",
        "papermill",
        shlex.quote(str(path)),
        shlex.quote(str(output_path)),
        f"-k {_NOTEBOOK_DEFAULT_KERNEL}",
    ]
    for key in sorted(parameters):
        value = parameters[key]
        if isinstance(value, bool) or value is None:
            yaml_value = "true" if value is True else "false" if value is False else "null"
            parts.append(f"-y {shlex.quote(f'{key}: {yaml_value}')}")
        else:
            parts.append(f"-p {key} {shlex.quote(str(value))}")
    parts.append(f"--cwd {shlex.quote(str(effective_cwd))}")
    return " ".join(parts)
