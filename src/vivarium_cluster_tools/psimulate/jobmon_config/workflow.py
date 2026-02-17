"""
========================
Jobmon Workflow Builder
========================

Build and configure Jobmon workflows for psimulate runs.

"""

import json
import shutil
import sys
from pathlib import Path
from typing import Any

from loguru import logger

from vivarium_cluster_tools.psimulate.cluster.interface import NativeSpecification
from vivarium_cluster_tools.psimulate.jobs import generate_task_id
from vivarium_cluster_tools.psimulate.paths import OutputPaths


def _write_job_spec(
    job_spec_dir: Path,
    task_id: str,
    command: str,
    job_parameters: dict[str, Any],
) -> None:
    """Write a job spec JSON file for a single task.

    Parameters
    ----------
    job_spec_dir
        Directory to write the job spec file.
    task_id
        The deterministic task ID.
    command
        The psimulate command (run, restart, expand, load_test).
    job_parameters
        The job parameters dictionary.
    """
    spec = {
        "command": command,
        "job_parameters": job_parameters,
    }
    spec_path = job_spec_dir / f"{task_id}.json"
    with open(spec_path, "w") as f:
        json.dump(spec, f, default=str)


def _build_compute_resources(
    native_specification: NativeSpecification,
    cluster_logging_root: Path,
) -> dict[str, Any]:
    """Build the Jobmon compute resources dict from a NativeSpecification.

    Notes
    -----
    * ``memory`` is passed in **GB** because the Jobmon SLURM plugin performs
      its own GB → MB conversion internally.
    * ``constraints`` is a pipe-separated string of SLURM feature names
      (e.g. ``"r650|r650v2"``), included only when hardware is requested.
    * ``standard_output`` and ``standard_error`` route SLURM stdout/stderr
      to the cluster logs directory. The Jobmon SLURM plugin appends the
      task name and SLURM job ID to these paths automatically.
    """
    resources: dict[str, Any] = {
        "queue": native_specification.queue,
        "project": native_specification.project,
        "memory": native_specification.peak_memory,  # GB – Jobmon converts to MB
        "runtime": _runtime_to_seconds(native_specification.max_runtime),
        "cores": native_specification.NUM_THREADS,
        "standard_output": str(cluster_logging_root),
        "standard_error": str(cluster_logging_root),
    }
    if native_specification.hardware:
        resources["constraints"] = "|".join(native_specification.hardware)
    return resources


def build_workflow(
    workflow_name: str,
    command: str,
    job_parameters_list: list[dict[str, Any]],
    output_paths: OutputPaths,
    native_specification: NativeSpecification,
    max_workers: int,
) -> Any:
    """Build a Jobmon workflow for a psimulate run.

    Creates a Jobmon Tool, TaskTemplate, and one Task per job. Also writes
    job spec JSON files to ``output_paths.job_spec_dir``.

    Parameters
    ----------
    workflow_name
        A unique name for the workflow.
    command
        The psimulate command (run, restart, expand, load_test).
    job_parameters_list
        List of job parameter dictionaries (one per task).
    output_paths
        The output paths container.
    native_specification
        SLURM resource specification.
    max_workers
        Maximum number of concurrent tasks.

    Returns
    -------
        A ready-to-run Jobmon Workflow object.
    """
    from jobmon.client.api import Tool

    python_path = shutil.which("python") or sys.executable

    tool = Tool(name="vivarium_cluster_tools_psimulate")
    task_template = tool.get_task_template(
        template_name="psimulate_task",
        command_template=(
            f"{python_path} -m vivarium_cluster_tools.psimulate.worker.task_runner "
            "--job-spec-dir {job_spec_dir} "
            "--task-id {task_id} "
            "--results-dir {results_dir} "
            "--worker-log-dir {worker_log_dir}"
        ),
        node_args=["task_id"],
        task_args=["job_spec_dir", "results_dir", "worker_log_dir"],
        op_args=[],
        default_cluster_name="slurm",
        default_compute_resources=_build_compute_resources(
            native_specification, output_paths.cluster_logging_root
        ),
    )

    workflow = tool.create_workflow(
        workflow_args=workflow_name,
        name=workflow_name,
        max_concurrently_running=max_workers,
        default_cluster_name="slurm",
        default_max_attempts=3,
    )

    # Write job specs and create tasks
    tasks = []
    for job_params in job_parameters_list:
        task_id = generate_task_id(
            input_draw=job_params["input_draw"],
            random_seed=job_params["random_seed"],
            branch_configuration=job_params["branch_configuration"],
        )
        _write_job_spec(
            job_spec_dir=output_paths.job_spec_dir,
            task_id=task_id,
            command=command,
            job_parameters=job_params,
        )

        task = task_template.create_task(
            name=f"psim_{task_id[:12]}",
            task_id=task_id,
            job_spec_dir=str(output_paths.job_spec_dir),
            results_dir=str(output_paths.results_dir),
            worker_log_dir=str(output_paths.worker_logging_root),
        )
        tasks.append(task)

    workflow.add_tasks(tasks)

    logger.info(
        f"Built Jobmon workflow '{workflow_name}' with {len(tasks)} tasks "
        f"(max {max_workers} concurrent)."
    )
    return workflow


def _runtime_to_seconds(runtime_str: str) -> int:
    """Convert HH:MM:SS runtime string to seconds.

    Parameters
    ----------
    runtime_str
        Runtime in HH:MM:SS format.

    Returns
    -------
        Runtime in seconds.
    """
    parts = runtime_str.split(":")
    if len(parts) == 3:
        h, m, s = parts
        return int(h) * 3600 + int(m) * 60 + int(s)
    elif len(parts) == 2:
        m, s = parts
        return int(m) * 60 + int(s)
    else:
        return int(parts[0])
