"""
================
Workflow Builder
================

Build Jobmon workflows from workflow configuration.

"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from jobmon.client.api import Tool

from vivarium_cluster_tools.psimulate.workflow_config.config import StepConfig, WorkflowConfig

if TYPE_CHECKING:
    from jobmon.client.workflow import Workflow

# Mapping of types to functions that resolve a command string from the step's configuration
COMMAND_RESOLVERS: dict[str, Callable[[str | list[str] | None, str | None, Path], str]] = {
    "pytest": lambda path, args, output_directory: f"pytest {_join_paths(path)} {args or ''}".strip(),
    "notebook": lambda path, args, output_directory: (
        f"papermill {_get_single_path(path)} {output_directory}/executed/"
        f"{Path(_get_single_path(path)).name} {args or ''}"
    ).strip(),
    "python": lambda path, args, output_directory: f"python {_join_paths(path)} {args or ''}".strip(),
    "shell": lambda path, args, output_directory: f"bash {_join_paths(path)} {args or ''}".strip(),
}


class WorkflowBuilder:
    """Build a complete Jobmon workflow from a workflow configuration.

    For each step in the workflow, creates a Jobmon task and wires
    dependencies so that steps execute in sequential order in which
    they are defined in the configuration file.
    """

    def __init__(self, config: WorkflowConfig) -> None:
        self.config = config
        self._tool = Tool(name="vivarium_cluster_tools")

    def build(self) -> Workflow:
        """Build the full workflow DAG and return the Jobmon Workflow."""
        # TODO: MIC-6997 - encapsulate Jobmon UI in one place
        task_template = self._tool.get_task_template(
            template_name="workflow_command_step",
            command_template="conda run --no-capture-output -n {env} {command}",
            node_args=["command"],
            task_args=[],
            op_args=["env"],
            default_cluster_name="slurm",
        )

        # TODO: MIC-6997 - encapsulate Jobmon UI in one place
        workflow = self._tool.create_workflow(
            name=self.config.name,
            default_cluster_name="slurm",
            default_max_attempts=3,
        )

        tasks = []
        for step in self.config.steps:
            command = resolve_command(step, self.config.output_directory)
            env = (
                step.environment
                or self.config.default_environment
                or os.environ.get("CONDA_DEFAULT_ENV", "base")
            )
            resources = step.resources
            compute_resources = {
                "queue": self.config.queue,
                "project": self.config.project,
                "memory": resources.memory_gb,
                "runtime": resources.runtime,
                "cores": resources.cores,
            }

            task = task_template.create_task(
                name=step.name,
                compute_resources=compute_resources,
                env=env,
                command=command,
            )
            tasks.append(task)

        # Wire sequential dependencies: each step depends on the previous
        for i in range(1, len(tasks)):
            tasks[i].add_upstream(tasks[i - 1])

        workflow.add_tasks(tasks)

        return workflow


def _get_single_path(path: str | list[str] | None) -> str:
    """Extract a single path string from various path formats."""
    if path is None:
        return ""
    if isinstance(path, list):
        return str(path[0]) if path else ""
    return str(path)


def _join_paths(path: str | list[str] | None) -> str:
    """Normalize path to a space-separated string."""
    if path is None:
        return ""
    if isinstance(path, list):
        return " ".join(str(p) for p in path)
    return str(path)


def resolve_command(step: StepConfig, output_directory: Path) -> str:
    """Resolve a step's configuration into a shell command string.

    For raw command steps, returns the command as-is.
    For structured steps, uses the type to infer the command.
    """
    # Check command directly for type narrowing
    if step.command is not None:
        return step.command
    # Check type and path directly for type narrowing
    if step.type is not None and step.path is not None:
        if step.type not in COMMAND_RESOLVERS:
            raise ValueError(f"Step '{step.name}': unsupported step type '{step.type}'.")
        resolver = COMMAND_RESOLVERS[step.type]
        return resolver(step.path, step.args, output_directory)
    raise ValueError(f"Step '{step.name}' has no command, type, or recognized bespoke name.")
