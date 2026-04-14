"""
========================
Pipeline Workflow Builder
========================

Build Jobmon workflows from pipeline configuration.

"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

from jobmon.client.api import Tool

from vivarium_cluster_tools.psimulate.pipeline_config.config import PipelineConfig, StepConfig

if TYPE_CHECKING:
    from jobmon.client.workflow import Workflow

COMMAND_RESOLVERS = {
    "pytest": lambda path, args: f"pytest {_join_paths(path)} {args or ''}".strip(),
    "notebook": lambda path, args: (
        f"papermill {path} {{output_directory}}/executed/{Path(path).name} {args or ''}"
    ).strip(),
    "python": lambda path, args: f"python {path} {args or ''}".strip(),
    "shell": lambda path, args: f"bash {path} {args or ''}".strip(),
}


class PipelineWorkflowBuilder:
    """Builds a complete Jobmon workflow from a pipeline configuration.

    For each step in the pipeline, creates a Jobmon task and wires
    dependencies so that steps execute in the configured order.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    def build(self) -> Workflow:
        """Build the full pipeline DAG and return the Jobmon Workflow."""
        tool = Tool(name="vivarium_cluster_tools")

        task_template = tool.get_task_template(
            template_name="pipeline_command_step",
            command_template="conda run --no-banner -n {env} {command}",
            node_args=["step_name"],
            task_args=[],
            op_args=["env", "command"],
            default_cluster_name="slurm",
        )

        workflow = tool.create_workflow(
            name=self.config.name,
            default_cluster_name="slurm",
            default_max_attempts=3,
        )

        tasks = []
        for step in self.config.steps:
            command = resolve_command(step)
            env = (
                step.environment
                or self.config.default_environment
                or os.environ.get("CONDA_DEFAULT_ENV", "base")
            )
            resources = step.resources
            compute_resources = {
                "queue": self.config.queue,
                "project": self.config.project,
                "memory": resources.memory if resources and resources.memory else 4,
                "runtime": resources.runtime
                if resources and resources.runtime
                else "01:00:00",
                "cores": resources.cores if resources else 1,
            }

            task = task_template.create_task(
                name=step.name,
                compute_resources=compute_resources,
                step_name=step.name,
                env=env,
                command=command,
            )
            tasks.append(task)

        # Wire sequential dependencies: each step depends on the previous
        for i in range(1, len(tasks)):
            tasks[i].add_upstream(tasks[i - 1])

        workflow.add_tasks(tasks)

        return workflow


def _join_paths(path: str | list[str]) -> str:
    """Normalize path to a space-separated string."""
    if isinstance(path, list):
        return " ".join(str(p) for p in path)
    return str(path)


def resolve_command(step: StepConfig) -> str:
    """Resolve a step's configuration into a shell command string.

    For raw command steps, returns the command as-is.
    For structured steps, uses the type to infer the command.
    """
    if step.is_raw_command:
        return step.command
    if step.is_structured:
        resolver = COMMAND_RESOLVERS[step.type]
        return resolver(step.path, step.args)
    raise ValueError(f"Step '{step.name}' has no command, type, or recognized bespoke name.")
