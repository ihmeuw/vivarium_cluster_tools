"""
================
Workflow Builder
================

Build Jobmon workflows from workflow configuration.

"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

from jobmon.client.api import Tool

from vivarium_cluster_tools.psimulate.workflow_config.config import WorkflowConfig
from vivarium_cluster_tools.psimulate.workflow_config.interface import (
    get_command_step_tasks,
    get_notebook_step_tasks,
    get_pytest_step_tasks,
    get_python_step_tasks,
    get_simulation_step_tasks,
)
from vivarium_cluster_tools.psimulate.workflow_config.utilities import is_resume

if TYPE_CHECKING:
    from jobmon.client.task import Task
    from jobmon.client.workflow import Workflow


STEP_TYPE_TO_API_FN: dict[str, Callable[..., list[Task]]] = {
    "command": get_command_step_tasks,
    "simulation": get_simulation_step_tasks,
    "pytest": get_pytest_step_tasks,
    "python": get_python_step_tasks,
    "notebook": get_notebook_step_tasks,
}
"""Maps each ``step_type`` to the interface API function that builds its tasks."""


class WorkflowBuilder:
    """Build a complete Jobmon workflow from a workflow configuration.

    For each step in the workflow, dispatches to the matching interface API
    function (one of ``get_*_step_tasks``) and wires dependencies so that
    steps execute in sequential order (all tasks from step *N* must complete
    before any task in step *N+1* starts).
    """

    def __init__(self, config: WorkflowConfig) -> None:
        self.config = config
        self._tool = Tool(name="vivarium_cluster_tools")

    def build(self, workflow_args: str) -> Workflow:
        """Build the full workflow DAG and return the Jobmon Workflow.

        Parameters
        ----------
        workflow_args
            Deterministic string that Jobmon uses to identify the workflow.
            Must be identical across runs for resume to work.
        """
        # TODO: MIC-6997 - encapsulate Jobmon UI in one place
        workflow = self._tool.create_workflow(
            workflow_args=workflow_args,
            name=self.config.name,
            default_cluster_name="slurm",
            default_max_attempts=self.config.max_attempts,
        )

        # Resume must be checked before any step runs: the build-timestamp
        # marker is what is_resume looks for, and the first interface API
        # call will write that marker as a side-effect of running.
        resuming = is_resume(self.config.output_directory)

        previous_step_tasks: list[Task] = []
        all_tasks: list[Task] = []

        for parsed_step in self.config.steps:
            api_fn = STEP_TYPE_TO_API_FN[parsed_step.step_type]
            kwargs = self._resolve_environment(parsed_step.api_kwargs)
            step_tasks = api_fn(**kwargs, tool=self._tool, is_resume=resuming)

            # Wire sequential dependencies: every task in this step
            # depends on every task from the previous step.
            for task in step_tasks:
                for prev_task in previous_step_tasks:
                    task.add_upstream(prev_task)

            all_tasks.extend(step_tasks)
            previous_step_tasks = step_tasks

        workflow.add_tasks(all_tasks)

        return workflow

    def _resolve_environment(self, api_kwargs: dict[str, Any]) -> dict[str, Any]:
        """Apply workflow-level ``default_environment`` to a step's kwargs.

        Step-level ``environment`` wins; otherwise the workflow default is
        substituted. Returns a new dict so the cached ``ParsedStep.api_kwargs``
        is not mutated.
        """
        if api_kwargs.get("environment") is not None:
            return api_kwargs
        return {**api_kwargs, "environment": self.config.default_environment}
