"""
================
Workflow Builder
================

Build Jobmon workflows from workflow configuration.

"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from jobmon.client.api import Tool

from vivarium_cluster_tools.psimulate.workflow_config.config import WorkflowConfig

if TYPE_CHECKING:
    from jobmon.client.workflow import Workflow


class WorkflowBuilder:
    """Build a complete Jobmon workflow from a workflow configuration.

    For each step in the workflow, creates one or more Jobmon tasks and
    wires dependencies so that steps execute in sequential order (all
    tasks from step *N* must complete before any task in step *N+1*
    starts).
    """

    def __init__(self, config: WorkflowConfig) -> None:
        self.config = config
        self._tool = Tool(name="vivarium_cluster_tools")

    def build(self) -> Workflow:
        """Build the full workflow DAG and return the Jobmon Workflow."""
        # TODO: MIC-6997 - encapsulate Jobmon UI in one place
        workflow = self._tool.create_workflow(
            name=self.config.name,
            default_cluster_name="slurm",
            default_max_attempts=self.config.max_attempts,
        )

        previous_step_tasks: list = []
        all_tasks: list = []

        for step in self.config.steps:
            env = (
                step.environment
                or self.config.default_environment
                or os.environ.get("CONDA_DEFAULT_ENV")
            )
            if not env or env == "base":
                raise ValueError(
                    f"Step '{step.name}': a non-base conda environment is required. "
                    "Set 'environment' on the step, 'default_environment' on the workflow, "
                    "or activate a conda environment before running."
                )

            step_tasks = step.get_tasks(
                self._tool,
                project=self.config.project,
                queue=self.config.queue,
                env=env,
            )

            # Wire sequential dependencies: every task in this step
            # depends on every task from the previous step.
            for task in step_tasks:
                for prev_task in previous_step_tasks:
                    task.add_upstream(prev_task)

            all_tasks.extend(step_tasks)
            previous_step_tasks = step_tasks

        workflow.add_tasks(all_tasks)

        return workflow
