"""
========================
Pipeline Workflow Builder
========================

Build Jobmon workflows from pipeline configuration.

"""

from __future__ import annotations

from typing import TYPE_CHECKING

from jobmon.client.api import Tool

from vivarium_cluster_tools.psimulate.pipeline_config.config import PipelineConfig, StepConfig

if TYPE_CHECKING:
    from jobmon.client.workflow import Workflow


def resolve_command(step: StepConfig) -> str:
    """Resolve a step's configuration into a shell command string.

    For raw command steps, returns the command as-is.
    For structured steps, uses the type to infer the command.
    """
    raise NotImplementedError


class PipelineWorkflowBuilder:
    """Builds a complete Jobmon workflow from a pipeline configuration.

    For each step in the pipeline, creates a Jobmon task and wires
    dependencies so that steps execute in the configured order.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    def build(self) -> Workflow:
        """Build the full pipeline DAG and return the Jobmon Workflow."""
        raise NotImplementedError
