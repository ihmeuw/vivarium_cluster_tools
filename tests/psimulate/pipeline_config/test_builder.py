"""Unit tests for the pipeline workflow builder."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from vivarium_cluster_tools.psimulate.pipeline_config.builder import PipelineWorkflowBuilder
from vivarium_cluster_tools.psimulate.pipeline_config.config import (
    PipelineConfig,
    ResourceConfig,
    StepConfig,
)


@pytest.fixture()
def three_step_config() -> PipelineConfig:
    """A ``PipelineConfig`` with three sequential raw-command steps."""
    return PipelineConfig(
        name="test_pipeline",
        project="proj_simscience",
        queue="all.q",
        output_directory=Path("/tmp/results"),
        default_environment=None,
        steps=[
            StepConfig(name="step1", command="echo step1"),
            StepConfig(name="step2", command="echo step2"),
            StepConfig(name="step3", command="echo step3"),
        ],
    )


@pytest.fixture()
def mock_tool_cls(mocker: MockerFixture) -> MagicMock:
    """Patch the Jobmon ``Tool`` class at the builder's import site."""
    return mocker.patch("vivarium_cluster_tools.psimulate.pipeline_config.builder.Tool")


class TestPipelineWorkflowBuilder:
    """Verify that ``PipelineWorkflowBuilder`` builds a correct Jobmon workflow."""

    @pytest.mark.xfail(reason="Not yet implemented", strict=True)
    def test_valid_config_builds_workflow(
        self,
        three_step_config: PipelineConfig,
        mock_tool_cls: MagicMock,
    ) -> None:
        """A valid config produces a Jobmon Workflow with tasks added."""
        builder = PipelineWorkflowBuilder(three_step_config)
        workflow = builder.build()

        expected_workflow = mock_tool_cls.return_value.create_workflow.return_value
        assert workflow is expected_workflow

    @pytest.mark.xfail(reason="Not yet implemented", strict=True)
    def test_dag_preserves_step_ordering(
        self,
        three_step_config: PipelineConfig,
        mock_tool_cls: MagicMock,
    ) -> None:
        """A 3-step pipeline produces a DAG: step1 -> step2 -> step3."""
        task1 = MagicMock(name="task_step1")
        task2 = MagicMock(name="task_step2")
        task3 = MagicMock(name="task_step3")

        template_mock = mock_tool_cls.return_value.get_task_template.return_value
        template_mock.create_task.side_effect = [task1, task2, task3]

        builder = PipelineWorkflowBuilder(three_step_config)
        builder.build()

        # Each config step produces exactly one task
        assert template_mock.create_task.call_count == 3

        # step1 has no upstream
        task1.add_upstream.assert_not_called()
        # step2 depends on step1
        task2.add_upstream.assert_called_once_with(task1)
        # step3 depends on step2
        task3.add_upstream.assert_called_once_with(task2)
