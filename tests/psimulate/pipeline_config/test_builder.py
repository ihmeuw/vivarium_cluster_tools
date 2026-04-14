"""Unit tests for the pipeline workflow builder."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from vivarium_cluster_tools.psimulate.pipeline_config.builder import (
    PipelineWorkflowBuilder,
    resolve_command,
)
from vivarium_cluster_tools.psimulate.pipeline_config.config import PipelineConfig, StepConfig


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


class TestResolveCommand:
    """Verify that ``resolve_command`` produces the correct shell command for each step type."""

    def test_raw_command(self) -> None:
        """A raw-command step returns the command as-is."""
        step = StepConfig(name="raw", command="python scripts/analyze.py --input /results")
        assert resolve_command(step) == "python scripts/analyze.py --input /results"

    def test_pytest_single_path(self) -> None:
        """A pytest step with a single path."""
        step = StepConfig(name="tests", type="pytest", path="tests/test_foo.py")
        assert resolve_command(step) == "pytest tests/test_foo.py"

    def test_pytest_multiple_paths(self) -> None:
        """A pytest step with multiple paths."""
        step = StepConfig(
            name="tests",
            type="pytest",
            path=["tests/test_a.py", "tests/test_b.py"],
        )
        assert resolve_command(step) == "pytest tests/test_a.py tests/test_b.py"

    def test_pytest_with_args(self) -> None:
        """A pytest step with extra args."""
        step = StepConfig(name="tests", type="pytest", path="tests/", args="--runslow -x")
        assert resolve_command(step) == "pytest tests/ --runslow -x"

    def test_notebook(self) -> None:
        """A notebook step produces a papermill command."""
        step = StepConfig(name="nb", type="notebook", path="notebooks/results.ipynb")
        result = resolve_command(step)
        assert result.startswith("papermill notebooks/results.ipynb")
        assert "executed/results.ipynb" in result

    def test_python(self) -> None:
        """A python step produces a python command."""
        step = StepConfig(name="py", type="python", path="scripts/run.py")
        assert resolve_command(step) == "python scripts/run.py"

    def test_shell_with_args(self) -> None:
        """A shell step with args."""
        step = StepConfig(name="sh", type="shell", path="scripts/setup.sh", args="--env prod")
        assert resolve_command(step) == "bash scripts/setup.sh --env prod"
