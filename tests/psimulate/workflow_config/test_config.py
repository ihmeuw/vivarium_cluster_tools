"""Unit tests for the workflow configuration parser."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests.psimulate.workflow_config.utilities import (
    make_step_dict,
    make_workflow_dict,
    write_workflow_yaml,
)
from vivarium_cluster_tools.psimulate.workflow_config.config import (
    ResourceConfig,
    StepConfig,
    WorkflowConfig,
)


class TestWorkflowConfigFromYaml:
    """Verify that ``WorkflowConfig.from_yaml`` correctly parses valid YAML."""

    def test_parses_name(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml(valid_workflow_yaml)
        assert config.name == "test_pipeline"

    def test_parses_project(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml(valid_workflow_yaml)
        assert config.project == "proj_simscience"

    def test_parses_queue(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml(valid_workflow_yaml)
        assert config.queue == "all.q"

    def test_parses_output_directory(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml(valid_workflow_yaml)
        assert config.output_directory == Path("/tmp/results")

    def test_parses_default_environment(self, tmp_path: Path) -> None:
        data = make_workflow_dict(default_environment="my_env")
        yaml_path = write_workflow_yaml(tmp_path, data)
        config = WorkflowConfig.from_yaml(yaml_path)
        assert config.default_environment == "my_env"

    def test_parses_default_environment_absent(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml(valid_workflow_yaml)
        assert config.default_environment is None

    def test_parses_steps_count(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml(valid_workflow_yaml)
        assert len(config.steps) == 2

    def test_step_ordering_preserved(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml(valid_workflow_yaml)
        assert config.steps[0].name == "pre_tests"
        assert config.steps[1].name == "post_analysis"

    def test_parses_structured_step(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml(valid_workflow_yaml)
        step = config.steps[0]
        assert step.type == "pytest"
        assert step.path == ["tests/test_lbwsg.py", "tests/test_mortality.py"]
        assert step.args == "--runslow"
        assert step.command is None

    def test_parses_raw_command_step(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml(valid_workflow_yaml)
        step = config.steps[1]
        assert step.command == "python scripts/analyze.py --input /results"
        assert step.type is None
        assert step.path is None

    def test_parses_step_resources(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml(valid_workflow_yaml)
        res = config.steps[0].resources
        assert res is not None
        assert res.memory == 10
        assert res.runtime == "01:00:00"

    def test_parses_step_resources_cores(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml(valid_workflow_yaml)
        res = config.steps[1].resources
        assert res is not None
        assert res.cores == 2

    def test_parses_step_environment(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml(valid_workflow_yaml)
        assert config.steps[1].environment == "analysis_env"

    def test_step_environment_defaults_to_none(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml(valid_workflow_yaml)
        assert config.steps[0].environment is None

    def test_parses_path_as_list(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml(valid_workflow_yaml)
        assert isinstance(config.steps[0].path, list)

    def test_parses_path_as_string(self, tmp_path: Path) -> None:
        steps = [make_step_dict(name="single_path", path="tests/test_one.py")]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        config = WorkflowConfig.from_yaml(yaml_path)
        assert config.steps[0].path == "tests/test_one.py"


class TestStepConfigProperties:
    """Verify ``StepConfig`` classification properties."""

    def test_is_structured_true(self) -> None:
        step = StepConfig(name="s", type="pytest", path="tests/")
        assert step.is_structured is True

    def test_is_structured_false(self) -> None:
        step = StepConfig(name="s", command="echo hello")
        assert step.is_structured is False

    def test_is_raw_command_true(self) -> None:
        step = StepConfig(name="s", command="echo hello")
        assert step.is_raw_command is True

    def test_is_raw_command_false(self) -> None:
        step = StepConfig(name="s", type="pytest", path="tests/")
        assert step.is_raw_command is False


class TestWorkflowConfigValidation:
    """Verify that invalid configurations raise ``ValueError``."""

    def test_rejects_missing_name(self, tmp_path: Path) -> None:
        data = make_workflow_dict()
        del data["workflow"]["name"]
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(ValueError, match="name"):
            WorkflowConfig.from_yaml(yaml_path)

    def test_parses_missing_project_as_none(self, tmp_path: Path) -> None:
        data = make_workflow_dict()
        del data["workflow"]["project"]
        yaml_path = write_workflow_yaml(tmp_path, data)
        config = WorkflowConfig.from_yaml(yaml_path)
        assert config.project is None

    def test_parses_missing_queue_as_none(self, tmp_path: Path) -> None:
        data = make_workflow_dict()
        del data["workflow"]["queue"]
        yaml_path = write_workflow_yaml(tmp_path, data)
        config = WorkflowConfig.from_yaml(yaml_path)
        assert config.queue is None

    def test_parses_missing_output_directory_as_none(self, tmp_path: Path) -> None:
        data = make_workflow_dict()
        del data["workflow"]["output_directory"]
        yaml_path = write_workflow_yaml(tmp_path, data)
        config = WorkflowConfig.from_yaml(yaml_path)
        assert config.output_directory is None

    def test_rejects_missing_steps(self, tmp_path: Path) -> None:
        data = make_workflow_dict()
        del data["workflow"]["steps"]
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(ValueError, match="steps"):
            WorkflowConfig.from_yaml(yaml_path)

    def test_rejects_empty_steps(self, tmp_path: Path) -> None:
        data = make_workflow_dict(steps=[])
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(ValueError, match="steps"):
            WorkflowConfig.from_yaml(yaml_path)

    def test_rejects_step_with_both_command_and_type(self, tmp_path: Path) -> None:
        steps = [
            make_step_dict(
                name="bad_step",
                command="echo hello",
                type="pytest",
                path="tests/",
            )
        ]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(ValueError, match="bad_step"):
            WorkflowConfig.from_yaml(yaml_path)

    def test_rejects_step_with_command_and_path_without_type(self, tmp_path: Path) -> None:
        steps = [
            {
                "name": "bad_step",
                "command": "echo hello",
                "path": "tests/test_something.py",
            }
        ]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(ValueError, match="bad_step"):
            WorkflowConfig.from_yaml(yaml_path)

    def test_rejects_step_with_command_and_type_without_path(self, tmp_path: Path) -> None:
        steps = [
            {
                "name": "bad_step",
                "command": "echo hello",
                "type": "pytest",
            }
        ]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(ValueError, match="bad_step"):
            WorkflowConfig.from_yaml(yaml_path)

    def test_rejects_step_with_neither_command_nor_type(self, tmp_path: Path) -> None:
        steps = [{"name": "empty_step", "resources": {"memory": 4}}]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(ValueError, match="empty_step"):
            WorkflowConfig.from_yaml(yaml_path)

    def test_rejects_duplicate_step_names(self, tmp_path: Path) -> None:
        steps = [
            make_step_dict(name="dupe"),
            make_step_dict(name="dupe", command="echo hi", type=None, path=None),
        ]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(ValueError, match="unique"):
            WorkflowConfig.from_yaml(yaml_path)

    def test_rejects_invalid_step_type(self, tmp_path: Path) -> None:
        steps = [make_step_dict(name="bad_type", type="invalid")]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(ValueError, match="invalid"):
            WorkflowConfig.from_yaml(yaml_path)

    def test_rejects_step_with_type_but_no_path(self, tmp_path: Path) -> None:
        steps = [{"name": "no_path", "type": "pytest", "resources": {"memory": 4}}]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(ValueError, match="no_path"):
            WorkflowConfig.from_yaml(yaml_path)
