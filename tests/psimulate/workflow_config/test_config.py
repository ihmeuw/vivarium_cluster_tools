"""Unit tests for the workflow configuration parser."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from tests.psimulate.workflow_config.utilities import (
    make_step_dict,
    make_workflow_dict,
    write_workflow_yaml,
)
from vivarium_cluster_tools.psimulate.workflow_config.config import (
    CommandStepConfig,
    ResourceConfig,
    SimulationStepConfig,
    WorkflowConfig,
)


class TestWorkflowConfigFromYaml:
    """Verify that ``WorkflowConfig.from_yaml_with_cli_overrides`` correctly parses valid YAML."""

    def test_parses_workflow_fields(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        assert config.name == "test_pipeline"
        assert config.project == "proj_simscience"
        assert config.queue == "all.q"
        assert config.output_directory == Path("/tmp/results")
        assert config.default_environment is None

    def test_parses_default_environment(self, tmp_path: Path) -> None:
        data = make_workflow_dict(default_environment="my_env")
        yaml_path = write_workflow_yaml(tmp_path, data)
        config = WorkflowConfig.from_yaml_with_cli_overrides(yaml_path)
        assert config.default_environment == "my_env"

    def test_step_ordering_preserved(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        assert len(config.steps) == 2
        assert config.steps[0].name == "pre_tests"
        assert config.steps[1].name == "post_analysis"

    @pytest.mark.parametrize(
        "index, expected_command",
        [
            (0, "pytest tests/test_lbwsg.py tests/test_mortality.py --runslow"),
            (1, "python scripts/analyze.py --input /results"),
        ],
        ids=["structured_command", "raw_command"],
    )
    def test_parses_command_steps(
        self, valid_workflow_yaml: Path, index: int, expected_command: str
    ) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        step = config.steps[index]
        assert isinstance(step, CommandStepConfig)
        assert step.command == expected_command

    def test_parses_step_resources(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        assert config.steps[0].resources.memory_gb == 10
        assert config.steps[0].resources.runtime == "01:00:00"
        assert config.steps[1].resources.cores == 2

    def test_parses_step_environment(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        assert config.steps[0].environment is None
        assert config.steps[1].environment == "analysis_env"

    def test_routes_to_simulation_step(
        self, tmp_path: Path, valid_model_spec_file: Path, valid_branch_config_file: Path
    ) -> None:
        steps = [
            {
                "name": "sim",
                "type": "simulation",
                "args": {
                    "model_specification": str(valid_model_spec_file),
                    "branch_configuration": str(valid_branch_config_file),
                },
                "resources": {"memory_gb": 5, "runtime": "03:00:00"},
            }
        ]
        workflow_dict = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, workflow_dict)

        config = WorkflowConfig.from_yaml_with_cli_overrides(yaml_path)
        assert len(config.steps) == 1
        assert isinstance(config.steps[0], SimulationStepConfig)
        assert config.steps[0].name == "sim"


class TestWorkflowConfigValidation:
    """Verify that invalid configurations raise errors."""

    @pytest.mark.parametrize("field", ["name", "steps"])
    def test_rejects_missing_required_field(self, tmp_path: Path, field: str) -> None:
        data = make_workflow_dict()
        del data["workflow"][field]
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(KeyError, match=field):
            WorkflowConfig.from_yaml_with_cli_overrides(yaml_path)

    def test_rejects_empty_steps(self, tmp_path: Path) -> None:
        data = make_workflow_dict(steps=[])
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(KeyError, match="steps"):
            WorkflowConfig.from_yaml_with_cli_overrides(yaml_path)

    def test_rejects_duplicate_step_names(self, tmp_path: Path) -> None:
        steps = [
            make_step_dict(name="dupe"),
            make_step_dict(name="dupe", command="echo hi"),
        ]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(KeyError, match="unique"):
            WorkflowConfig.from_yaml_with_cli_overrides(yaml_path)

    def test_rejects_step_without_command(self, tmp_path: Path) -> None:
        steps = [{"name": "no_cmd", "resources": {"memory_gb": 4}}]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(KeyError, match="command"):
            WorkflowConfig.from_yaml_with_cli_overrides(yaml_path)

    def test_rejects_step_without_resources(self, tmp_path: Path) -> None:
        steps = [{"name": "no_resources", "command": "echo hello"}]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(KeyError, match="resources"):
            WorkflowConfig.from_yaml_with_cli_overrides(yaml_path)

    def test_rejects_missing_workflow_key(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "workflow.yaml"
        yaml_path.write_text("not_workflow:\n  name: oops\n")
        with pytest.raises(KeyError, match="workflow"):
            WorkflowConfig.from_yaml_with_cli_overrides(yaml_path)

    def test_rejects_step_with_both_command_and_type(self, tmp_path: Path) -> None:
        steps = [
            {
                "name": "bad_step",
                "command": "echo hello",
                "type": "simulation",
                "resources": {"memory_gb": 4},
            }
        ]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(ValueError, match="Cannot specify both 'command' and 'type'"):
            WorkflowConfig.from_yaml_with_cli_overrides(yaml_path)


class TestWorkflowConfigFromYamlWithCliOverrides:
    """Verify that ``from_yaml_with_cli_overrides`` merges CLI args and validates."""

    @pytest.mark.parametrize(
        "field, value",
        [
            ("project", "proj_simscience_prod"),
            ("queue", "long.q"),
            ("output_directory", Path("/cli/output")),
        ],
    )
    def test_cli_overrides_yaml(
        self, valid_workflow_yaml: Path, field: str, value: str | Path
    ) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(
            valid_workflow_yaml, **{field: value}  # type: ignore[arg-type]
        )
        assert getattr(config, field) == value

    @pytest.mark.parametrize("field", ["project", "queue", "output_directory"])
    def test_rejects_missing_field_everywhere(self, tmp_path: Path, field: str) -> None:
        data = make_workflow_dict()
        del data["workflow"][field]
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(
            KeyError, match=f"{field.replace('_', ' ').title().split()[0]}.*required"
        ):
            WorkflowConfig.from_yaml_with_cli_overrides(yaml_path)

    @pytest.mark.parametrize(
        "field, cli_value",
        [
            ("project", "proj_simscience"),
            ("queue", "long.q"),
            ("output_directory", Path("/from/cli")),
        ],
    )
    def test_cli_fills_missing_yaml_field(
        self, tmp_path: Path, field: str, cli_value: str | Path
    ) -> None:
        data = make_workflow_dict()
        del data["workflow"][field]
        yaml_path = write_workflow_yaml(tmp_path, data)
        kwargs: dict[str, str | Path | None] = {field: cli_value}
        config = WorkflowConfig.from_yaml_with_cli_overrides(yaml_path, **kwargs)  # type: ignore[arg-type]
        assert getattr(config, field) == cli_value


class TestResourceConfigValidation:
    """Verify ``ResourceConfig`` validation."""

    def test_accepts_valid_runtime(self) -> None:
        rc = ResourceConfig(memory_gb=1, runtime="01:30:00")
        assert rc.runtime == "01:30:00"

    @pytest.mark.parametrize("bad_runtime", ["90m", "1:00:00", "abc"])
    def test_rejects_invalid_runtime(self, bad_runtime: str) -> None:
        with pytest.raises(ValueError, match="hh:mm:ss"):
            ResourceConfig(memory_gb=1, runtime=bad_runtime)

    def test_uses_default_runtime(self) -> None:
        rc = ResourceConfig(memory_gb=1)
        assert rc.runtime == "01:00:00"

    def test_from_dict_defaults(self) -> None:
        rc = ResourceConfig.from_dict({"memory_gb": 4})
        assert rc.memory_gb == 4
        assert rc.runtime == "01:00:00"
        assert rc.cores == 1

    def test_from_dict_all_fields(self) -> None:
        rc = ResourceConfig.from_dict({"memory_gb": 8, "runtime": "02:00:00", "cores": 4})
        assert rc.memory_gb == 8
        assert rc.runtime == "02:00:00"
        assert rc.cores == 4


class TestCommandStepConfig:
    """Tests for CommandStepConfig - the default command-based step type."""

    def test_supported_arguments_returns_none(self) -> None:
        config = CommandStepConfig(
            name="test_step",
            resources=ResourceConfig(memory_gb=4),
            command="echo test",
            output_directory=Path("/tmp/results"),
        )
        assert config.supported_arguments() is None

    def test_get_tasks_creates_single_task(self) -> None:
        config = CommandStepConfig(
            name="test_step",
            resources=ResourceConfig(memory_gb=4, project="proj_simscience", queue="all.q"),
            command="echo hello world",
            output_directory=Path("/tmp/results"),
        )
        mock_tool = MagicMock()
        mock_template = MagicMock()
        mock_task = MagicMock()
        mock_tool.get_task_template.return_value = mock_template
        mock_template.create_task.return_value = mock_task

        tasks = config.get_tasks(mock_tool, env="my_env")

        assert tasks == [mock_task]
        mock_template.create_task.assert_called_once_with(
            name="test_step",
            compute_resources={
                "queue": "all.q",
                "project": "proj_simscience",
                "memory": 4,
                "runtime": "01:00:00",
                "cores": 1,
            },
            env="my_env",
            command="echo hello world",
        )

    def test_to_dict(self) -> None:
        config = CommandStepConfig(
            name="test_step",
            resources=ResourceConfig(memory_gb=4),
            command="echo test",
            output_directory=Path("/tmp/results"),
            environment="my_env",
        )
        result = config.to_dict()
        assert result == {
            "name": "test_step",
            "command": "echo test",
            "resources": {"memory_gb": 4, "runtime": "01:00:00"},
            "environment": "my_env",
        }

    @pytest.mark.parametrize(
        "missing_field,kwargs",
        [
            (
                "name",
                {
                    "resources": ResourceConfig(memory_gb=4),
                    "command": "echo test",
                    "output_directory": Path("/tmp/results"),
                },
            ),
            (
                "resources",
                {
                    "name": "test_step",
                    "command": "echo test",
                    "output_directory": Path("/tmp/results"),
                },
            ),
        ],
        ids=["missing_name", "missing_resources"],
    )
    def test_requires_required_fields(
        self, missing_field: str, kwargs: dict[str, Any]
    ) -> None:
        with pytest.raises(TypeError, match=missing_field):
            CommandStepConfig(**kwargs)


class TestSimulationStepConfig:
    """Tests for SimulationStepConfig - the simulation step type."""

    @pytest.mark.parametrize(
        "omitted_field, provided_field, provided_fixture",
        [
            ("model_specification", "branch_configuration", "valid_branch_config_file"),
            ("branch_configuration", "model_specification", "valid_model_spec_file"),
        ],
    )
    def test_requires_required_args(
        self,
        omitted_field: str,
        provided_field: str,
        provided_fixture: str,
        request: pytest.FixtureRequest,
    ) -> None:
        fixture_value = request.getfixturevalue(provided_fixture)
        with pytest.raises(TypeError, match=omitted_field):
            SimulationStepConfig(
                name="sim",
                resources=ResourceConfig(memory_gb=5),
                output_directory=Path("/tmp/results"),
                **{provided_field: fixture_value},
            )

    def test_accepts_all_fields(
        self,
        valid_model_spec_file: Path,
        valid_branch_config_file: Path,
        valid_artifact_file: Path,
    ) -> None:
        config = SimulationStepConfig(
            name="sim",
            resources=ResourceConfig(memory_gb=5, hardware=["r650", "r650v2"]),
            output_directory=Path("/tmp/results"),
            model_specification=valid_model_spec_file,
            branch_configuration=valid_branch_config_file,
            artifact_path=valid_artifact_file,
        )
        assert config.model_specification == valid_model_spec_file
        assert config.branch_configuration == valid_branch_config_file
        assert config.artifact_path == valid_artifact_file
        assert config.resources.hardware == ["r650", "r650v2"]

    def test_optional_fields_default_to_none(
        self, valid_model_spec_file: Path, valid_branch_config_file: Path
    ) -> None:
        config = SimulationStepConfig(
            name="sim",
            resources=ResourceConfig(memory_gb=5),
            output_directory=Path("/tmp/results"),
            model_specification=valid_model_spec_file,
            branch_configuration=valid_branch_config_file,
        )
        assert config.artifact_path is None
        assert config.resources.hardware is None

    def test_from_dict_deserialization(
        self,
        valid_model_spec_file: Path,
        valid_branch_config_file: Path,
    ) -> None:
        step_dict: dict[str, Any] = {
            "name": "sim",
            "type": "simulation",
            "resources": {"memory_gb": 5, "runtime": "03:00:00"},
            "args": {
                "model_specification": str(valid_model_spec_file),
                "branch_configuration": str(valid_branch_config_file),
            },
        }

        config = SimulationStepConfig.from_dict(
            step_dict,
            output_directory=Path("/tmp/results"),
            project="proj_simscience",
            queue="all.q",
        )
        assert isinstance(config, SimulationStepConfig)
        assert config.name == "sim"
        assert config.model_specification == valid_model_spec_file
        assert config.branch_configuration == valid_branch_config_file

    def test_from_dict_rejects_unsupported_args(
        self,
        valid_model_spec_file: Path,
        valid_branch_config_file: Path,
    ) -> None:
        step_dict: dict[str, Any] = {
            "name": "sim",
            "type": "simulation",
            "resources": {"memory_gb": 5},
            "args": {
                "model_specification": str(valid_model_spec_file),
                "branch_configuration": str(valid_branch_config_file),
                "bogus_field": "nope",
            },
        }
        with pytest.raises(ValueError, match="unsupported args"):
            SimulationStepConfig.from_dict(
                step_dict,
                output_directory=Path("/tmp/results"),
                project="proj_simscience",
                queue="all.q",
            )

    def test_to_dict_serialization(
        self,
        valid_model_spec_file: Path,
        valid_branch_config_file: Path,
        valid_artifact_file: Path,
    ) -> None:
        config = SimulationStepConfig(
            name="sim",
            resources=ResourceConfig(memory_gb=5, runtime="03:00:00", hardware=["r650"]),
            output_directory=Path("/tmp/results"),
            model_specification=valid_model_spec_file,
            branch_configuration=valid_branch_config_file,
            artifact_path=valid_artifact_file,
        )
        result = config.to_dict()
        assert result["type"] == "simulation"
        assert result["name"] == "sim"
        assert result["args"]["model_specification"] == str(valid_model_spec_file)
        assert result["args"]["branch_configuration"] == str(valid_branch_config_file)
        assert result["args"]["artifact_path"] == str(valid_artifact_file)
        assert result["resources"]["hardware"] == ["r650"]

    def test_to_dict_omits_none_optional_fields(
        self,
        valid_model_spec_file: Path,
        valid_branch_config_file: Path,
    ) -> None:
        config = SimulationStepConfig(
            name="sim",
            resources=ResourceConfig(memory_gb=5),
            output_directory=Path("/tmp/results"),
            model_specification=valid_model_spec_file,
            branch_configuration=valid_branch_config_file,
        )
        result = config.to_dict()
        assert "artifact_path" not in result["args"]
        assert "hardware" not in result["resources"]
