"""Unit tests for the workflow configuration parser."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from tests.psimulate.workflow_config.utilities import (
    make_step_dict,
    make_workflow_dict,
    write_psimulate_config,
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

    def test_parses_name(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        assert config.name == "test_pipeline"

    def test_parses_project(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        assert config.project == "proj_simscience"

    def test_parses_queue(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        assert config.queue == "all.q"

    def test_parses_output_directory(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        assert config.output_directory == Path("/tmp/results")

    def test_parses_default_environment(self, tmp_path: Path) -> None:
        data = make_workflow_dict(default_environment="my_env")
        yaml_path = write_workflow_yaml(tmp_path, data)
        config = WorkflowConfig.from_yaml_with_cli_overrides(yaml_path)
        assert config.default_environment == "my_env"

    def test_parses_default_environment_absent(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        assert config.default_environment is None

    def test_parses_steps_count(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        assert len(config.steps) == 2

    def test_step_ordering_preserved(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        assert config.steps[0].name == "pre_tests"
        assert config.steps[1].name == "post_analysis"

    def test_parses_command_step(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        step = config.steps[0]
        assert isinstance(step, CommandStepConfig)
        assert step.command == "pytest tests/test_lbwsg.py tests/test_mortality.py --runslow"

    def test_parses_raw_command_step(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        step = config.steps[1]
        assert isinstance(step, CommandStepConfig)
        assert step.command == "python scripts/analyze.py --input /results"

    def test_parses_step_resources(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        res = config.steps[0].resources
        assert res is not None
        assert res.memory_gb == 10
        assert res.runtime == "01:00:00"

    def test_parses_step_resources_cores(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        res = config.steps[1].resources
        assert res is not None
        assert res.cores == 2

    def test_parses_step_environment(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        assert config.steps[1].environment == "analysis_env"

    def test_step_environment_defaults_to_none(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        assert config.steps[0].environment is None

    def test_routes_to_simulation_step(
        self, tmp_path: Path, valid_model_spec_file: Path, valid_branch_config_file: Path
    ) -> None:
        """WorkflowConfig routes type=simulation to SimulationStepConfig."""
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
    """Verify that invalid configurations raise ``KeyError``."""

    @pytest.mark.parametrize(
        "field",
        ["name", "steps"],
    )
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
        steps = [
            {
                "name": "no_resources",
                "command": "echo hello",
            }
        ]
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
        """Steps cannot have both 'command' and 'type' fields."""
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

    def test_cli_project_overrides_yaml(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(
            valid_workflow_yaml, project="proj_simscience_prod"
        )
        assert config.project == "proj_simscience_prod"

    def test_cli_queue_overrides_yaml(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(
            valid_workflow_yaml, queue="long.q"
        )
        assert config.queue == "long.q"

    def test_cli_output_directory_overrides_yaml(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(
            valid_workflow_yaml, output_directory=Path("/cli/output")
        )
        assert config.output_directory == Path("/cli/output")

    def test_falls_back_to_yaml_values(self, valid_workflow_yaml: Path) -> None:
        config = WorkflowConfig.from_yaml_with_cli_overrides(valid_workflow_yaml)
        assert config.project == "proj_simscience"
        assert config.queue == "all.q"
        assert config.output_directory == Path("/tmp/results")

    @pytest.mark.parametrize(
        "field",
        ["project", "queue", "output_directory"],
    )
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

    def test_rejects_invalid_runtime_format(self) -> None:
        with pytest.raises(ValueError, match="hh:mm:ss"):
            ResourceConfig(memory_gb=1, runtime="90m")

    def test_rejects_runtime_missing_leading_zeros(self) -> None:
        with pytest.raises(ValueError, match="hh:mm:ss"):
            ResourceConfig(memory_gb=1, runtime="1:00:00")

    def test_uses_default_runtime(self) -> None:
        rc = ResourceConfig(memory_gb=1)
        assert rc.runtime == "01:00:00"

    def test_from_dict_defaults(self) -> None:
        rc = ResourceConfig.from_dict({"memory_gb": 4})
        assert rc is not None
        assert rc.memory_gb == 4
        assert rc.runtime == "01:00:00"
        assert rc.cores == 1

    def test_from_dict_all_fields(self) -> None:
        rc = ResourceConfig.from_dict({"memory_gb": 8, "runtime": "02:00:00", "cores": 4})
        assert rc is not None
        assert rc.memory_gb == 8
        assert rc.runtime == "02:00:00"
        assert rc.cores == 4


class TestCommandStepConfig:
    """Tests for CommandStepConfig - the default command-based step type."""

    def test_supported_arguments(self) -> None:
        """CommandStepConfig doesn't have an args section, returns None."""
        config = CommandStepConfig(
            name="test_step",
            resources=ResourceConfig(memory_gb=4),
            command="echo test",
        )
        supported = config.supported_arguments()
        assert supported is None

    def test_resolve_command_returns_command(self) -> None:
        """resolve_command returns the command string as-is."""
        config = CommandStepConfig(
            name="test_step",
            resources=ResourceConfig(memory_gb=4),
            command="echo hello world",
        )
        assert config.resolve_command() == "echo hello world"

    def test_to_dict(self) -> None:
        """to_dict includes environment when provided."""
        config = CommandStepConfig(
            name="test_step",
            resources=ResourceConfig(memory_gb=4),
            command="echo test",
            environment="my_env",
        )
        result = config.to_dict()
        assert result == {
            "name": "test_step",
            "command": "echo test",
            "resources": {
                "memory_gb": 4,
                "runtime": "01:00:00",
            },
            "environment": "my_env",
        }

    @pytest.mark.parametrize(
        "missing_field,kwargs",
        [
            ("name", {"resources": ResourceConfig(memory_gb=4), "command": "echo test"}),
            ("resources", {"name": "test_step", "command": "echo test"}),
        ],
        ids=["missing_name", "missing_resources"],
    )
    def test_requires_required_fields(
        self, missing_field: str, kwargs: dict[str, Any]
    ) -> None:
        """CommandStepConfig requires name and resources."""
        with pytest.raises(TypeError, match=missing_field):
            CommandStepConfig(**kwargs)


class TestSimulationStepConfig:
    """Tests for SimulationStepConfig - the simulation step type."""

    def test_requires_model_specification_or_config(self, tmp_path: Path) -> None:
        """SimulationStepConfig requires either model_specification or config file."""
        with pytest.raises(ValueError, match="model_specification"):
            SimulationStepConfig(
                name="sim",
                resources=ResourceConfig(memory_gb=5),
                branch_configuration=tmp_path / "branches.yaml",
            )

    def test_accepts_model_specification_inline(
        self, valid_model_spec_file: Path, valid_branch_config_file: Path
    ) -> None:
        """SimulationStepConfig accepts inline model_specification."""
        config = SimulationStepConfig(
            name="sim",
            resources=ResourceConfig(memory_gb=5),
            model_specification=valid_model_spec_file,
            branch_configuration=valid_branch_config_file,
        )
        assert config.model_specification == valid_model_spec_file
        assert config.branch_configuration == valid_branch_config_file

    def test_parses_config_file_fields(
        self, tmp_path: Path, valid_model_spec_file: Path, valid_branch_config_file: Path
    ) -> None:
        """SimulationStepConfig accepts and parses config file, extracting required fields."""
        config_file = write_psimulate_config(
            tmp_path,
            model_specification=str(valid_model_spec_file),
            branch_configuration=str(valid_branch_config_file),
        )
        step_config = SimulationStepConfig(
            name="sim",
            resources=ResourceConfig(memory_gb=5),
            config=config_file,
        )

        assert step_config.model_specification == valid_model_spec_file
        assert step_config.branch_configuration == valid_branch_config_file

    def test_parse_args_from_multiple_sources(
        self,
        tmp_path: Path,
        valid_model_spec_file: Path,
        valid_branch_config_file: Path,
        valid_artifact_file: Path,
    ) -> None:
        """Inline args merge with and override config file values."""
        # Config has model_spec and branch_config
        inline_model_spec = tmp_path / "inline_model.yaml"
        inline_model_spec.write_text("inline model")

        config_file = write_psimulate_config(
            tmp_path,
            model_specification=str(valid_model_spec_file),
            branch_configuration=str(valid_branch_config_file),
        )

        step_config = SimulationStepConfig(
            name="sim",
            resources=ResourceConfig(memory_gb=5),
            config=config_file,
            model_specification=inline_model_spec,  # Override config file value
            artifact_path=valid_artifact_file,  # Merge: not in config file
        )
        command = step_config.resolve_command()

        # Inline model_specification overrides config file value
        assert str(inline_model_spec) in command
        assert str(valid_model_spec_file) not in command
        # branch_configuration comes from config file
        assert str(valid_branch_config_file) in command
        # artifact_path comes from inline (not in config file)
        assert str(valid_artifact_file) in command

    def test_validates_required_fields_after_merge(self, tmp_path: Path) -> None:
        """Raises ValueError if model_spec or branch_config missing after merge."""
        # Config file missing branch_configuration
        config_file = write_psimulate_config(
            tmp_path,
            model_specification=str(tmp_path / "model.yaml"),
        )
        (tmp_path / "model.yaml").write_text("model")

        with pytest.raises(ValueError, match="branch_configuration"):
            SimulationStepConfig(
                name="sim",
                resources=ResourceConfig(memory_gb=5),
                config=config_file,
            )

    def test_resolve_command_generates_psimulate_run(
        self,
        tmp_path: Path,
        valid_model_spec_file: Path,
        valid_branch_config_file: Path,
        valid_artifact_file: Path,
    ) -> None:
        """resolve_command() generates psimulate run command with args."""
        # Test with inline args
        config = SimulationStepConfig(
            name="sim",
            resources=ResourceConfig(memory_gb=5),
            model_specification=valid_model_spec_file,
            branch_configuration=valid_branch_config_file,
            artifact_path=valid_artifact_file,
        )
        command = config.resolve_command()
        assert command.startswith("psimulate run")
        assert str(valid_model_spec_file) in command
        assert str(valid_branch_config_file) in command
        assert str(valid_artifact_file) in command

        # Test with config file
        config_file = write_psimulate_config(
            tmp_path,
            model_specification=str(valid_model_spec_file),
            branch_configuration=str(valid_branch_config_file),
        )
        config2 = SimulationStepConfig(
            name="sim",
            resources=ResourceConfig(memory_gb=5),
            config=config_file,
        )
        command2 = config2.resolve_command()
        assert command2.startswith("psimulate run")
        assert str(valid_model_spec_file) in command2

    def test_from_dict_rejects_both_command_and_type(self) -> None:
        """from_dict() rejects step dicts with both 'command' and 'type'."""
        step_dict = {
            "name": "bad_step",
            "type": "simulation",
            "command": "echo hello",
            "resources": {"memory_gb": 5, "runtime": "03:00:00"},
        }
        with pytest.raises(ValueError, match="Cannot specify both 'command' and 'type'"):
            SimulationStepConfig.from_dict(step_dict)

    @pytest.mark.parametrize("config_source", ["inline_args", "config_file"])
    def test_from_dict_deserialization(
        self,
        valid_model_spec_file: Path,
        valid_branch_config_file: Path,
        psimulate_config_file: Path,
        config_source: str,
    ) -> None:
        """SimulationStepConfig.from_dict deserializes various configurations."""
        base_dict: dict[str, Any] = {
            "name": "sim",
            "type": "simulation",
            "resources": {"memory_gb": 5, "runtime": "03:00:00"},
        }

        if config_source == "inline_args":
            base_dict["args"] = {
                "model_specification": str(valid_model_spec_file),
                "branch_configuration": str(valid_branch_config_file),
            }
        elif config_source == "config_file":
            base_dict["args"] = {
                "config": str(psimulate_config_file),
            }

        config = SimulationStepConfig.from_dict(base_dict)
        assert isinstance(config, SimulationStepConfig)
        assert config.name == "sim"

        # Validate fields were deserialized correctly
        if config_source == "inline_args":
            assert config.model_specification == valid_model_spec_file
            assert config.branch_configuration == valid_branch_config_file
        elif config_source == "config_file":
            assert config.config == psimulate_config_file

    @pytest.mark.parametrize("config_source", ["inline_args", "config_file"])
    def test_to_dict_serialization(
        self,
        valid_model_spec_file: Path,
        valid_branch_config_file: Path,
        psimulate_config_file: Path,
        config_source: str,
    ) -> None:
        """to_dict() serializes configuration with type: simulation."""
        if config_source == "inline_args":
            constructor_kwargs: dict[str, Any] = {
                "model_specification": valid_model_spec_file,
                "branch_configuration": valid_branch_config_file,
            }
            expected_args = {
                "model_specification": str(valid_model_spec_file),
                "branch_configuration": str(valid_branch_config_file),
            }
        else:  # config_file
            constructor_kwargs = {"config": psimulate_config_file}
            expected_args = {"config": str(psimulate_config_file)}

        config = SimulationStepConfig(
            name="sim",
            resources=ResourceConfig(memory_gb=5, runtime="03:00:00"),
            **constructor_kwargs,
        )
        result = config.to_dict()
        assert result["type"] == "simulation"
        assert result["name"] == "sim"
        assert "args" in result

        for field_name, expected_value in expected_args.items():
            assert result["args"][field_name] == expected_value
