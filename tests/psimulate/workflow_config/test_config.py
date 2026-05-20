"""Unit tests for the workflow configuration parser."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from tests.psimulate.workflow_config.utilities import (
    make_notebook_step_dict,
    make_pytest_step_dict,
    make_python_step_dict,
    make_step_dict,
    make_workflow_dict,
    write_workflow_yaml,
)
from vivarium_cluster_tools.psimulate import TASK_RUNNER_MODULE
from vivarium_cluster_tools.psimulate.workflow_config.config import (
    ParsedStep,
    ResourceConfig,
    WorkflowConfig,
)
from vivarium_cluster_tools.psimulate.workflow_config.parsing import (
    load_workflow_config,
    parse_command_step_from_yaml,
    parse_notebook_step_from_yaml,
    parse_pytest_step_from_yaml,
    parse_python_step_from_yaml,
    parse_simulation_step_from_yaml,
    parse_step_from_yaml,
    serialize_command_step_to_yaml,
    serialize_notebook_step_to_yaml,
    serialize_pytest_step_to_yaml,
    serialize_python_step_to_yaml,
    serialize_simulation_step_to_yaml,
)
from vivarium_cluster_tools.psimulate.workflow_config.task_builders import (
    _build_notebook_command,
    _build_pytest_command,
    _build_python_command,
    build_command_step_tasks,
    build_pytest_step_tasks,
    build_simulation_step_tasks,
)
from vivarium_cluster_tools.psimulate.workflow_config.validation import (
    validate_notebook_step,
    validate_pytest_step,
    validate_python_step,
)

_SUBPROCESS_WRAPPER_PREFIX = f"python -m {TASK_RUNNER_MODULE} subprocess "


class TestWorkflowConfigFromYaml:
    """Verify that ``load_workflow_config`` correctly parses valid YAML."""

    def test_parses_workflow_fields(self, valid_workflow_yaml: Path) -> None:
        config = load_workflow_config(valid_workflow_yaml)
        assert config.name == "test_pipeline"
        assert config.project == "proj_simscience"
        assert config.queue == "all.q"
        assert config.output_directory == Path("/tmp/results")
        assert config.default_environment is None

    def test_parses_default_environment(self, tmp_path: Path) -> None:
        data = make_workflow_dict(default_environment="my_env")
        yaml_path = write_workflow_yaml(tmp_path, data)
        config = load_workflow_config(yaml_path)
        assert config.default_environment == "my_env"

    def test_step_ordering_preserved(self, valid_workflow_yaml: Path) -> None:
        config = load_workflow_config(valid_workflow_yaml)
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
        config = load_workflow_config(valid_workflow_yaml)
        step = config.steps[index]
        assert step.step_type == "command"
        assert step.api_kwargs["command"] == expected_command

    def test_parses_step_resources(self, valid_workflow_yaml: Path) -> None:
        config = load_workflow_config(valid_workflow_yaml)
        assert config.steps[0].api_kwargs["resources"].memory_gb == 10
        assert config.steps[0].api_kwargs["resources"].runtime == "01:00:00"
        assert config.steps[1].api_kwargs["resources"].cores == 2

    def test_parses_step_environment(self, valid_workflow_yaml: Path) -> None:
        config = load_workflow_config(valid_workflow_yaml)
        assert config.steps[0].api_kwargs["environment"] is None
        assert config.steps[1].api_kwargs["environment"] == "analysis_env"

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

        config = load_workflow_config(yaml_path)
        assert len(config.steps) == 1
        assert config.steps[0].step_type == "simulation"
        assert config.steps[0].name == "sim"


class TestWorkflowConfigValidation:
    """Verify that invalid configurations raise errors."""

    @pytest.mark.parametrize("field", ["name", "steps"])
    def test_rejects_missing_required_field(self, tmp_path: Path, field: str) -> None:
        data = make_workflow_dict()
        del data["workflow"][field]
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(KeyError, match=field):
            load_workflow_config(yaml_path)

    def test_rejects_empty_steps(self, tmp_path: Path) -> None:
        data = make_workflow_dict(steps=[])
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(KeyError, match="steps"):
            load_workflow_config(yaml_path)

    def test_rejects_duplicate_step_names(self, tmp_path: Path) -> None:
        steps = [
            make_step_dict(name="dupe"),
            make_step_dict(name="dupe", command="echo hi"),
        ]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(ValueError, match="unique"):
            load_workflow_config(yaml_path)

    def test_rejects_step_without_command(self, tmp_path: Path) -> None:
        steps = [{"name": "no_cmd", "resources": {"memory_gb": 4}}]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(
            ValueError, match="command-based steps require a top-level 'command' field"
        ):
            load_workflow_config(yaml_path)

    def test_rejects_step_without_resources(self, tmp_path: Path) -> None:
        steps = [{"name": "no_resources", "command": "echo hello"}]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(KeyError, match="resources"):
            load_workflow_config(yaml_path)

    def test_rejects_missing_workflow_key(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "workflow.yaml"
        yaml_path.write_text("not_workflow:\n  name: oops\n")
        with pytest.raises(KeyError, match="workflow"):
            load_workflow_config(yaml_path)

    def test_rejects_command_with_mismatched_type(self, tmp_path: Path) -> None:
        """When ``command`` is set, ``type`` must be omitted or ``"command"``;
        any other ``type`` value is rejected."""
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
        with pytest.raises(ValueError, match="cannot specify both 'command' and 'type"):
            load_workflow_config(yaml_path)

    def test_accepts_command_with_explicit_type_command(self, tmp_path: Path) -> None:
        """``type: command`` paired with a top-level ``command`` is accepted."""
        steps = [
            {
                "name": "explicit",
                "type": "command",
                "command": "echo hello",
                "resources": {"memory_gb": 4},
            }
        ]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        config = load_workflow_config(yaml_path)
        assert config.steps[0].step_type == "command"
        assert config.steps[0].api_kwargs["command"] == "echo hello"

    def test_rejects_type_command_without_command_field(self, tmp_path: Path) -> None:
        """``type: command`` alone is rejected; a top-level ``command`` field
        is still required for command steps."""
        steps = [
            {
                "name": "bad_step",
                "type": "command",
                "resources": {"memory_gb": 4},
            }
        ]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(
            ValueError, match="command-based steps require a top-level 'command' field"
        ):
            load_workflow_config(yaml_path)

    def test_rejects_unsupported_step_type(self, tmp_path: Path) -> None:
        """An unknown ``type`` value raises; the error message lists every
        valid step type, including ``command``."""
        steps = [
            {
                "name": "bad_step",
                "type": "not_a_type",
                "resources": {"memory_gb": 4},
            }
        ]
        data = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(ValueError, match="unsupported type 'not_a_type'") as excinfo:
            load_workflow_config(yaml_path)
        assert "command" in str(excinfo.value)


class TestWorkflowConfigFromYamlWithCliOverrides:
    """Verify that ``load_workflow_config`` merges CLI args and validates."""

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
        config = load_workflow_config(
            valid_workflow_yaml, **{field: value}  # type: ignore[arg-type]
        )
        assert getattr(config, field) == value

    @pytest.mark.parametrize("field", ["project", "queue", "output_directory"])
    def test_rejects_missing_field_everywhere(self, tmp_path: Path, field: str) -> None:
        data = make_workflow_dict()
        del data["workflow"][field]
        yaml_path = write_workflow_yaml(tmp_path, data)
        with pytest.raises(
            ValueError, match=f"(?i){field.replace('_', ' ').split()[0]}.*required"
        ):
            load_workflow_config(yaml_path)

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
        config = load_workflow_config(yaml_path, **kwargs)  # type: ignore[arg-type]
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

    def test_requires_archive_node_defaults_false(self) -> None:
        rc = ResourceConfig(memory_gb=1)
        assert rc.requires_archive_node is False

    def test_from_dict_reads_requires_archive_node(self) -> None:
        rc = ResourceConfig.from_dict({"memory_gb": 4, "requires_archive_node": True})
        assert rc.requires_archive_node is True

    def test_to_dict_emits_requires_archive_node_only_when_true(self) -> None:
        rc_default = ResourceConfig(memory_gb=4, project="proj_simscience", queue="all.q")
        assert "requires_archive_node" not in rc_default.to_dict()

        rc_archive = ResourceConfig(
            memory_gb=4,
            project="proj_simscience",
            queue="all.q",
            requires_archive_node=True,
        )
        assert rc_archive.to_dict()["requires_archive_node"] is True

    def test_to_native_specification_passes_archive_flag(self) -> None:
        rc = ResourceConfig(
            memory_gb=4,
            project="proj_simscience",
            queue="all.q",
            hardware=["r650"],
            requires_archive_node=True,
        )
        native = rc.to_native_specification(job_name="job")
        assert native.requires_archive_node is True
        assert native.hardware == ["r650"]


def _common_resources() -> ResourceConfig:
    return ResourceConfig(memory_gb=4, project="proj_simscience", queue="all.q")


class TestCommandStep:
    """Tests for the command step type (parsing, serialization, task building)."""

    def test_serialize(self) -> None:
        result = serialize_command_step_to_yaml(
            name="test_step",
            resources=_common_resources(),
            command="echo test",
            output_directory=Path("/tmp/results"),
            environment="my_env",
        )
        assert result == {
            "name": "test_step",
            "command": "echo test",
            "resources": {
                "memory_gb": 4,
                "project": "proj_simscience",
                "queue": "all.q",
                "runtime": "01:00:00",
            },
            "environment": "my_env",
        }

    def test_parse(self) -> None:
        step_dict = make_step_dict(name="cmd", command="echo hi")
        kwargs = parse_command_step_from_yaml(
            step_dict,
            output_directory=Path("/tmp/results"),
            project="proj_simscience",
            queue="all.q",
        )
        assert kwargs["name"] == "cmd"
        assert kwargs["command"] == "echo hi"
        assert kwargs["resources"].memory_gb == 4

    def test_build_command_task_creates_single_task(self) -> None:
        """build_command_step_tasks wires the command into a single Jobmon task."""
        mock_tool = MagicMock()
        mock_template = MagicMock()
        mock_task = MagicMock()
        mock_tool.get_task_template.return_value = mock_template
        mock_template.create_task.return_value = mock_task

        tasks = build_command_step_tasks(
            name="test_step",
            resources=_common_resources(),
            command="echo hello world",
            output_directory=Path("/tmp/results"),
            tool=mock_tool,
            env_prefix="/path/to/envs/my_env",
            build_timestamp="2026_04_24_10_00_00",
        )

        assert tasks == [mock_task]
        mock_template.create_task.assert_called_once_with(
            name="test_step",
            compute_resources={
                "queue": "all.q",
                "project": "proj_simscience",
                "memory": 4.0,
                "runtime": 3600,
                "cores": 1,
                "stdout": "/tmp/results",
                "stderr": "/tmp/results",
            },
            env_prefix="/path/to/envs/my_env",
            command=_SUBPROCESS_WRAPPER_PREFIX + "echo hello world",
        )

    def test_command_task_is_wrapped_with_runner(self) -> None:
        """The raw command is wrapped with the ``task_runner subprocess`` prefix
        so the child process's output replays to SLURM stderr on failure."""
        mock_tool = MagicMock()
        mock_template = MagicMock()
        mock_tool.get_task_template.return_value = mock_template

        build_command_step_tasks(
            name="raw",
            resources=_common_resources(),
            command="echo hello world",
            output_directory=Path("/tmp/results"),
            tool=mock_tool,
            env_prefix="/env",
            build_timestamp="2026_04_24_10_00_00",
        )

        cmd = mock_template.create_task.call_args.kwargs["command"]
        assert _SUBPROCESS_WRAPPER_PREFIX in cmd
        assert "echo hello world" in cmd

    def test_build_command_task_includes_env_prefix_in_node_args(self) -> None:
        """env_prefix must be a node_arg so two steps with the same command
        but different envs produce distinct Jobmon task hashes."""
        mock_tool = MagicMock()
        build_command_step_tasks(
            name="test_step",
            resources=_common_resources(),
            command="echo hello world",
            output_directory=Path("/tmp/results"),
            tool=mock_tool,
            env_prefix="/path/to/envs/my_env",
            build_timestamp="2026_04_24_10_00_00",
        )

        template_kwargs = mock_tool.get_task_template.call_args.kwargs
        assert "env_prefix" in template_kwargs["node_args"]
        assert "env_prefix" not in template_kwargs["op_args"]


class TestSimulationStep:
    """Tests for the simulation step type."""

    def test_parse(
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

        kwargs = parse_simulation_step_from_yaml(
            step_dict,
            output_directory=Path("/tmp/results"),
            project="proj_simscience",
            queue="all.q",
        )
        assert kwargs["name"] == "sim"
        assert kwargs["model_specification"] == valid_model_spec_file
        assert kwargs["branch_configuration"] == valid_branch_config_file

    def test_parse_rejects_unsupported_args(
        self,
        valid_model_spec_file: Path,
        valid_branch_config_file: Path,
    ) -> None:
        step_dict = {
            "name": "sim",
            "type": "simulation",
            "resources": {"memory_gb": 5, "runtime": "03:00:00"},
            "args": {
                "model_specification": str(valid_model_spec_file),
                "branch_configuration": str(valid_branch_config_file),
                "bogus_field": "nope",
            },
        }
        with pytest.raises(ValueError, match="unsupported args"):
            parse_simulation_step_from_yaml(
                step_dict,
                output_directory=Path("/tmp/results"),
                project="proj_simscience",
                queue="all.q",
            )

    def test_serialize(
        self,
        valid_model_spec_file: Path,
        valid_branch_config_file: Path,
        valid_artifact_file: Path,
    ) -> None:
        result = serialize_simulation_step_to_yaml(
            name="sim",
            resources=ResourceConfig(
                memory_gb=5,
                runtime="03:00:00",
                hardware=["r650"],
                project="proj_simscience",
                queue="all.q",
            ),
            output_directory=Path("/tmp/results"),
            model_specification=valid_model_spec_file,
            branch_configuration=valid_branch_config_file,
            artifact_path=valid_artifact_file,
        )
        assert result["type"] == "simulation"
        assert result["name"] == "sim"
        assert result["args"]["model_specification"] == str(valid_model_spec_file)
        assert result["args"]["branch_configuration"] == str(valid_branch_config_file)
        assert result["args"]["artifact_path"] == str(valid_artifact_file)
        assert result["resources"]["hardware"] == ["r650"]

    def test_serialize_omits_none_optional_fields(
        self,
        valid_model_spec_file: Path,
        valid_branch_config_file: Path,
    ) -> None:
        result = serialize_simulation_step_to_yaml(
            name="sim",
            resources=ResourceConfig(memory_gb=5, project="proj_simscience", queue="all.q"),
            output_directory=Path("/tmp/results"),
            model_specification=valid_model_spec_file,
            branch_configuration=valid_branch_config_file,
        )
        assert "artifact_path" not in result["args"]
        assert "hardware" not in result["resources"]

    def test_build_tasks_wires_arguments(
        self,
        valid_model_spec_file: Path,
        valid_branch_config_file: Path,
        valid_artifact_file: Path,
    ) -> None:
        """Verify build_simulation_step_tasks passes the right arguments through the pipeline."""
        _tb = "vivarium_cluster_tools.psimulate.workflow_config.task_builders"
        with (
            patch(f"{_tb}.OutputPaths") as mock_output_paths_cls,
            patch(f"{_tb}.branches.Keyspace") as mock_keyspace_cls,
            patch(f"{_tb}.build_job_parameters_from_keyspace") as mock_build_job_params,
            patch(f"{_tb}.get_task_list") as mock_get_task_list,
        ):
            # -- Arrange --
            mock_output_paths = MagicMock()
            mock_output_paths.root = Path("/out/root")
            mock_output_paths.worker_logging_root = Path("/out/logs")
            mock_output_paths.backup_dir = Path("/out/backup")
            mock_output_paths.backup_metadata_path = Path("/out/backup_meta.csv")
            mock_output_paths.metadata_dir = Path("/out/metadata")
            mock_output_paths.results_dir = Path("/out/results")
            mock_output_paths_cls.from_entry_point_args.return_value = mock_output_paths

            mock_keyspace = MagicMock()
            mock_keyspace_cls.from_branch_configuration.return_value = mock_keyspace

            sentinel_job_params = [MagicMock(), MagicMock()]
            mock_build_job_params.return_value = sentinel_job_params

            sentinel_tasks = [MagicMock(), MagicMock(), MagicMock()]
            mock_get_task_list.return_value = sentinel_tasks

            resources = ResourceConfig(
                memory_gb=8,
                runtime="02:00:00",
                project="proj_simscience",
                queue="all.q",
            )

            mock_tool = MagicMock()
            build_ts = "2026_04_24_10_00_00"

            # -- Act --
            result = build_simulation_step_tasks(
                name="sim_step",
                resources=resources,
                output_directory=Path("/tmp/results"),
                model_specification=valid_model_spec_file,
                branch_configuration=valid_branch_config_file,
                artifact_path=valid_artifact_file,
                backup_freq=300,
                sim_verbosity=1,
                tool=mock_tool,
                env_prefix="/envs/test_env",
                build_timestamp=build_ts,
            )

            # -- Assert: OutputPaths created correctly --
            mock_output_paths_cls.from_entry_point_args.assert_called_once_with(
                command="run",
                input_artifact_path=valid_artifact_file,
                result_directory=Path("/tmp/results"),
                input_model_spec_path=valid_model_spec_file,
                launch_time=build_ts,
                is_resume=False,
            )
            mock_output_paths.touch.assert_called_once()

            # -- Assert: Keyspace parsed from branch config --
            mock_keyspace_cls.from_branch_configuration.assert_called_once_with(
                valid_branch_config_file,
            )

            # -- Assert: job parameters built with correct args --
            mock_build_job_params.assert_called_once()
            call_kwargs = mock_build_job_params.call_args
            assert call_kwargs.args[0] is mock_keyspace
            assert call_kwargs.kwargs["model_specification_path"] == valid_model_spec_file
            assert call_kwargs.kwargs["output_root"] == Path("/out/root")
            assert call_kwargs.kwargs["worker_logging_root"] == Path("/out/logs")
            backup_cfg = call_kwargs.kwargs["backup_configuration"]
            assert backup_cfg["backup_dir"] == str(Path("/out/backup"))
            assert backup_cfg["backup_freq"] == 300
            assert backup_cfg["backup_metadata_path"] == str(Path("/out/backup_meta.csv"))
            assert call_kwargs.kwargs["extras"] == {"sim_verbosity": 1}

            # -- Assert: get_task_list called with pipeline outputs --
            mock_get_task_list.assert_called_once_with(
                tool=mock_tool,
                command="run",
                job_parameters_list=sentinel_job_params,
                metadata_dir=Path("/out/metadata"),
                results_dir=Path("/out/results"),
                worker_logging_root=Path("/out/logs"),
                native_specification=resources.to_native_specification("sim_step"),
                env_prefix="/envs/test_env",
                template_name="psimulate_sim_step",
            )

            # -- Assert: returns whatever get_task_list returns --
            assert result is sentinel_tasks

    def test_multiple_steps_register_distinct_jobmon_templates(
        self,
        valid_model_spec_file: Path,
        valid_branch_config_file: Path,
    ) -> None:
        """Two simulation steps in one workflow must register distinct
        Jobmon TaskTemplates so their ``create_tasks`` calls don't collide."""
        _tb = "vivarium_cluster_tools.psimulate.workflow_config.task_builders"
        _wf = "vivarium_cluster_tools.psimulate.jobmon_config.workflow"
        with (
            patch(f"{_tb}.OutputPaths") as mock_output_paths_cls,
            patch(f"{_tb}.branches.Keyspace") as mock_keyspace_cls,
            patch(f"{_tb}.build_job_parameters_from_keyspace") as mock_build_job_params,
            patch(f"{_wf}.write_metadata"),
        ):
            mock_output_paths = MagicMock()
            mock_output_paths.root = Path("/out/root")
            mock_output_paths.worker_logging_root = Path("/out/logs")
            mock_output_paths.backup_dir = Path("/out/backup")
            mock_output_paths.backup_metadata_path = Path("/out/backup_meta.csv")
            mock_output_paths.metadata_dir = Path("/out/metadata")
            mock_output_paths.results_dir = Path("/out/results")
            mock_output_paths_cls.from_entry_point_args.return_value = mock_output_paths
            mock_keyspace_cls.from_branch_configuration.return_value = MagicMock()
            mock_build_job_params.return_value = [MagicMock(task_id=0)]

            resources = ResourceConfig(
                memory_gb=4,
                runtime="00:20:00",
                project="proj_simscience",
                queue="all.q",
            )

            mock_tool = MagicMock()
            for step_name in ("run_sim_ethiopia", "run_sim_nigeria"):
                build_simulation_step_tasks(
                    name=step_name,
                    resources=resources,
                    output_directory=Path("/tmp/results"),
                    model_specification=valid_model_spec_file,
                    branch_configuration=valid_branch_config_file,
                    tool=mock_tool,
                    env_prefix="/envs/test_env",
                    build_timestamp="ts",
                )

            template_names = [
                call.kwargs["template_name"]
                for call in mock_tool.get_task_template.call_args_list
            ]
            assert template_names == [
                "psimulate_run_sim_ethiopia",
                "psimulate_run_sim_nigeria",
            ]
            assert len(set(template_names)) == len(template_names)


class TestPytestStep:
    """Tests for the pytest step type."""

    def test_rejects_neither_path_nor_k(self) -> None:
        with pytest.raises(ValueError, match="at least one of 'path' or 'k'"):
            validate_pytest_step(
                name="tests",
                resources=_common_resources(),
                output_directory=Path("/tmp/results"),
            )

    def test_build_task_command_is_wrapped_with_runner(self, valid_pytest_path: str) -> None:
        """build_pytest_step_tasks wraps the pytest command with the
        ``task_runner subprocess`` prefix so failing-test output appears in
        the SLURM stderr file."""
        mock_tool = MagicMock()
        mock_template = MagicMock()
        mock_tool.get_task_template.return_value = mock_template

        build_pytest_step_tasks(
            name="run_tests",
            resources=_common_resources(),
            output_directory=Path("/tmp/results"),
            path=valid_pytest_path,
            tool=mock_tool,
            env_prefix="/env",
            build_timestamp="2026_04_24_10_00_00",
        )

        cmd = mock_template.create_task.call_args.kwargs["command"]
        assert _SUBPROCESS_WRAPPER_PREFIX in cmd
        assert "pytest" in cmd  # inner command preserved

    def test_parse(self, valid_pytest_path: str) -> None:
        step_dict = make_pytest_step_dict(
            args={"path": valid_pytest_path, "k": "test_foo", "runslow": True},
            resources={"memory_gb": 8, "runtime": "02:00:00", "cores": 4},
        )
        kwargs = parse_pytest_step_from_yaml(
            step_dict,
            output_directory=Path("/tmp/results"),
            project="proj_simscience",
            queue="all.q",
        )
        assert kwargs["name"] == "run_tests"
        assert kwargs["path"] == valid_pytest_path
        assert kwargs["k"] == "test_foo"
        assert kwargs["runslow"] is True
        assert kwargs["resources"].cores == 4

    def test_parse_rejects_unsupported_args(self, valid_pytest_path: str) -> None:
        step_dict = make_pytest_step_dict(
            args={"path": valid_pytest_path, "bogus_flag": "nope"},
        )
        with pytest.raises(ValueError, match="unsupported args"):
            parse_pytest_step_from_yaml(
                step_dict,
                output_directory=Path("/tmp/results"),
                project="proj_simscience",
                queue="all.q",
            )

    def test_serialize(self, valid_pytest_path: str) -> None:
        result = serialize_pytest_step_to_yaml(
            name="tests",
            resources=ResourceConfig(
                memory_gb=8, project="proj_simscience", queue="all.q", cores=4
            ),
            output_directory=Path("/tmp/results"),
            path=valid_pytest_path,
            k="test_foo",
            runslow=True,
        )
        assert result == {
            "name": "tests",
            "type": "pytest",
            "resources": {
                "memory_gb": 8,
                "runtime": "01:00:00",
                "project": "proj_simscience",
                "queue": "all.q",
                "cores": 4,
            },
            "args": {
                "path": valid_pytest_path,
                "k": "test_foo",
                "runslow": True,
            },
        }

    def test_serialize_omits_unset_optional_fields(self, valid_pytest_path: str) -> None:
        result = serialize_pytest_step_to_yaml(
            name="tests",
            resources=_common_resources(),
            output_directory=Path("/tmp/results"),
            path=valid_pytest_path,
        )
        assert result["args"] == {"path": valid_pytest_path}

    def test_build_command_full(self, valid_pytest_path: str) -> None:
        command = _build_pytest_command(
            path=valid_pytest_path,
            k="test_foo or test_bar",
            runslow=True,
            cores=4,
        )
        assert command == (
            f"pytest {valid_pytest_path} -k 'test_foo or test_bar' --runslow --numprocesses 4"
        )

    def test_build_command_path_only(self, valid_pytest_path: str) -> None:
        assert (
            _build_pytest_command(path=valid_pytest_path, k=None, runslow=False, cores=1)
            == f"pytest {valid_pytest_path}"
        )

    def test_build_command_k_only(self) -> None:
        assert (
            _build_pytest_command(path=None, k="test_specific", runslow=False, cores=1)
            == "pytest -k test_specific"
        )

    def test_build_command_single_core_omits_numprocesses(self) -> None:
        assert "--numprocesses" not in _build_pytest_command(
            path="tests/", k=None, runslow=False, cores=1
        )

    def test_build_command_multiple_paths(self, valid_pytest_paths: list[str]) -> None:
        assert (
            _build_pytest_command(path=valid_pytest_paths, k=None, runslow=False, cores=1)
            == f"pytest {valid_pytest_paths[0]} {valid_pytest_paths[1]}"
        )

    def test_serialize_multiple_paths(self, valid_pytest_paths: list[str]) -> None:
        result = serialize_pytest_step_to_yaml(
            name="tests",
            resources=_common_resources(),
            output_directory=Path("/tmp/results"),
            path=valid_pytest_paths,
        )
        assert result["args"] == {"path": valid_pytest_paths}

    def test_parse_multiple_paths(self, valid_pytest_paths: list[str]) -> None:
        step_dict = make_pytest_step_dict(
            args={"path": valid_pytest_paths},
        )
        kwargs = parse_pytest_step_from_yaml(
            step_dict,
            output_directory=Path("/tmp/results"),
            project="proj_simscience",
            queue="all.q",
        )
        assert kwargs["path"] == valid_pytest_paths

    def test_routes_to_pytest_step_from_yaml(self, tmp_path: Path) -> None:
        steps = [make_pytest_step_dict()]
        workflow_dict = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, workflow_dict)

        config = load_workflow_config(yaml_path)
        assert len(config.steps) == 1
        assert config.steps[0].step_type == "pytest"
        assert config.steps[0].name == "run_tests"


class TestPythonStep:
    """Tests for the python step type."""

    @pytest.mark.parametrize(
        "extra_args, match",
        [
            ({"path": "script_no_ext"}, r"\.py"),  # non-.py path
            (
                {"keyword_args": {"nested": {"a": 1}}},
                "scalar",
            ),  # non-scalar keyword arg value
            (
                {"keyword_args": {"bad key!": "val"}},
                "identifier",
            ),  # shell metacharacters in keyword key
            (
                {"positional_args": "not_a_list"},
                "list",
            ),  # positional_args must be a list
            (
                {"positional_args": [{"nested": 1}]},
                "scalar",
            ),  # positional_args items must be scalar
            (
                {"keyword_args": ["not", "a", "dict"]},
                "dict",
            ),  # keyword_args must be a dict
        ],
        ids=[
            "non_py_path",
            "non_scalar_keyword_value",
            "invalid_keyword_key",
            "positional_not_list",
            "positional_non_scalar_item",
            "keyword_args_not_dict",
        ],
    )
    def test_validate_rejects_invalid_configurations(
        self, valid_python_script: str, extra_args: dict[str, Any], match: str
    ) -> None:
        kwargs: dict[str, Any] = {
            "name": "bad",
            "resources": _common_resources(),
            "output_directory": Path("/tmp/results"),
            "path": extra_args.pop("path", valid_python_script),
            **extra_args,
        }
        with pytest.raises(ValueError, match=match):
            validate_python_step(**kwargs)

    def test_validate_rejects_missing_path(self) -> None:
        with pytest.raises(ValueError, match="path"):
            validate_python_step(
                name="bad",
                resources=_common_resources(),
                output_directory=Path("/tmp/results"),
                path="",
            )

    def test_parse_rejects_unexpected_top_level_key(self, valid_python_script: str) -> None:
        step_dict = make_python_step_dict(
            args={"path": valid_python_script, "unknown_key": "val"},
        )
        with pytest.raises(ValueError, match="unsupported args"):
            parse_python_step_from_yaml(
                step_dict,
                output_directory=Path("/tmp/results"),
                project="proj_simscience",
                queue="all.q",
            )

    @pytest.mark.parametrize(
        "args, expected_command",
        [
            (
                {},
                "python {path}",
            ),
            (
                {"positional_args": ["cal_raleigh", "home_runs"]},
                "python {path} cal_raleigh home_runs",
            ),
            (
                {"keyword_args": {"alpha": "hello", "beta": 42}},
                "python {path} --alpha hello --beta 42",
            ),
            (
                {"keyword_args": {"verbose": True}},
                "python {path} --verbose",
            ),
            (
                {"keyword_args": {"debug": False}},
                "python {path}",
            ),
            (
                {"keyword_args": {"flag": None}},
                "python {path} --flag",
            ),
            (
                {"keyword_args": {"msg": "hello world"}},
                "python {path} --msg 'hello world'",
            ),
            (
                {
                    "positional_args": ["input.csv", 42],
                    "keyword_args": {"verbose": True, "output": "/tmp/out"},
                },
                "python {path} input.csv 42 --output /tmp/out --verbose",
            ),
        ],
        ids=[
            "path_only",
            "positional_args_only",
            "keyword_args_sorted",
            "bool_true_as_flag",
            "bool_false_omitted",
            "none_as_flag",
            "value_with_spaces_quoted",
            "positional_and_keyword_mixed",
        ],
    )
    def test_build_command(
        self, valid_python_script: str, args: dict[str, Any], expected_command: str
    ) -> None:
        command = _build_python_command(
            path=valid_python_script,
            positional_args=args.get("positional_args"),
            keyword_args=args.get("keyword_args"),
        )
        assert command == expected_command.format(path=valid_python_script)

    def test_parse(self, valid_python_script: str) -> None:
        step_dict = make_python_step_dict(
            args={
                "path": valid_python_script,
                "positional_args": ["/mnt/data"],
                "keyword_args": {"verbose": True},
            },
        )
        kwargs = parse_python_step_from_yaml(
            step_dict,
            output_directory=Path("/tmp/results"),
            project="proj_simscience",
            queue="all.q",
        )
        assert kwargs["name"] == "run_script"
        assert kwargs["path"] == valid_python_script
        assert kwargs["positional_args"] == ["/mnt/data"]
        assert kwargs["keyword_args"]["verbose"] is True

    def test_parse_rejects_missing_path(self) -> None:
        step_dict = make_python_step_dict()
        step_dict["args"] = {}
        with pytest.raises(ValueError, match="path"):
            parse_python_step_from_yaml(
                step_dict,
                output_directory=Path("/tmp/results"),
                project="proj_simscience",
                queue="all.q",
            )

    def test_parse_rejects_missing_args_key(self) -> None:
        step_dict = make_python_step_dict()
        del step_dict["args"]
        with pytest.raises(KeyError, match="args"):
            parse_python_step_from_yaml(
                step_dict,
                output_directory=Path("/tmp/results"),
                project="proj_simscience",
                queue="all.q",
            )

    def test_validate_rejects_none_in_positional_args(self, valid_python_script: str) -> None:
        with pytest.raises(ValueError, match="scalar"):
            validate_python_step(
                name="bad",
                resources=_common_resources(),
                output_directory=Path("/tmp/results"),
                path=valid_python_script,
                positional_args=[None],
            )

    def test_yaml_round_trip(self, valid_python_script: str) -> None:
        original_kwargs: dict[str, Any] = {
            "name": "run_script",
            "resources": _common_resources(),
            "output_directory": Path("/tmp/results"),
            "path": valid_python_script,
            "positional_args": ["/mnt/data"],
            "keyword_args": {"verbose": True},
        }
        serialized = serialize_python_step_to_yaml(**original_kwargs)
        restored = parse_python_step_from_yaml(
            serialized,
            output_directory=Path("/tmp/results"),
            project="proj_simscience",
            queue="all.q",
        )
        assert restored["name"] == original_kwargs["name"]
        assert restored["path"] == original_kwargs["path"]
        assert restored["positional_args"] == original_kwargs["positional_args"]
        assert restored["keyword_args"] == original_kwargs["keyword_args"]

    def test_routes_to_python_step_from_yaml(
        self, tmp_path: Path, valid_python_script: str
    ) -> None:
        steps = [make_python_step_dict(args={"path": valid_python_script})]
        workflow_dict = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, workflow_dict)

        config = load_workflow_config(yaml_path)
        assert len(config.steps) == 1
        assert config.steps[0].step_type == "python"
        assert config.steps[0].name == "run_script"


class TestNotebookStep:
    """Tests for the notebook step type."""

    @staticmethod
    def _base_kwargs(valid_notebook_path: Path) -> dict[str, Any]:
        return {
            "name": "run_notebook",
            "resources": _common_resources(),
            "output_directory": Path("/tmp/results"),
            "path": valid_notebook_path,
            "output_path": Path("/tmp/results/run_notebook.ipynb"),
        }

    @pytest.mark.parametrize(
        "overrides, match",
        [
            ({"path": Path("/tmp/foo.py")}, r"\.ipynb"),
            ({"output_path": Path("/tmp/out.txt")}, r"\.ipynb"),
            ({"parameters": [1, 2, 3]}, "dict"),
            ({"parameters": {"x": {"nested": 1}}}, "scalar"),
            ({"parameters": {"bad key!": 1}}, "identifier"),
            ({"parameters": {"hyphen-key": 1}}, "Python identifier"),
            ({"parameters": {"2startsdigit": 1}}, "Python identifier"),
        ],
        ids=[
            "non_ipynb_path",
            "non_ipynb_output_path",
            "parameters_not_dict",
            "non_scalar_parameter_value",
            "invalid_parameter_key",
            "hyphenated_parameter_key",
            "digit_start_parameter_key",
        ],
    )
    def test_validate_rejects_invalid_configurations(
        self,
        valid_notebook_path: Path,
        overrides: dict[str, Any],
        match: str,
    ) -> None:
        kwargs = {**self._base_kwargs(valid_notebook_path), **overrides}
        with pytest.raises(ValueError, match=match):
            validate_notebook_step(**kwargs)

    def test_parse_rejects_missing_output_path(self, valid_notebook_path: Path) -> None:
        step_dict = make_notebook_step_dict(
            args={"path": str(valid_notebook_path)},
        )
        with pytest.raises(ValueError, match="output_path"):
            parse_notebook_step_from_yaml(
                step_dict,
                output_directory=Path("/tmp/results"),
                project="proj_simscience",
                queue="all.q",
            )

    def test_parse_rejects_unsupported_args(self, valid_notebook_path: Path) -> None:
        step_dict = make_notebook_step_dict(
            args={"path": str(valid_notebook_path), "bogus": "nope"},
        )
        with pytest.raises(ValueError, match="unsupported args"):
            parse_notebook_step_from_yaml(
                step_dict,
                output_directory=Path("/tmp/results"),
                project="proj_simscience",
                queue="all.q",
            )

    @pytest.mark.parametrize(
        "field_overrides, expected_command_template",
        [
            (
                {},
                "mkdir -p {out_parent} && papermill {input} {output} -k python3 --cwd {input_parent}",
            ),
            (
                {"parameters": {"name": "alice"}},
                "mkdir -p {out_parent} && papermill {input} {output} -k python3"
                " -p name alice --cwd {input_parent}",
            ),
            (
                {"parameters": {"verbose": True}},
                "mkdir -p {out_parent} && papermill {input} {output} -k python3"
                " -y 'verbose: true' --cwd {input_parent}",
            ),
            (
                {"parameters": {"flag": False}},
                "mkdir -p {out_parent} && papermill {input} {output} -k python3"
                " -y 'flag: false' --cwd {input_parent}",
            ),
            (
                {"parameters": {"missing": None}},
                "mkdir -p {out_parent} && papermill {input} {output} -k python3"
                " -y 'missing: null' --cwd {input_parent}",
            ),
            (
                {"parameters": {"name": "alice", "verbose": True, "year": 2020}},
                "mkdir -p {out_parent} && papermill {input} {output} -k python3"
                " -p name alice -y 'verbose: true' -p year 2020 --cwd {input_parent}",
            ),
            (
                {"cwd": Path("/tmp/notebooks")},
                "mkdir -p {out_parent} && papermill {input} {output} -k python3 --cwd /tmp/notebooks",
            ),
            (
                {"parameters": {"msg": "hello world"}},
                "mkdir -p {out_parent} && papermill {input} {output} -k python3"
                " -p msg 'hello world' --cwd {input_parent}",
            ),
        ],
        ids=[
            "no_parameters_defaults_cwd_to_path_parent",
            "string_parameter",
            "bool_true_via_y",
            "bool_false_via_y",
            "none_via_y",
            "mixed_sorted_by_key",
            "with_cwd",
            "value_with_spaces_quoted",
        ],
    )
    def test_build_command(
        self,
        valid_notebook_path: Path,
        field_overrides: dict[str, Any],
        expected_command_template: str,
    ) -> None:
        base = self._base_kwargs(valid_notebook_path)
        output_path: Path = base["output_path"]
        command = _build_notebook_command(
            path=base["path"],
            output_path=output_path,
            parameters=field_overrides.get("parameters") or {},
            cwd=field_overrides.get("cwd"),
        )
        expected = expected_command_template.format(
            out_parent=output_path.parent,
            input=valid_notebook_path,
            input_parent=valid_notebook_path.parent,
            output=output_path,
        )
        assert command == expected

    def test_serialize(self, valid_notebook_path: Path) -> None:
        result = serialize_notebook_step_to_yaml(
            name="run_notebook",
            resources=ResourceConfig(
                memory_gb=8,
                runtime="02:00:00",
                project="proj_simscience",
                queue="all.q",
            ),
            output_directory=Path("/tmp/results"),
            path=valid_notebook_path,
            output_path=Path("/tmp/results/executed/run_notebook.ipynb"),
            parameters={"year": 2020, "verbose": True},
            cwd=Path("/tmp/notebooks"),
        )
        assert result["name"] == "run_notebook"
        assert result["type"] == "notebook"
        assert result["args"]["path"] == str(valid_notebook_path)
        assert result["args"]["parameters"] == {"year": 2020, "verbose": True}
        assert result["args"]["output_path"] == str(
            Path("/tmp/results/executed/run_notebook.ipynb")
        )
        assert result["args"]["cwd"] == str(Path("/tmp/notebooks"))

    def test_yaml_round_trip(self, valid_notebook_path: Path) -> None:
        original_kwargs: dict[str, Any] = {
            "name": "run_notebook",
            "resources": _common_resources(),
            "output_directory": Path("/tmp/results"),
            "path": valid_notebook_path,
            "output_path": Path("/tmp/results/run_notebook.ipynb"),
            "parameters": {"year": 2020},
        }
        serialized = serialize_notebook_step_to_yaml(**original_kwargs)
        restored = parse_notebook_step_from_yaml(
            serialized,
            output_directory=Path("/tmp/results"),
            project="proj_simscience",
            queue="all.q",
        )
        assert restored["name"] == original_kwargs["name"]
        assert restored["path"] == original_kwargs["path"]
        assert restored["parameters"] == original_kwargs["parameters"]
        assert restored["output_path"] == original_kwargs["output_path"]

    def test_parse_resolves_paths(self, tmp_path: Path, valid_notebook_path: Path) -> None:
        step_dict = make_notebook_step_dict(
            args={
                "path": str(valid_notebook_path),
                "output_path": "out.ipynb",
                "cwd": ".",
            },
        )
        kwargs = parse_notebook_step_from_yaml(
            step_dict,
            output_directory=Path("/tmp/results"),
            project="proj_simscience",
            queue="all.q",
        )
        assert kwargs["path"].is_absolute()
        assert kwargs["output_path"].is_absolute()
        assert kwargs["cwd"].is_absolute()

    def test_routes_to_notebook_step_from_yaml(
        self, tmp_path: Path, valid_notebook_path: Path
    ) -> None:
        steps = [
            make_notebook_step_dict(
                args={
                    "path": str(valid_notebook_path),
                    "output_path": "out.ipynb",
                },
            )
        ]
        workflow_dict = make_workflow_dict(steps=steps)
        yaml_path = write_workflow_yaml(tmp_path, workflow_dict)

        config = load_workflow_config(yaml_path)
        assert len(config.steps) == 1
        assert config.steps[0].step_type == "notebook"
        assert config.steps[0].name == "run_notebook"


class TestParsedStepValidatesPathExistence:
    """The python-step validator and others raise FileNotFoundError for missing paths."""

    def test_validate_python_step_rejects_nonexistent(self) -> None:
        with pytest.raises(FileNotFoundError, match="does not exist"):
            validate_python_step(
                name="bad",
                resources=_common_resources(),
                output_directory=Path("/tmp/results"),
                path="/nonexistent/script.py",
            )


class TestParseStepFromYaml:
    """Verify parse_step_from_yaml dispatches per step type."""

    def test_command_step(self) -> None:
        raw = make_step_dict(name="cmd", command="echo hi")
        step = parse_step_from_yaml(
            raw,
            output_directory=Path("/tmp/results"),
            project="proj_simscience",
            queue="all.q",
        )
        assert step.step_type == "command"
        assert step.name == "cmd"
        assert step.api_kwargs["command"] == "echo hi"
