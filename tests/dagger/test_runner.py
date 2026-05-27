"""Unit tests for the dagger runner."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from vivarium_cluster_tools.dagger.runner import write_workflow_configuration


def _read_configuration_yaml(output_root: Path) -> dict[str, Any]:
    """Read and parse the configuration.yaml written by ``write_workflow_configuration``."""
    config_file = output_root / "configuration.yaml"
    assert config_file.exists()
    result: dict[str, Any] = yaml.safe_load(config_file.read_text())
    return result


def test_write_workflow_configuration_writes_round_trippable_yaml(tmp_path: Path) -> None:
    """``write_workflow_configuration`` writes a YAML that captures every
    top-level workflow field plus the step list."""
    output_dir = tmp_path / "workflow_output"
    output_dir.mkdir()

    from vivarium_cluster_tools.psimulate.workflow_config.config import (
        ParsedStep,
        ResourceConfig,
        WorkflowConfig,
    )

    step_kwargs: dict[str, Any] = {
        "name": "test_step",
        "command": "pytest tests/",
        "resources": ResourceConfig(
            memory_gb=4,
            runtime="01:00:00",
            project="proj_simscience",
            queue="all.q",
        ),
        "output_directory": output_dir,
        "environment": None,
    }
    workflow_config = WorkflowConfig(
        name="test_workflow",
        project="proj_simscience",
        queue="all.q",
        output_directory=output_dir,
        default_environment=None,
        steps=[
            ParsedStep(
                step_type="bash",
                name=step_kwargs["name"],
                api_kwargs=step_kwargs,
            )
        ],
    )

    write_workflow_configuration(output_dir, workflow_config)

    config = _read_configuration_yaml(output_dir)
    assert config["workflow"]["name"] == "test_workflow"
    assert config["workflow"]["project"] == "proj_simscience"
    assert config["workflow"]["queue"] == "all.q"
    assert config["workflow"]["output_directory"] == str(output_dir)
    assert config["workflow"]["max_attempts"] == 2
    assert len(config["workflow"]["steps"]) == 1
    assert config["workflow"]["steps"][0]["name"] == "test_step"
    assert config["workflow"]["steps"][0]["command"] == "pytest tests/"


@pytest.mark.skip(reason="dagger CLI lands in phase 5; will be re-enabled then")
def test_workflow_configuration_includes_cli_overrides(tmp_path: Path) -> None:
    """CLI overrides are reflected in the written configuration.yaml."""
    from vivarium_cluster_tools.dagger.cli import dagger  # noqa: F401 -- pulled by phase 5

    output_dir = tmp_path / "workflow_output"
    output_dir.mkdir()

    pipeline_yaml = tmp_path / "pipeline.yaml"
    pipeline_yaml.write_text(
        yaml.dump(
            {
                "workflow": {
                    "name": "test_workflow",
                    "project": "proj_simscience",
                    "queue": "all.q",
                    "output_directory": str(output_dir),
                    "steps": [
                        {
                            "name": "test_step",
                            "command": "echo test",
                            "resources": {"memory_gb": 4},
                        }
                    ],
                }
            }
        )
    )

    cli_runner = CliRunner()
    with patch("vivarium_cluster_tools.dagger.runner.workflow_main") as mock_workflow_main:

        def mock_impl(**kwargs: Any) -> None:
            write_workflow_configuration(output_dir, kwargs["workflow_config"])

        mock_workflow_main.side_effect = mock_impl

        result = cli_runner.invoke(
            dagger,
            [
                "run",
                "--config",
                str(pipeline_yaml),
                "-P",
                "proj_simscience_prod",
                "-q",
                "long.q",
            ],
        )

    assert result.exit_code == 0, result.output

    config = _read_configuration_yaml(output_dir)
    assert config["workflow"]["project"] == "proj_simscience_prod"
    assert config["workflow"]["queue"] == "long.q"
