"""Shared fixtures for the pipeline_config test suite."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml


def make_step_dict(**overrides: Any) -> dict[str, Any]:
    """Create a minimal valid structured step dict with sensible defaults."""
    defaults: dict[str, Any] = {
        "name": "test_step",
        "type": "pytest",
        "path": "tests/test_something.py",
        "resources": {
            "memory": 4,
            "runtime": "01:00:00",
        },
    }
    defaults.update(overrides)
    return defaults


def make_pipeline_dict(**overrides: Any) -> dict[str, Any]:
    """Create a valid minimal pipeline config dict.

    Returns a dict with the ``pipeline`` top-level key containing
    two steps: one structured (pytest) and one raw command.
    Override any top-level pipeline field or replace ``steps`` entirely.
    """
    steps = overrides.pop("steps", None)
    if steps is None:
        steps = [
            {
                "name": "pre_tests",
                "type": "pytest",
                "path": [
                    "tests/test_lbwsg.py",
                    "tests/test_mortality.py",
                ],
                "args": "--runslow",
                "resources": {
                    "memory": 10,
                    "runtime": "01:00:00",
                },
            },
            {
                "name": "post_analysis",
                "command": "python scripts/analyze.py --input /results",
                "environment": "analysis_env",
                "resources": {
                    "memory": 20,
                    "runtime": "02:00:00",
                    "cores": 2,
                },
            },
        ]

    pipeline: dict[str, Any] = {
        "name": "test_pipeline",
        "project": "proj_simscience",
        "queue": "all.q",
        "output_directory": "/tmp/results",
        "steps": steps,
    }
    pipeline.update(overrides)
    return {"pipeline": pipeline}


def write_pipeline_yaml(tmp_path: Path, data: dict[str, Any]) -> Path:
    """Write a pipeline config dict to a YAML file and return the path."""
    yaml_path = tmp_path / "pipeline.yaml"
    yaml_path.write_text(yaml.dump(data, sort_keys=False))
    return yaml_path


@pytest.fixture()
def valid_pipeline_dict() -> dict[str, Any]:
    """A valid minimal pipeline config dict."""
    return make_pipeline_dict()


@pytest.fixture()
def valid_pipeline_yaml(tmp_path: Path, valid_pipeline_dict: dict[str, Any]) -> Path:
    """Write the valid pipeline dict to a YAML file and return the path."""
    return write_pipeline_yaml(tmp_path, valid_pipeline_dict)
