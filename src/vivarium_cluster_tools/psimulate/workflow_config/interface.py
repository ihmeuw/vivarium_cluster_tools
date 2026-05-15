"""
=========================
Workflow Config Interface
=========================

Python API for building workflow step configurations programmatically,
as an alternative to authoring a YAML workflow file. Each function
returns an instance of the corresponding step config class.

"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from vivarium_cluster_tools.psimulate.workflow_config.config import (
    DEFAULT_BACKUP_FREQ_SECONDS,
    CommandStepConfig,
    NotebookStepConfig,
    PytestStepConfig,
    PythonStepConfig,
    ResourceConfig,
    SimulationStepConfig,
)


def get_command_step(
    *,
    name: str,
    resources: ResourceConfig,
    command: str,
    output_directory: Path,
    environment: str | None = None,
) -> CommandStepConfig:
    """[stub] Implement in Phase 2."""
    raise NotImplementedError


def get_simulation_step(
    *,
    name: str,
    resources: ResourceConfig,
    output_directory: Path,
    model_specification: Path,
    branch_configuration: Path,
    environment: str | None = None,
    artifact_path: Path | None = None,
    backup_freq: float | None = DEFAULT_BACKUP_FREQ_SECONDS,
    sim_verbosity: int = 0,
) -> SimulationStepConfig:
    """[stub] Implement in Phase 2."""
    raise NotImplementedError


def get_pytest_step(
    *,
    name: str,
    resources: ResourceConfig,
    output_directory: Path,
    environment: str | None = None,
    path: str | list[str] | None = None,
    k: str | None = None,
    runslow: bool = False,
) -> PytestStepConfig:
    """[stub] Implement in Phase 2."""
    raise NotImplementedError


def get_python_step(
    *,
    name: str,
    resources: ResourceConfig,
    output_directory: Path,
    path: str,
    environment: str | None = None,
    positional_args: list[Any] | None = None,
    keyword_args: dict[str, Any] | None = None,
) -> PythonStepConfig:
    """[stub] Implement in Phase 2."""
    raise NotImplementedError


def get_notebook_step(
    *,
    name: str,
    resources: ResourceConfig,
    output_directory: Path,
    path: Path,
    output_path: Path,
    environment: str | None = None,
    parameters: dict[str, Any] | None = None,
    cwd: Path | None = None,
) -> NotebookStepConfig:
    """[stub] Implement in Phase 2."""
    raise NotImplementedError
