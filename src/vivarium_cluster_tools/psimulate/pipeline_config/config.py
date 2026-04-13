"""
========================
Pipeline Config Parser
========================

Parse and validate pipeline YAML configuration files.

"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

SUPPORTED_STEP_TYPES = {"pytest", "notebook", "python", "shell"}


@dataclass
class ResourceConfig:
    """Compute resource specification for a pipeline step."""

    memory: float | None = None
    runtime: str | None = None
    cores: int = 1

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> ResourceConfig | None:
        """Create a ResourceConfig from a dictionary, or return None."""
        raise NotImplementedError


@dataclass
class StepConfig:
    """Configuration for a single pipeline step."""

    name: str
    command: str | None = None
    type: str | None = None
    path: str | list[str] | None = None
    args: str | None = None
    environment: str | None = None
    resources: ResourceConfig | None = None

    @property
    def is_structured(self) -> bool:
        """True if the step uses type + path."""
        raise NotImplementedError

    @property
    def is_raw_command(self) -> bool:
        """True if the step uses a raw command string."""
        raise NotImplementedError


@dataclass
class PipelineConfig:
    """Parsed and validated pipeline configuration."""

    name: str
    project: str
    queue: str
    output_directory: Path
    default_environment: str | None
    steps: list[StepConfig]

    @classmethod
    def from_yaml(cls, path: Path) -> PipelineConfig:
        """Load, validate, and return a PipelineConfig from a YAML file."""
        raise NotImplementedError
