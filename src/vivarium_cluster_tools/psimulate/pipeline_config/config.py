"""
========================
Pipeline Config Parser
========================

Parse and validate pipeline YAML configuration files.

"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

SUPPORTED_STEP_TYPES = {"pytest", "notebook", "python", "shell"}

REQUIRED_PIPELINE_FIELDS = {"name", "project", "queue", "output_directory", "steps"}


@dataclass
class ResourceConfig:
    """Compute resource specification for a pipeline step."""

    memory: float | None = None
    runtime: str | None = None
    cores: int = 1

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> ResourceConfig | None:
        """Create a ResourceConfig from a dictionary, or return None."""
        if data is None:
            return None
        return cls(
            memory=data.get("memory"),
            runtime=data.get("runtime"),
            cores=data.get("cores", 1),
        )


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
        return self.type is not None and self.path is not None

    @property
    def is_raw_command(self) -> bool:
        """True if the step uses a raw command string."""
        return self.command is not None


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
        with open(path) as f:
            raw = yaml.safe_load(f)

        pipeline = raw["pipeline"]

        # Check required top-level fields
        for field_name in REQUIRED_PIPELINE_FIELDS:
            if field_name not in pipeline:
                raise ValueError(
                    f"Pipeline configuration is missing required field '{field_name}'."
                )

        raw_steps = pipeline["steps"]
        if not raw_steps:
            raise ValueError("Pipeline 'steps' must not be empty.")

        steps = []
        for step_dict in raw_steps:
            step = StepConfig(
                name=step_dict["name"],
                command=step_dict.get("command"),
                type=step_dict.get("type"),
                path=step_dict.get("path"),
                args=step_dict.get("args"),
                environment=step_dict.get("environment"),
                resources=ResourceConfig.from_dict(step_dict.get("resources")),
            )
            steps.append(step)

        config = cls(
            name=pipeline["name"],
            project=pipeline["project"],
            queue=pipeline["queue"],
            output_directory=Path(pipeline["output_directory"]),
            default_environment=pipeline.get("default_environment"),
            steps=steps,
        )
        config._validate()
        return config

    def _validate(self) -> None:
        """Validate the pipeline configuration. Raise on errors."""
        # Unique step names
        names = [s.name for s in self.steps]
        if len(names) != len(set(names)):
            raise ValueError("Step names must be unique.")

        for step in self.steps:
            # Validate step type if provided
            if step.type is not None and step.type not in SUPPORTED_STEP_TYPES:
                raise ValueError(
                    f"Step '{step.name}': unsupported type '{step.type}'. "
                    f"Must be one of {sorted(SUPPORTED_STEP_TYPES)}."
                )

            # type requires path
            if step.type is not None and step.path is None:
                raise ValueError(
                    f"Step '{step.name}': 'type' requires 'path' to be provided."
                )

            # Must not have both command and type+path
            if step.is_raw_command and step.is_structured:
                raise ValueError(
                    f"Step '{step.name}': provide 'command' OR 'type'+'path', not both."
                )

            # Must have at least one of command or type+path
            if not step.is_raw_command and not step.is_structured:
                raise ValueError(
                    f"Step '{step.name}': must provide 'command' or 'type'+'path'."
                )
