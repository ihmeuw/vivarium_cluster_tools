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

REQUIRED_PIPELINE_FIELDS = {"name", "steps"}


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

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary, omitting None values and default cores."""
        result: dict[str, Any] = {}
        if self.memory is not None:
            result["memory"] = self.memory
        if self.runtime is not None:
            result["runtime"] = self.runtime
        if self.cores != 1:  # Only include if not default
            result["cores"] = self.cores
        return result


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

    def _validate(self) -> None:
        """Validate this step's internal consistency. Raise on errors."""
        # Validate step type if provided
        if self.type is not None and self.type not in SUPPORTED_STEP_TYPES:
            raise ValueError(
                f"Step '{self.name}': unsupported type '{self.type}'. "
                f"Must be one of {sorted(SUPPORTED_STEP_TYPES)}."
            )

        # type requires path
        if self.type is not None and self.path is None:
            raise ValueError(f"Step '{self.name}': 'type' requires 'path' to be provided.")

        # Must not have both command and type+path
        if self.is_raw_command and self.is_structured:
            raise ValueError(
                f"Step '{self.name}': provide 'command' OR 'type'+'path', not both."
            )

        # Must have at least one of command or type+path
        if not self.is_raw_command and not self.is_structured:
            raise ValueError(f"Step '{self.name}': must provide 'command' or 'type'+'path'.")

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary, omitting None values."""
        result: dict[str, Any] = {"name": self.name}

        # Add command or type+path (only non-None values)
        if self.command is not None:
            result["command"] = self.command
        if self.type is not None:
            result["type"] = self.type
        if self.path is not None:
            result["path"] = self.path
        if self.args is not None:
            result["args"] = self.args

        # Add environment if specified
        if self.environment is not None:
            result["environment"] = self.environment

        # Add resources if specified and non-empty
        if self.resources is not None:
            resources_dict = self.resources.to_dict()
            if resources_dict:  # Only add if there are non-default values
                result["resources"] = resources_dict

        return result


@dataclass
class PipelineConfig:
    """Parsed and validated pipeline configuration."""

    name: str
    project: str | None
    queue: str | None
    output_directory: Path | None
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
            step._validate()
            steps.append(step)

        config = cls(
            name=pipeline["name"],
            project=pipeline.get("project"),
            queue=pipeline.get("queue"),
            output_directory=Path(pipeline["output_directory"])
            if "output_directory" in pipeline
            else None,
            default_environment=pipeline.get("default_environment"),
            steps=steps,
        )
        config._validate()
        return config

    def _validate(self) -> None:
        """Validate pipeline-level constraints. Raise on errors."""
        # Unique step names
        names = [s.name for s in self.steps]
        if len(names) != len(set(names)):
            raise ValueError(
                f"Step names must be unique. Duplicate names found: {set([name for name in names if names.count(name) > 1])}"
            )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary suitable for YAML output."""
        result: dict[str, Any] = {
            "name": self.name,
        }

        if self.project is not None:
            result["project"] = self.project

        if self.queue is not None:
            result["queue"] = self.queue

        if self.output_directory is not None:
            result["output_directory"] = str(self.output_directory)

        if self.default_environment is not None:
            result["default_environment"] = self.default_environment

        result["steps"] = [step.to_dict() for step in self.steps]

        return result
