"""
========================
Workflow Config Parser
========================

Parse and validate workflow YAML configuration files.

"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REQUIRED_WORKFLOW_FIELDS = {"name", "steps"}


@dataclass
class ResourceConfig:
    """Compute resource specification for a workflow step."""

    memory_gb: float = 4
    """Memory in GB. Default is 4."""
    runtime: str = "01:00:00"
    """Maximum runtime in 'hh:mm:ss' format. Default is '01:00:00'."""
    cores: int = 1
    """Number of CPU cores to request. Default is 1."""

    _RUNTIME_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")

    def __post_init__(self) -> None:
        if not self._RUNTIME_RE.match(self.runtime):
            raise ValueError(f"Invalid runtime '{self.runtime}'. Expected format 'hh:mm:ss'.")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ResourceConfig:
        """Create a ResourceConfig from a dictionary."""
        return cls(
            memory_gb=data.get("memory_gb", 4),
            runtime=data.get("runtime", "01:00:00"),
            cores=data.get("cores", 1),
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary, omitting None values and default cores."""
        result: dict[str, Any] = {}
        if self.memory_gb is not None:
            result["memory_gb"] = self.memory_gb
        if self.runtime is not None:
            result["runtime"] = self.runtime
        if self.cores != 1:  # Only include if not default
            result["cores"] = self.cores
        return result


@dataclass
class StepConfig:
    """Configuration for a single workflow step."""

    name: str
    """Unique name for this step within the workflow."""
    resources: ResourceConfig
    """Resource configuration for this step."""
    command: str
    """Command string to execute for this step."""
    environment: str | None = None
    """Optional environment name to use for this step."""

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary, omitting None values."""
        result: dict[str, Any] = {"name": self.name}

        result["command"] = self.command

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
class WorkflowConfig:
    """Parsed and validated workflow configuration."""

    name: str
    """Name of the workflow. This is what will be displayed in Jobmon"""
    project: str | None
    """Project that this workflow will be run under. E.g. 'proj_simscience'."""
    queue: str | None
    """Queue to submit the workflow to."""
    output_directory: Path | None
    """Directory where workflow outputs will be stored."""
    default_environment: str | None
    """Default environment to use for steps that do not specify one."""
    steps: list[StepConfig]
    """List of sequential steps in the workflow."""

    @classmethod
    def from_yaml(cls, path: Path) -> WorkflowConfig:
        """Load, validate, and return a WorkflowConfig from a YAML file."""
        with path.open() as f:
            raw = yaml.safe_load(f)

        if not isinstance(raw, dict) or "workflow" not in raw:
            raise KeyError("Workflow configuration must contain a top-level 'workflow' key.")

        workflow = raw["workflow"]

        # Check required top-level fields
        for field_name in REQUIRED_WORKFLOW_FIELDS:
            if field_name not in workflow:
                raise ValueError(
                    f"Workflow configuration is missing required field '{field_name}'."
                )

        raw_steps = workflow["steps"]
        if not raw_steps:
            raise ValueError("Workflow 'steps' must not be empty.")

        steps = []
        for step_dict in raw_steps:
            step_name = step_dict["name"]
            raw_resources = step_dict.get("resources")
            if raw_resources is None:
                raise ValueError(f"Step '{step_name}': 'resources' is required.")
            command = step_dict.get("command")
            if command is None:
                raise ValueError(f"Step '{step_name}': 'command' is required.")
            step = StepConfig(
                name=step_name,
                resources=ResourceConfig.from_dict(raw_resources),
                command=command,
                environment=step_dict.get("environment"),
            )
            steps.append(step)

        config = cls(
            name=workflow["name"],
            project=workflow.get("project"),
            queue=workflow.get("queue"),
            output_directory=Path(workflow["output_directory"])
            if "output_directory" in workflow
            else None,
            default_environment=workflow.get("default_environment"),
            steps=steps,
        )
        config._validate()
        return config

    def _validate(self) -> None:
        """Validate workflow-level constraints."""
        # Unique step names
        names = [step.name for step in self.steps]
        if len(names) != len(set(names)):
            raise ValueError(
                f"Step names must be unique. Duplicate names found: {[name for name in names if names.count(name) > 1]}"
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
