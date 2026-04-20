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

SUPPORTED_STEP_TYPES = {"pytest", "notebook", "python", "shell"}
# NOTE: Each step type will map to a specific execution strategy. Pytest will run pytest
# test suites, notebook will execute Juypter notebooks, python will run Python scripts,
# and shell will execute raw shell commands. Users will only need to know the support types,
# and on the backend developers can choose how these are implemented, leaving room for future flexibility.

REQUIRED_WORKFLOW_FIELDS = {"name", "project", "queue", "output_directory", "steps"}


@dataclass
class ResourceConfig:
    """Compute resource specification for a workflow step."""

    memory_gb: float | None = None
    """Memory in GB."""
    runtime: str | None = None
    """Maximum runtime in 'hh:mm:ss' format."""
    cores: int = 1
    """Number of CPU cores to request. Default is 1."""

    _RUNTIME_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")

    def __post_init__(self) -> None:
        if self.runtime is not None and not self._RUNTIME_RE.match(self.runtime):
            raise ValueError(f"Invalid runtime '{self.runtime}'. Expected format 'hh:mm:ss'.")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ResourceConfig:
        """Create a ResourceConfig from a dictionary."""
        return cls(
            memory_gb=data.get("memory_gb"),
            runtime=data.get("runtime"),
            cores=data.get("cores", 1),
        )


@dataclass
class StepConfig:
    """Configuration for a single workflow step."""

    name: str
    """Unique name for this step within the workflow."""
    resources: ResourceConfig
    """Resource configuration for this step."""
    command: str | None = None
    """Raw command string to execute for this step. Mutually exclusive with 'type' and 'path'."""
    type: str | None = None
    """Structured step type (e.g. 'pytest', 'notebook'). Requires 'path' to be provided."""
    path: str | list[str] | None = None
    """Path(s) to the module or directory for structured steps. Required if 'type' is provided."""
    args: str | None = None
    """Optional additional arguments for structured steps, passed as a single string."""
    environment: str | None = None
    """Optional environment name to use for this step."""

    @property
    def is_structured(self) -> bool:
        """True if the step uses type + path."""
        return self.type is not None and self.path is not None

    @property
    def is_raw_command(self) -> bool:
        """True if the step uses a raw command string."""
        return self.command is not None

    def _validate(self) -> None:
        """Validate this step's internal consistency."""
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

        # Command should not be mixed with type, path, or args
        if self.command is not None and (
            self.type is not None or self.path is not None or self.args is not None
        ):
            raise ValueError(
                f"Step '{self.name}': 'command' cannot be combined with 'type', 'path', or 'args'. "
                "Use 'command' alone for raw commands, or 'type'+'path' for structured steps."
            )

        # Must have at least one of command or type+path
        if not self.is_raw_command and not self.is_structured:
            raise ValueError(f"Step '{self.name}': must provide 'command' or 'type'+'path'.")


@dataclass
class WorkflowConfig:
    """Parsed and validated workflow configuration."""

    name: str
    """Name of the workflow. This is what will be displayed in Jobmon"""
    project: str
    """Project that this workflow will be run under. E.g. 'proj_simscience'."""
    queue: str
    """Queue to submit the workflow to."""
    output_directory: Path
    """Directory where workflow outputs will be stored."""
    default_environment: str | None
    """Default environment to use for steps that do not specify one."""
    steps: list[StepConfig]
    """List of steps in the workflow."""

    @classmethod
    def from_yaml(cls, path: Path) -> WorkflowConfig:
        """Load, validate, and return a WorkflowConfig from a YAML file."""
        with path.open() as f:
            raw = yaml.safe_load(f)

        if not isinstance(raw, dict) or "workflow" not in raw:
            raise ValueError(
                "Workflow configuration must contain a top-level 'workflow' key."
            )

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
            step = StepConfig(
                name=step_name,
                resources=ResourceConfig.from_dict(raw_resources),
                command=step_dict.get("command"),
                type=step_dict.get("type"),
                path=step_dict.get("path"),
                args=step_dict.get("args"),
                environment=step_dict.get("environment"),
            )
            step._validate()
            steps.append(step)

        config = cls(
            name=workflow["name"],
            project=workflow["project"],
            queue=workflow["queue"],
            output_directory=Path(workflow["output_directory"]),
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
