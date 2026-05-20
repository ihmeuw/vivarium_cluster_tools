"""
========================
Workflow Config Parser
========================

Parse and validate workflow YAML configuration files.

"""

from __future__ import annotations

import copy
import re
import shlex
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

import yaml

from vivarium_cluster_tools.psimulate import COMMANDS, TASK_RUNNER_MODULE, branches
from vivarium_cluster_tools.psimulate.cluster.interface import NativeSpecification
from vivarium_cluster_tools.psimulate.cluster.validation import (
    validate_hardware,
    validate_project,
    validate_runtime_and_queue,
)
from vivarium_cluster_tools.psimulate.jobmon_config.workflow import get_task_list
from vivarium_cluster_tools.psimulate.jobs import (
    BackupConfiguration,
    build_job_parameters_from_keyspace,
)
from vivarium_cluster_tools.psimulate.paths import OutputPaths
from vivarium_cluster_tools.psimulate.workflow_config.utilities import (
    check_scalar,
    validate_scalar_dict,
)

if TYPE_CHECKING:
    from jobmon.client.api import Tool
    from jobmon.client.task import Task

REQUIRED_WORKFLOW_FIELDS = {"name", "steps"}

DEFAULT_MAX_ATTEMPTS = 2


DEFAULT_BACKUP_FREQ_SECONDS = 30.0 * 60.0
"""Default backup frequency in seconds (30 minutes), matching ``psimulate run``."""


@dataclass
class ResourceConfig:
    """Compute resource specification for a workflow step."""

    memory_gb: int
    """Memory in GB."""
    project: str | None = None
    """Cluster project to charge. Falls back to the workflow-level project."""
    queue: str | None = None
    """Cluster queue to submit to. Falls back to the workflow-level queue."""
    runtime: str = "01:00:00"
    """Maximum runtime in ``hh:mm:ss`` format. Default is ``01:00:00``."""
    cores: int = 1
    """Number of CPU cores to request. Default is 1."""
    hardware: list[str] | None = None
    """Optional list of hardware types to target (e.g. ``["r650", "r650v2"]``)."""
    requires_archive_node: bool = False
    """Whether to enforce landing on an archive node."""

    _RUNTIME_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")

    def __post_init__(self) -> None:
        if not self._RUNTIME_RE.match(self.runtime):
            raise ValueError(
                f"Invalid runtime '{self.runtime}'. Expected format ``hh:mm:ss``."
            )
        if self.project is not None:
            validate_project(self.project)
        if self.queue is not None or self.project is not None:
            # Validate runtime against queue if queue is specified at step level
            if self.queue is not None:
                validate_runtime_and_queue(self.runtime, self.queue)
        if self.hardware is not None:
            validate_hardware(self.hardware)
        if not isinstance(self.requires_archive_node, bool):
            raise TypeError(
                f"'requires_archive_node' must be a bool, "
                f"got {type(self.requires_archive_node).__name__}: "
                f"{self.requires_archive_node!r}."
            )

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        *,
        workflow_project: str | None = None,
        workflow_queue: str | None = None,
    ) -> ResourceConfig:
        """Create a ResourceConfig from a dictionary.

        Step-level values take precedence; workflow-level defaults fill in
        any that are absent.

        Parameters
        ----------
        data
            Resource dictionary from a step's ``resources`` section.
        workflow_project
            Workflow-level project used as fallback.
        workflow_queue
            Workflow-level queue used as fallback.
        """
        kwargs: dict[str, Any] = {"memory_gb": data["memory_gb"]}
        kwargs["project"] = data.get("project") or workflow_project
        kwargs["queue"] = data.get("queue") or workflow_queue
        if "runtime" in data:
            kwargs["runtime"] = data["runtime"]
        if "cores" in data:
            kwargs["cores"] = data["cores"]
        if "hardware" in data:
            kwargs["hardware"] = data["hardware"]
        if "requires_archive_node" in data:
            kwargs["requires_archive_node"] = data["requires_archive_node"]
        return cls(**kwargs)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary, omitting None values and defaults."""
        result: dict[str, Any] = {
            "memory_gb": self.memory_gb,
            "runtime": self.runtime,
        }
        if self.project is not None:
            result["project"] = self.project
        if self.queue is not None:
            result["queue"] = self.queue
        if self.cores != 1:  # Only include if not default
            result["cores"] = self.cores
        if self.hardware is not None:
            result["hardware"] = self.hardware
        if self.requires_archive_node:
            result["requires_archive_node"] = True
        return result

    def to_native_specification(self, job_name: str) -> NativeSpecification:
        """Convert to a :class:`~vivarium_cluster_tools.psimulate.cluster.interface.NativeSpecification` for Jobmon task submission.

        Parameters
        ----------
        job_name
            The SLURM job name for this step's tasks.
        """
        if not isinstance(self.project, str) or not isinstance(self.queue, str):
            raise TypeError(
                f"'project' and 'queue' must be strings, got "
                f"project={self.project!r}, queue={self.queue!r}."
            )
        return NativeSpecification(
            job_name=job_name,
            project=self.project,
            queue=self.queue,
            peak_memory=float(self.memory_gb),
            max_runtime=self.runtime,
            hardware=self.hardware or [],
            cores=self.cores,
            requires_archive_node=self.requires_archive_node,
        )


class BaseStepConfig(ABC):
    """Abstract base class for all workflow step configurations.

    Concrete subclasses **must** be decorated with ``@dataclass``. The base
    class defines the runtime interface (``get_tasks``, ``native_specification``)
    used by API functions to produce Jobmon tasks. Validation and YAML
    (de)serialization live in classmethods so that the YAML parser can run
    them without constructing an instance.
    """

    name: str
    resources: ResourceConfig
    output_directory: Path
    environment: str | None

    _SUPPORTED_ARGS: ClassVar[set[str] | None] = None
    """Arguments supported in the 'args' section of the step configuration. Arguments not
    in this set will be rejected with a validation error."""

    @property
    def native_specification(self) -> NativeSpecification:
        """The Jobmon-facing resource specification for this step."""
        return self.resources.to_native_specification(self.name)

    @classmethod
    def _validate_common(cls, name: str, resources: ResourceConfig) -> None:
        """Validation shared by every step type: name + resource resolution."""
        if not name:
            raise ValueError("Step 'name' is required.")
        if not resources:
            raise ValueError(f"Step '{name}': 'resources' is required.")
        if not isinstance(resources.queue, str) or not isinstance(resources.project, str):
            raise ValueError(
                f"Step '{name}': resources 'queue' and 'project' must be "
                "configured. Set them at the step level or provide workflow-level defaults."
            )

    @staticmethod
    def _validate_required_paths(name: str, paths: list[Path]) -> None:
        """Raise FileNotFoundError if any path does not exist."""
        for path in paths:
            if not path.exists():
                raise FileNotFoundError(f"Step '{name}': path does not exist: {path}")

    # Each concrete subclass defines:
    #
    #   @classmethod
    #   def validate(cls, *, <api kwargs>) -> None: ...
    #
    #   @classmethod
    #   def to_yaml_dict(cls, *, <api kwargs>) -> dict[str, Any]: ...
    #
    # The signatures match the corresponding ``get_*_step_tasks`` function,
    # so they can't satisfy a single base-class signature (LSP). They live
    # on each subclass directly; the YAML parser and API functions dispatch
    # to them via the ``STEP_TYPES`` registry.

    @classmethod
    @abstractmethod
    def kwargs_from_yaml(
        cls,
        data: dict[str, Any],
        output_directory: Path,
        *,
        project: str,
        queue: str,
    ) -> dict[str, Any]:
        """Parse a raw YAML step dict into kwargs for the matching API function.

        Does no validation beyond the type-level path/string coercions
        needed to land in the API kwargs shape; semantic validation lives
        in each subclass's ``validate`` classmethod.
        """
        pass

    @classmethod
    def _check_supported_args(cls, args: dict[str, Any], step_name: str) -> None:
        """Raise ValueError if args contains keys not in _SUPPORTED_ARGS."""
        if cls._SUPPORTED_ARGS is None:
            return
        unsupported = set(args) - cls._SUPPORTED_ARGS
        if unsupported:
            raise ValueError(
                f"Step '{step_name}': unsupported args {sorted(unsupported)}. "
                f"Supported args: {sorted(cls._SUPPORTED_ARGS)}."
            )

    def get_tasks(
        self,
        tool: Tool,
        *,
        env_prefix: str,
        build_timestamp: str,
        is_resume: bool = False,
    ) -> list[Task]:
        """Create Jobmon Tasks for this step.

        The default implementation creates a single task by calling
        ``_build_command``.  Subclasses that need multiple tasks
        (e.g. simulation steps) should override this method.

        Parameters
        ----------
        tool
            The Jobmon Tool instance to create task templates from.
        env_prefix
            Absolute filesystem path to the conda environment prefix.
            Resolved on the runner so worker commands can invoke the env's
            ``bin/`` directly without needing ``conda`` on worker PATH.
        build_timestamp
            Stable timestamp string (``YYYY_MM_DD_HH_MM_SS``) generated once
            per workflow build. Steps that create output directories should
            use this to ensure paths are deterministic across resume builds.
        is_resume
            Whether this is a resumed workflow build. When True, simulation
            steps generate a fresh logging directory timestamp.

        Returns
        -------
            A list of Jobmon Task instances ready to be added to a workflow.
        """
        return [
            self._create_single_command_task(
                tool, env_prefix=env_prefix, command=self._build_command()
            )
        ]

    @abstractmethod
    def _build_command(self) -> str:
        """Build the command string for this step.

        Returns
        -------
            The shell command to execute.
        """
        pass

    def _wrap_for_logging(self, command: str) -> str:
        """Prepend the ``task_runner subprocess`` wrapper.

        The returned command runs through
        :mod:`vivarium_cluster_tools.psimulate.worker.task_runner` so the
        child's output is replayed to the SLURM stderr file on failure
        (and thus surfaces in the Jobmon GUI).

        The module path is pulled from ``psimulate.TASK_RUNNER_MODULE``
        rather than imported from ``task_runner`` itself, so workflow-config
        parsing does not pay for ``task_runner``'s transitive work-horse
        imports (``vivarium.framework.engine``, ``dill``, ``pandas``).
        """
        return f"python -m {TASK_RUNNER_MODULE} subprocess {command}"

    def _create_single_command_task(
        self, tool: Tool, *, env_prefix: str, command: str
    ) -> Task:
        """Create a single Jobmon task that runs a command in a conda env.

        Parameters
        ----------
        tool
            The Jobmon Tool instance to create task templates from.
        env_prefix
            Absolute filesystem path to the conda environment prefix.
            Prepended to the worker's PATH so binaries from the env
            (``python``, ``pytest``, etc.) resolve correctly.
        command
            The command string to execute.

        Returns
        -------
            A Jobmon Task instance.
        """
        task_template = tool.get_task_template(
            template_name="workflow_command_step",
            command_template="PATH={env_prefix}/bin:$PATH {command}",
            node_args=["command", "env_prefix"],
            task_args=[],
            op_args=[],
            default_cluster_name="slurm",
        )
        compute_resources = self.native_specification.to_jobmon_spec(
            worker_logging_root=self.output_directory,
        )
        return task_template.create_task(
            name=self.name,
            compute_resources=compute_resources,
            env_prefix=env_prefix,
            command=self._wrap_for_logging(command),
        )


@dataclass
class CommandStepConfig(BaseStepConfig):
    """Configuration for a command-based workflow step.

    Step type for steps that provide a raw command string.
    The command is executed as-is (wrapped with conda run by WorkflowBuilder).
    """

    name: str
    """Unique name for this step within the workflow."""
    resources: ResourceConfig
    """Resource configuration for this step."""
    command: str
    """Command string to execute for this step."""
    output_directory: Path
    """Output directory for this step. Inherited from the workflow's output_directory."""
    environment: str | None = None
    """Optional environment name to use for this step."""

    @classmethod
    def validate(
        cls,
        *,
        name: str,
        resources: ResourceConfig,
        command: str,
        output_directory: Path,
        environment: str | None = None,
    ) -> None:
        """Validate kwargs for :func:`~vivarium_cluster_tools.psimulate.workflow_config.interface.get_command_step_tasks`."""
        cls._validate_common(name, resources)
        if not command:
            raise ValueError(f"Step '{name}': 'command' is required.")

    @classmethod
    def to_yaml_dict(
        cls,
        *,
        name: str,
        resources: ResourceConfig,
        command: str,
        output_directory: Path,
        environment: str | None = None,
    ) -> dict[str, Any]:
        """Serialize command-step kwargs to a YAML-ready dict."""
        result: dict[str, Any] = {
            "name": name,
            "command": command,
            "resources": resources.to_dict(),
        }
        if environment is not None:
            result["environment"] = environment
        return result

    @classmethod
    def kwargs_from_yaml(
        cls,
        data: dict[str, Any],
        output_directory: Path,
        *,
        project: str,
        queue: str,
    ) -> dict[str, Any]:
        """Parse a raw command-step YAML dict into API kwargs.

        The YAML form for a command step requires a top-level ``command``
        field. The optional ``type`` field, when present, must be ``"command"``.
        """
        step_name = data.get("name", "<unnamed>")
        cls._check_supported_args(data.get("args", {}), step_name)
        if "command" not in data:
            raise ValueError(
                f"Step '{step_name}': command-based steps require a top-level "
                "'command' field."
            )
        explicit_type = data.get("type")
        if explicit_type is not None and explicit_type != "command":
            raise ValueError(
                f"Step '{step_name}': cannot specify both 'command' and "
                f"'type: {explicit_type}'. When 'command' is set, 'type' "
                "must be omitted or set to 'command'."
            )
        return {
            "name": data["name"],
            "resources": ResourceConfig.from_dict(
                data["resources"], workflow_project=project, workflow_queue=queue
            ),
            "command": data["command"],
            "output_directory": output_directory,
            "environment": data.get("environment"),
        }

    def _build_command(self) -> str:
        """Return the raw command string."""
        return self.command


@dataclass
class SimulationStepConfig(BaseStepConfig):
    """Configuration for a parallel simulation workflow step.

    This step type directly creates parallel simulation tasks — one per
    (input_draw, random_seed, branch) combination — rather than launching
    ``psimulate run`` as a subprocess. The tasks use the same task runner
    infrastructure as ``psimulate run``.

    The ``resources`` field specifies compute resources for each individual
    simulation task (memory, runtime, cores).

    Examples
    --------
    Inline configuration::

        steps:
          - name: model_sims
            type: simulation
            resources:
              memory_gb: 3
              runtime: "24:00:00"
            args:
              model_specification: /path/to/model.yaml
              branch_configuration: /path/to/branches.yaml
              artifact_path: /path/to/artifact.hdf
              backup_freq: 1800
              sim_verbosity: 1
    """

    _SUPPORTED_ARGS: ClassVar[set[str]] = {
        "model_specification",
        "branch_configuration",
        "artifact_path",
        "backup_freq",
        "sim_verbosity",
    }

    name: str
    """Unique name for this step within the workflow."""
    resources: ResourceConfig
    """Compute resources for each individual simulation task."""
    output_directory: Path
    """Output directory for this step. Inherited from the workflow's output_directory."""
    model_specification: Path
    """Path to model specification YAML file. Both relative and absolute paths are
    accepted."""
    branch_configuration: Path
    """Path to branch configuration YAML file. Both relative and absolute paths are
    accepted."""
    environment: str | None = None
    """Optional environment name to use for this step."""
    artifact_path: Path | None = None
    """Optional path to artifact file. Both relative and absolute paths are
    accepted."""
    backup_freq: float | None = DEFAULT_BACKUP_FREQ_SECONDS
    """Backup frequency in seconds, or ``None`` to disable. Default is 30 minutes."""
    sim_verbosity: int = 0
    """Vivarium simulation logging verbosity level. Default is 0."""

    @classmethod
    def validate(
        cls,
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
    ) -> None:
        """Validate kwargs for :func:`~vivarium_cluster_tools.psimulate.workflow_config.interface.get_simulation_step_tasks`."""
        cls._validate_common(name, resources)
        if not model_specification:
            raise ValueError(
                f"Step '{name}': simulation type requires 'model_specification'."
            )
        if not branch_configuration:
            raise ValueError(
                f"Step '{name}': simulation type requires 'branch_configuration'."
            )
        paths = [model_specification, branch_configuration]
        if artifact_path is not None:
            paths.append(artifact_path)
        cls._validate_required_paths(name, paths)

    def _build_command(self) -> str:
        """Not used -- simulation steps override get_tasks directly."""
        raise NotImplementedError(
            "SimulationStepConfig does not use _build_command. " "Use get_tasks() instead."
        )

    def get_tasks(
        self,
        tool: Tool,
        *,
        env_prefix: str,
        build_timestamp: str,
        is_resume: bool = False,
    ) -> list[Task]:
        """Create parallel simulation Jobmon Tasks.

        Uses :class:`~vivarium_cluster_tools.psimulate.paths.OutputPaths`
        to create the same ``model_name / timestamp`` directory layout as
        ``psimulate run``.  Parses the branch configuration into a keyspace,
        builds one :class:`~vivarium_cluster_tools.psimulate.jobs.JobParameters`
        per (draw, seed, branch) combination, writes per-task metadata,
        and returns the full list of Jobmon tasks.
        """
        # Build output paths using the same layout as psimulate run:
        # output_directory / model_name / timestamp / ...
        output_paths = OutputPaths.from_entry_point_args(
            command=COMMANDS.run,
            input_artifact_path=self.artifact_path,
            result_directory=self.output_directory,
            input_model_spec_path=self.model_specification,
            launch_time=build_timestamp,
            is_resume=is_resume,
        )
        output_paths.touch()

        # Parse branch configuration into keyspace
        keyspace = branches.Keyspace.from_branch_configuration(self.branch_configuration)

        # Build job parameters for each (draw, seed, branch) combination
        job_parameters = build_job_parameters_from_keyspace(
            keyspace,
            model_specification_path=self.model_specification,
            output_root=output_paths.root,
            worker_logging_root=output_paths.worker_logging_root,
            backup_configuration=BackupConfiguration(
                backup_dir=str(output_paths.backup_dir),
                backup_freq=self.backup_freq,
                backup_metadata_path=str(output_paths.backup_metadata_path),
            ),
            extras={
                "sim_verbosity": self.sim_verbosity,
            },
        )

        return get_task_list(
            tool=tool,
            command=COMMANDS.run,
            job_parameters_list=job_parameters,
            metadata_dir=output_paths.metadata_dir,
            results_dir=output_paths.results_dir,
            worker_logging_root=output_paths.worker_logging_root,
            native_specification=self.native_specification,
            env_prefix=env_prefix,
            template_name=f"psimulate_{self.name}",
        )

    @classmethod
    def to_yaml_dict(
        cls,
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
    ) -> dict[str, Any]:
        """Serialize simulation-step kwargs to a YAML-ready dict."""
        result: dict[str, Any] = {
            "name": name,
            "type": "simulation",
            "resources": resources.to_dict(),
        }
        if environment is not None:
            result["environment"] = environment

        args: dict[str, Any] = {
            "model_specification": str(model_specification),
            "branch_configuration": str(branch_configuration),
        }
        if artifact_path is not None:
            args["artifact_path"] = str(artifact_path)
        if backup_freq != DEFAULT_BACKUP_FREQ_SECONDS:
            args["backup_freq"] = backup_freq
        if sim_verbosity != 0:
            args["sim_verbosity"] = sim_verbosity

        result["args"] = args
        return result

    @classmethod
    def kwargs_from_yaml(
        cls,
        data: dict[str, Any],
        output_directory: Path,
        *,
        project: str,
        queue: str,
    ) -> dict[str, Any]:
        """Parse a raw simulation-step YAML dict into API kwargs."""
        args = data.get("args", {}) or {}
        cls._check_supported_args(args, data.get("name", "<unnamed>"))

        kwargs: dict[str, Any] = {
            "name": data["name"],
            "resources": ResourceConfig.from_dict(
                data["resources"], workflow_project=project, workflow_queue=queue
            ),
            "output_directory": output_directory,
            "environment": data.get("environment"),
            "model_specification": Path(args["model_specification"]).resolve(),
            "branch_configuration": Path(args["branch_configuration"]).resolve(),
        }
        if "artifact_path" in args:
            kwargs["artifact_path"] = Path(args["artifact_path"]).resolve()
        if "backup_freq" in args:
            kwargs["backup_freq"] = args["backup_freq"]
        if "sim_verbosity" in args:
            kwargs["sim_verbosity"] = args["sim_verbosity"]
        return kwargs


@dataclass
class PytestStepConfig(BaseStepConfig):
    """Configuration for a pytest-based workflow step.

    This step type constructs a ``pytest`` command from structured arguments
    and runs it as a single Jobmon task.

    Examples
    --------
    YAML configuration::

        steps:
          - name: unit_tests
            type: pytest
            resources:
              memory_gb: 8
              runtime: "01:00:00"
              cores: 4
            args:
              path: tests/
              k: "test_foo"
              runslow: true

    Multiple paths::

        steps:
          - name: unit_and_integration
            type: pytest
            resources:
              memory_gb: 8
              runtime: "01:00:00"
            args:
              path:
                - tests/unit
                - tests/integration
    """

    _SUPPORTED_ARGS: ClassVar[set[str]] = {
        "path",
        "k",
        "runslow",
    }

    name: str
    """Unique name for this step within the workflow."""
    resources: ResourceConfig
    """Compute resources for this step."""
    output_directory: Path
    """Output directory for this step. Inherited from the workflow's output_directory."""
    environment: str | None = None
    """Optional environment name to use for this step."""
    path: str | list[str] | None = None
    """Test path(s) (file or directory) to pass to pytest. Can be a single string
    or a list of strings. Both relative and absolute paths are accepted.
    At least one of ``path`` or ``k`` is required."""
    k: str | None = None
    """Pytest ``-k`` expression to filter tests by name. At least one of ``path`` or ``k`` is required."""
    runslow: bool = False
    """Whether to pass --runslow flag."""

    @classmethod
    def validate(
        cls,
        *,
        name: str,
        resources: ResourceConfig,
        output_directory: Path,
        environment: str | None = None,
        path: str | list[str] | None = None,
        k: str | None = None,
        runslow: bool = False,
    ) -> None:
        """Validate kwargs for :func:`~vivarium_cluster_tools.psimulate.workflow_config.interface.get_pytest_step_tasks`."""
        cls._validate_common(name, resources)
        if not path and not k:
            raise ValueError(
                f"Step '{name}': pytest type requires at least one of 'path' or 'k'."
            )
        if path is not None:
            raw_paths = path if isinstance(path, list) else [path]
            cls._validate_required_paths(name, [Path(p) for p in raw_paths])

    def _build_command(self) -> str:
        """Build the pytest command string from structured arguments."""
        parts = ["pytest"]
        if self.path:
            if isinstance(self.path, list):
                parts.extend(shlex.quote(p) for p in self.path)
            else:
                parts.append(shlex.quote(self.path))
        if self.k:
            parts.append(f"-k {shlex.quote(self.k)}")
        if self.runslow:
            parts.append("--runslow")
        if self.resources.cores > 1:
            parts.append(f"--numprocesses {self.resources.cores}")
        return " ".join(parts)

    @classmethod
    def to_yaml_dict(
        cls,
        *,
        name: str,
        resources: ResourceConfig,
        output_directory: Path,
        environment: str | None = None,
        path: str | list[str] | None = None,
        k: str | None = None,
        runslow: bool = False,
    ) -> dict[str, Any]:
        """Serialize pytest-step kwargs to a YAML-ready dict."""
        result: dict[str, Any] = {
            "name": name,
            "type": "pytest",
            "resources": resources.to_dict(),
        }
        if environment is not None:
            result["environment"] = environment

        args: dict[str, Any] = {}
        if path is not None:
            args["path"] = path
        if k is not None:
            args["k"] = k
        if runslow:
            args["runslow"] = True

        result["args"] = args
        return result

    @classmethod
    def kwargs_from_yaml(
        cls,
        data: dict[str, Any],
        output_directory: Path,
        *,
        project: str,
        queue: str,
    ) -> dict[str, Any]:
        """Parse a raw pytest-step YAML dict into API kwargs."""
        args = data.get("args", {}) or {}
        cls._check_supported_args(args, data.get("name", "<unnamed>"))

        kwargs: dict[str, Any] = {
            "name": data["name"],
            "resources": ResourceConfig.from_dict(
                data["resources"], workflow_project=project, workflow_queue=queue
            ),
            "output_directory": output_directory,
        }
        if "environment" in data:
            kwargs["environment"] = data["environment"]
        if "path" in args:
            raw_path = args["path"]
            if isinstance(raw_path, list):
                kwargs["path"] = [str(Path(p).resolve()) for p in raw_path]
            else:
                kwargs["path"] = str(Path(raw_path).resolve())
        if "k" in args:
            kwargs["k"] = args["k"]
        if "runslow" in args:
            kwargs["runslow"] = args["runslow"]
        return kwargs


@dataclass
class PythonStepConfig(BaseStepConfig):
    """Configuration for a Python script workflow step.

    This step type constructs a ``python`` command from a script path and
    optional arguments, running it as a single Jobmon task.

    Examples
    --------
    YAML configuration::

        steps:
          - name: postprocess
            type: python
            resources:
              memory_gb: 8
              runtime: "00:30:00"
            args:
              path: scripts/postprocess.py
              positional_args:
                - "foo"
                - "bar"
              keyword_args:
                input_dir: /mnt/results/model_29
                verbose: true
                num_workers: 4

    Notes
    -----
    Positional arguments are appended in list order. Keyword arguments are
    emitted in alphabetical order by key.

    Keyword argument value handling:

    - ``true`` → bare flag (``--key``)
    - ``false`` → omitted from the command entirely
    - ``null`` → bare flag (``--key``), same as ``true``
    - Any other scalar → ``--key value``

    """

    _SUPPORTED_ARGS: ClassVar[set[str]] = {"path", "positional_args", "keyword_args"}

    name: str
    """Unique name for this step within the workflow."""
    resources: ResourceConfig
    """Compute resources for this step."""
    output_directory: Path
    """Output directory for this step. Inherited from the workflow's output_directory."""
    environment: str | None = None
    """Optional environment name to use for this step."""
    args: dict[str, Any] = field(default_factory=dict)
    """Args dictionary from YAML. Must contain a 'path' to a Python file.
    Both relative and absolute paths are accepted. May optionally contain
    'positional_args' (list of scalars passed in order) and 'keyword_args'
    (dict of named arguments; see class-level Notes for how values map to
    CLI flags)."""

    @classmethod
    def validate(
        cls,
        *,
        name: str,
        resources: ResourceConfig,
        output_directory: Path,
        path: str,
        environment: str | None = None,
        positional_args: list[Any] | None = None,
        keyword_args: dict[str, Any] | None = None,
    ) -> None:
        """Validate kwargs for :func:`~vivarium_cluster_tools.psimulate.workflow_config.interface.get_python_step_tasks`."""
        cls._validate_common(name, resources)
        if not path:
            raise ValueError(f"Step '{name}': python type requires 'path' in args.")
        if not isinstance(path, str) or not path.endswith(".py"):
            raise ValueError(
                f"Step '{name}': 'path' must be a string ending with .py, got {path!r}."
            )
        if positional_args is not None:
            cls._validate_positional_args(name, positional_args)
        if keyword_args is not None:
            validate_scalar_dict(
                keyword_args,
                field_name="keyword_args",
                step_name=name,
            )
        cls._validate_required_paths(name, [Path(path)])

    @staticmethod
    def _validate_positional_args(step_name: str, positional_args: Any) -> None:
        """Validate that positional_args is a list of scalar values."""
        if not isinstance(positional_args, list):
            raise ValueError(
                f"Step '{step_name}': 'positional_args' must be a list, "
                f"got {type(positional_args).__name__}."
            )
        for arg_index, item in enumerate(positional_args):
            check_scalar(
                item,
                label=f"positional_args[{arg_index}]",
                step_name=step_name,
                allow_none=False,
            )

    def _build_command(self) -> str:
        """Build the python command string from the script path and args.

        Positional arguments are appended first (in the order provided),
        followed by keyword arguments (sorted alphabetically by key).
        """
        parts = ["python", shlex.quote(self.args["path"])]
        # Positional args: appended in list order
        for value in self.args.get("positional_args", []):
            parts.append(shlex.quote(str(value)))
        # Keyword args: sorted by key, with --key prefix
        for key in sorted(self.args.get("keyword_args", {})):
            value = self.args["keyword_args"][key]
            if value is True or value is None:
                parts.append(f"--{key}")
            elif value is False:
                continue
            else:
                parts.append(f"--{key} {shlex.quote(str(value))}")
        return " ".join(parts)

    @classmethod
    def to_yaml_dict(
        cls,
        *,
        name: str,
        resources: ResourceConfig,
        output_directory: Path,
        path: str,
        environment: str | None = None,
        positional_args: list[Any] | None = None,
        keyword_args: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Serialize python-step kwargs to a YAML-ready dict."""
        result: dict[str, Any] = {
            "name": name,
            "type": "python",
            "resources": resources.to_dict(),
        }
        if environment is not None:
            result["environment"] = environment

        args: dict[str, Any] = {"path": path}
        if positional_args is not None:
            args["positional_args"] = copy.deepcopy(positional_args)
        if keyword_args is not None:
            args["keyword_args"] = copy.deepcopy(keyword_args)
        result["args"] = args
        return result

    @classmethod
    def kwargs_from_yaml(
        cls,
        data: dict[str, Any],
        output_directory: Path,
        *,
        project: str,
        queue: str,
    ) -> dict[str, Any]:
        """Parse a raw python-step YAML dict into API kwargs."""
        args = copy.deepcopy(data["args"])
        step_name = data.get("name", "<unnamed>")
        cls._check_supported_args(args, step_name)
        if "path" not in args:
            raise ValueError(f"Step '{step_name}': python type requires 'path' in args.")
        kwargs: dict[str, Any] = {
            "name": data["name"],
            "resources": ResourceConfig.from_dict(
                data["resources"], workflow_project=project, workflow_queue=queue
            ),
            "output_directory": output_directory,
            "environment": data.get("environment"),
            "path": str(Path(args["path"]).resolve()),
        }
        if "positional_args" in args:
            kwargs["positional_args"] = args["positional_args"]
        if "keyword_args" in args:
            kwargs["keyword_args"] = args["keyword_args"]
        return kwargs


@dataclass
class NotebookStepConfig(BaseStepConfig):
    """Configuration for a notebook-based workflow step.

    Currently routes to ``papermill`` under the hood, but exposes a
    notebook-agnostic schema so the executor can be swapped later.

    Examples
    --------
    YAML configuration::

        steps:
          - name: post_notebook_neonatal
            type: notebook
            resources:
              memory_gb: 20
              runtime: "02:00:00"
            args:
              path: tests/model_notebooks/results/neonatal.ipynb
              output_path: /mnt/results/run_29/executed/neonatal.ipynb
              parameters:
                model_dir: /mnt/results/run_29
                year: 2020
                verbose: true

    Notes
    -----
    Parameter values map to papermill flags as follows:

    - ``str`` / ``int`` / ``float`` -> ``-p key value``
    - ``bool`` / ``None`` -> ``-y key {true,false,null}`` (YAML-typed)

    Parameters are emitted sorted by key for deterministic command output.
    """

    _SUPPORTED_ARGS: ClassVar[set[str]] = {"path", "parameters", "output_path", "cwd"}

    name: str
    """Unique name for this step within the workflow."""
    resources: ResourceConfig
    """Compute resources for this step."""
    output_directory: Path
    """Output directory for this step. Inherited from the workflow's output_directory."""
    path: Path
    """Path to the input notebook (.ipynb). Both relative and absolute paths are
    accepted; resolved to absolute when serialized."""
    output_path: Path
    """Where to write the executed notebook (.ipynb). Both relative and absolute
    paths are accepted."""
    environment: str | None = None
    """Optional environment name to use for this step."""
    parameters: dict[str, Any] = field(default_factory=dict)
    """Notebook parameters injected as cell-level variables. Scalar values only."""
    cwd: Path | None = None
    """Optional working directory for notebook execution. Both relative and absolute
    paths are accepted. If not provided, defaults to the parent directory of ``path``."""

    _DEFAULT_KERNEL: ClassVar[str] = "python3"
    """Jupyter kernel used for notebook execution. Not user-configurable;
    extracted here for discoverability."""

    _PYTHON_IDENTIFIER_RE: ClassVar[re.Pattern[str]] = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
    """Parameter keys must be valid Python identifiers because papermill
    injects them as variable assignments in a notebook cell."""

    @classmethod
    def validate(
        cls,
        *,
        name: str,
        resources: ResourceConfig,
        output_directory: Path,
        path: Path,
        output_path: Path,
        environment: str | None = None,
        parameters: dict[str, Any] | None = None,
        cwd: Path | None = None,
    ) -> None:
        """Validate kwargs for :func:`~vivarium_cluster_tools.psimulate.workflow_config.interface.get_notebook_step_tasks`."""
        cls._validate_common(name, resources)
        if not str(path).endswith(".ipynb"):
            raise ValueError(f"Step '{name}': 'path' must end with .ipynb, got {path!r}.")
        if not str(output_path).endswith(".ipynb"):
            raise ValueError(
                f"Step '{name}': 'output_path' must end with .ipynb, got {output_path!r}."
            )
        params = parameters if parameters is not None else {}
        validate_scalar_dict(params, field_name="parameters", step_name=name)
        for key in params:
            if not cls._PYTHON_IDENTIFIER_RE.match(key):
                raise ValueError(
                    f"Step '{name}': parameter key {key!r} is not a valid "
                    "Python identifier. Notebooks require parameter names that "
                    "are valid Python identifiers (letters, digits, underscores; "
                    "cannot start with a digit)."
                )
        cls._validate_required_paths(name, [path])

    def _build_command(self) -> str:
        cwd = self.cwd if self.cwd is not None else self.path.parent
        parts = [
            f"mkdir -p {shlex.quote(str(self.output_path.parent))}",
            "&&",
            "papermill",
            shlex.quote(str(self.path)),
            shlex.quote(str(self.output_path)),
            f"-k {self._DEFAULT_KERNEL}",
        ]
        for key in sorted(self.parameters):
            value = self.parameters[key]
            if isinstance(value, bool) or value is None:
                yaml_value = (
                    "true" if value is True else "false" if value is False else "null"
                )
                parts.append(f"-y {shlex.quote(f'{key}: {yaml_value}')}")
            else:
                parts.append(f"-p {key} {shlex.quote(str(value))}")
        parts.append(f"--cwd {shlex.quote(str(cwd))}")
        return " ".join(parts)

    @classmethod
    def to_yaml_dict(
        cls,
        *,
        name: str,
        resources: ResourceConfig,
        output_directory: Path,
        path: Path,
        output_path: Path,
        environment: str | None = None,
        parameters: dict[str, Any] | None = None,
        cwd: Path | None = None,
    ) -> dict[str, Any]:
        """Serialize notebook-step kwargs to a YAML-ready dict."""
        result: dict[str, Any] = {
            "name": name,
            "type": "notebook",
            "resources": resources.to_dict(),
        }
        if environment is not None:
            result["environment"] = environment

        args: dict[str, Any] = {
            "path": str(path),
            "output_path": str(output_path),
        }
        if parameters:
            args["parameters"] = copy.deepcopy(parameters)
        if cwd is not None:
            args["cwd"] = str(cwd)

        result["args"] = args
        return result

    @classmethod
    def kwargs_from_yaml(
        cls,
        data: dict[str, Any],
        output_directory: Path,
        *,
        project: str,
        queue: str,
    ) -> dict[str, Any]:
        """Parse a raw notebook-step YAML dict into API kwargs."""
        args = copy.deepcopy(data["args"])
        step_name = data.get("name", "<unnamed>")
        cls._check_supported_args(args, step_name)

        if "output_path" not in args:
            raise ValueError(
                f"Step '{step_name}': notebook type requires 'output_path' in args."
            )

        kwargs: dict[str, Any] = {
            "name": data["name"],
            "resources": ResourceConfig.from_dict(
                data["resources"], workflow_project=project, workflow_queue=queue
            ),
            "output_directory": output_directory,
            "path": Path(args["path"]).resolve(),
            "output_path": Path(args["output_path"]).resolve(),
        }
        if "environment" in data:
            kwargs["environment"] = data["environment"]
        if "parameters" in args:
            kwargs["parameters"] = args["parameters"]
        if "cwd" in args:
            kwargs["cwd"] = Path(args["cwd"]).resolve()
        return kwargs


STEP_TYPES: dict[str, type[BaseStepConfig]] = {
    "command": CommandStepConfig,
    "simulation": SimulationStepConfig,
    "pytest": PytestStepConfig,
    "python": PythonStepConfig,
    "notebook": NotebookStepConfig,
}
"""Maps each YAML ``step_type`` string to its step-config class. Adding a new
step type requires a matching entry in
:data:`vivarium_cluster_tools.psimulate.workflow_config.builder.STEP_TYPE_API_FNS`"""


def _resolve_step_type(step_dict: dict[str, Any]) -> str:
    """Pick the step-type key for ``step_dict``.

    Dispatch rules:

    - A top-level ``command`` field always resolves to ``"command"``;
      :meth:`CommandStepConfig.kwargs_from_yaml` enforces the rest of the
      command-step schema (including any conflicting ``type``).
    - Otherwise, an explicit ``type`` is used.
    - Otherwise, ``"command"`` is the default, so the command-step parser
      can raise a focused error about the missing ``command`` field.
    """
    if "command" in step_dict:
        return "command"
    step_type: str = step_dict.get("type", "command")
    if step_type not in STEP_TYPES:
        step_name = step_dict.get("name", "<unnamed>")
        raise ValueError(
            f"Step '{step_name}': unsupported type '{step_type}'. "
            f"Must be one of: {sorted(STEP_TYPES)}."
        )
    return step_type


@dataclass(frozen=True)
class ParsedStep:
    """A parsed workflow step ready to be passed to an interface API function.

    Produced by ``WorkflowConfig._parse_steps``. Holds the *inputs* to
    the matching ``get_*_step_tasks`` function (in ``api_kwargs``), plus the
    YAML-serializable form (``yaml_dict``) used for round-trip output.
    """

    step_type: str
    """One of "command", "simulation", "pytest", "python", "notebook"."""
    name: str
    """The step's unique name within the workflow."""
    api_kwargs: dict[str, Any]
    """Kwargs ready to send into the matching interface API function. Excludes
    ``tool`` and ``is_resume``, which are supplied by the builder."""
    yaml_dict: dict[str, Any]
    """YAML-serializable representation of the step (the output of the source
    step class's ``to_yaml_dict``)."""


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
    """Directory where workflow outputs will be stored. Both relative and absolute
    paths are accepted."""
    default_environment: str | None
    """Default environment to use for steps that do not specify one."""
    steps: list[ParsedStep]
    """Parsed workflow steps, each carrying the kwargs needed by the matching
    interface API function."""
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    """Maximum number of Jobmon task attempts. Default is 2."""

    @staticmethod
    def _parse_yaml_file(path: Path) -> dict[str, Any]:
        """Read and perform basic structural validation on a workflow YAML file.

        Returns the ``workflow`` dict from inside the top-level key.

        Parameters
        ----------
        path
            Path to the YAML file.

        Raises
        ------
        KeyError
            If the file does not contain a top-level workflow key, if required workflow-level
            fields are missing, or if the workflow 'steps' list is empty.

        Returns
        -------
        The raw workflow dictionary from the YAML file, without any further parsing or
        validation.
        """
        with path.open() as f:
            raw = yaml.safe_load(f)

        if not isinstance(raw, dict) or "workflow" not in raw:
            raise KeyError("Workflow configuration must contain a top-level 'workflow' key.")

        workflow: dict[str, Any] = raw["workflow"]

        # Check required top-level fields
        for field_name in REQUIRED_WORKFLOW_FIELDS:
            if field_name not in workflow:
                raise KeyError(
                    f"Workflow configuration is missing required field '{field_name}'."
                )

        raw_steps = workflow["steps"]
        if not raw_steps:
            raise KeyError("Workflow 'steps' must not be empty.")

        return workflow

    @staticmethod
    def _parse_steps(
        raw_steps: list[dict[str, Any]],
        output_directory: Path,
        *,
        project: str,
        queue: str,
    ) -> list[ParsedStep]:
        """Parse a list of raw step dicts into :class:`ParsedStep` objects."""

        parsed_steps: list[ParsedStep] = []
        for step_dict in raw_steps:
            step_type = _resolve_step_type(step_dict)
            step_class = STEP_TYPES[step_type]
            api_kwargs = step_class.kwargs_from_yaml(
                step_dict,
                output_directory=output_directory,
                project=project,
                queue=queue,
            )
            yaml_dict = step_class.to_yaml_dict(**api_kwargs)  # type: ignore[attr-defined]
            parsed_steps.append(
                ParsedStep(
                    step_type=step_type,
                    name=api_kwargs["name"],
                    api_kwargs=api_kwargs,
                    yaml_dict=yaml_dict,
                )
            )
        return parsed_steps

    @classmethod
    def from_yaml_with_cli_overrides(
        cls,
        path: Path,
        *,
        project: str | None = None,
        queue: str | None = None,
        output_directory: Path | None = None,
        max_attempts: int | None = None,
    ) -> WorkflowConfig:
        """Load a WorkflowConfig from YAML, merging CLI overrides.

        CLI arguments take precedence over values in the YAML file.
        Validates that ``project``, ``queue``, and ``output_directory`` are provided
        by at least one source

        Parameters
        ----------
        path
            Path to the workflow YAML configuration file.
        project
            CLI override for the project field.
        queue
            CLI override for the queue field.
        output_directory
            CLI override for the output directory.
        max_attempts
            CLI override for the maximum number of Jobmon task attempts.

        Raises
        ------
        KeyError
            If ``project``, ``queue``, or ``output_directory`` cannot be resolved
            from either the YAML file or CLI arguments.
        """
        workflow = cls._parse_yaml_file(path)

        resolved_project = project or workflow.get("project")
        resolved_queue = queue or workflow.get("queue")
        resolved_output_directory = output_directory or (
            Path(workflow["output_directory"]).resolve()
            if "output_directory" in workflow
            else None
        )

        if not resolved_project:
            raise ValueError(
                "Project is required. Provide it in the config file or via --project/-P."
            )
        if not resolved_queue:
            raise ValueError(
                "Queue is required. Provide it in the config file or via --queue/-q."
            )
        if not resolved_output_directory:
            raise ValueError(
                "Output directory is required. Provide it in the config file "
                "or via --output-directory/-o."
            )

        steps = cls._parse_steps(
            workflow["steps"],
            output_directory=resolved_output_directory,
            project=resolved_project,
            queue=resolved_queue,
        )

        return cls(
            name=workflow["name"],
            project=resolved_project,
            queue=resolved_queue,
            output_directory=resolved_output_directory,
            default_environment=workflow.get("default_environment"),
            steps=steps,
            max_attempts=max_attempts or workflow.get("max_attempts", DEFAULT_MAX_ATTEMPTS),
        )

    def __post_init__(self) -> None:
        """Validate workflow-level constraints."""
        validate_project(self.project)
        # Uses a placeholder value for runtime
        validate_runtime_and_queue("01:00:00", self.queue)  # validate queue value
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
            "project": self.project,
            "queue": self.queue,
            "output_directory": str(self.output_directory),
            "max_attempts": self.max_attempts,
        }
        if self.default_environment is not None:
            result["default_environment"] = self.default_environment
        result["steps"] = [step.yaml_dict for step in self.steps]

        return result
