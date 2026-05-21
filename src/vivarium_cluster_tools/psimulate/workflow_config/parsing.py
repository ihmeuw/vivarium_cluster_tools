"""
=====================
Workflow Step Parsing
=====================

YAML <-> API kwargs translation for workflow steps. Each step type has a
parser (raw YAML dict -> API kwargs) and a serializer (API kwargs -> YAML
dict). Parsers do YAML-shape validation (required fields, unsupported
``args`` keys, ``command``/``type`` conflicts) inline.

Also exposes workflow-level entry points: :func:`parse_step_from_yaml`,
:func:`load_workflow_config`, :func:`workflow_config_to_yaml_dict`.

"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Callable

from vivarium_cluster_tools.psimulate.workflow_config.config import (
    DEFAULT_BACKUP_FREQ_SECONDS,
    DEFAULT_MAX_ATTEMPTS,
    ParsedStep,
    ResourceConfig,
    WorkflowConfig,
)

_COMMAND_SUPPORTED_ARGS: set[str] | None = None
_SIMULATION_SUPPORTED_ARGS: set[str] = {
    "model_specification",
    "branch_configuration",
    "artifact_path",
    "backup_freq",
    "sim_verbosity",
}
_PYTEST_SUPPORTED_ARGS: set[str] = {"path", "k", "runslow"}
_PYTHON_SUPPORTED_ARGS: set[str] = {"path", "positional_args", "keyword_args"}
_NOTEBOOK_SUPPORTED_ARGS: set[str] = {"path", "parameters", "output_path", "cwd"}


def _parse_common_step_fields(
    data: dict[str, Any],
    *,
    project: str,
    queue: str,
) -> tuple[str, ResourceConfig]:
    """Validate and extract the fields required by every step type.

    Checks that the step has a ``name``, a ``resources`` block, and that the
    block contains ``memory_gb``. Returns the step name and the constructed
    :class:`ResourceConfig`.
    """
    if "name" not in data:
        raise ValueError("Step: missing required field 'name'.")
    step_name = data["name"]
    if "resources" not in data:
        raise ValueError(f"Step '{step_name}': missing required field 'resources'.")
    resources_data = data["resources"]
    if "memory_gb" not in resources_data:
        raise ValueError(f"Step '{step_name}': missing required 'memory_gb' in 'resources'.")
    resources = ResourceConfig.from_dict(
        resources_data, workflow_project=project, workflow_queue=queue
    )
    return step_name, resources


def _require_args_block(data: dict[str, Any], step_name: str) -> dict[str, Any]:
    """Return ``data['args']`` or raise ValueError when the block is missing."""
    if "args" not in data:
        raise ValueError(f"Step '{step_name}': missing required field 'args'.")
    return data["args"]


def _require_arg(args: dict[str, Any], field: str, step_name: str) -> Any:
    """Return ``args[field]`` or raise ValueError when the field is missing."""
    if field not in args:
        raise ValueError(f"Step '{step_name}': missing required '{field}' in 'args'.")
    return args[field]


def resolve_step_type(step_dict: dict[str, Any]) -> str:
    """Pick the step-type key for ``step_dict``.

    Dispatch rules:

    - A top-level ``command`` field always resolves to ``"command"``;
      :func:`parse_command_step_from_yaml` enforces the rest of the
      command-step schema (including any conflicting ``type``).
    - Otherwise, an explicit ``type`` is used.
    - Otherwise, ``"command"`` is the default, so the command-step parser
      can raise a focused error about the missing ``command`` field.
    """
    if "command" in step_dict:
        return "command"
    step_type: str = step_dict.get("type", "command")
    if step_type not in STEP_TYPE_YAML_PARSERS:
        step_name = step_dict.get("name", "<unnamed>")
        raise ValueError(
            f"Step '{step_name}': unsupported type '{step_type}'. "
            f"Must be one of: {sorted(STEP_TYPE_YAML_PARSERS)}."
        )
    return step_type


def parse_command_step_from_yaml(
    data: dict[str, Any],
    output_directory: Path,
    *,
    project: str,
    queue: str,
) -> dict[str, Any]:
    """Parse a raw command-step YAML dict into API kwargs.

    The YAML form for a command step requires a top-level ``command`` field.
    The optional ``type`` field, when present, must be ``"command"``. No
    ``args:`` block is accepted.

    Examples
    --------
    YAML configuration::

        steps:
          - name: post_analysis
            command: python scripts/analyze.py --input /results
            environment: analysis_env
            resources:
              memory_gb: 20
              runtime: "02:00:00"
              cores: 2
    """
    step_name, resources = _parse_common_step_fields(data, project=project, queue=queue)
    _check_supported_args(data.get("args", {}), step_name, _COMMAND_SUPPORTED_ARGS)
    if "command" not in data:
        raise ValueError(f"Step '{step_name}': missing required field 'command'.")
    explicit_type = data.get("type")
    if explicit_type is not None and explicit_type != "command":
        raise ValueError(
            f"Step '{step_name}': cannot specify both 'command' and "
            f"'type: {explicit_type}'. When 'command' is set, 'type' "
            "must be omitted or set to 'command'."
        )
    return {
        "name": step_name,
        "resources": resources,
        "command": data["command"],
        "output_directory": output_directory,
        "environment": data.get("environment"),
    }


def parse_simulation_step_from_yaml(
    data: dict[str, Any],
    output_directory: Path,
    *,
    project: str,
    queue: str,
) -> dict[str, Any]:
    """Parse a raw simulation-step YAML dict into API kwargs.

    Required ``args`` keys: ``model_specification``, ``branch_configuration``.
    Optional ``args`` keys: ``artifact_path``, ``backup_freq``,
    ``sim_verbosity``.

    Examples
    --------
    YAML configuration::

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
    step_name, resources = _parse_common_step_fields(data, project=project, queue=queue)
    args = _require_args_block(data, step_name)
    _check_supported_args(args, step_name, _SIMULATION_SUPPORTED_ARGS)
    model_specification = _require_arg(args, "model_specification", step_name)
    branch_configuration = _require_arg(args, "branch_configuration", step_name)

    kwargs: dict[str, Any] = {
        "name": step_name,
        "resources": resources,
        "output_directory": output_directory,
        "environment": data.get("environment"),
        "model_specification": Path(model_specification).resolve(),
        "branch_configuration": Path(branch_configuration).resolve(),
    }
    if "artifact_path" in args:
        kwargs["artifact_path"] = Path(args["artifact_path"]).resolve()
    if "backup_freq" in args:
        kwargs["backup_freq"] = args["backup_freq"]
    if "sim_verbosity" in args:
        kwargs["sim_verbosity"] = args["sim_verbosity"]
    return kwargs


def parse_pytest_step_from_yaml(
    data: dict[str, Any],
    output_directory: Path,
    *,
    project: str,
    queue: str,
) -> dict[str, Any]:
    """Parse a raw pytest-step YAML dict into API kwargs.

    Optional ``args`` keys: ``path``, ``k``, ``runslow``. At least one of
    ``path`` or ``k`` must be provided (enforced by
    :func:`~vivarium_cluster_tools.psimulate.workflow_config.validation.validate_pytest_step`).
    ``path`` may be a single string or a list of strings.

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
    step_name, resources = _parse_common_step_fields(data, project=project, queue=queue)
    args = data.get("args", {}) or {}
    _check_supported_args(args, step_name, _PYTEST_SUPPORTED_ARGS)

    kwargs: dict[str, Any] = {
        "name": step_name,
        "resources": resources,
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


def parse_python_step_from_yaml(
    data: dict[str, Any],
    output_directory: Path,
    *,
    project: str,
    queue: str,
) -> dict[str, Any]:
    """Parse a raw python-step YAML dict into API kwargs.

    Required ``args`` key: ``path`` (a ``.py`` script). Optional ``args``
    keys: ``positional_args`` (list of scalars) and ``keyword_args`` (dict
    of identifier-keyed scalars).

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
    """
    step_name, resources = _parse_common_step_fields(data, project=project, queue=queue)
    args = copy.deepcopy(_require_args_block(data, step_name))
    _check_supported_args(args, step_name, _PYTHON_SUPPORTED_ARGS)
    path = _require_arg(args, "path", step_name)
    kwargs: dict[str, Any] = {
        "name": step_name,
        "resources": resources,
        "output_directory": output_directory,
        "environment": data.get("environment"),
        "path": str(Path(path).resolve()),
    }
    if "positional_args" in args:
        kwargs["positional_args"] = args["positional_args"]
    if "keyword_args" in args:
        kwargs["keyword_args"] = args["keyword_args"]
    return kwargs


def parse_notebook_step_from_yaml(
    data: dict[str, Any],
    output_directory: Path,
    *,
    project: str,
    queue: str,
) -> dict[str, Any]:
    """Parse a raw notebook-step YAML dict into API kwargs.

    Required ``args`` keys: ``path`` (input ``.ipynb``) and ``output_path``
    (executed ``.ipynb``). Optional ``args`` keys: ``parameters`` (dict of
    identifier-keyed scalars injected into the notebook) and ``cwd``
    (working directory for execution; defaults to the parent of ``path``).

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
    """
    step_name, resources = _parse_common_step_fields(data, project=project, queue=queue)
    args = copy.deepcopy(_require_args_block(data, step_name))
    _check_supported_args(args, step_name, _NOTEBOOK_SUPPORTED_ARGS)
    path = _require_arg(args, "path", step_name)
    output_path = _require_arg(args, "output_path", step_name)

    kwargs: dict[str, Any] = {
        "name": step_name,
        "resources": resources,
        "output_directory": output_directory,
        "path": Path(path).resolve(),
        "output_path": Path(output_path).resolve(),
    }
    if "environment" in data:
        kwargs["environment"] = data["environment"]
    if "parameters" in args:
        kwargs["parameters"] = args["parameters"]
    if "cwd" in args:
        kwargs["cwd"] = Path(args["cwd"]).resolve()
    return kwargs


def serialize_command_step_to_yaml(
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


def serialize_simulation_step_to_yaml(
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


def serialize_pytest_step_to_yaml(
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


def serialize_python_step_to_yaml(
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


def serialize_notebook_step_to_yaml(
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


def _check_supported_args(
    args: dict[str, Any], step_name: str, supported: set[str] | None
) -> None:
    """Raise ValueError if args contains keys not in ``supported``."""
    if supported is None:
        return
    unsupported = set(args) - supported
    if unsupported:
        raise ValueError(
            f"Step '{step_name}': unsupported args {sorted(unsupported)}. "
            f"Supported args: {sorted(supported)}."
        )


STEP_TYPE_YAML_PARSERS: dict[str, Callable[..., dict[str, Any]]] = {
    "command": parse_command_step_from_yaml,
    "simulation": parse_simulation_step_from_yaml,
    "pytest": parse_pytest_step_from_yaml,
    "python": parse_python_step_from_yaml,
    "notebook": parse_notebook_step_from_yaml,
}
"""Maps each YAML ``step_type`` to its YAML -> API kwargs parser."""

STEP_TYPE_YAML_SERIALIZERS: dict[str, Callable[..., dict[str, Any]]] = {
    "command": serialize_command_step_to_yaml,
    "simulation": serialize_simulation_step_to_yaml,
    "pytest": serialize_pytest_step_to_yaml,
    "python": serialize_python_step_to_yaml,
    "notebook": serialize_notebook_step_to_yaml,
}
"""Maps each YAML ``step_type`` to its API kwargs -> YAML dict serializer."""


def parse_step_from_yaml(
    raw: dict[str, Any],
    output_directory: Path,
    *,
    project: str,
    queue: str,
) -> ParsedStep:
    """Build a ParsedStep from a raw YAML step dict.

    Dispatches to the matching per-type parser to produce ``api_kwargs``
    and tags the result with the resolved ``step_type`` for downstream
    dispatch (task building, YAML serialization).
    """
    step_type = resolve_step_type(raw)
    api_kwargs = STEP_TYPE_YAML_PARSERS[step_type](
        raw, output_directory, project=project, queue=queue
    )
    return ParsedStep(
        step_type=step_type,
        name=api_kwargs["name"],
        api_kwargs=api_kwargs,
    )


def load_workflow_config(
    path: Path,
    *,
    project: str | None = None,
    queue: str | None = None,
    output_directory: Path | None = None,
    max_attempts: int | None = None,
) -> WorkflowConfig:
    """Load a WorkflowConfig from YAML, merging CLI overrides.

    CLI arguments take precedence over values in the YAML file. Validates
    that ``project``, ``queue``, and ``output_directory`` are provided by
    at least one source.

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
    ValueError
        If ``project``, ``queue``, or ``output_directory`` cannot be resolved
        from either the YAML file or CLI arguments.
    """
    workflow = WorkflowConfig.parse_yaml_file(path)

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

    steps = [
        parse_step_from_yaml(
            raw,
            output_directory=resolved_output_directory,
            project=resolved_project,
            queue=resolved_queue,
        )
        for raw in workflow["steps"]
    ]

    return WorkflowConfig(
        name=workflow["name"],
        project=resolved_project,
        queue=resolved_queue,
        output_directory=resolved_output_directory,
        default_environment=workflow.get("default_environment"),
        steps=steps,
        max_attempts=max_attempts or workflow.get("max_attempts", DEFAULT_MAX_ATTEMPTS),
    )


def workflow_config_to_yaml_dict(config: WorkflowConfig) -> dict[str, Any]:
    """Serialize a :class:`~vivarium_cluster_tools.psimulate.workflow_config.config.WorkflowConfig` to a dict suitable for YAML output."""
    result: dict[str, Any] = {
        "name": config.name,
        "project": config.project,
        "queue": config.queue,
        "output_directory": str(config.output_directory),
        "max_attempts": config.max_attempts,
    }
    if config.default_environment is not None:
        result["default_environment"] = config.default_environment
    result["steps"] = [
        STEP_TYPE_YAML_SERIALIZERS[step.step_type](**step.api_kwargs) for step in config.steps
    ]
    return result
