# Simulation Step Plan: Bespoke `type: "simulation"` in `psimulate workflow`

## 1. Overview

Add support for `type: "simulation"` steps in the workflow YAML. A simulation step behaves the same as `psimulate run` — it reuses the existing job-generation and `build_workflow()` code to produce N parallel Jobmon tasks (one per draw/seed/branch combo). From the workflow DAG's perspective, the simulation step occupies one position but fans out into many tasks.

Multiple simulation steps are allowed (e.g., calibration sim then main sim).

### Target YAML

```yaml
workflow:
  name: "my_pipeline"
  project: "proj_simscience"
  queue: "all.q"
  output_directory: "/results/model_29"
  steps:
    - name: "pre_tests"
      type: pytest
      path: tests/
      resources: { memory: 4, runtime: "01:00:00" }

    - name: "run_simulation"
      type: simulation
      model_specification: "model_spec.yaml"
      branch_configuration: "branches.yaml"
      max_workers: 200
      resources: { memory: 5, runtime: "03:00:00" }

    - name: "post_analysis"
      command: "python analyze.py"
      resources: { memory: 10, runtime: "02:00:00" }
```

## 2. Design: `SimulationStepConfig` Subclass

### Why a subclass

`StepConfig` has fields for command/structured steps (`command`, `type`, `path`, `args`). Simulation steps need different fields (`model_specification`, `branch_configuration`, `artifact_path`, `max_workers`). Rather than adding 4 optional `None` fields to every `StepConfig`, a subclass keeps each class focused.

### Class hierarchy

```python
@dataclass
class StepConfig:
    """Configuration for a command or structured workflow step."""
    name: str
    command: str | None = None
    type: str | None = None
    path: str | list[str] | None = None
    args: str | None = None
    environment: str | None = None
    resources: ResourceConfig | None = None

    @property
    def is_simulation(self) -> bool:
        return False

    # ... existing is_structured, is_raw_command, _validate(), to_dict() unchanged


@dataclass
class SimulationStepConfig(StepConfig):
    """Configuration for the bespoke simulation step.

    Validates that the step has the same required configuration as
    ``psimulate run`` (model_specification, branch_configuration)
    and rejects fields that don't apply to simulation steps
    (command, path, args).
    """
    model_specification: str | None = None
    branch_configuration: str | None = None
    artifact_path: str | None = None
    max_workers: int | None = None

    @property
    def is_simulation(self) -> bool:
        return True

    def _validate(self) -> None:
        """Validate simulation-specific configuration.

        Mirrors the required inputs of ``psimulate run``.
        """
        if not self.model_specification:
            raise ValueError(
                f"Step '{self.name}': simulation type requires 'model_specification'."
            )
        if not self.branch_configuration:
            raise ValueError(
                f"Step '{self.name}': simulation type requires 'branch_configuration'."
            )
        if self.command is not None:
            raise ValueError(
                f"Step '{self.name}': simulation type cannot have 'command'."
            )
        if self.path is not None:
            raise ValueError(
                f"Step '{self.name}': simulation type cannot have 'path'."
            )
        if self.args is not None:
            raise ValueError(
                f"Step '{self.name}': simulation type cannot have 'args'."
            )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"name": self.name, "type": "simulation"}
        if self.model_specification is not None:
            result["model_specification"] = self.model_specification
        if self.branch_configuration is not None:
            result["branch_configuration"] = self.branch_configuration
        if self.artifact_path is not None:
            result["artifact_path"] = self.artifact_path
        if self.max_workers is not None:
            result["max_workers"] = self.max_workers
        if self.environment is not None:
            result["environment"] = self.environment
        if self.resources is not None:
            resources_dict = self.resources.to_dict()
            if resources_dict:
                result["resources"] = resources_dict
        return result
```

### Factory logic in `WorkflowConfig.from_yaml()`

The parser dispatches on `type == "simulation"` to build the right class:

```python
for step_dict in raw_steps:
    if step_dict.get("type") == "simulation":
        step = SimulationStepConfig(
            name=step_dict["name"],
            type="simulation",
            environment=step_dict.get("environment"),
            resources=ResourceConfig.from_dict(step_dict.get("resources")),
            model_specification=step_dict.get("model_specification"),
            branch_configuration=step_dict.get("branch_configuration"),
            artifact_path=step_dict.get("artifact_path"),
            max_workers=step_dict.get("max_workers"),
        )
    else:
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
```

The type annotation for `steps` stays `list[StepConfig]` since `SimulationStepConfig` IS-A `StepConfig`.

### `SUPPORTED_STEP_TYPES`

`"simulation"` is added to `SUPPORTED_STEP_TYPES`. The parent's `_validate()` is never called for simulation steps — `SimulationStepConfig` overrides `_validate()` entirely.

### No limit on simulation step count

Multiple simulation steps are allowed. No at-most-one check in `WorkflowConfig._validate()`.

## 3. Why `resolve_command()` is skipped for simulation steps

Simulation steps don't produce a single shell command. A regular step becomes 1 Jobmon task (`conda run -n env pytest tests/`). A simulation step fans out into N parallel tasks (one per draw/seed/branch) via the existing `build_workflow()` machinery.

`resolve_command()` returns a single `str` — the wrong abstraction for simulation steps. The builder dispatches differently:

```python
for step in steps:
    if step.is_simulation:
        # creates N tasks via build_workflow()
        sim_tasks = self._build_simulation_tasks(step, workflow)
    else:
        # creates 1 task from a resolved command string
        command = resolve_command(step)
        task = template.create_task(command=command, ...)
```

`resolve_command()` raises `ValueError` if called on a simulation step as a guard.

## 4. How the builder uses `build_workflow()`

The simulation step reuses the existing `psimulate run` code path:

1. **Reuse job-generation logic from `runner.main()`** — parse `branch_configuration` into a `Keyspace`, parse `model_specification`, call `jobs.build_job_list()` to get `JobParameters`. Same pipeline as `psimulate run`.

2. **Call existing `build_workflow()`** from `psimulate/jobmon_config/workflow.py` — creates the task template and N tasks, added to the builder's single Jobmon `Workflow`.

3. **Wire the N tasks into the DAG** — all N sim tasks depend on all tasks from the previous step; next step depends on all N sim tasks.

The builder only needs the *job generation* portion of `runner.main()`. Output directory setup, pip env validation, restart logic, and performance reporting are handled at the `workflow_main()` level for the pipeline as a whole — not per-simulation-step.

### Builder DAG wiring (uniform loop)

Since multiple simulation steps are allowed, there's no "find the one sim step and split around it" logic. The builder iterates steps in order, accumulating `previous_tasks` at each position:

```python
previous_tasks = []
for step in self.config.steps:
    if step.is_simulation:
        current_tasks = self._build_simulation_tasks(step, workflow)
    else:
        current_tasks = [self._build_command_task(step)]

    # Wire: every task in current_tasks depends on every task in previous_tasks
    for current in current_tasks:
        for prev in previous_tasks:
            current.add_upstream(prev)

    previous_tasks = current_tasks
```

This handles any mix of regular and simulation steps naturally — a step that produced 1 task followed by a simulation step that produced 100 tasks wires 100 upstream edges. A simulation step followed by another simulation step wires N×M edges.

## 5. Files Modified

| File | Change |
|------|--------|
| `src/.../workflow_config/config.py` | Add `SimulationStepConfig(StepConfig)` subclass; add `"simulation"` to `SUPPORTED_STEP_TYPES`; add `is_simulation` property to `StepConfig` (returns `False`); update `from_yaml()` factory dispatch |
| `src/.../workflow_config/builder.py` | Handle simulation steps in `WorkflowBuilder.build()` — skip `resolve_command()`, delegate to `build_workflow()`; add `resolve_command()` guard for simulation steps; update DAG wiring to uniform loop |
| `src/.../workflow_config/__init__.py` | Export `SimulationStepConfig` |
| `tests/.../workflow_config/test_config.py` | New test classes for simulation step parsing and validation |
| `tests/.../workflow_config/test_builder.py` | New tests for simulation step handling in the builder |
| `tests/.../test_cli.py` | New tests verifying simulation step config errors surface through `psimulate workflow` CLI |
| `tests/.../workflow_config/utilities.py` | Add `make_simulation_step_dict()` helper |

## 6. TDD Plan

### Phase 1: Tests + Stubs (xfail)

Add all tests marked `@pytest.mark.xfail(reason="simulation step not yet implemented")` and the minimal code stubs needed for them to be importable and runnable (but not passing).

#### 1a. Code stubs in `config.py`

- Add `"simulation"` to `SUPPORTED_STEP_TYPES`
- Add `SimulationStepConfig(StepConfig)` dataclass with fields + `is_simulation` property
- Add `is_simulation` property to `StepConfig` returning `False`
- Add stub `_validate()` and `to_dict()` on `SimulationStepConfig`
- **Do NOT yet update `from_yaml()` factory** — that's Phase 2

#### 1b. Test utility in `utilities.py`

```python
def make_simulation_step_dict(**overrides: Any) -> dict[str, Any]:
    """Create a minimal valid simulation step dict."""
    defaults = {
        "name": "simulation",
        "type": "simulation",
        "model_specification": "/path/to/model_spec.yaml",
        "branch_configuration": "/path/to/branches.yaml",
        "resources": {"memory": 5, "runtime": "03:00:00"},
    }
    defaults.update(overrides)
    return defaults
```

#### 1c. Tests in `test_config.py`

**`TestSimulationStepConfigProperties`** (no xfail — tests the class directly):

- `test_is_simulation_true` — `SimulationStepConfig(name="s", model_specification="x", branch_configuration="y").is_simulation is True`
- `test_is_simulation_false_on_base` — `StepConfig(name="s", command="echo").is_simulation is False`
- `test_is_instance_of_step_config` — `isinstance(SimulationStepConfig(...), StepConfig)`

**`TestSimulationStepConfigValidation`** (no xfail — tests `SimulationStepConfig._validate()` directly):

- `test_rejects_missing_required_field` — parametrized over `model_specification` and `branch_configuration`
- `test_rejects_with_command` — sim step with `command="echo"` → `ValueError`
- `test_rejects_with_path` — sim step with `path="foo"` → `ValueError`
- `test_rejects_with_args` — sim step with `args="--flag"` → `ValueError`
- `test_accepts_valid_config` — valid config passes
- `test_accepts_without_optional_fields` — no `artifact_path` / `max_workers` → OK

**`TestSimulationStepParsing`** (xfail — depends on `from_yaml` factory logic):

- `test_parses_simulation_step_as_simulation_step_config` — `isinstance(config.steps[0], SimulationStepConfig)`
- `test_parses_model_specification` — field value correct
- `test_parses_branch_configuration` — field value correct
- `test_parses_artifact_path` — optional field
- `test_parses_max_workers` — optional field
- `test_simulation_step_mixed_with_regular_steps` — 3-step workflow (pre, sim, post) parses correctly

#### 1d. Tests in `test_builder.py`

**`TestSimulationStepInBuilder`** (xfail):

- `test_resolve_command_raises_for_simulation_step` — `resolve_command()` on a `SimulationStepConfig` → `ValueError`
- `test_builder_skips_simulation_step_in_command_resolution` — builder with a sim step doesn't call `resolve_command` on it

#### 1e. Tests in `test_cli.py`

**`TestWorkflowSimulationStep`** (xfail):

- `test_workflow_rejects_simulation_missing_model_spec` — YAML with `type: simulation` but no `model_specification` → error
- `test_workflow_rejects_simulation_missing_branch_config` — same for `branch_configuration`
- `test_workflow_accepts_valid_simulation_step` — valid sim step YAML → `workflow_main` is called (mocked), exit 0
- `test_workflow_rejects_simulation_with_command` — sim step with extra `command` field → error

### Phase 2: Implementation

Implement in this order:

1. **`SimulationStepConfig._validate()`** — fill in the real validation logic (makes direct-construction tests pass)
2. **`from_yaml()` factory** — dispatch on `type == "simulation"` to build `SimulationStepConfig` (makes parsing tests pass)
3. **`resolve_command()` guard** — raise `ValueError` for simulation steps
4. **`WorkflowBuilder.build()`** — detect simulation steps, skip `resolve_command()`, wire DAG with uniform loop using `previous_tasks` accumulator (builder tests pass)
5. **CLI pass-through** — no CLI changes needed, errors from config layer surface naturally (CLI tests pass)

### Phase 3: Remove xfail

Remove all `@pytest.mark.xfail` markers. All tests green.

## 7. Halt-on-Failure

The existing Jobmon DAG dependency wiring ensures downstream steps only run when upstream tasks succeed. Since simulation tasks are wired as upstreams of the next step's tasks, if any sim task fails, the next step won't launch. No additional code needed for this requirement.

## 8. Key Benefits of the Subclass Approach

| Concern | Resolution |
|---------|-----------|
| No extra `None` fields on regular `StepConfig` | `model_specification`, `branch_configuration`, `artifact_path`, `max_workers` only exist on `SimulationStepConfig` |
| Type safety | `isinstance(step, SimulationStepConfig)` in builder; `step.is_simulation` for quick checks |
| Separate validation | `SimulationStepConfig._validate()` mirrors `psimulate run` requirements; `StepConfig._validate()` unchanged |
| `list[StepConfig]` type annotation | Still works — Liskov substitution; `SimulationStepConfig` IS-A `StepConfig` |
| Serialization | Each class owns its `to_dict()` |
| Multiple sim steps | No artificial limit; uniform loop in builder handles any mix |
