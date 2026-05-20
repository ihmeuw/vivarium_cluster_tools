"""
================
Jobmon Client
================

Single point of contact for the Jobmon SDK. All Jobmon imports and SDK
calls live in this module so that future changes to the Jobmon API surface
only require updates here.

Callers should import :class:`Task` and :class:`Workflow` types from this
module rather than from ``jobmon.*`` directly, and use the helper functions
below instead of calling SDK methods themselves.

Project-wide defaults (tool name, cluster name) are encoded here so they
cannot drift between call sites.
"""

from __future__ import annotations

from typing import Any

from jobmon.client.api import Tool
from jobmon.client.task import Task
from jobmon.client.task_template import TaskTemplate
from jobmon.client.workflow import Workflow
from jobmon.core.configuration import JobmonConfig

__all__ = [
    "Task",
    "TaskTemplate",
    "Tool",
    "Workflow",
    "add_tasks",
    "add_upstream",
    "bind_workflow",
    "count_done",
    "create_task",
    "create_tasks",
    "make_task_template",
    "make_tool",
    "make_workflow",
    "run_workflow",
]


TOOL_NAME = "vivarium_cluster_tools"
CLUSTER_NAME = "slurm"


def make_tool() -> Tool:
    """Instantiate the project's Jobmon ``Tool``."""
    return Tool(name=TOOL_NAME)


def make_task_template(
    tool: Tool,
    *,
    template_name: str,
    command_template: str,
    node_args: list[str],
    task_args: list[str],
    op_args: list[str],
    default_compute_resources: dict[str, Any] | None = None,
) -> TaskTemplate:
    """Register a Jobmon ``TaskTemplate`` on *tool*."""
    kwargs: dict[str, Any] = {
        "template_name": template_name,
        "command_template": command_template,
        "node_args": node_args,
        "task_args": task_args,
        "op_args": op_args,
        "default_cluster_name": CLUSTER_NAME,
    }
    if default_compute_resources is not None:
        kwargs["default_compute_resources"] = default_compute_resources
    return tool.get_task_template(**kwargs)


def create_task(template: TaskTemplate, **kwargs: Any) -> Task:
    """Create a single Jobmon ``Task`` from *template*."""
    return template.create_task(**kwargs)


def create_tasks(template: TaskTemplate, **kwargs: Any) -> list[Task]:
    """Batch-create Jobmon ``Task``\\s from *template*."""
    return template.create_tasks(**kwargs)


def add_upstream(task: Task, upstream: Task) -> None:
    """Wire a step-to-step dependency between two tasks."""
    task.add_upstream(upstream)


def make_workflow(
    tool: Tool,
    *,
    workflow_args: str,
    name: str,
    max_attempts: int,
    max_concurrently_running: int | None = None,
) -> Workflow:
    """Create a Jobmon ``Workflow`` on *tool*."""
    kwargs: dict[str, Any] = {
        "workflow_args": workflow_args,
        "name": name,
        "default_cluster_name": CLUSTER_NAME,
        "default_max_attempts": max_attempts,
    }
    if max_concurrently_running is not None:
        kwargs["max_concurrently_running"] = max_concurrently_running
    return tool.create_workflow(**kwargs)


def add_tasks(workflow: Workflow, tasks: list[Task]) -> None:
    """Add *tasks* to *workflow*."""
    workflow.add_tasks(tasks)


def bind_workflow(workflow: Workflow) -> str:
    """Bind *workflow* and return the Jobmon GUI monitoring URL.

    Returns an empty string if no GUI URL is configured.
    """
    workflow.bind()
    gui_url = JobmonConfig().get("http", "gui_url")
    if not gui_url:
        return ""
    return f"{gui_url}/#/workflow/{workflow.workflow_id}"


def run_workflow(
    workflow: Workflow,
    *,
    resume: bool,
    seconds_until_timeout: int | None = None,
) -> str:
    """Submit *workflow* and return its Jobmon status string."""
    run_kwargs: dict[str, Any] = {"resume": resume}
    if seconds_until_timeout is not None:
        run_kwargs["seconds_until_timeout"] = seconds_until_timeout
    status = workflow.run(**run_kwargs)
    if status is None:
        raise RuntimeError("Jobmon workflow.run() returned None unexpectedly.")
    return status


def count_done(workflow: Workflow) -> int:
    """Count tasks in *workflow* whose ``final_status`` is ``"D"`` (done)."""
    return sum(1 for t in workflow.tasks.values() if t.final_status == "D")
