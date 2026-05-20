"""
=========
psimulate
=========

Parallel runner for :mod:`vivarium` jobs.

"""
from typing import NamedTuple


class __Commands(NamedTuple):
    run: str
    restart: str
    expand: str
    load_test: str


COMMANDS = __Commands(*__Commands._fields)

del NamedTuple
del __Commands

TASK_RUNNER_MODULE: str = "vivarium_cluster_tools.psimulate.worker.task_runner"


def wrap_for_subprocess(command: str) -> str:
    """Prepend the ``task_runner subprocess`` watcher to ``command``.

    The returned string runs ``command`` as a child of
    :mod:`vivarium_cluster_tools.psimulate.worker.task_runner` in
    ``subprocess`` mode, so the child's stdout is mirrored live and the
    captured output is replayed to stderr on failure (and thus surfaces in
    the Jobmon GUI).

    Lives here, next to ``TASK_RUNNER_MODULE``, so callers that need the
    wrapper (workflow step builders, simulation task list builder) don't
    have to import ``task_runner`` itself and pay for its transitive
    work-horse imports (``vivarium.framework.engine``, ``dill``,
    ``pandas``).
    """
    return f"python -m {TASK_RUNNER_MODULE} subprocess {command}"
