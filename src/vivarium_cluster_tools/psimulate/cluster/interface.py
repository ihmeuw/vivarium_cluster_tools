"""
=================
Cluster Interface
=================

"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, NamedTuple

from vivarium_cluster_tools.psimulate.environment import ENV_VARIABLES

VALID_PROJECTS = frozenset(
    {
        "proj_simscience",
        "proj_simscience_prod",
        "proj_csu",
    }
)

VALID_QUEUES = frozenset({"all.q", "long.q"})
# Sorted from shortest to longest allowed runtime.
QUEUE_MAX_RUNTIME_HOURS: dict[str, int] = {
    "all.q": 3 * 24,
    "long.q": 16 * 24,
}

RUNTIME_FORMAT = "hh:mm:ss"
_RUNTIME_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")
# https://docs.cluster.ihme.washington.edu/#hpc-execution-host-hardware-specifications
AVAILABLE_HARDWARE = [
    "c6320",  # typical
    "r630",  # high capacity
    "c6420v1",  # batch 1
    "c6420v2",  # batch 2
    "r650",  # high capacity
    "r650v2",  # high capacity
    "r650xs",  # high speed
]


def validate_project(project: str) -> str:
    """Validate that *project* is a recognised cluster project.

    Returns the project unchanged on success.

    Raises
    ------
    ValueError
        If *project* is not in ``VALID_PROJECTS``.
    """
    if project not in VALID_PROJECTS:
        raise ValueError(
            f"Invalid project '{project}'. " f"Must be one of: {sorted(VALID_PROJECTS)}."
        )
    return project


def validate_runtime_and_queue(runtime: str, queue: str | None) -> tuple[str, str]:
    """Validate and reconcile a runtime string with a queue.

    * Validates runtime format (``hh:mm:ss``).
    * If *queue* is ``None``, selects the shortest queue that can
      accommodate *runtime*.
    * If *queue* is given, checks that *runtime* does not exceed the
      queue's maximum.

    Returns the ``(runtime, queue)`` pair (queue may be resolved from
    ``None``).

    Raises
    ------
    ValueError
        On bad format, runtime exceeding all queues, or runtime
        exceeding the selected queue.
    """
    if not _RUNTIME_RE.match(runtime):
        raise ValueError(
            f"Invalid runtime '{runtime}'. " f"Expected format {RUNTIME_FORMAT}."
        )

    hours, minutes, seconds = runtime.split(":")
    total_hours = int(hours) + float(minutes) / 60.0 + float(seconds) / 3600.0

    max_runtime = max(QUEUE_MAX_RUNTIME_HOURS.values())
    if total_hours > max_runtime:
        raise ValueError(
            f"Runtime '{runtime}' exceeds the maximum cluster runtime "
            f"of {max_runtime}:00:00 ({RUNTIME_FORMAT})."
        )

    if queue is not None:
        if queue not in QUEUE_MAX_RUNTIME_HOURS:
            raise ValueError(
                f"Invalid queue '{queue}'. "
                f"Must be one of: {sorted(QUEUE_MAX_RUNTIME_HOURS)}."
            )
        if total_hours > QUEUE_MAX_RUNTIME_HOURS[queue]:
            raise ValueError(
                f"Runtime '{runtime}' exceeds the maximum for queue "
                f"'{queue}' ({QUEUE_MAX_RUNTIME_HOURS[queue]}:00:00)."
            )
        return runtime, queue

    # No queue specified — pick the shortest one that fits.
    for q, max_q_hours in QUEUE_MAX_RUNTIME_HOURS.items():
        if total_hours <= max_q_hours:
            return runtime, q

    # Should be unreachable due to the max_runtime check above.
    raise ValueError(f"No queue can accommodate runtime '{runtime}'.")


def validate_hardware(hardware: list[str]) -> list[str]:
    """Validate that every element of *hardware* is a known hardware type.

    Returns *hardware* unchanged on success.

    Raises
    ------
    ValueError
        If any entry is not in ``AVAILABLE_HARDWARE``.
    """
    bad = set(hardware) - set(AVAILABLE_HARDWARE)
    if bad:
        raise ValueError(
            f"Unsupported hardware: {sorted(bad)}. " f"Supported: {AVAILABLE_HARDWARE}."
        )
    return hardware


def validate_cluster_environment() -> None:
    if "slurm" not in ENV_VARIABLES.HOSTNAME.value:
        raise RuntimeError("This tool must be run from the IHME cluster.")

    submit_host_marker = "slogin"
    if submit_host_marker in ENV_VARIABLES.HOSTNAME.value:
        raise RuntimeError("This tool must not be run from a submit host.")


class NativeSpecification(NamedTuple):
    job_name: str
    project: str
    queue: str
    peak_memory: float  # Memory in GB
    max_runtime: str
    hardware: list[str]

    # Class constant
    NUM_THREADS: int = 1

    def to_jobmon_spec(self, worker_logging_root: Path) -> dict[str, Any]:
        """Build the Jobmon compute resources dict from this NativeSpecification.

        Parameters
        ----------
        worker_logging_root
            Root directory for worker logs.

        Returns
        -------
            Dictionary of compute resources for Jobmon.

        Notes
        -----
        * ``memory`` is passed in **GB** because the Jobmon SLURM plugin performs
          its own GB → MB conversion internally.
        * ``constraints`` is a pipe-separated string of SLURM feature names
          (e.g. ``"r650|r650v2"``), included only when hardware is requested.
        * ``standard_output`` and ``standard_error`` route SLURM stdout/stderr
          to the cluster logs directory. The Jobmon SLURM plugin appends the
          task name and SLURM job ID to these paths automatically.
        """
        resources: dict[str, Any] = {
            "queue": self.queue,
            "project": self.project,
            "memory": self.peak_memory,  # GB – Jobmon converts to MB
            "runtime": self._runtime_to_seconds(self.max_runtime),
            "cores": self.NUM_THREADS,
            "stdout": str(worker_logging_root),
            "stderr": str(worker_logging_root),
        }
        if self.hardware:
            resources["constraints"] = "|".join(self.hardware)
        return resources

    @staticmethod
    def _runtime_to_seconds(runtime_str: str) -> int:
        """Convert HH:MM:SS runtime string to seconds.

        Parameters
        ----------
        runtime_str
            Runtime in HH:MM:SS format.

        Returns
        -------
            Runtime in seconds.
        """
        parts = runtime_str.split(":")
        if len(parts) == 3:
            h, m, s = parts
            return int(h) * 3600 + int(m) * 60 + int(s)
        elif len(parts) == 2:
            m, s = parts
            return int(m) * 60 + int(s)
        else:
            return int(parts[0])
