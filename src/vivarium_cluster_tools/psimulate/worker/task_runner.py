"""
==================
Jobmon Task Runner
==================

CLI entry point for Jobmon worker tasks. Dispatches on the first positional
argument:

* ``simulation`` — load the task's metadata JSON and run the work horse
  in-process. Invoked directly by ``psimulate run`` / ``restart`` /
  ``expand`` / ``load_test``; workflow simulation steps invoke it nested
  inside ``subprocess`` (below).
* ``subprocess`` — spawn the following argv as a child, mirror its stdout
  live, and replay the captured tail to stderr on non-zero exit so failures
  surface in the SLURM stderr file and the Jobmon GUI. Used by every
  workflow step type via
  :func:`~vivarium_cluster_tools.psimulate.wrap_for_subprocess`.

Both modes call ``_configure_dual_sink`` so INFO+ logs land in stdout
(workflow log file) and WARNING+ logs land in stderr (Jobmon GUI).

"""

import argparse
import json
import signal
import subprocess
import sys
from collections import deque
from pathlib import Path

from loguru import logger

from vivarium_cluster_tools.psimulate import COMMANDS
from vivarium_cluster_tools.psimulate.jobs import JobParameters
from vivarium_cluster_tools.psimulate.results.writing import write_task_results
from vivarium_cluster_tools.psimulate.worker.load_test_work_horse import (
    work_horse as load_test_work_horse,
)
from vivarium_cluster_tools.psimulate.worker.vivarium_work_horse import work_horse

BUFFER_MAXLEN: int = 10_000
"""Maximum number of subprocess output lines retained for the failure replay.
Exposed at module level so tests can monkeypatch a smaller cap."""

CHILD_TERMINATE_GRACE_SECONDS: float = 15.0
"""Seconds to wait for the child to exit after SIGTERM before SIGKILLing it.
Sized as a sub-budget of SLURM's default 30s ``KillWait`` so the parent still
has time to replay the buffer to stderr afterwards."""


def _configure_dual_sink() -> None:
    """Route INFO+ to stdout and WARNING+ to stderr.

    Called once at the top of each worker entry point so warnings and
    errors land in the SLURM stderr file and the Jobmon GUI surfaces them.
    Removes loguru's default stderr handler first so INFO-level messages
    don't end up duplicated on stderr.
    """
    logger.remove()
    logger.add(sys.stdout, level="INFO")
    logger.add(sys.stderr, level="WARNING")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse argv for ``simulation`` mode."""
    parser = argparse.ArgumentParser(description="Run a single Jobmon worker task.")
    subparsers = parser.add_subparsers(dest="mode", required=True)

    sim = subparsers.add_parser(
        "simulation",
        help="Run a simulation work_horse in-process from a metadata JSON.",
    )
    sim.add_argument(
        "--metadata-dir",
        type=Path,
        required=True,
        help="Directory containing task metadata JSON files.",
    )
    sim.add_argument(
        "--task-id",
        type=str,
        required=True,
        help="The deterministic task ID.",
    )
    sim.add_argument(
        "--results-dir",
        type=Path,
        required=True,
        help="Directory to write results to.",
    )
    sim.add_argument(
        "--command",
        type=str,
        required=True,
        help="The psimulate command (e.g. run, restart, expand, load_test).",
    )

    return parser.parse_args(argv)


def _run_simulation(args: argparse.Namespace) -> int:
    """Load the task's metadata JSON, dispatch to the work horse, write results."""
    metadata_path = args.metadata_dir / f"{args.task_id}.json"
    logger.info(f"Loading task metadata from {metadata_path}")
    with open(metadata_path) as f:
        task_metadata = json.load(f)

    command = args.command
    job_parameters = JobParameters(**task_metadata)
    task_id = args.task_id

    logger.info(f"Running task {task_id} with command '{command}'")

    if command in (COMMANDS.run, COMMANDS.restart, COMMANDS.expand):
        results_dict = work_horse(job_parameters)
    elif command == COMMANDS.load_test:
        results_df = load_test_work_horse(job_parameters)
        results_dict = {"load_test": results_df}
    else:
        raise ValueError(f"Unknown command: {command}")

    logger.info(f"Task {task_id} completed, writing results.")

    write_task_results(
        results_dir=args.results_dir,
        job_parameters=job_parameters,
        results_dict=results_dict,
    )
    logger.info(f"Task {task_id} results written successfully.")
    return 0


def _run_subprocess(inner_argv: list[str]) -> int:
    """Spawn ``inner_argv`` as a child process with dual-stream logging.

    The child's stdout (with stderr merged in) is mirrored to ``sys.stdout``
    in real time and buffered in a capped deque. On non-zero exit, the
    buffered output is replayed to ``sys.stderr`` so the SLURM stderr file
    (and the Jobmon GUI's "Task Instance stderr" pane) surfaces the failing
    command's output.

    The finally block guarantees the buffer is still replayed — and the child
    is terminated, not orphaned — on parent-killed and exception paths.
    """
    logger.info(f"Running subprocess: {' '.join(inner_argv)}")
    buffered: deque[str] = deque(maxlen=BUFFER_MAXLEN)
    proc = subprocess.Popen(
        inner_argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        text=True,
    )

    def _forward_signal(signum: int, _frame: object) -> None:
        try:
            proc.send_signal(signum)
        except ProcessLookupError:
            pass

    prev_term = signal.signal(signal.SIGTERM, _forward_signal)
    prev_int = signal.signal(signal.SIGINT, _forward_signal)
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            buffered.append(line)
        return proc.wait()
    finally:
        signal.signal(signal.SIGTERM, prev_term)
        signal.signal(signal.SIGINT, prev_int)
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=CHILD_TERMINATE_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        if proc.returncode != 0:
            logger.error(
                f"Subprocess exited with code {proc.returncode}; "
                "replaying output to stderr."
            )
            sys.stderr.writelines(buffered)
            sys.stderr.flush()


def main(argv: list[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    _configure_dual_sink()

    if raw and raw[0] == "subprocess":
        inner_argv = raw[1:]
        if not inner_argv:
            raise ValueError("subprocess mode requires argv to execute.")
        return _run_subprocess(inner_argv)

    return _run_simulation(parse_args(raw))


if __name__ == "__main__":
    sys.exit(main())
