"""
========================
Jobmon Task Runner
========================

Single CLI entry point for Jobmon worker tasks. Two execution modes,
dispatched by the first positional argument:

* ``simulation`` — load a task's metadata JSON, run the appropriate work
  horse in-process, and write its results. Used by simulation steps and
  the legacy ``psimulate run``/``restart``/``expand``/``load_test`` paths.

* ``subprocess`` — spawn the argv after ``--`` as a child process, mirror
  its stdout in real time, and replay the captured output to stderr on
  non-zero exit so the SLURM stderr file (and the Jobmon GUI) surface the
  failing command's output. Used by typed steps (pytest, python, notebook,
  command) via ``BaseStepConfig._wrap_for_logging``.

Both modes share ``configure_dual_sink`` so INFO+ logs land in stdout
(workflow log file) and WARNING+ logs land in stderr (Jobmon GUI).

Usage::

    python -m vivarium_cluster_tools.psimulate.worker.task_runner simulation \\
        --metadata-dir /path/to/metadata \\
        --task-id <task_id> \\
        --results-dir /path/to/results \\
        --command run

    python -m vivarium_cluster_tools.psimulate.worker.task_runner subprocess \\
        -- pytest tests/ -k some_filter

"""

import argparse
import json
import subprocess
import sys
from collections import deque
from pathlib import Path

from loguru import logger

from vivarium_cluster_tools.psimulate import COMMANDS
from vivarium_cluster_tools.psimulate.jobs import JobParameters
from vivarium_cluster_tools.psimulate.results.writing import write_task_results
from vivarium_cluster_tools.psimulate.worker._logging import configure_dual_sink
from vivarium_cluster_tools.psimulate.worker.load_test_work_horse import (
    work_horse as load_test_work_horse,
)
from vivarium_cluster_tools.psimulate.worker.vivarium_work_horse import work_horse

BUFFER_MAXLEN: int = 10_000
"""Maximum number of subprocess output lines retained for the failure replay.
Exposed at module level so tests can monkeypatch a smaller cap."""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse argv for either ``simulation`` or ``subprocess`` mode.

    Returns a namespace with ``mode`` set to one of those strings and
    additional fields depending on the mode.
    """
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

    sub = subparsers.add_parser(
        "subprocess",
        help="Run the argv after '--' as a child process with dual-stream logging.",
    )
    # Everything after `--` is collected by argparse into `inner_argv`.
    sub.add_argument(
        "inner_argv",
        nargs=argparse.REMAINDER,
        help="Argv to execute; must be preceded by a literal '--' separator.",
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


def _run_subprocess(args: argparse.Namespace) -> int:
    """Spawn ``args.inner_argv`` as a child process with dual-stream logging.

    The child's stdout (with stderr merged in) is mirrored to ``sys.stdout``
    in real time and buffered in a capped deque. On non-zero exit, the
    buffered output is replayed to ``sys.stderr`` so the SLURM stderr file
    (and the Jobmon GUI's "Task Instance stderr" pane) surfaces the failing
    command's output.
    """
    inner_argv = list(args.inner_argv)
    if not inner_argv or inner_argv[0] != "--":
        raise ValueError(
            "subprocess mode requires a literal '--' separator before the "
            "argv to run (got: " + repr(inner_argv) + ")."
        )
    inner_argv = inner_argv[1:]
    if not inner_argv:
        raise ValueError("subprocess mode requires argv after the '--' separator.")

    logger.info(f"Running subprocess: {' '.join(inner_argv)}")
    buffered: deque[str] = deque(maxlen=BUFFER_MAXLEN)
    proc = subprocess.Popen(
        inner_argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        text=True,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        buffered.append(line)
    exit_code = proc.wait()

    if exit_code != 0:
        logger.error(f"Subprocess exited with code {exit_code}; replaying output to stderr.")
        sys.stderr.writelines(buffered)
        sys.stderr.flush()
    return exit_code


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_dual_sink()

    if args.mode == "simulation":
        return _run_simulation(args)
    if args.mode == "subprocess":
        return _run_subprocess(args)
    raise ValueError(f"Unknown mode: {args.mode!r}")


if __name__ == "__main__":
    sys.exit(main())
