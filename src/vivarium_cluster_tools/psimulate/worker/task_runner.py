"""
========================
Jobmon Task Runner
========================

CLI entry point for individual Jobmon tasks. Each task loads its job spec
JSON file, runs the appropriate work horse, and writes results directly
to the results directory (one parquet per metric per task, one metadata
CSV per task).

Usage::

    python -m vivarium_cluster_tools.psimulate.worker.task_runner \
        --job-spec-dir /path/to/job_specs \
        --task-id <task_id> \
        --results-dir /path/to/results \
        --worker-log-dir /path/to/worker_logs

"""

import argparse
import json
import sys
from pathlib import Path

from loguru import logger

from vivarium_cluster_tools.psimulate import COMMANDS
from vivarium_cluster_tools.psimulate.jobs import JobParameters


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a single Jobmon task for psimulate.")
    parser.add_argument(
        "--job-spec-dir",
        type=Path,
        required=True,
        help="Directory containing job spec JSON files.",
    )
    parser.add_argument(
        "--task-id",
        type=str,
        required=True,
        help="The deterministic task ID.",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        required=True,
        help="Directory to write results to.",
    )
    parser.add_argument(
        "--worker-log-dir",
        type=Path,
        required=True,
        help="Directory for worker log files.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    # Configure worker logging to a per-task log file
    log_file = args.worker_log_dir / f"{args.task_id}.log"
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add(log_file, level="DEBUG")

    # Set VIVARIUM_LOGGING_DIRECTORY for performance logging inside work horses
    import os

    os.environ["VIVARIUM_LOGGING_DIRECTORY"] = str(args.worker_log_dir)

    job_spec_path = args.job_spec_dir / f"{args.task_id}.json"
    logger.info(f"Loading job spec from {job_spec_path}")
    with open(job_spec_path) as f:
        job_spec = json.load(f)

    command = job_spec["command"]
    job_parameters = JobParameters(**job_spec["job_parameters"])
    task_id = args.task_id

    logger.info(f"Running task {task_id} with command '{command}'")

    if command in (COMMANDS.run, COMMANDS.restart, COMMANDS.expand):
        from vivarium_cluster_tools.psimulate.worker.vivarium_work_horse import work_horse

        metadata_df, results_dict = work_horse(job_parameters, task_id=task_id)
    elif command == COMMANDS.load_test:
        from vivarium_cluster_tools.psimulate.worker.load_test_work_horse import (
            work_horse as load_test_work_horse,
        )
        from vivarium_cluster_tools.psimulate.worker.vivarium_work_horse import (
            format_and_record_details,
        )

        results_df = load_test_work_horse(job_parameters, task_id=task_id)
        metadata_df = format_and_record_details(job_parameters, {"load_test": results_df})
        results_dict = {"load_test": results_df}
    else:
        raise ValueError(f"Unknown command: {command}")

    logger.info(f"Task {task_id} completed, writing results.")

    from vivarium_cluster_tools.psimulate.results.writing import write_task_results

    write_task_results(
        results_dir=args.results_dir,
        task_id=task_id,
        metadata_df=metadata_df,
        results_dict=results_dict,
    )
    logger.info(f"Task {task_id} results written successfully.")


if __name__ == "__main__":
    main()
