"""
================
psimulate Runner
================

The main process loop for `psimulate` runs.

"""
from collections import defaultdict
from pathlib import Path
from time import sleep, time
from typing import Optional

import glob
import json
import math
import pandas as pd
from loguru import logger

from vivarium_cluster_tools import logs
from vivarium_cluster_tools.psimulate import (
    COMMANDS,
    branches,
    cluster,
    jobs,
    model_specification,
    paths,
    pip_env,
    redis_dbs,
    results,
    worker,
)
from vivarium_cluster_tools.vipin.perf_report import report_performance
from vivarium_cluster_tools.psimulate.paths import CENTRAL_PERFORMANCE_LOGS_DIRECTORY


def process_job_results(
    registry_manager: redis_dbs.RegistryManager,
    existing_outputs: pd.DataFrame,
    output_directory: Path,
    no_batch: bool,
) -> defaultdict:
    written_results = existing_outputs
    unwritten_results = []
    batch_size = 0 if no_batch else 200
    status = defaultdict(int)

    logger.info("Entering main processing loop.")
    start_time = time()
    try:
        while registry_manager.jobs_to_finish:
            sleep(5)
            unwritten_results.extend(registry_manager.get_results())

            if len(unwritten_results) > batch_size:
                written_results, unwritten_results = results.write_results_batch(
                    output_directory,
                    written_results,
                    unwritten_results,
                    batch_size,
                )

            status = registry_manager.update_and_report()
            logger.info(f"Unwritten results: {len(unwritten_results)}")
            logger.info(f"Elapsed time: {(time() - start_time)/60:.1f} minutes.")
    finally:
        batch_size = 500
        while unwritten_results:
            written_results, unwritten_results = results.write_results_batch(
                output_directory,
                written_results,
                unwritten_results,
                batch_size=batch_size,
            )
            logger.info(f"Unwritten results: {len(unwritten_results)}")
            logger.info(f"Elapsed time: {(time() - start_time) / 60:.1f} minutes.")
        return status


def load_existing_outputs(result_path: Path, restart: bool) -> pd.DataFrame:
    try:
        existing_outputs = pd.read_hdf(result_path)
    except FileNotFoundError:
        existing_outputs = pd.DataFrame()
    assert (
        existing_outputs.empty or restart
    ), "How do you have existing outputs on an initial run?"
    return existing_outputs


def report_initial_status(
    num_jobs_completed: int, existing_outputs: pd.DataFrame, keyspace: branches.Keyspace
) -> None:
    if num_jobs_completed:
        logger.info(
            f"{num_jobs_completed} of {len(keyspace)} jobs completed in previous run."
        )
    if num_jobs_completed != len(existing_outputs):
        extra_jobs_completed = num_jobs_completed - len(existing_outputs)
        logger.warning(
            f"There are {extra_jobs_completed} jobs from the previous run which would not have been created "
            "with the configuration saved with the run. That either means that code "
            "has changed between then and now or that the outputs or configuration data "
            "have been modified. This may represent a serious error so give it some thought."
        )


def append_perf_data_to_central_logs(perf_df: pd.DataFrame, log_path: Path) -> None:
    """Append performance data to the central logs."""
    NUM_ROWS_PER_FILE = 100_000

    central_perf_df = perf_df.reset_index().copy()
    # add location data to central_perf_df
    artifact_path_col = "scenario_input_data_artifact_path"
    if (
        artifact_path_col in central_perf_df.columns
    ):  # if we parallelized across artifact paths
        central_perf_df["location"] = central_perf_df[artifact_path_col].apply(
            lambda filepath: Path(filepath).stem
        )
    else:  # else get from output directory
        central_perf_df["location"] = log_path.parents[3].stem

    ## aggregate scenario information into one column
    all_scenario_cols = [
        col for col in central_perf_df.columns if col.startswith("scenario_")
    ]
    # remove duplicate scenario information
    unique_scenario_cols = [
        col for col in all_scenario_cols if not col.startswith("scenario_run_configuration")
    ]
    central_perf_df["scenario_parameters"] = central_perf_df[unique_scenario_cols].to_dict(
        orient="records"
    )
    central_perf_df["scenario_parameters"] = central_perf_df["scenario_parameters"].apply(
        json.dumps
    )
    central_perf_df = central_perf_df.drop(all_scenario_cols, axis=1)
    # drop compound scenario col if it exists
    central_perf_df = central_perf_df.drop("compound_scenario", axis=1, errors="ignore")

    # append child job data
    log_files = glob.glob(
        CENTRAL_PERFORMANCE_LOGS_DIRECTORY.as_posix() + "/log_summary_*.csv"
    )
    most_recent_file_path = sorted(log_files)[-1]
    most_recent_data = pd.read_csv(most_recent_file_path)
    first_file_with_data = most_recent_file_path

    data_fits_in_file = (len(most_recent_data) + len(central_perf_df)) <= NUM_ROWS_PER_FILE

    if data_fits_in_file:
        central_perf_df.to_csv(most_recent_file_path, mode="a", header=False, index=False)
    else:
        # fill up the most recent file (possibly with 0 rows)
        rows_to_append = NUM_ROWS_PER_FILE - len(most_recent_data)
        central_perf_df[:rows_to_append].to_csv(
            most_recent_file_path, mode="a", header=False, index=False
        )
        # remove rows appended to most recent file from dataframe
        central_perf_df = central_perf_df[rows_to_append:]

        most_recent_index = int(Path(most_recent_file_path).stem.replace("log_summary_", ""))
        new_index = most_recent_index + 1

        # record first file with data as new file if no data was appended to most recent file
        if rows_to_append == 0:
            formatted_new_index = str(new_index).zfill(4)
            first_file_with_data = (
                    CENTRAL_PERFORMANCE_LOGS_DIRECTORY / f"log_summary_{formatted_new_index}.csv"
            )

        num_new_files = math.ceil(len(central_perf_df) / NUM_ROWS_PER_FILE)

        for file_num in range(num_new_files):
            formatted_new_index = str(new_index).zfill(4)
            new_file = (
                    CENTRAL_PERFORMANCE_LOGS_DIRECTORY / f"log_summary_{formatted_new_index}.csv"
            )
            start_idx = NUM_ROWS_PER_FILE * file_num
            end_idx = NUM_ROWS_PER_FILE * (file_num + 1)
            central_perf_df[start_idx:end_idx].to_csv(new_file, index=False)
            new_index += 1

    # append runner data
    runner_data = pd.DataFrame(
        {"job_number": [int(central_perf_df["job_number"].unique()[0])]}
    )  # only one job number
    runner_data["project_name"] = log_path.parents[6].stem
    runner_data["root_path"] = log_path.parents[3]
    runner_data["original_run_date"] = log_path.parents[2].stem
    full_run_date = log_path.parents[0].stem
    runner_data["run_date"] = full_run_date[: full_run_date.rindex("_")]
    runner_data["run_type"] = full_run_date[full_run_date.rindex("_") + 1 :]
    runner_data["log_summary_file_path"] = first_file_with_data
    runner_data["original_log_file_path"] = (log_path / "log_summary.csv").as_posix()
    runner_data_file = CENTRAL_PERFORMANCE_LOGS_DIRECTORY / "runner_data.csv"
    runner_data.to_csv(runner_data_file, mode="a", header=False, index=False)


def try_run_vipin(log_path: Path) -> None:
    try:
        perf_df = report_performance(
            input_directory=log_path, output_directory=log_path, output_hdf=False, verbose=1
        )
    except Exception as e:
        logger.warning(f"Performance reporting failed with: {e}")

    try:
        append_perf_data_to_central_logs(perf_df, log_path)
    except Exception as e:
        logger.warning(f"Appending performance data to central logs failed with: {e}")


def main(
    command: str,
    input_paths: paths.InputPaths,
    native_specification: cluster.NativeSpecification,
    max_workers: Optional[int],
    redis_processes: int,
    no_batch: bool,
    extra_args: dict,
) -> None:
    logger.info("Validating cluster environment.")
    cluster.validate_cluster_environment()

    # Generate programmatic representation of the output directory structure
    output_paths = paths.OutputPaths.from_entry_point_args(
        command=command,
        input_artifact_path=input_paths.artifact,
        result_directory=input_paths.result_directory,
        input_model_spec_path=input_paths.model_specification,
    )
    logger.info("Setting up output directory and all subdirectories.")
    output_paths.touch()

    logger.info("Setting up logging to files.")
    # Start sending logs to a file now that it exists.
    logs.configure_main_process_logging_to_file(output_paths.logging_root)
    logger.info("Validating programming environment.")
    # Either write a requirements.txt with the current environment
    # or verify the current environment matches the prior environment
    # used when doing a restart.
    pip_env.validate(output_paths.environment_file)

    logger.info(
        "Parsing input arguments into model specification and branches and writing to disk."
    )
    # Parse the branches configuration into a parameter space
    # and a flat representation of all parameters to be run.
    keyspace = branches.Keyspace.from_entry_point_args(
        input_branch_configuration_path=input_paths.branch_configuration,
        keyspace_path=output_paths.keyspace,
        branches_path=output_paths.branches,
        extras=extra_args,
    )
    # Throw that into our output directory. The keyspace output is
    # a cartesian product representation of the parameter space and
    # branches is a flat representation with the product expanded out.
    keyspace.persist(output_paths.keyspace, output_paths.branches)

    # Parse the model specification and resolve the artifact path
    # and then write to the output directory.
    model_spec = model_specification.parse(
        command=command,
        input_model_specification_path=input_paths.model_specification,
        artifact_path=input_paths.artifact,
        model_specification_path=output_paths.model_specification,
        results_root=output_paths.root,
        keyspace=keyspace,
    )
    model_specification.persist(model_spec, output_paths.model_specification)

    logger.info("Loading existing outputs if present.")
    # Load in any existing partial outputs if present.
    existing_outputs = load_existing_outputs(
        result_path=output_paths.results,
        restart=command in [COMMANDS.restart, COMMANDS.expand],
    )

    logger.info("Parsing arguments into worker job parameters.")
    # Translate the keyspace into the list of jobs to actually run
    # after accounting for any partially present results.
    job_parameters, num_jobs_completed = jobs.build_job_list(
        command=command,
        model_specification_path=output_paths.model_specification,
        output_root=output_paths.root,
        keyspace=keyspace,
        existing_outputs=existing_outputs,
        extras=extra_args,
    )
    # Let the user know if something is fishy at this point.
    report_initial_status(num_jobs_completed, existing_outputs, keyspace)
    if len(job_parameters) == 0:
        logger.info("No jobs to run, exiting.")
        return
    else:
        logger.info(f"Found {len(job_parameters)} jobs to run.")

    num_workers = len(job_parameters)
    if max_workers:
        num_workers = min(max_workers, num_workers)

    logger.info("Spinning up Redis DBs and connecting to main process.")
    # Spin up the job & result dbs and get back (hostname, port) pairs for all the dbs.
    redis_ports = redis_dbs.launch(
        num_processes=redis_processes,
        num_workers=num_workers,
        redis_logging_root=output_paths.logging_root,
    )
    # Spin up a unified interface to all the redis databases
    registry_manager = redis_dbs.RegistryManager(redis_ports, num_workers, num_jobs_completed)
    logger.info("Enqueuing jobs on Redis queues.")
    # Distribute all the remaining jobs across the job queues
    # in the redis databases.
    registry_manager.enqueue(
        jobs=job_parameters, workhorse_import_path=worker.WORK_HORSE_PATHS[command]
    )
    # Generate a worker template that chooses a redis DB at random to connect to.
    # This should (approximately) evenly distribute the workers over the work.
    redis_urls = [f"redis://{hostname}:{port}" for hostname, port in redis_ports]
    worker_template = (
        f"import random\nredis_urls = {redis_urls}\nREDIS_URL = random.choice(redis_urls)\n\n"
    )
    # Dump the worker config to a file we can pass to the workers on startup.
    output_paths.worker_settings.write_text(worker_template)
    worker_launch_script = worker.build_launch_script(
        worker_settings_file=output_paths.worker_settings,
        worker_log_directory=output_paths.worker_logging_root,
    )
    logger.info(f"Submitting redis workers to the cluster.")
    # Start an rq worker for every job using the cluster scheduler, up to an
    # optional user-specified limit. The workers will start as soon as they get
    # scheduled and start looking for work. They run in burst mode which means
    # they shut down if they can't find anything to do. This means it's
    # critical that we put the jobs on the queue before the workers land,
    # otherwise they'll just show up and shut down.

    cluster.submit_worker_jobs(
        num_workers=num_workers,
        worker_launch_script=worker_launch_script,
        cluster_logging_root=output_paths.cluster_logging_root,
        native_specification=native_specification,
    )

    logger.info(
        "Entering monitoring and results processing loop. "
        f"Results will be written to {str(output_paths.root)}"
    )
    # Enter the main monitoring and processing loop, which will check on
    # all the queues periodically, report status updates, and gather
    # and write results when they are available.
    status = process_job_results(
        registry_manager=registry_manager,
        existing_outputs=existing_outputs,
        output_directory=output_paths.root,
        no_batch=no_batch,
    )

    # Spit out a performance report for the workers.
    try_run_vipin(output_paths.worker_logging_root)

    # Emit warning if any jobs failed
    if status["failed"] > 0:
        logger.warning(
            f"*** NOTE: There {'was' if status['failed'] == 1 else 'were'} "
            f"{status['failed']} failed job{'' if status['failed'] == 1 else 's'}. ***"
        )

    logger.info(
        f"{status['finished']} of {status['total']} jobs completed successfully. "
        f"Results written to: {str(output_paths.root)}"
    )
