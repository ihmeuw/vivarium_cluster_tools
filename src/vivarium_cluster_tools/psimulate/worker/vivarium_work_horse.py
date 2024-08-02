"""
===============
Vivarium Worker
===============

RQ worker executable for running simulation jobs.

"""

import json
import math
import os
from pathlib import Path
from time import time
from traceback import format_exc
from typing import Dict, Optional, Tuple, Union

import dill
import pandas as pd
from layered_config_tree import LayeredConfigTree
from loguru import logger
from rq import get_current_job
from vivarium.framework.engine import SimulationContext
from vivarium.framework.utilities import collapse_nested_dict

import vivarium_cluster_tools.utilities as vct_utils
from vivarium_cluster_tools.psimulate.environment import ENV_VARIABLES
from vivarium_cluster_tools.psimulate.jobs import JobParameters
from vivarium_cluster_tools.vipin.perf_counters import CounterSnapshot

VIVARIUM_WORK_HORSE_IMPORT_PATH = f"{__name__}.work_horse"


class ParallelSimulationContext(SimulationContext):
    """Identical to SimulationContext except that it does not write out the
    results to disk in order to allow them to be batch-written.
    """

    def _write_results(self, results: dict[str, pd.DataFrame]) -> None:
        pass


def work_horse(job_parameters: dict) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    node = f"{ENV_VARIABLES.HOSTNAME.value}"
    job = f"{ENV_VARIABLES.JOB_ID.value}:{ENV_VARIABLES.TASK_ID.value}"

    job_parameters = JobParameters(**job_parameters)

    logger.info(f"Launching new job {job} on {node}")
    logger.info(f"Starting job: {job_parameters}")

    try:
        start_snapshot = CounterSnapshot()
        event = {"start": time()}  # timestamps of application events
        logger.info("Beginning simulation setup.")
        backup = get_backup(job_parameters)
        if backup:
            logger.info(f"Restarting simulation from saved backup")
            sim = backup
            event["simulation_start"] = time()
            exec_time = {
                "setup_minutes": (event["simulation_start"] - event["start"]) / 60
            }  # execution event
            logger.info(
                f'Simulation setup from backup completed in {exec_time["setup_minutes"]:.3f} minutes.'
            )
            logger.info(f"Starting main simulation loop")
        else:
            sim = setup_sim(job_parameters)

            start_time = pd.Timestamp(**sim.configuration.time.start.to_dict())
            end_time = pd.Timestamp(**sim.configuration.time.end.to_dict())
            step_size = pd.Timedelta(days=sim.configuration.time.step_size)
            num_steps = int(math.ceil((end_time - start_time) / step_size))

            start_snapshot = CounterSnapshot()
            event = {"start": time()}  # timestamps of application events
            logger.info("Beginning simulation setup.")
            sim.setup()
            event["simulant_initialization_start"] = time()
            exec_time = {
                "setup_minutes": (event["simulant_initialization_start"] - event["start"])
                / 60
            }  # execution event
            logger.info(
                f'Simulation setup completed in {exec_time["setup_minutes"]:.3f} minutes.'
            )

            sim.initialize_simulants()
            event["simulation_start"] = time()
            exec_time["simulant_initialization_minutes"] = (
                event["simulation_start"] - event["simulant_initialization_start"]
            ) / 60
            logger.info(
                f'Simulant initialization completed in {exec_time["simulant_initialization_minutes"]:.3f} minutes.'
            )
            logger.info(f"Starting main simulation loop with {num_steps} time steps")
        run(sim, job_parameters)
        event["results_start"] = time()
        exec_time["main_loop_minutes"] = (
            event["results_start"] - event["simulation_start"]
        ) / 60
        exec_time["step_mean_seconds"] = (
            event["results_start"] - event["simulation_start"]
        ) / num_steps
        logger.info(
            f'Simulation main loop completed in {exec_time["main_loop_minutes"]:.3f} minutes.'
        )
        logger.info(f'Average step length was {exec_time["step_mean_seconds"]:.3f} seconds.')

        sim.finalize()
        sim.report(print_results=False)
        event["end"] = time()
        end_snapshot = CounterSnapshot()
        do_sim_epilogue(start_snapshot, end_snapshot, event, exec_time, job_parameters)
        results = sim.get_results()  # Dict[measure, results dataframe]
        finished_results_metadata = format_and_record_details(job_parameters, results)

        return finished_results_metadata, results

    except Exception:
        logger.exception("Unhandled exception in worker")
        job = get_current_job()
        job.meta["root_exception"] = format_exc()
        job.save_meta()
        raise
    finally:
        logger.info(f"Exiting job: {job_parameters}")


def setup_sim(job_parameters: JobParameters) -> ParallelSimulationContext:
    """Set up a simulation context with the branch/job-specific configuration parameters."""
    configuration = LayeredConfigTree(
        job_parameters.branch_configuration, layers=["branch_base", "branch_expanded"]
    )

    configuration.update(
        parameter_update_format(job_parameters),
        layer="branch_expanded",
        source="branch_config",
    )
    job_parameters.branch_configuration.update(configuration.to_dict())
    sim = ParallelSimulationContext(
        job_parameters.model_specification, configuration=configuration
    )
    logger.info("Simulation configuration:")
    logger.info(str(sim.configuration))

    return sim


def parameter_update_format(
    job_parameters: JobParameters,
) -> Dict[str, Dict[str, Union[str, int, dict]]]:
    return {
        "run_configuration": {
            "run_id": str(get_current_job().id) + "_" + str(time()),
            "results_directory": job_parameters.results_path,
            "run_key": job_parameters.job_specific,
        },
        "randomness": {
            "random_seed": job_parameters.random_seed,
            "additional_seed": job_parameters.input_draw,
        },
        "input_data": {
            "input_draw_number": job_parameters.input_draw,
        },
    }


def do_sim_epilogue(
    start: CounterSnapshot,
    end: CounterSnapshot,
    event: dict,
    exec_time: dict,
    parameters: JobParameters,
) -> None:
    exec_time["results_minutes"] = (event["end"] - event["results_start"]) / 60
    logger.info(f'Results reporting completed in {exec_time["results_minutes"]:.3f} minutes.')
    exec_time["total_minutes"] = (event["end"] - event["start"]) / 60
    logger.info(f'Total simulation run time {exec_time["total_minutes"]:.3f} minutes.')

    perf_log = logger.add(
        Path(ENV_VARIABLES.VIVARIUM_LOGGING_DIRECTORY.value)
        / f"perf.{ENV_VARIABLES.JOB_ID.value}.{ENV_VARIABLES.TASK_ID.value}.log",
        level="DEBUG",
        serialize=True,
    )
    logger.debug(
        json.dumps(
            {
                "host": ENV_VARIABLES.HOSTNAME.value,
                "job_number": ENV_VARIABLES.JOB_ID.value,
                "task_number": ENV_VARIABLES.TASK_ID.value,
                "draw": parameters.input_draw,
                "seed": parameters.random_seed,
                "scenario": parameters.branch_configuration,
                "event": event,
                "exec_time": exec_time,
                "counters": (end - start).to_dict(),
            }
        )
    )
    logger.remove(perf_log)


def format_and_record_details(
    job_parameters: JobParameters, results: Dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Add finished simulation details to results and metadata."""
    finished_results_metadata = pd.DataFrame(index=[0])
    for key, val in collapse_nested_dict(job_parameters.branch_configuration):
        # Exclude the run_configuration values from branch_configuration
        # since they are duplicates. Also do not include the additional_seed
        # value since it is identical to input_draw
        col_name = key.split(".")[-1]
        col_name = "input_draw" if col_name == "input_draw_number" else col_name
        if not (key.startswith("run_configuration") or "additional_seed" in key):
            for df in results.values():
                # insert the new columns second from the right and use the
                # last part of the key as the column name
                df.insert(df.shape[1] - 1, col_name, val)
        finished_results_metadata[key] = val
    return finished_results_metadata


def run(sim: SimulationContext, job_parameters: JobParameters):
    backup_freq = job_parameters.backup_configuration["backup_freq"]
    if backup_freq:
        backup_dir = job_parameters.backup_configuration["backup_dir"]
        backup_path = (backup_dir / str(get_current_job().id)).with_suffix(".pkl")
        time_to_save = time() + backup_freq
        while not sim.past_stop_time():
            sim.step()
            if time() >= time_to_save:
                sim.write_backup(backup_path)
                time_to_save = time() + backup_freq
    else:
        sim.run()


def get_backup(job_parameters: JobParameters) -> Optional[SimulationContext]:
    backup_dir = job_parameters.backup_configuration["backup_dir"]
    if not backup_dir.exists() or not os.listdir(backup_dir):
        # No sim backups to load
        return None
    metadata_path = job_parameters.backup_configuration["backup_metadata_path"]
    try:
        pickle_metadata = pd.read_csv(metadata_path)
    except FileNotFoundError:
        return None
    query_conditions = f"input_draw == {job_parameters.input_draw} & random_seed == {job_parameters.random_seed}"

    # Add branch parameter conditions to the query
    for k, v in collapse_nested_dict(job_parameters.branch_configuration):
        query_conditions += f' & `{k}` == "{v}"'

    # Use the query method to find rows that match the lookup parameters
    run_ids = pickle_metadata.query(query_conditions)["job_id"].to_list()
    if run_ids:
        filename = Path(backup_dir / run_ids[0]).with_suffix(".pkl")
        if filename.exists():
            with open(filename, "rb") as f:
                sim = dill.load(f)
            return sim
    return None
