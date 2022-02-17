"""
================
psimulate Runner
================

The main process loop for `psimulate` runs.

"""
import atexit
import math
from pathlib import Path
from time import sleep, time
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import yaml
from loguru import logger
from vivarium.framework.artifact import parse_artifact_path_config
from vivarium.framework.configuration import (
    ConfigurationError,
    build_model_specification,
)
from vivarium.framework.utilities import collapse_nested_dict

from vivarium_cluster_tools import logs
from vivarium_cluster_tools.psimulate import cluster
from vivarium_cluster_tools.psimulate import globals as vct_globals
from vivarium_cluster_tools.psimulate import paths, programming_environment, results
from vivarium_cluster_tools.psimulate.branches import Keyspace
from vivarium_cluster_tools.psimulate.paths import OutputPaths, delete_on_catastrophic_failure
from vivarium_cluster_tools.psimulate.registry import RegistryManager
from vivarium_cluster_tools.vipin.perf_report import report_performance


class RunContext:
    def __init__(
        self,
        input_paths: paths.InputPaths,
        output_paths: paths.OutputPaths,
        restart: bool,
        expand: Dict[str, int],
        no_batch: bool,
    ):
        self.number_already_completed = 0
        self.output_paths = output_paths
        self.no_batch = no_batch

        if restart:
            self.keyspace = Keyspace.from_previous_run(self.output_paths.keyspace, self.output_paths.branches)
            self.existing_outputs = pd.read_hdf(self.output_paths.results)
            if expand:
                self.keyspace.add_draws(expand["num_draws"])
                self.keyspace.add_seeds(expand["num_seeds"])
                self.keyspace.persist(self.output_paths.keyspace, self.output_paths.branches)
        else:

            self.existing_outputs = None

            # Log some basic stuff about the simulation to be run.
            self.keyspace.persist(self.output_paths.root)


def build_job_list(ctx: RunContext) -> List[dict]:
    jobs = []
    number_already_completed = 0

    for (input_draw, random_seed, branch_config) in ctx.keyspace:
        parameters = {
            "model_specification_file": str(ctx.model_specification),
            "branch_configuration": branch_config,
            "input_draw": int(input_draw),
            "random_seed": int(random_seed),
            "results_path": ctx.output_directory,
        }

        do_schedule = True
        if ctx.existing_outputs is not None:
            mask = ctx.existing_outputs.input_draw == int(input_draw)
            mask &= ctx.existing_outputs.random_seed == int(random_seed)
            if branch_config:
                for k, v in collapse_nested_dict(branch_config):
                    if isinstance(v, float):
                        mask &= np.isclose(ctx.existing_outputs[k], v)
                    else:
                        mask &= ctx.existing_outputs[k] == v
            do_schedule = not np.any(mask)

        if do_schedule:
            jobs.append(parameters)
        else:
            number_already_completed += 1

    if number_already_completed:
        logger.info(
            f"{number_already_completed} of {len(ctx.keyspace)} jobs completed in previous run."
        )
        if number_already_completed != len(ctx.existing_outputs):
            logger.warning(
                "There are jobs from the previous run which would not have been created "
                "with the configuration saved with the run. That either means that code "
                "has changed between then and now or that the outputs or configuration data "
                "have been modified. This may represent a serious error so give it some thought."
            )

    ctx.number_already_completed = number_already_completed
    np.random.shuffle(jobs)
    return jobs


def process_job_results(registry_manager: RegistryManager, ctx: RunContext):
    start_time = time()

    if ctx.existing_outputs is not None:
        written_results = ctx.existing_outputs
    else:
        written_results = pd.DataFrame()
    unwritten_results = []

    logger.info("Entering main processing loop.")
    batch_size = 200
    try:
        while registry_manager.jobs_to_finish:
            sleep(5)
            unwritten_results.extend(registry_manager.get_results())
            if ctx.no_batch and unwritten_results:
                written_results, unwritten_results = results.write_results_batch(
                    ctx.output_directory,
                    written_results,
                    unwritten_results,
                    len(unwritten_results),
                )
            elif len(unwritten_results) > batch_size:
                written_results, unwritten_results = results.write_results_batch(
                    ctx.output_directory,
                    written_results,
                    unwritten_results,
                    batch_size,
                )

            registry_manager.update_and_report()
            logger.info(f"Unwritten results: {len(unwritten_results)}")
            logger.info(f"Elapsed time: {(time() - start_time)/60:.1f} minutes.")
    finally:
        batch_size = 500
        while unwritten_results:
            written_results, unwritten_results = results.write_results_batch(
                ctx.output_directory,
                written_results,
                unwritten_results,
                batch_size=batch_size,
            )
            logger.info(f"Unwritten results: {len(unwritten_results)}")
            logger.info(f"Elapsed time: {(time() - start_time) / 60:.1f} minutes.")


def build_model_specification(
    input_model_specification_file: Optional[str],
    artifact_path: Optional[str],
    keyspace
):

    model_specification = build_model_specification(model_specification_file)
    if artifact_path:
        if vct_globals.FULL_ARTIFACT_PATH_KEY in self.keyspace:
            raise ConfigurationError(
                "Artifact path cannot be specified both in the branch specification file"
                " and as a command line argument.",
                artifact_path,
            )
        if not Path(artifact_path).exists():
            raise FileNotFoundError(f"Cannot find artifact at path {artifact_path}")

    elif (
            vct_globals.ARTIFACT_PATH_KEY
            in model_specification.configuration[vct_globals.INPUT_DATA_KEY]
    ):
        artifact_path = parse_artifact_path_config(model_specification.configuration)

    if artifact_path:
        model_specification.configuration[vct_globals.INPUT_DATA_KEY].update(
            {vct_globals.ARTIFACT_PATH_KEY: artifact_path}, source=__file__
        )

    with self.output_paths.model_specification_file.open('w') as config_file:
        yaml.dump(model_specification.to_dict(), config_file)


def try_run_vipin(log_path: Path):

    try:
        report_performance(
            input_directory=log_path, output_directory=log_path, output_hdf=False, verbose=1
        )
    except Exception as e:
        logger.warning(f"Performance reporting failed with: {e}")


def main(
    input_paths: paths.InputPaths,
    native_specification: dict,
    redis_processes: int,
    restart: bool = False,
    expand: Dict[str, int] = None,
    no_batch: bool = False,
    no_cleanup: bool = False,
):
    cluster.exit_if_on_submit_host(cluster.get_hostname())

    # Generate programmatic representation of the output directory structure
    output_paths = paths.OutputPaths.from_entry_point_args(
        input_model_specification_path=input_paths.model_specification,
        result_directory=input_paths.result_directory,
        restart=restart,
        expand=bool(expand),
    )
    # Make output root and all subdirectories.
    output_paths.touch()

    logs.configure_main_process_logging_to_file(output_paths.logging_root)
    programming_environment.validate(output_paths.environment_file)

    keyspace = Keyspace.from_entry_point_args(
        input_branch_configuration_path=input_paths.branch_configuration,
        restart=restart,
        expand=expand,
        keyspace_path=output_paths.keyspace,
        branches_path=output_paths.branches,
    )
    keyspace.persist(output_paths.keyspace, output_paths.branches)

    native_specification["job_name"] = output_paths.root.parts[-2]
    native_specification = cluster.NativeSpecification(**native_specification)

    ctx = RunContext(
        input_paths,
        output_paths,
        restart,
        expand,
        no_batch,
    )
    cluster.check_user_sge_config()
    jobs = build_job_list(ctx)

    if len(jobs) == 0:
        logger.info("Nothing to do")
        return

    logger.info(f"Starting jobs. Results will be written to: {str(output_paths.root)}")

    if redis_processes == -1:
        redis_processes = int(math.ceil(len(jobs) / cluster.DEFAULT_JOBS_PER_REDIS_INSTANCE))

    worker_template, redis_ports = cluster.launch_redis_processes(
        redis_processes,
        output_paths.logging_root,
    )
    output_paths.worker_settings.write_text(worker_template)

    registry_manager = RegistryManager(redis_ports, ctx.number_already_completed)
    registry_manager.enqueue(jobs)

    cluster.start_cluster(
        len(jobs),
        output_paths.cluster_logging_root,
        output_paths.worker_logging_root,
        output_paths.worker_settings,
        native_specification,
    )

    atexit.register(lambda: logger.remove())
    if not no_cleanup:
        atexit.register(delete_on_catastrophic_failure, output_paths=output_paths)

    process_job_results(registry_manager, ctx)

    try_run_vipin(output_paths.worker_logging_root)

    logger.info(f"Jobs completed. Results written to: {str(output_paths.root)}")
