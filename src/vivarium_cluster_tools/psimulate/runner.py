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
from typing import Dict, List, Tuple

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

from vivarium_cluster_tools.psimulate import cluster
from vivarium_cluster_tools.psimulate import globals as vct_globals
from vivarium_cluster_tools.psimulate import programming_environment, utilities
from vivarium_cluster_tools.psimulate.branches import Keyspace
from vivarium_cluster_tools.psimulate.registry import RegistryManager
from vivarium_cluster_tools.vipin.perf_report import report_performance


class RunContext:
    def __init__(
        self,
        model_specification_file: str,
        branch_configuration_file: str,
        artifact_path: str,
        output_directory: Path,
        logging_directories: Dict[str, Path],
        num_input_draws: int,
        num_random_seeds: int,
        restart: bool,
        expand: Dict[str, int],
        no_batch: bool,
    ):
        self.number_already_completed = 0
        self.output_directory = output_directory
        self.no_batch = no_batch
        self.sge_log_directory = logging_directories["sge"]
        self.worker_log_directory = logging_directories["worker"]

        if restart:
            self.keyspace = Keyspace.from_previous_run(self.output_directory)
            self.existing_outputs = pd.read_hdf(self.output_directory / "output.hdf")
            if expand:
                self.keyspace.add_draws(expand["num_draws"])
                self.keyspace.add_seeds(expand["num_seeds"])
                self.keyspace.persist(self.output_directory)
        else:
            model_specification = build_model_specification(model_specification_file)

            self.keyspace = Keyspace.from_branch_configuration(
                num_input_draws, num_random_seeds, branch_configuration_file
            )
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

            with open(
                self.output_directory / vct_globals.MODEL_SPEC_FILENAME, "w"
            ) as config_file:
                yaml.dump(model_specification.to_dict(), config_file)

            self.existing_outputs = None

            # Log some basic stuff about the simulation to be run.
            self.keyspace.persist(self.output_directory)
        self.model_specification = self.output_directory / vct_globals.MODEL_SPEC_FILENAME


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


def concat_preserve_types(df_list: List[pd.DataFrame]) -> pd.DataFrame:
    """Concatenation preserves all ``numpy`` dtypes but does not preserve any
    pandas specific dtypes (e.g., categories become objects."""
    dtypes = df_list[0].dtypes
    columns_by_dtype = [list(dtype_group.index) for _, dtype_group in dtypes.groupby(dtypes)]

    splits = []
    for columns in columns_by_dtype:
        slices = [df.filter(columns) for df in df_list]
        splits.append(pd.DataFrame(data=np.concatenate(slices), columns=columns))
    return pd.concat(splits, axis=1)


def concat_results(
    old_results: pd.DataFrame, new_results: List[pd.DataFrame]
) -> pd.DataFrame:
    # Skips all the pandas index checking because columns are in the same order.
    start = time()

    to_concat = [d.reset_index(drop=True) for d in new_results]
    if not old_results.empty:
        to_concat += [old_results.reset_index(drop=True)]

    results = concat_preserve_types(to_concat)

    end = time()
    logger.info(f"Concatenated {len(new_results)} results in {end - start:.2f}s.")
    return results


def write_results_batch(
    ctx: RunContext,
    written_results: pd.DataFrame,
    unwritten_results: List[pd.DataFrame],
    batch_size: int = 50,
) -> Tuple[pd.DataFrame, List[pd.DataFrame]]:
    new_results_to_write, unwritten_results = (
        unwritten_results[:batch_size],
        unwritten_results[batch_size:],
    )
    results_to_write = concat_results(written_results, new_results_to_write)

    start = time()
    retries = 3
    while retries:
        try:
            output_path = ctx.output_directory / "output.hdf"
            # Writing to an hdf over and over balloons the file size so write to new file and move it over to avoid
            temp_output_path = output_path.with_name(output_path.name + "update")
            results_to_write.to_hdf(temp_output_path, "data")
            temp_output_path.replace(output_path)
            break
        except Exception as e:
            logger.warning(
                f"Error trying to write results to hdf, retries remaining {retries}"
            )
            sleep(30)
            retries -= 1
            if not retries:
                logger.warning(f"Retries exhausted.")
                raise e
    end = time()
    logger.info(f"Updated output.hdf in {end - start:.4f}s.")
    return results_to_write, unwritten_results


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
                written_results, unwritten_results = write_results_batch(
                    ctx, written_results, unwritten_results, len(unwritten_results)
                )
            elif len(unwritten_results) > batch_size:
                written_results, unwritten_results = write_results_batch(
                    ctx, written_results, unwritten_results, batch_size
                )

            registry_manager.update_and_report()
            logger.info(f"Unwritten results: {len(unwritten_results)}")
            logger.info(f"Elapsed time: {(time() - start_time)/60:.1f} minutes.")
    finally:
        batch_size = 500
        while unwritten_results:
            written_results, unwritten_results = write_results_batch(
                ctx, written_results, unwritten_results, batch_size=batch_size
            )
            logger.info(f"Unwritten results: {len(unwritten_results)}")
            logger.info(f"Elapsed time: {(time() - start_time) / 60:.1f} minutes.")


def try_run_vipin(log_path: Path):

    try:
        report_performance(
            input_directory=log_path, output_directory=log_path, output_hdf=False, verbose=1
        )
    except Exception as e:
        logger.warning(f"Performance reporting failed with: {e}")


def main(
    model_specification_file: str,
    branch_configuration_file: str,
    artifact_path: str,
    result_directory: str,
    native_specification: dict,
    redis_processes: int,
    num_input_draws: int = None,
    num_random_seeds: int = None,
    restart: bool = False,
    expand: Dict[str, int] = None,
    no_batch: bool = False,
    no_cleanup: bool = False,
):
    cluster.exit_if_on_submit_host(cluster.get_hostname())

    output_dir, logging_dirs = utilities.setup_directories(
        model_specification_file,
        result_directory,
        restart,
        expand=bool(num_input_draws or num_random_seeds),
    )

    if not no_cleanup:
        atexit.register(utilities.check_for_empty_results_dir, output_dir=output_dir)

    atexit.register(lambda: logger.remove())

    native_specification["job_name"] = output_dir.parts[-2]
    native_specification = cluster.NativeSpecification(**native_specification)

    utilities.configure_main_process_logging_to_file(logging_dirs["main"])
    programming_environment.validate(output_dir)

    ctx = RunContext(
        model_specification_file,
        branch_configuration_file,
        artifact_path,
        output_dir,
        logging_dirs,
        num_input_draws,
        num_random_seeds,
        restart,
        expand,
        no_batch,
    )
    cluster.check_user_sge_config()
    jobs = build_job_list(ctx)

    if len(jobs) == 0:
        logger.info("Nothing to do")
        return

    logger.info("Starting jobs. Results will be written to: {}".format(ctx.output_directory))

    if redis_processes == -1:
        redis_processes = int(math.ceil(len(jobs) / cluster.DEFAULT_JOBS_PER_REDIS_INSTANCE))

    worker_template, redis_ports = cluster.launch_redis_processes(
        redis_processes, logging_dirs
    )
    worker_file = output_dir / "settings.py"
    with worker_file.open("w") as f:
        f.write(worker_template)

    registry_manager = RegistryManager(redis_ports, ctx.number_already_completed)
    registry_manager.enqueue(jobs)

    cluster.start_cluster(
        len(jobs),
        ctx.sge_log_directory,
        ctx.worker_log_directory,
        worker_file,
        native_specification,
    )

    process_job_results(registry_manager, ctx)

    try_run_vipin(logging_dirs["worker"])

    logger.info("Jobs completed. Results written to: {}".format(ctx.output_directory))
