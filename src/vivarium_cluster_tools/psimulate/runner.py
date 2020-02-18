import os
import atexit
import math
import tempfile
import shutil
import socket
import subprocess
from pathlib import Path
from time import sleep, time
from types import SimpleNamespace

from loguru import logger
import numpy as np
import pandas as pd

from vivarium.framework.configuration import build_model_specification
from vivarium.framework.utilities import collapse_nested_dict
from vivarium.framework.artifact import parse_artifact_path_config

from vivarium_cluster_tools.psimulate.branches import Keyspace
from vivarium_cluster_tools.psimulate import globals as vtc_globals, utilities
from vivarium_cluster_tools.psimulate.registry import RegistryManager

drmaa = utilities.get_drmaa()


def init_job_template(jt, peak_memory, max_runtime, priority, queue, sge_log_directory,
                      worker_log_directory, project, job_name, worker_settings_file):
    launcher = tempfile.NamedTemporaryFile(mode='w', dir='.', prefix='vivarium_cluster_tools_launcher_',
                                           suffix='.sh', delete=False)
    atexit.register(lambda: os.remove(launcher.name))
    output_dir = str(worker_settings_file.resolve().parent)
    launcher.write(f'''
    export VIVARIUM_LOGGING_DIRECTORY={worker_log_directory}
    export PYTHONPATH={output_dir}:$PYTHONPATH

    {shutil.which('rq')} worker -c {worker_settings_file.stem} --name ${{JOB_ID}}.${{SGE_TASK_ID}} --burst -w "vivarium_cluster_tools.psimulate.distributed_worker.ResilientWorker" --exception-handler "vivarium_cluster_tools.psimulate.distributed_worker.retry_handler" vivarium

    ''')
    launcher.close()

    jt.workingDirectory = os.getcwd()
    jt.remoteCommand = shutil.which('sh')
    jt.args = [launcher.name]
    jt.outputPath = f":{sge_log_directory}"
    jt.errorPath = f":{sge_log_directory}"
    sge_cluster = utilities.get_cluster_name()
    jt.jobEnvironment = {
        'LC_ALL': 'en_US.UTF-8',
        'LANG': 'en_US.UTF-8',
        'SGE_CLUSTER_NAME': sge_cluster,
    }
    jt.joinFiles = True
    jt.nativeSpecification = utilities.get_uge_specification(peak_memory, max_runtime, priority,
                                                             queue, project, job_name)
    return jt


def get_random_free_port():
    # NOTE: this implementation is vulnerable to rare race conditions where some other process gets the same
    # port after we free our socket but before we use the port number we got. Should be so rare in practice
    # that it doesn't matter.
    s = socket.socket()
    s.bind(('', 0))
    port = s.getsockname()[1]
    s.close()
    return port


def launch_redis(port):
    try:
        # inline config for redis server.
        redis_process = subprocess.Popen(["redis-server", "--port", f"{port}",
                                          "--timeout", "2",
                                          "--protected-mode", "no"], stdout=subprocess.DEVNULL,
                                         stderr=subprocess.DEVNULL)
    except FileNotFoundError:
        raise OSError("In order for redis to launch you need both the redis client and the python bindings. "
                      "You seem to be missing the redis client.  Do 'conda install redis' and try again. If "
                      "failures continue you may need to download redis yourself, make it and add it to PATH.")
    atexit.register(redis_process.kill)
    return redis_process


def launch_redis_processes(num_processes):
    hostname = socket.getfqdn()
    redis_ports = []
    for i in range(num_processes):
        port = get_random_free_port()
        logger.info(f'Starting Redis Broker at {hostname}:{port}')
        launch_redis(port)
        redis_ports.append((hostname, port))

    redis_urls = [f'redis://{hostname}:{port}' for hostname, port in redis_ports]
    worker_config = f"import random\nredis_urls = {redis_urls}\nREDIS_URL = random.choice(redis_urls)\n\n"
    return worker_config, redis_ports


def start_cluster(drmaa_session, num_workers, peak_memory, max_runtime, priority, queue,
                  sge_log_directory, worker_log_directory, project, worker_settings_file, job_name="vivarium"):
    s = drmaa_session
    jt = init_job_template(s.createJobTemplate(), peak_memory, max_runtime, priority, queue,
                           sge_log_directory, worker_log_directory, project, job_name, worker_settings_file)
    if num_workers:
        job_ids = s.runBulkJobs(jt, 1, num_workers, 1)
        array_job_id = job_ids[0].split('.')[0]

        def kill_jobs():
            if "drmaa" not in dir():
                # FIXME: The global drmaa should be available here.
                # This is maybe a holdover from old code?
                # Maybe something to do with atexit?
                drmaa = utilities.get_drmaa()

            try:
                s.control(array_job_id, drmaa.JobControlAction.TERMINATE)
            # FIXME: Hack around issue where drmaa.errors sometimes doesn't
            # exist.
            except Exception as e:
                if 'There are no jobs registered' in str(e):
                    # This is the case where all our workers have already shut down
                    # on their own, which isn't actually an error.
                    pass
                elif 'Discontinued delete' in str(e):
                    # sge has already cleaned up some of the jobs.
                    pass
                else:
                    raise

        atexit.register(kill_jobs)


class RunContext:
    def __init__(self, arguments):
        # TODO This constructor has side effects (it creates directories under some circumstances) which is weird.
        # It should probably be split into two phases with the side effects in the second phase.

        self.cluster_project = arguments.project
        self.peak_memory = arguments.peak_memory
        self.max_runtime = arguments.max_runtime
        self.priority = arguments.priority,
        self.queue = arguments.queue,
        self.number_already_completed = 0
        self.output_directory = arguments.output_directory
        self.job_name = arguments.output_directory.parts[-2]  # The model specification name.
        self.no_batch = arguments.no_batch
        self.sge_log_directory = arguments.logging_directories['sge']
        self.worker_log_directory = arguments.logging_directories['worker']

        if arguments.restart:
            self.keyspace = Keyspace.from_previous_run(self.output_directory)
            self.existing_outputs = pd.read_hdf(self.output_directory / 'output.hdf')
            if arguments.expand:
                self.keyspace.add_draws(arguments.expand['num_draws'])
                self.keyspace.add_seeds(arguments.expand['num_seeds'])
                self.keyspace.persist(self.output_directory)
        else:
            model_specification = build_model_specification(arguments.model_specification_file)

            self.keyspace = Keyspace.from_branch_configuration(arguments.num_input_draws, arguments.num_random_seeds,
                                                               arguments.branch_configuration_file)

            if "input_data.artifact_path" in self.keyspace.get_data():
                raise ValueError("An artifact path can only be supplied in the model specification file, "
                                 "not the branches configuration.")

            if "artifact_path" in model_specification.configuration.input_data:
                artifact_path = parse_artifact_path_config(model_specification.configuration)
                model_specification.configuration.input_data.update(
                    {"artifact_path": artifact_path},
                    source=__file__)

            model_specification_path = self.output_directory / 'model_specification.yaml'
            shutil.copy(arguments.model_specification_file, model_specification_path)

            self.existing_outputs = None

            # Log some basic stuff about the simulation to be run.
            self.keyspace.persist(self.output_directory)
        self.model_specification = self.output_directory / 'model_specification.yaml'


def build_job_list(ctx):
    jobs = []
    number_already_completed = 0

    for (input_draw, random_seed, branch_config) in ctx.keyspace:
        parameters = {'model_specification_file': str(ctx.model_specification),
                      'branch_configuration': branch_config,
                      'input_draw': int(input_draw),
                      'random_seed': int(random_seed),
                      'results_path': ctx.output_directory,
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
        logger.info(f"{number_already_completed} of {len(ctx.keyspace)} jobs completed in previous run.")
        if number_already_completed != len(ctx.existing_outputs):
            logger.warning("There are jobs from the previous run which would not have been created "
                           "with the configuration saved with the run. That either means that code "
                           "has changed between then and now or that the outputs or configuration data "
                           "have been modified. This may represent a serious error so give it some thought.")

    ctx.number_already_completed = number_already_completed
    np.random.shuffle(jobs)
    return jobs


def concat_preserve_types(df_list):
    """Concatenation preserves all ``numpy`` dtypes but does not preserve any
    pandas speciifc dtypes (e.g., categories become objects."""
    dtypes = df_list[0].dtypes
    columns_by_dtype = [list(dtype_group.index) for _, dtype_group in dtypes.groupby(dtypes)]

    splits = []
    for columns in columns_by_dtype:
        slices = [df.filter(columns) for df in df_list]
        splits.append(pd.DataFrame(data=np.concatenate(slices), columns=columns))
    return pd.concat(splits, axis=1)


def concat_results(old_results, new_results):
    # Skips all the pandas index checking because columns are in the same order.
    start = time()

    to_concat = [d.reset_index(drop=True) for d in new_results]
    if not old_results.empty:
        to_concat += [old_results.reset_index(drop=True)]

    results = concat_preserve_types(to_concat)

    results = results.set_index(['input_draw', 'random_seed'], drop=False)
    results.index.names = ['input_draw_number', 'random_seed']
    end = time()
    logger.info(f"Concatenated {len(new_results)} results in {end - start:.2f}s.")
    return results


def write_results_batch(ctx, written_results, unwritten_results, batch_size=50):
    new_results_to_write, unwritten_results = (unwritten_results[:batch_size], unwritten_results[batch_size:])
    results_to_write = concat_results(written_results, new_results_to_write)

    start = time()
    retries = 3
    while retries:
        try:
            output_path = ctx.output_directory / 'output.hdf'
            # Writing to an hdf over and over balloons the file size so write to new file and move it over to avoid
            temp_output_path = output_path.with_name(output_path.name + 'update')
            results_to_write.to_hdf(temp_output_path, 'data')
            temp_output_path.replace(output_path)
            break
        except Exception as e:
            logger.warning(f'Error trying to write results to hdf, retries remaining {retries}')
            sleep(30)
            retries -= 1
    end = time()
    logger.info(f"Updated output.hdf in {end - start:.4f}s.")
    return results_to_write, unwritten_results


def process_job_results(registry_manager, ctx):
    start_time = time()

    if ctx.existing_outputs is not None:
        written_results = ctx.existing_outputs
    else:
        written_results = pd.DataFrame()
    unwritten_results = []

    logger.info('Entering main processing loop.')
    batch_size = 200
    while registry_manager.jobs_to_finish:
        sleep(5)
        unwritten_results.extend(registry_manager.get_results())
        if ctx.no_batch and unwritten_results:
            written_results, unwritten_results = write_results_batch(ctx, written_results,
                                                                     unwritten_results, len(unwritten_results))
        elif len(unwritten_results) > batch_size:
            written_results, unwritten_results = write_results_batch(ctx, written_results,
                                                                     unwritten_results, batch_size)

        registry_manager.update_and_report()
        logger.info(f'Unwritten results: {len(unwritten_results)}')
        logger.info(f'Elapsed time: {(time() - start_time)/60:.1f} minutes.')

    batch_size = 500
    while unwritten_results:
        written_results, unwritten_results = write_results_batch(ctx, written_results, unwritten_results,
                                                                 batch_size=batch_size)
        logger.info(f'Unwritten results: {len(unwritten_results)}')
        logger.info(f'Elapsed time: {(time() - start_time) / 60:.1f} minutes.')


def check_user_sge_config():
    """Warn if a user has set their stdout and stderr output paths
    in a home directory config file. This overrides settings from py-drmaa."""

    sge_config = Path().home() / ".sge_request"

    if sge_config.exists():
        with sge_config.open('r') as f:
            for line in f:
                line = line.strip()
                if (('-o ' in line) or ('-e' in line)) and not line.startswith("#"):
                    logger.warning("You may have settings in your .sge_request file "
                                   "that could overwrite the log location set by this script. "
                                   f"Your .sge_request file is here: {sge_config}.  Look for "
                                   "-o and -e and comment those lines to receive logs side-by-side"
                                   "with the worker logs.")


def main(model_specification_file, branch_configuration_file, result_directory, project, peak_memory, max_runtime,
         priority, queue, redis_processes, num_input_draws=None, num_random_seeds=None, restart=False, expand=None,
         no_batch=False):

    utilities.exit_if_on_submit_host(utilities.get_hostname())

    output_dir, logging_dirs = utilities.setup_directories(model_specification_file, result_directory,
                                                           restart, expand=(num_input_draws or num_random_seeds))

    utilities.configure_master_process_logging_to_file(logging_dirs['main'])
    utilities.validate_environment(output_dir)
    project = utilities.validate_project(project)
    queue = utilities.validate_queue(queue, max_runtime)

    arguments = SimpleNamespace(model_specification_file=model_specification_file,
                                branch_configuration_file=branch_configuration_file,
                                output_directory=output_dir,
                                logging_directories=logging_dirs,
                                project=project,
                                peak_memory=peak_memory,
                                max_runtime=max_runtime,
                                priority=priority,
                                queue=queue,
                                num_input_draws=num_input_draws,
                                num_random_seeds=num_random_seeds,
                                restart=restart,
                                expand=expand,
                                no_batch=no_batch)
    ctx = RunContext(arguments)
    check_user_sge_config()
    jobs = build_job_list(ctx)

    if len(jobs) == 0:
        logger.info("Nothing to do")
        return

    logger.info('Starting jobs. Results will be written to: {}'.format(ctx.output_directory))

    if redis_processes == -1:
        redis_processes = int(math.ceil(len(jobs) / vtc_globals.DEFAULT_JOBS_PER_REDIS_INSTANCE))

    worker_template, redis_ports = launch_redis_processes(redis_processes)
    worker_file = output_dir / 'settings.py'
    with worker_file.open('w') as f:
        f.write(worker_template)

    registry_manager = RegistryManager(redis_ports, ctx.number_already_completed)
    registry_manager.enqueue(jobs)

    drmaa_session = drmaa.Session()
    drmaa_session.initialize()

    start_cluster(drmaa_session, len(jobs), ctx.peak_memory, ctx.max_runtime, ctx.priority,
                  ctx.queue, ctx.sge_log_directory, ctx.worker_log_directory, project, worker_file, ctx.job_name)

    process_job_results(registry_manager, ctx)

    logger.info('Jobs completed. Results written to: {}'.format(ctx.output_directory))
