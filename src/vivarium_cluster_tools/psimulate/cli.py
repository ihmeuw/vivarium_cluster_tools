import click
from loguru import logger

from vivarium.framework.utilities import handle_exceptions
from vivarium_cluster_tools.psimulate import globals as vct_globals, runner, utilities

shared_options = [
    click.option('--project', '-P',
                 type=click.Choice(vct_globals.CLUSTER_PROJECTS),
                 default=vct_globals.DEFAULT_CLUSTER_PROJECT,
                 help='The cluster project under which to run the simulation.'),
    click.option('--peak-memory', '-m',
                 type=int,
                 default=3,
                 help=('The estimated maximum memory usage in GB of an individual simulate job. The simulations will '
                       'be run with this as a limit.')),
    click.option('--max-runtime', '-r',
                 type=str,
                 default='24:00:00',
                 help=('The estimated maximum runtime (DD:HH:MM) of the simulation jobs. By default, the cluster will '
                       'terminate jobs after 24h regardless of queue. The maximum supported runtime is 3 days. Keep in '
                       'mind that the session you are launching from must be able to live at least as long as the '
                       'simulation jobs, and that runtimes by node vary wildly.')),
    click.option('--priority', '-p',
                 type=click.IntRange(-1023, 0),
                 default=0,
                 help='Defines a priority for worker jobs relative to other jobs on the cluster. Higher priority jobs '
                      'are eligible to be dispatched by the scheduler earlier. Acceptable values are zero or less.'),
    click.option('--queue', '-q',
                 type=click.Choice(['all.q', 'long.q']),
                 default=None,  # dynamically set based on runtime
                 help='The cluster queue to assign the jobs to, defaults to the appropriate queue based on max-runtime.'
                      'long.q allows for much longer runtimes but there may be reasons to send jobs to that queue even '
                      'if they don\'t have runtime constraints, such as open node availability.'),
    click.option('--pdb', 'with_debugger',
                 is_flag=True,
                 help='Drop into python debugger if an error occurs.'),
    click.option('--redis',
                 type=int,
                 default=-1,
                 help=(f'Number of redis databases to use.  Defaults to a redis instance for every '
                       f'{vct_globals.DEFAULT_JOBS_PER_REDIS_INSTANCE} jobs.')),
    click.option('-v', 'verbose',
                 count=True,
                 help='Configure logging verbosity.'),
    click.option('--no-batch',
                 is_flag=True,
                 help="Don't batch results, write them as they come in.")
]


def pass_shared_options(func):
    # add all the shared options to the command
    for option in shared_options:
        func = option(func)
    return func


@click.group()
def psimulate():
    """A command line utility for running many simulations in parallel.

    You may initiate a new run with the ``run`` sub-command or restart a run
    from where it was stopped by using the ``restart`` sub-command.

    """
    pass


@psimulate.command()
@click.argument('model_specification', type=click.Path(exists=True, dir_okay=False))
@click.argument('branch_configuration', type=click.Path(exists=True, dir_okay=False))
@click.option('--result-directory', '-o', default=None,
              help='The directory to write results to. A folder will be created in this directory with the same name '
                   'as the configuration file.')
@pass_shared_options
def run(model_specification, branch_configuration, result_directory, **options):
    """Run a parallel simulation.

    The simulation itself is defined by a MODEL_SPECIFICATION yaml file
    and the parameter changes across runs are defined by a BRANCH_CONFIGURATION
    yaml file.

    If a results directory is provided, a subdirectory will be created with the
    same name as the MODEL_SPECIFICATION if one does not exist. Results will be
    written to a further subdirectory named after the start time of the
    simulation run.

    If a results directory is not provided the base results_directory is taken
    to be /share/costeffectiveness/results.

    """
    utilities.configure_master_process_logging_to_terminal(options['verbose'])
    main = handle_exceptions(runner.main, logger, options['with_debugger'])

    main(model_specification, branch_configuration, result_directory,
         options['project'], options['peak_memory'], options['max_runtime'],
         options['priority'], options['queue'], redis_processes=options['redis'],
         no_batch=options['no_batch'])


@psimulate.command()
@click.argument('results-root', type=click.Path(exists=True, file_okay=False, writable=True))
@pass_shared_options
def restart(results_root, **options):
    """Restart a parallel simulation from a previous run at RESULTS_ROOT.

    Restarting will not erase existing results, but will start workers to
    perform the remaining simulations.  RESULTS_ROOT is expected to be an
    output directory from a previous ``psimulate run`` invocation.

    """
    utilities.configure_master_process_logging_to_terminal(options['verbose'])
    main = handle_exceptions(runner.main, logger, options['with_debugger'])

    main(None, None, results_root,
         options['project'], options['peak_memory'], options['max_runtime'],
         options['priority'], options['queue'], redis_processes=options['redis'],
         restart=True, no_batch=options['no_batch'])


@psimulate.command()
@click.argument('results-root', type=click.Path(exists=True, file_okay=False, writable=True))
@click.option('--add-draws', type=int, default=0, help='The number of input draws to add to a previous run.')
@click.option('--add-seeds', type=int, default=0, help='The number of random seeds to add to a previous run.')
@pass_shared_options
def expand(results_root, **options):
    """Expand a previous run at RESULTS_ROOT by adding input draws and/or
    random seeds.

    Expanding will not erase existing results, but will start workers to perform
    the additional simulations determined by the added draws/seeds.
    RESULTS_ROOT is expected to be an output directory from a previous
    ``psimulate run`` invocation.

    """
    utilities.configure_master_process_logging_to_terminal(options['verbose'])
    main = handle_exceptions(runner.main, logger, options['with_debugger'])

    main(None, None, results_root,
         options['project'], options['peak_memory'], options['max_runtime'],
         options['priority'], options['queue'],
         redis_processes=options['redis'],
         restart=True,
         expand={'num_draws': options['add_draws'],
                 'num_seeds': options['add_seeds']},
         no_batch=options['no_batch'])
