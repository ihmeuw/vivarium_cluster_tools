from pathlib import Path

import click
from loguru import logger

from vivarium_cluster_tools.psimulate import globals as vct_globals, runner, utilities
from vivarium_cluster_tools.run_general import run_multiple

shared_options = [
    click.option('--project', '-P',
                 type=click.Choice(vct_globals.CLUSTER_PROJECTS),
                 default=vct_globals.DEFAULT_CLUSTER_PROJECT,
                 help='The cluster project under which to run the command.'),
    click.option('--queue', '-q',
                 type=click.Choice(['all.q', 'long.q']),
                 default='long.q',  # dynamically set based on max-runtime
                 help='The cluster queue to assign the jobs to. Queue defaults to the '
                      'appropriate queue based on max-runtime. long.q allows for much longer '
                      'runtimes but there may be reasons to send jobs to that queue even '
                      'if they don\'t have runtime constraints, such as node availability.'),
    click.option('--peak-memory', '-m',
                 type=int,
                 default=1,
                 help=('The estimated maximum memory usage in GB of an individual simulate job. '
                       'The simulations will be run with this as a limit.')),
    click.option('--max-runtime', '-r',
                 type=str,
                 default='24:00:00',
                 help=('The estimated maximum runtime (DD:HH:MM) of the simulation jobs. '
                       'By default, the cluster will terminate jobs after 24h regardless of '
                       'queue. The maximum supported runtime is 3 days. Keep in mind that the '
                       'session you are launching from must be able to live at least as long '
                       'as the simulation jobs, and that runtimes by node vary wildly.')),
    click.option('--pdb', 'with_debugger',
                 is_flag=True,
                 help='Drop into python debugger if an error occurs.'),
    click.option('-v', 'verbose',
                 count=True,
                 help='Configure logging verbosity.'),
]


def pass_shared_options(func):
    # add all the shared options to the command
    for option in shared_options:
        func = option(func)
    return func

@click.command()
@click.argument('script_to_run', type=click.Path(exists=True, dir_okay=False))
@click.option('--num_simulants', type=int, default=10000, help='Provide the number of simulants')
@click.option('--draws', type=int, default=1000, help='Provide the number of draws 1-1000')
@click.option('--random_seed', type=int, default=43, help='Provide the random seed')
@click.option('--take_mean', type=bool, default=False, help='Specify whether to take the mean of all draws')
@pass_shared_options
def run_lsff(script_to_run, num_simulants, draws, random_seed, take_mean, **options):
    locations = [
        'Ethiopia',
        'Uganda',
        'Nigeria',        
    ]
    job_map = {c: [script_to_run, c, num_simulants, draws, random_seed, take_mean] for c in locations}
    run_multiple.run_cluster_jobs('IronBirthweight', Path('.'), job_map, options)
