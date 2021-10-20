from pathlib import Path

import click

from vivarium_cluster_tools.psimulate import globals as vct_globals
from vivarium_cluster_tools.psimulate.branches import Keyspace
from vivarium_cluster_tools.run_general import run_multiple

shared_options = [
    click.option('--project', '-P',
                 type=click.Choice(vct_globals.CLUSTER_PROJECTS),
                 default=vct_globals.DEFAULT_CLUSTER_PROJECT,
                 help='The cluster project under which to run the command.'),
    click.option('--queue', '-q',
                 type=click.Choice(['all.q', 'long.q']),
                 default='all.q',
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
]


def pass_shared_options(func):
    # add all the shared options to the command
    for option in shared_options:
        func = option(func)
    return func


@click.command()
@click.argument('script_to_run', type=click.Path(exists=True, dir_okay=False))
@click.argument('vivarium_research_lsff_path', type=click.Path(exists=True))
@click.argument('output_directory', type=click.Path(exists=True))
@click.option('--num_simulants', type=int, default=10000, help='Provide the number of simulants')
@click.option('--draws', type=int, default=1000, help='Provide the number of draws 1-1000')
@click.option('--random_seed', type=int, default=43, help='Provide the random seed')
@click.option('--take_mean', type=bool, default=False, help='Specify whether to take the mean of all draws')
@pass_shared_options
def run_lsff(script_to_run, vivarium_research_lsff_path, output_directory, num_simulants, draws, random_seed, take_mean,
             **options):
    locations = [
        'Ethiopia',
        'Uganda',
        'Nigeria',        
    ]
    job_map = {c: [script_to_run, vivarium_research_lsff_path, output_directory, c, num_simulants, draws, random_seed,
                   take_mean] for c in locations}
    run_multiple.run_cluster_jobs('IronBirthweight', Path('.'), job_map, options)


@click.command()
@click.argument('script_to_run', type=click.Path(exists=True, dir_okay=False))
@click.option('--sim_config', '-c', type=click.Path(exists=True))
@click.option('--branches_config', '-b', type=click.Path(exists=True))
@click.option('--output_directory', '-o', type=click.Path(exists=False))
@pass_shared_options
def run_script(script_to_run, sim_config, branches_config, output_directory, **options):
    keyspace = Keyspace.from_branch_configuration(None, None, branches_config)
    branch_args = [(permutation[0], permutation[1], *permutation[2].values()) for permutation in keyspace]
    job_map = {'_'.join([str(val) for val in args]): [script_to_run, sim_config, output_directory, *args]
               for args in branch_args}
    run_multiple.run_cluster_jobs('InteractiveSimScript', Path(output_directory), job_map, options)
