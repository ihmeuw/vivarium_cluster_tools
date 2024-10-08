"""
====================
Redis DB CLI options
====================

Command line options for configuring job/result queue Redis DBs in psimulate runs.

"""

import click

from vivarium_cluster_tools.psimulate.redis_dbs.launcher import (
    DEFAULT_NUM_REDIS_DBS,
    DEFAULT_WORKERS_PER_REDIS_INSTANCE,
)

with_redis = click.option(
    "--redis",
    type=int,
    default=DEFAULT_NUM_REDIS_DBS,
    help=(
        f"Number of redis databases to use.  Defaults to a redis instance for every "
        f"{DEFAULT_WORKERS_PER_REDIS_INSTANCE} workers."
    ),
)

with_max_workers = click.option(
    "--max-workers",
    "-w",
    type=click.IntRange(min=1),
    default=8000,
    show_default=True,
    help=("The maximum number of workers (and therefore jobs) to run concurrently."),
)
