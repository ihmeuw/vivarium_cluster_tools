"""
===================
Cluster CLI options
===================

Command line options for configuring the cluster environment in psimulate runs.

"""

import click

# Tombstone value for internal use. Default options are not shown by
# click unless explicitly requested.
_DEFAULT_QUEUE = "max_runtime_queue.q"
_RUNTIME_FORMAT = "hh:mm:ss"

with_project = click.option(
    "--project",
    "-P",
    type=click.Choice(
        [
            "proj_cost_effect",
            "proj_cost_effect_diarrhea",
            "proj_cost_effect_dcpn",
            "proj_cost_effect_conic",
            "proj_csu",
        ]
    ),
    default="proj_cost_effect",
    help="The cluster project under which to run the simulation.",
)


def with_queue_and_max_runtime(func):
    """Provide a single decorator for both queue and max runtime
    since they are tightly coupled.

    """
    func = _with_queue(func)
    func = _with_max_runtime(func)
    return func


with_peak_memory = click.option(
    "--peak-memory",
    "-m",
    type=int,
    default=3,
    help=(
        "The estimated maximum memory usage in GB of an individual simulate job. "
        "The simulations will be run with this as a limit."
    ),
)


_with_queue = click.option(
    "--queue",
    "-q",
    type=click.Choice(["all.q", "long.q"]),
    default=_DEFAULT_QUEUE,
    help="The cluster queue to assign psimulate jobs to. Queue defaults to the "
    "appropriate queue based on max-runtime. long.q allows for much longer "
    "runtimes but there may be reasons to send jobs to that queue even "
    "if they don't have runtime constraints, such as node availability.",
    is_eager=True,
)


def _runtime_callback(ctx: click.Context, _: str, runtime_string: str) -> str:
    try:
        hours, minutes, seconds = runtime_string.split(":")
    except ValueError:
        raise click.BadParameter(
            f"Invalid --max-runtime supplied. Format should be {_RUNTIME_FORMAT}."
        )
    total_hours = int(hours) + float(minutes) / 60.0 + float(seconds) / 3600.0

    queue = ctx.params["queue"]

    # Sorted from shortest to longest.
    max_runtime_map = {
        "all.q": 3 * 24,
        "long.q": 16 * 24,
    }
    max_runtime = max(max_runtime_map.values())
    if max_runtime < total_hours:
        raise click.BadParameter(
            f"Invalid --max-runtime {runtime_string} supplied. "
            f"Max cluster runtime is {max_runtime}:00:00 with format {_RUNTIME_FORMAT}."
        )
    elif queue in max_runtime_map and max_runtime_map[queue] < total_hours:
        raise click.BadParameter(
            f"Invalid combination of --max-runtime {runtime_string} and "
            f"--queue {queue} specified. Max runtime for the {queue} is "
            f"{max_runtime_map[queue]}:00:00 with format {_RUNTIME_FORMAT}."
        )
    elif queue in max_runtime_map:
        # Things are peachy
        pass
    else:
        # We need to set a default based on the runtime.
        assert queue == _DEFAULT_QUEUE
        # First queue we qualify for.
        queue = [
            q
            for q, max_queue_runtime in max_runtime_map.items()
            if total_hours <= max_queue_runtime
        ][0]

    ctx.params["queue"] = queue
    return runtime_string


_with_max_runtime = click.option(
    "--max-runtime",
    "-r",
    type=str,
    default="24:00:00",
    help=(
        f"The estimated maximum runtime ({_RUNTIME_FORMAT}) of the simulation jobs. "
        "By default, the cluster will terminate jobs after 24h regardless of "
        "queue. The maximum supported runtime is 3 days. Keep in mind that the "
        "session you are launching from must be able to live at least as long "
        "as the simulation jobs, and that runtimes by node vary wildly."
    ),
    callback=_runtime_callback,
)
