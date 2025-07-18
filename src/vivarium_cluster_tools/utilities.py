"""
================================
vivarium_cluster_tools Utilities
================================

Making directories is hard.

"""

import functools
import os
import socket
import time
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

from vivarium_cluster_tools.psimulate.environment import ENV_VARIABLES

NUM_ROWS_PER_CENTRAL_LOG_FILE = 100_000

P = ParamSpec("P")
T = TypeVar("T")


def get_cluster_name() -> str:
    """Returns the hostname"""
    return socket.gethostname()


def get_drmaa() -> Any:
    try:
        import drmaa
    except (RuntimeError, OSError):
        if "slurm" in ENV_VARIABLES.HOSTNAME.value:
            ENV_VARIABLES.DRMAA_LIB_PATH.update("/opt/slurm-drmaa/lib/libdrmaa.so")
            import drmaa
        else:
            drmaa = object()
    return drmaa


def mkdir(
    path: str | Path, umask: int = 0o002, exists_ok: bool = False, parents: bool = False
) -> None:
    """Utility method to create a directory with specified permissions.

    Parameters
    ----------
    path
        Path of the directory to create.
    umask
        Umask specifying the desired permissions - defaults to 0o002.
    exists_ok
        If False, raises FileExistsError if the directory already exists.
    parents
        If False, raises FileNotFoundError if the directory's parent doesn't exist.
    """
    path = Path(path)
    old_umask = os.umask(umask)
    try:
        path.mkdir(exist_ok=exists_ok, parents=parents)
    finally:
        os.umask(old_umask)


def backoff_and_retry(
    backoff_seconds: int | float = 30,
    num_retries: int = 3,
    log_function: Callable[[str], None] = warnings.warn,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Adds a retry handler to the decorated function.

    Parameters
    ----------
    backoff_seconds
        Number of seconds to wait until retrying the operation.
    num_retries
        Number of times to retry before failing.
    log_function
        A callable used to emit retry messages.  Defaults to emitting
        a standard library warning.

    Returns
    -------
        A function that retries the decorated function a specified number of times.
    """

    def _wrap(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def _wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
            retries = num_retries
            while retries:
                try:
                    result = func(*args, **kwargs)
                    break
                except Exception as e:
                    log_function(
                        f"Error trying to write results, retries remaining {retries}"
                    )
                    time.sleep(backoff_seconds)
                    retries -= 1
                    if not retries:
                        log_function(f"Retries exhausted.")
                        raise e
            return result

        return _wrapped

    return _wrap
