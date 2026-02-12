"""
=================
Cluster Interface
=================

"""

from __future__ import annotations

from typing import NamedTuple

from vivarium_cluster_tools.psimulate.environment import ENV_VARIABLES


def validate_cluster_environment() -> None:
    if "slurm" not in ENV_VARIABLES.HOSTNAME.value:
        raise RuntimeError("This tool must be run from the IHME cluster.")

    submit_host_marker = "slogin"
    if submit_host_marker in ENV_VARIABLES.HOSTNAME.value:
        raise RuntimeError("This tool must not be run from a submit host.")


class NativeSpecification(NamedTuple):
    job_name: str
    project: str
    queue: str
    peak_memory: float  # Memory in GB
    max_runtime: str
    hardware: list[str]

    # Class constant
    NUM_THREADS: int = 1

    def to_cli_args(self) -> str:
        return (
            f"-J {self.job_name} "
            f"-A {self.project} "
            f"-p {self.queue} "
            f"--mem={self.peak_memory*1024} "
            f"-t {self.max_runtime} "
            f"-c {self.NUM_THREADS} "
            f"{'-C ' + '|'.join(self.hardware) if self.hardware else ''}"
        ).strip()
