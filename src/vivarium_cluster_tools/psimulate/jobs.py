# mypy: ignore-errors
"""
==============
psimulate Jobs
==============

"""

from pathlib import Path
from typing import NamedTuple

import numpy as np
import pandas as pd
from vivarium.framework.utilities import collapse_nested_dict

from vivarium_cluster_tools.psimulate import COMMANDS, branches


class JobParameters(NamedTuple):
    """Parameters for a single distributed simulation job."""

    model_specification: str
    branch_configuration: dict
    input_draw: int
    random_seed: int
    results_path: str
    backup_configuration: dict
    extras: dict

    @property
    def shared(self) -> dict:
        """Parameters shared by all jobs in a psimulate run."""
        return {
            "model_specification": self.model_specification,
            "results_path": self.results_path,
            "backup_configuration": self.backup_configuration,
        }

    @property
    def job_specific(self) -> dict:
        """Parameters that vary by job in a psimulate run."""
        return {
            **self.branch_configuration,
            "input_draw": self.input_draw,
            "random_seed": self.random_seed,
        }

    @property
    def sim_config(self) -> dict:
        """Parameters for the simulation configuration."""
        return {
            **self.branch_configuration,
            "randomness": {
                "random_seed": self.random_seed,
            },
            "input_data": {
                "input_draw_number": self.input_draw,
            },
        }

    def to_dict(self) -> dict:
        # I will never understand why this is a private
        # method of named tuples.
        return self._asdict()

    def __repr__(self) -> str:
        return (
            f"({self.input_draw}, {self.random_seed}, "
            f"{self.model_specification}, {self.branch_configuration})"
        )


def build_job_list(
    command: str,
    model_specification_path: Path,
    output_root: Path,
    keyspace: branches.Keyspace,
    finished_sim_metadata: pd.DataFrame,
    backup_freq: int | None,
    backup_dir: Path,
    backup_metadata_path: Path,
    extras: dict,
) -> tuple[list[JobParameters], int]:
    jobs = []
    number_already_completed = 0

    if command in [COMMANDS.run, COMMANDS.restart, COMMANDS.expand]:
        for input_draw, random_seed, branch_config in keyspace:
            parameters = JobParameters(
                model_specification=str(model_specification_path),
                branch_configuration=branch_config,
                input_draw=int(input_draw),
                random_seed=int(random_seed),
                results_path=str(output_root),
                backup_configuration={
                    "backup_dir": backup_dir,
                    "backup_freq": backup_freq,
                    "backup_metadata_path": backup_metadata_path,
                },
                extras=extras,
            )

            if already_complete(parameters, finished_sim_metadata):
                number_already_completed += 1
            else:
                jobs.append(parameters.to_dict())
    else:
        assert command == COMMANDS.load_test
        for i in range(extras["num_workers"]):
            parameters = JobParameters(
                model_specification=str(model_specification_path),
                branch_configuration={},
                input_draw=0,
                random_seed=i,
                results_path=str(output_root),
                extras={"test_type": extras["test_type"]},
            )
            jobs.append(parameters.to_dict())

    np.random.shuffle(jobs)
    return jobs, number_already_completed


def already_complete(
    job_parameters: JobParameters, finished_sim_metadata: pd.DataFrame
) -> bool:
    if finished_sim_metadata.empty:
        return False

    job_parameter_list = collapse_nested_dict(job_parameters.job_specific)
    mask = pd.Series(True, index=finished_sim_metadata.index)
    for k, v in job_parameter_list:
        if isinstance(v, float):
            mask &= np.isclose(finished_sim_metadata[k], v)
        else:
            mask &= finished_sim_metadata[k] == v
    return np.any(mask)
