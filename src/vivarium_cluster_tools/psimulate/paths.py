import shutil
from datetime import datetime
from pathlib import Path
from typing import NamedTuple, Optional

from vivarium_cluster_tools import utilities as vct_utils


DEFAULT_OUTPUT_DIRECTORY = "/share/costeffectiveness/results"


class OutputPaths(NamedTuple):
    # Directories
    root: Path

    logging_root: Path
    cluster_logging_root: Path
    worker_logging_root: Path

    # Files
    # Environment configuration
    worker_settings: Path
    environment_file: Path

    # Simulation configuration
    model_specification: Path
    keyspace: Path
    branches: Path

    # outputs
    results: Path

    @classmethod
    def from_entry_point_args(
        cls,
        input_model_specification_path: Optional[str],
        result_directory: str,
        restart: bool,
        expand: bool,
    ) -> 'OutputPaths':
        command = _resolve_command(restart, expand)
        launch_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        output_directory = Path(result_directory)
        if command == 'run':
            output_directory = output_directory / Path(input_model_specification_path).stem / launch_time

        logging_directory = output_directory / "logs" / f"{launch_time}_{command}"
        logging_dirs = {
            "logging_root": logging_directory,
            "cluster_logging_root": logging_directory / "sge_logs",
            "worker_logging_root": logging_directory / "worker_logs",
        }

        output_paths = OutputPaths(
            root=output_directory,
            **logging_dirs,
            worker_settings=output_directory / 'settings.py',
            environment_file=output_directory / 'requirements.txt',
            model_specification=output_directory / 'model_specification.yaml',
            keyspace=output_directory / 'keyspace.yaml',
            branches=output_directory / 'branches.yaml',
            results=output_directory / 'output.hdf',
        )
        return output_paths

    def touch(self):
        vct_utils.mkdir(self.root, exists_ok=True, parents=True)
        for d in [self.logging_root, self.cluster_logging_root, self.worker_logging_root]:
            vct_utils.mkdir(d, parents=True)


def delete_on_catastrophic_failure(output_paths: OutputPaths):
    """Deletes the entire results root if no results are found.

    Remove the results directory including runner and worker logs if
    the simulation produced no results. This failure category usually
    happens when there is a code or data error that would cause
    individual ``simulate`` runs to fail.

    """
    if not output_paths.results.exists():
        shutil.rmtree(output_paths.root)


def _resolve_command(restart: bool, expand: bool):
    command = {
        (False, False): 'run',
        (False, True): 'invalid',
        (True, False): 'restart',
        (True, True): 'expand',
    }[(restart, expand)]

    # Should be impossible from entry points.
    if command == 'invalid':
        raise ValueError("Unknown command configuration")
    return command
