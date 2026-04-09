from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest
import yaml
from click.testing import CliRunner
from pandas.testing import assert_frame_equal

from vivarium_cluster_tools.psimulate.cli import psimulate
from vivarium_cluster_tools.psimulate.cluster.interface import NativeSpecification
from vivarium_cluster_tools.psimulate.jobs import JobParameters, generate_task_id
from vivarium_cluster_tools.psimulate.runner import (
    report_initial_status,
    write_backup_metadata,
    write_requested_resources,
)

_RUNNER_MAIN = "vivarium_cluster_tools.psimulate.runner.main"


def _write_yaml(tmp_path: Path, data: dict[str, Any], name: str = "config.yaml") -> Path:
    p = tmp_path / name
    p.write_text(yaml.dump(data))
    return p


def _parse_resource_file(path: Path) -> dict[str, str]:
    """Parse a requested_resources.txt file into a key->value dict."""
    result: dict[str, str] = {}
    for line in path.read_text().strip().splitlines():
        key, _, value = line.partition(": ")
        result[key] = value
    return result


def test_report_initial_status() -> None:
    number_existing_jobs = 10
    finished_sim_metadata = pd.DataFrame(index=range(number_existing_jobs))
    report_initial_status(number_existing_jobs, finished_sim_metadata, 100)
    with pytest.raises(RuntimeError, match="There are 1 jobs from the previous run"):
        report_initial_status(number_existing_jobs + 1, finished_sim_metadata, 100)


def test_write_backup_metadata(tmp_path: Path) -> None:
    metadata_path = tmp_path / "metadata.csv"
    job_parameters_list = [
        JobParameters(
            model_specification="test_model_spec.yaml",
            branch_configuration={"category": {"detail": 9}},
            input_draw=1337,
            random_seed=42,
            results_path="~/tmp",
            worker_logging_root="/tmp/worker_logs",
            backup_configuration={},
            extras={},
        ),
    ]
    expected_task_id_1 = generate_task_id(1337, 42, {"category": {"detail": 9}})
    write_backup_metadata(metadata_path, job_parameters_list)
    assert metadata_path.exists()
    metadata = pd.read_csv(metadata_path)
    expected_df = pd.DataFrame(
        {
            "input_draw": [1337],
            "random_seed": [42],
            "job_id": [expected_task_id_1],
            "category.detail": [9],
        }
    )
    assert_frame_equal(metadata, expected_df)

    # Check that we append to the existing metadata
    # upon second execution
    append_job_parameters_list = [
        JobParameters(
            model_specification="test_model_spec.yaml",
            branch_configuration={"category": {"detail": 10}},
            input_draw=1338,
            random_seed=43,
            results_path="~/tmp",
            worker_logging_root="/tmp/worker_logs",
            backup_configuration={},
            extras={},
        ),
    ]
    expected_task_id_2 = generate_task_id(1338, 43, {"category": {"detail": 10}})
    write_backup_metadata(metadata_path, append_job_parameters_list)
    metadata = pd.read_csv(metadata_path)
    expected_df = pd.DataFrame(
        {
            "input_draw": [1337, 1338],
            "random_seed": [42, 43],
            "job_id": [expected_task_id_1, expected_task_id_2],
            "category.detail": [9, 10],
        }
    )
    assert_frame_equal(metadata, expected_df)


def test_write_requested_resources_from_config(tmp_path: Path) -> None:
    """Verify that write_requested_resources produces a file with the correct
    resource values when called directly (simulating values from a config)."""
    native_spec = NativeSpecification(
        job_name="my_sim",
        project="proj_simscience",
        queue="all.q",
        peak_memory=8,
        max_runtime="04:30:00",
        hardware=["r650", "r650xs"],
    )

    write_requested_resources(
        output_root=tmp_path,
        native_specification=native_spec,
        max_workers=500,
        max_attempts=5,
    )

    resource_file = tmp_path / "requested_resources.txt"
    assert resource_file.exists()

    resources = _parse_resource_file(resource_file)
    assert resources["project"] == "proj_simscience"
    assert resources["queue"] == "all.q"
    assert resources["peak_memory_gb"] == "8"
    assert resources["max_runtime"] == "04:30:00"
    assert resources["hardware"] == "r650,r650xs"
    assert resources["max_workers"] == "500"
    assert resources["max_attempts"] == "5"


def test_write_requested_resources_cli_and_config(tmp_path: Path) -> None:
    """Verify that values from a combination of CLI args and a config file
    are captured correctly in the requested_resources.txt file.

    The config file supplies baseline values; CLI flags override some of them.
    We assert the *actually-used* values end up in the resource file.
    """
    model_spec = tmp_path / "model_spec.yaml"
    model_spec.write_text("components: {}\n")
    branch_config = tmp_path / "branches.yaml"
    branch_config.write_text("input_draw_count: 1\nrandom_seed_count: 1\n")
    result_dir = tmp_path / "results"
    result_dir.mkdir()

    # Config file provides baseline resource values
    config = _write_yaml(
        tmp_path,
        {
            "model_specification": str(model_spec),
            "branch_configuration": str(branch_config),
            "result_directory": str(result_dir),
            "project": "proj_simscience",
            "peak_memory": 4,
            "max_runtime": "02:00:00",
            "queue": "all.q",
            "max_workers": 200,
            "max_attempts": 2,
            "hardware": "r650",
        },
    )

    # CLI overrides peak_memory and max_workers
    cli_args = [
        "run",
        "--run-config",
        str(config),
        "--peak-memory",
        "16",
        "--max-workers",
        "1000",
    ]

    cli_runner = CliRunner()
    with patch(_RUNNER_MAIN) as mock_main:
        result = cli_runner.invoke(psimulate, cli_args)

    assert result.exit_code == 0, result.output
    mock_main.assert_called_once()
    call_kwargs = mock_main.call_args.kwargs
    native_spec = call_kwargs["native_specification"]

    # Now write the resource file using the values the CLI resolved
    output_dir = tmp_path / "resource_check"
    output_dir.mkdir()
    write_requested_resources(
        output_root=output_dir,
        native_specification=native_spec,
        max_workers=call_kwargs["max_workers"],
        max_attempts=call_kwargs["max_attempts"],
    )

    resource_file = output_dir / "requested_resources.txt"
    assert resource_file.exists()

    resources = _parse_resource_file(resource_file)
    # CLI overrides
    assert resources["peak_memory_gb"] == "16"
    assert resources["max_workers"] == "1000"
    # Config file values (not overridden by CLI)
    assert resources["project"] == "proj_simscience"
    assert resources["queue"] == "all.q"
    assert resources["max_runtime"] == "02:00:00"
    assert resources["max_attempts"] == "2"
    assert resources["hardware"] == "r650"
