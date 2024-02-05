import os
from datetime import datetime
from math import ceil
from pathlib import Path

import pandas as pd
import pytest
from _pytest.logging import LogCaptureFixture
from loguru import logger

from vivarium_cluster_tools.psimulate.paths import OutputPaths
from vivarium_cluster_tools.psimulate.performance_logger import (
    append_child_job_data,
    append_perf_data_to_central_logs,
    generate_runner_job_data,
    transform_perf_df_for_appending,
)


@pytest.fixture
def artifact_perf_df():
    filepath = Path(__file__).parent / "data/artifact_perf_df.csv"
    df = pd.read_csv(filepath)
    index_cols = ["host", "job_number", "task_number", "draw", "seed"]
    scenario_cols = [col for col in df.columns if col.startswith("scenario")]
    df = df.set_index(index_cols + scenario_cols)
    return df


@pytest.fixture
def artifactless_perf_df():
    filepath = Path(__file__).parent / "data/artifactless_perf_df.csv"
    df = pd.read_csv(filepath)
    index_cols = ["host", "job_number", "task_number", "draw", "seed"]
    scenario_cols = [col for col in df.columns if col.startswith("scenario")]
    df = df.set_index(index_cols + scenario_cols)
    return df


@pytest.fixture
def result_directory():
    return Path("/mnt/team/simulation_science/pub/models/project_name/results/model_version/")


@pytest.fixture
def caplog(caplog: LogCaptureFixture):
    handler_id = logger.add(
        caplog.handler,
        format="{message}",
        level=0,
        filter=lambda record: record["level"].no >= caplog.handler.level,
        enqueue=False,  # Set to 'True' if your test is spawning child processes.
    )
    yield caplog
    logger.remove(handler_id)


def get_output_paths_from_output_directory(output_directory):
    model_name = "artifact"
    original_launch_time = "YYYY_MM_DD_HH_MM_SS"
    launch_time = "yyyy_mm_dd_hh_mm_ss"
    output_directory = output_directory / model_name / original_launch_time

    logging_directory = output_directory / "logs" / f"{launch_time}_runtype"
    logging_dirs = {
        "logging_root": logging_directory,
        "cluster_logging_root": logging_directory / "cluster_logs",
        "worker_logging_root": logging_directory / "worker_logs",
    }

    output_paths = OutputPaths(
        root=output_directory,
        **logging_dirs,
        worker_settings=output_directory / "settings.py",
        environment_file=output_directory / "requirements.txt",
        model_specification=output_directory / "model_specification.yaml",
        keyspace=output_directory / "keyspace.yaml",
        branches=output_directory / "branches.yaml",
        results=output_directory / "output.hdf",
    )

    return output_paths


@pytest.mark.parametrize("df_name", ["artifact_perf_df", "artifactless_perf_df"])
def test_expected_columns(df_name, result_directory, tmp_path, request):
    perf_df = request.getfixturevalue(df_name)
    # transform df
    output_paths = get_output_paths_from_output_directory(result_directory)
    central_perf_df = transform_perf_df_for_appending(perf_df, output_paths)
    expected_columns = (
        ["host", "job_number", "task_number", "draw", "seed"]
        + perf_df.columns.tolist()
        + ["artifact_name", "scenario_parameters"]
    )
    assert list(central_perf_df.columns) == expected_columns


@pytest.mark.parametrize("df_name", ["artifact_perf_df", "artifactless_perf_df"])
def test_data_parsing(df_name, result_directory, tmp_path, request):
    perf_df = request.getfixturevalue(df_name)
    output_paths = get_output_paths_from_output_directory(result_directory)
    central_perf_df = transform_perf_df_for_appending(perf_df, output_paths)
    assert (central_perf_df["artifact_name"] == "artifact").all()

    if df_name == "artifact_perf_df":
        expected_scenario_parameters = [
            '{"scenario_parameter_one": "value_one", "scenario_input_data_artifact_path": "/path/to/artifact.hdf"}',
            '{"scenario_parameter_one": "value_two", "scenario_input_data_artifact_path": "/path/to/artifact.hdf"}',
        ] * 6
    else:
        expected_scenario_parameters = [
            '{"scenario_parameter_one": "value_one"}',
            '{"scenario_parameter_one": "value_two"}',
        ] * 6
    assert (central_perf_df["scenario_parameters"] == expected_scenario_parameters).all()

    job_number = int(central_perf_df["job_number"].unique().squeeze())
    runner_data = generate_runner_job_data(job_number, output_paths, "first_file_with_data")

    assert runner_data["project_name"].squeeze() == "project_name"
    assert runner_data["root_path"].squeeze() == Path(
        "/mnt/team/simulation_science/pub/models/project_name/results/model_version/artifact"
    )
    assert runner_data["original_run_date"].squeeze() == "YYYY_MM_DD_HH_MM_SS"
    assert runner_data["run_date"].squeeze() == "yyyy_mm_dd_hh_mm_ss"
    assert runner_data["run_type"].squeeze() == "runtype"
    assert runner_data["log_summary_file_path"].squeeze() == "first_file_with_data"
    assert (
        runner_data["original_log_file_path"].squeeze()
        == "/mnt/team/simulation_science/pub/models/project_name/results/model_version/artifact/YYYY_MM_DD_HH_MM_SS/logs/yyyy_mm_dd_hh_mm_ss_runtype/worker_logs/log_summary.csv"
    )


def test_valid_log_path(result_directory, artifact_perf_df, caplog, tmp_path, monkeypatch):
    monkeypatch.setattr(
        "vivarium_cluster_tools.psimulate.performance_logger.CENTRAL_PERFORMANCE_LOGS_DIRECTORY",
        tmp_path,
    )
    monkeypatch.setattr(
        "vivarium_cluster_tools.psimulate.performance_logger.NUM_ROWS_PER_CENTRAL_LOG_FILE", 4
    )
    # add some data to central logs directory to allow appending
    output_paths = get_output_paths_from_output_directory(result_directory)
    central_perf_df = transform_perf_df_for_appending(artifact_perf_df, output_paths)
    pd.DataFrame(columns=central_perf_df).to_csv(tmp_path / "log_summary_0000.csv")
    # test no warnings were raised
    append_perf_data_to_central_logs(artifact_perf_df, output_paths)
    assert not caplog.records


@pytest.mark.parametrize(
    "invalid_log_path",
    [
        Path("/mnt/team/simulation_science/pub/models/project_name/model_version/"),
        Path("/mnt/team/simulation_science/pub/project_and_model_version/"),
        Path("/ihme/homes/user/model_version/"),
    ],
)
def test_invalid_log_path(invalid_log_path, artifact_perf_df, caplog):
    # test we raise specific warning
    output_paths = get_output_paths_from_output_directory(invalid_log_path)
    append_perf_data_to_central_logs(artifact_perf_df, output_paths)
    assert "Skipping appending central performance logs." in caplog.text


@pytest.mark.parametrize(
    "num_rows_in_most_recent_file, num_rows_to_append", [(2, 2), (2, 6), (4, 4), (2, 10)]
)
def test_appending(
    num_rows_in_most_recent_file,
    num_rows_to_append,
    artifact_perf_df,
    result_directory,
    tmp_path,
    monkeypatch,
):
    MAX_NUM_ROWS = 4
    monkeypatch.setattr(
        "vivarium_cluster_tools.psimulate.performance_logger.NUM_ROWS_PER_CENTRAL_LOG_FILE",
        MAX_NUM_ROWS,
    )
    monkeypatch.setattr(
        "vivarium_cluster_tools.psimulate.performance_logger.CENTRAL_PERFORMANCE_LOGS_DIRECTORY",
        tmp_path,
    )

    # create most recent file
    most_recent_file = tmp_path / "log_summary_0000.csv"
    output_paths = get_output_paths_from_output_directory(result_directory)
    central_perf_df = transform_perf_df_for_appending(artifact_perf_df, output_paths)
    pd.DataFrame(
        index=range(num_rows_in_most_recent_file), columns=central_perf_df.columns
    ).to_csv(most_recent_file, index=False)
    # append data
    data_to_append = central_perf_df[:num_rows_to_append]
    append_child_job_data(data_to_append)

    # test appended data
    total_num_rows = num_rows_in_most_recent_file + num_rows_to_append
    expected_num_of_files = int(ceil(total_num_rows / MAX_NUM_ROWS))

    output_files = [tmp_path / filename for filename in os.listdir(tmp_path)]
    assert len(output_files) == expected_num_of_files
    assert all([len(pd.read_csv(file)) == MAX_NUM_ROWS for file in output_files])
