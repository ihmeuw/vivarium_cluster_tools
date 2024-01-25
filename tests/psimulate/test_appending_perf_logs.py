from loguru import logger
import mock
import os
import pandas as pd
import pytest

from math import ceil
from _pytest.logging import LogCaptureFixture
from pathlib import Path

from vivarium_cluster_tools.psimulate.runner import (append_perf_data_to_central_logs,
                                                    transform_perf_df_for_appending,
                                                     append_child_job_data,
                                                     generate_runner_job_data)


@pytest.fixture
def artifact_perf_df():
    df = pd.read_csv('data/artifact_perf_df.csv')
    index_cols = ['host', 'job_number', 'task_number', 'draw', 'seed']
    scenario_cols = [col for col in df.columns if col.startswith('scenario')]
    df = df.set_index(index_cols + scenario_cols)
    return df

@pytest.fixture
def artifactless_perf_df():
    df = pd.read_csv("data/artifactless_perf_df.csv")
    index_cols = ['host', 'job_number', 'task_number', 'draw', 'seed']
    scenario_cols = [col for col in df.columns if col.startswith('scenario')]
    df = df.set_index(index_cols + scenario_cols)
    return df

@pytest.fixture
def log_path():
    return Path('/mnt/team/simulation_science/pub/models/project_name/results/model_version/artifact/YYYY_MM_DD_HH_MM_SS/logs/yyyy_mm_dd_hh_mm_ss_runtype/worker_logs')

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


@pytest.mark.parametrize('df_name', ['artifact_perf_df', 'artifactless_perf_df'])
def test_expected_columns(df_name, log_path, tmp_path, request):
    perf_df = request.getfixturevalue(df_name)
    # transform df
    central_perf_df = transform_perf_df_for_appending(perf_df, log_path)
    expected_columns = ['host','job_number','task_number','draw','seed'] + perf_df.columns.tolist() + ['artifact_name', 'scenario_parameters']
    assert list(central_perf_df.columns) == expected_columns


@pytest.mark.parametrize('df_name', ['artifact_perf_df', 'artifactless_perf_df'])
def test_data_parsing(df_name, log_path, tmp_path, request):
    perf_df = request.getfixturevalue(df_name)
    central_perf_df = transform_perf_df_for_appending(perf_df, log_path)
    assert (central_perf_df['artifact_name'] == 'artifact').all()

    if df_name == 'artifact_perf_df':
        scenario_parameters = ['{"scenario_parameter_one": "value_one", "scenario_input_data_artifact_path": "/path/to/artifact.hdf"}',
                               '{"scenario_parameter_one": "value_two", "scenario_input_data_artifact_path": "/path/to/artifact.hdf"}'] * 6
    else:
        scenario_parameters = ['{"scenario_parameter_one": "value_one"}',
                               '{"scenario_parameter_one": "value_two"}'] * 6
    assert (central_perf_df['scenario_parameters'] == scenario_parameters).all()

    runner_data = generate_runner_job_data(central_perf_df, log_path, 'first_file_with_data')

    assert runner_data['project_name'].squeeze() == 'project_name'
    assert runner_data['root_path'].squeeze() == Path('/mnt/team/simulation_science/pub/models/project_name/results/model_version/artifact')
    assert runner_data['original_run_date'].squeeze() == 'YYYY_MM_DD_HH_MM_SS'
    assert runner_data['run_date'].squeeze() == 'yyyy_mm_dd_hh_mm_ss'
    assert runner_data['run_type'].squeeze() == 'runtype'
    assert runner_data['log_summary_file_path'].squeeze() == 'first_file_with_data'
    assert runner_data['original_log_file_path'].squeeze() == str(Path(log_path) / 'log_summary.csv')


def test_valid_log_path(log_path, artifact_perf_df, caplog, tmp_path, monkeypatch):
    monkeypatch.setattr("vivarium_cluster_tools.psimulate.runner.CENTRAL_PERFORMANCE_LOGS_DIRECTORY", tmp_path)
    monkeypatch.setattr('vivarium_cluster_tools.psimulate.runner.NUM_ROWS_PER_CENTRAL_LOG_FILE', 4)
    # add some data to central logs directory
    central_perf_df = transform_perf_df_for_appending(artifact_perf_df, log_path)
    pd.DataFrame(columns=central_perf_df).to_csv(tmp_path / 'log_summary_0000.csv')
    # test no warnings were raised
    append_perf_data_to_central_logs(artifact_perf_df, log_path)
    assert not caplog.records


@pytest.mark.parametrize("invalid_log_path", [
    Path('/mnt/team/simulation_science/pub/models/project_name/model_version/artifact/YYYY_MM_DD_HH_MM_SS/logs/yyyy_mm_dd_hh_mm_ss_runtype/worker_logs'),
    Path('/mnt/team/simulation_science/pub/project_and_model_version/artifact/YYYY_MM_DD_HH_MM_SS/logs/yyyy_mm_dd_hh_mm_ss_runtype/worker_logs'),
    Path('/ihme/homes/user/run/artifact/YYYY_MM_DD_HH_MM_SS/logs/yyyy_mm_dd_hh_mm_ss_runtype/worker_logs')
])
def test_invalid_log_path(invalid_log_path, artifact_perf_df, caplog):
    # parametrize plausible path names
    invalid_log_path = Path('/mnt/team/simulation_science/priv/model_run')
    # test we raise specific warning
    append_perf_data_to_central_logs(artifact_perf_df, invalid_log_path)
    assert 'Skipping appending central performance logs.' in caplog.text


@pytest.mark.parametrize("num_rows_in_most_recent_file, num_rows_to_append",
                         [(2, 2), (2, 6), (4, 4), (2, 10)])
def test_appending(num_rows_in_most_recent_file, num_rows_to_append, artifact_perf_df, tmp_path, monkeypatch):
    MAX_NUM_ROWS = 4
    monkeypatch.setattr('vivarium_cluster_tools.psimulate.runner.NUM_ROWS_PER_CENTRAL_LOG_FILE', MAX_NUM_ROWS)
    monkeypatch.setattr("vivarium_cluster_tools.psimulate.runner.CENTRAL_PERFORMANCE_LOGS_DIRECTORY", tmp_path)

    # create most recent file
    most_recent_file = tmp_path / 'log_summary_0000.csv'
    central_perf_df = transform_perf_df_for_appending(artifact_perf_df, log_path)
    pd.DataFrame(index=range(num_rows_in_most_recent_file), columns=central_perf_df.columns).to_csv(most_recent_file, index=False)
    # append data
    data_to_append = central_perf_df[:num_rows_to_append]
    _ = append_child_job_data(data_to_append)

    # test appended data
    total_num_rows = num_rows_in_most_recent_file + num_rows_to_append
    expected_num_of_files = int(ceil(total_num_rows / MAX_NUM_ROWS))

    output_files = [tmp_path / filename for filename in os.listdir(tmp_path)]
    assert len(output_files) == expected_num_of_files
    assert all([len(pd.read_csv(file)) == MAX_NUM_ROWS for file in output_files])