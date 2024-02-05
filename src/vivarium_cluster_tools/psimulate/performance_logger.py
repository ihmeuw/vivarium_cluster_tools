import glob
import json
from pathlib import Path

import pandas as pd
from loguru import logger

from vivarium_cluster_tools.psimulate.paths import (
    CENTRAL_PERFORMANCE_LOGS_DIRECTORY,
    OutputPaths,
)
from vivarium_cluster_tools.utilities import NUM_ROWS_PER_CENTRAL_LOG_FILE


def transform_perf_df_for_appending(
    perf_df: pd.DataFrame, output_paths: OutputPaths
) -> pd.DataFrame:
    """
    Take performance dataframe from performance report and 1) turn index into columns so
    we can write to csv, 2) add artifact name column, and 3) aggregate scenario information
    into one column.

    Parameters
    ----------
    perf_df
        DataFrame pulled from performance report with index values uniquely identifying each child
        job and column values containing their performance data.
    output_paths
        OutputPaths object from paths.py containing information about the results directory.

    Returns
    --------
    DataFrame
        The transformed DataFrame which can be directly appended to our central logs. The data now
        has a simple RangeIndex, the index values as columns, a new artifact name column, and a new
        scenario parameters column.
    """
    central_perf_df = perf_df.reset_index()
    # add artifact name to central_perf_df
    # TODO: [MIC-4859] refer to key from job_parameters from worker/vivarium_work_horse.py
    #  instead of 'scenario'
    artifact_path_col = "scenario_input_data_artifact_path"
    if (
        artifact_path_col in central_perf_df.columns
    ):  # if we parallelized across artifact paths
        central_perf_df["artifact_name"] = central_perf_df[artifact_path_col].apply(
            lambda filepath: Path(filepath).stem
        )
    else:  # else get from output directory
        central_perf_df["artifact_name"] = output_paths.artifact_name

    ## aggregate scenario information into one column
    all_scenario_cols = [
        col for col in central_perf_df.columns if col.startswith("scenario_")
    ]
    # remove duplicate scenario information
    unique_scenario_cols = [
        col for col in all_scenario_cols if not col.startswith("scenario_run_configuration")
    ]
    scenario_parameters = central_perf_df[unique_scenario_cols].to_dict(orient="records")
    central_perf_df["scenario_parameters"] = pd.Series(scenario_parameters).apply(json.dumps)

    central_perf_df = central_perf_df.drop(all_scenario_cols, axis=1)

    return central_perf_df


def append_child_job_data(child_job_performance_data: pd.DataFrame) -> str:
    """Append child job data and return name of first file containing this data.

    Parameters
    ----------
    child_job_performance_data
        DataFrame pulled from transform_perf_df_for_appending.

    Returns
    --------
    str
       str of the first file in our central logs containing child job data.
    """
    log_files = glob.glob(
        CENTRAL_PERFORMANCE_LOGS_DIRECTORY.as_posix() + "/log_summary_*.csv"
    )

    most_recent_file_path = sorted(log_files)[-1]
    most_recent_file_index = int(Path(most_recent_file_path).stem.replace("log_summary_", ""))
    most_recent_data = pd.read_csv(most_recent_file_path)
    most_recent_file_is_full = len(most_recent_data) >= NUM_ROWS_PER_CENTRAL_LOG_FILE

    if most_recent_file_is_full:
        new_file_index = most_recent_file_index + 1
        formatted_new_file_index = str(new_file_index).zfill(4)
        first_file_with_data = (
            CENTRAL_PERFORMANCE_LOGS_DIRECTORY / f"log_summary_{formatted_new_file_index}.csv"
        )
    else:
        first_file_with_data = most_recent_file_path

    # fill up the most recent file (possibly with 0 rows)
    rows_to_append = NUM_ROWS_PER_CENTRAL_LOG_FILE - len(most_recent_data)
    child_job_performance_data[:rows_to_append].to_csv(
        most_recent_file_path, mode="a", header=False, index=False
    )
    # remove rows appended to most recent file from dataframe
    child_job_performance_data = child_job_performance_data[rows_to_append:]

    new_file_index = most_recent_file_index + 1

    while len(child_job_performance_data) != 0:
        # define new filename
        formatted_new_file_index = str(new_file_index).zfill(4)
        new_file = (
            CENTRAL_PERFORMANCE_LOGS_DIRECTORY / f"log_summary_{formatted_new_file_index}.csv"
        )
        # write to file
        child_job_performance_data[:NUM_ROWS_PER_CENTRAL_LOG_FILE].to_csv(
            new_file, index=False
        )
        # remove written rows and update index
        child_job_performance_data = child_job_performance_data[
            NUM_ROWS_PER_CENTRAL_LOG_FILE:
        ]
        new_file_index += 1

    return first_file_with_data


def generate_runner_job_data(
    job_number: int, output_paths: OutputPaths, first_file_with_data: str
) -> pd.DataFrame:
    """Create runner job data to append to central logs.

    Parameters
    ----------
    job_number
        int of job number for our runner job.
    output_paths
        OutputPaths object from paths.py containing information about the results directory.
    first_file_with_data
        str of the first file in our central logs containing child job data
        launched by our runner job.
    """
    runner_data = pd.DataFrame({"job_number": job_number}, index=[0])
    runner_data["project_name"] = output_paths.project_name
    runner_data["root_path"] = output_paths.root_path
    runner_data["original_run_date"] = output_paths.original_run_date
    runner_data["run_date"] = output_paths.run_date
    runner_data["run_type"] = output_paths.run_type
    runner_data["log_summary_file_path"] = first_file_with_data
    runner_data["original_log_file_path"] = (
        output_paths.worker_logging_root / "log_summary.csv"
    ).as_posix()

    return runner_data


def append_perf_data_to_central_logs(
    perf_df: pd.DataFrame, output_paths: OutputPaths
) -> None:
    """Append performance data to the central logs. This consists of child job data
    and runner data. The child job data will contain performance information and identifying
    information for each child job and the runner data will contain data about the runner job
    that launched these child jobs.

    Parameters
    ----------
    perf_df
        DataFrame pulled from performance report.
    output_paths
        OutputPaths object from paths.py containing information about the results directory.
    """
    if not output_paths.logging_to_central_results_directory:
        logger.warning(
            f"Log path {output_paths.worker_logging_root} not in central results directory. Skipping appending central performance logs."
        )
        return

    child_job_performance_data = transform_perf_df_for_appending(perf_df, output_paths)
    job_number = int(child_job_performance_data["job_number"].unique().squeeze())
    first_file_with_data = append_child_job_data(child_job_performance_data)

    runner_data = generate_runner_job_data(job_number, output_paths, first_file_with_data)
    runner_data_file = CENTRAL_PERFORMANCE_LOGS_DIRECTORY / "runner_data.csv"
    runner_data.to_csv(runner_data_file, mode="a", header=False, index=False)
