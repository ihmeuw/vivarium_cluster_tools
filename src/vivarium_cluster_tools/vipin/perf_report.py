import json
import re
from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd
import requests
from loguru import logger
from pandas.io.json import json_normalize


BASE_PERF_INDEX_COLS = ['host', 'job_number', 'task_number', 'draw', 'seed']


class PerformanceSummary:
    """
    A class to implement a getter for data in the workers' performance logs.

    Given a Path, a PerformanceSummary class provides a generator to get at each
    entry in the workers' performance logs. The class also provides a method
    to get all entries in a pd.DataFrame. This class is intended as a singleton
    to provide data about a single Vivarium simulation run.

    Attributes
    ----------
    log_dir : Path
        Path of log_dir

    Methods
    -------
    get_summaries():
        Generator to retrieve entries in the performance logs as dict objects.
    to_df():
        Returns the performance log data as a pd.DataFrame.
    """

    def __init__(self, log_dir: Path):
        self.log_dir: Path = log_dir
        self.errors: int = 0

    def get_summaries(self) -> dict:
        """Generator to get all performance summary log messages in PerformanceSummary"""
        for log in [f for f in self.log_dir.iterdir() if self.PERF_LOG_PATTERN.fullmatch(f.name)]:
            with log.open('r') as f:
                count: int = 0
                for line in f.readlines():
                    count += 1
                    try:
                        message = json.loads(line)['record']['message']
                    except Exception as e:
                        logger.warning(f"Exception: {e}. Malformed message in {log} line {count}, skipping...")
                        self.errors += 1
                        continue
                    m = self.TELEMETRY_PATTERN.fullmatch(str(message))
                    if m:
                        yield json_normalize(json.loads(message), sep='_')

    def to_df(self) -> pd.DataFrame:
        perf_data = []
        for item in self.get_summaries():
            perf_data.append(item)
        perf_df = pd.concat(perf_data)

        # Convert the Unix timestamps to datetimes
        for col in [col for col in perf_df.columns if col.startswith("event_")]:
            perf_df[col] = pd.to_datetime(perf_df[col], unit='s')

        # Remove trailing "_scenario" from normalized label
        perf_df.columns = perf_df.columns.str.replace('_scenario', '', regex=False)
        return perf_df

    TELEMETRY_PATTERN = re.compile(r'^{\"host\".+\"job_number\".+}$')
    PERF_LOG_PATTERN = re.compile(r'^perf\.([0-9]+)\.([0-9]+)\.log$')


def report_performance(input_directory: Union[Path, str], output_directory: Union[Path, str], output_hdf: bool):
    input_directory, output_directory = Path(input_directory), Path(output_directory)
    perf_summary = PerformanceSummary(input_directory)

    perf_df = perf_summary.to_df()

    # Get jobapi data about the job
    try:
        job_numbers = perf_df['job_number'].unique()
        assert (len(job_numbers) == 1)
        jobapi_data = requests.get("http://jobapi.ihme.washington.edu/fair/queryjobids",
                                   params=[('job_number', job_numbers[0]), ('limit', 50000)]).json()
        jobapi_df = pd.DataFrame(jobapi_data["data"])
        perf_df = perf_df.astype({'job_number': np.int64, 'task_number': np.int64})
        perf_df = perf_df.merge(jobapi_df, on=['job_number', 'task_number'])
    except Exception as e:
        logger.warning(f'Job API request failed with {e}')

    # Set index to include branch configuration/scenario columns
    index_cols = BASE_PERF_INDEX_COLS
    index_cols.extend([col for col in perf_df.columns if col.startswith("scenario_")])
    perf_df = perf_df.set_index(index_cols)

    # Write to file
    out_file = output_directory / 'log_summary'
    if output_hdf:
        out_file = out_file.with_suffix(".hdf")
        perf_df.to_hdf(out_file, key='worker_data')
    else:
        out_file = out_file.with_suffix(".csv")
        perf_df.to_csv(out_file)

    if perf_summary.errors > 0:
        logger.warning(f'{perf_summary.errors} log row{"s were" if perf_summary.errors > 1 else " was"} unreadable.')
    logger.info(f'Performance summary {"hdf" if output_hdf else "csv"} can be found at {out_file}, with '
                f'{perf_df.shape[0]} row{"s" if perf_df.shape[0] > 1 else ""}.')
