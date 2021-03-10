from loguru import logger
from pathlib import Path
from random import sample
import numpy as np
import pandas as pd
import json
import re
from pandas.io.json import json_normalize
from typing import Union
import requests


class WorkerLog:

    def __init__(self, file: Path):
        self.file = file
        name = file.stem.split('.')  # TODO: validate name is <sge job id>.<task id>.log
        self.sge_job_id = int(name[0])
        self.task_id = int(name[1])

    def get_summaries(self) -> dict:
        """Get all performance summary log messages in WorkerLog"""
        telemetry_pattern = re.compile(r'^\{\'host\'.+\'job_number\'.+\}$')

        # Ideally we'd only need to look at the tail of the file, but because a worker can do multiple draws, we need
        # to iterate over all the lines...
        # TODO: either change worker to cut a new log for each draw and check tail reversed(list(log))
        #  *OR* parallelize this
        f = self.file.open('r')
        for line in f.readlines():
            message = json.loads(line)['record']['message']
            m = telemetry_pattern.fullmatch(str(message))
            if m:
                yield json_normalize(message, sep='_')
        f.close()


def parse_log_directory(input_directory: Union[Path, str], output_directory: Union[Path, str], output_hdf: bool,
                        random_draw: int = 0):
    input_directory, output_directory = Path(input_directory), Path(output_directory)
    worker_log_pattern = re.compile(r'^([0-9]+)\.([0-9]+)\.log$')
    log_files = [f for f in input_directory.iterdir() if worker_log_pattern.fullmatch(f.name)]

    if random_draw:
        log_files = sample(log_files, random_draw)

    worker_data = []
    for i, file in enumerate(log_files):
        # TODO: set up tdqm progress bar
        logger.info(f'Parsing file {i+1} of {len(log_files)}.')
        log = WorkerLog(file)
        for item in log.get_summaries():
            worker_data.append(item)
    worker_df = pd.concat(worker_data)

    # Convert the Unix timestamps to datetimes
    for col in [col for col in worker_df.columns if col.startswith("event_")]:
        worker_df[col] = pd.to_datetime(worker_df[col], unit='s')

    # Simplify name of scenario normalized JSON label
    scenario_label = [col for col in worker_df.columns if 'scenario' in col]
    assert(len(scenario_label) == 1)
    worker_df = worker_df.rename(columns={scenario_label[0]: 'scenario'})

    # Get jobapi data about the job
    try:
        job_numbers = worker_df['job_number'].unique()
        assert(job_numbers == 1)
        jobapi_data = requests.get("http://jobapi.ihme.washington.edu/fair/queryjobids",
                         params=[('job_number', job_numbers[0]), ('limit', 50000)]).json()
        jobapi_df = pd.DataFrame(jobapi_data["data"])
        worker_df = worker_df.astype({'job_number': np.int64, 'task_number': np.int64})
        worker_df = worker_df.merge(jobapi_df, on=['job_number', 'task_number'])
    except Exception as e:
        logger.warning(f'Job API request failed with {e}')

    worker_df = worker_df.set_index(['host', 'job_id', 'task_id', 'draw', 'seed', 'scenario'])

    logger.info("\nStatistics by scenario:")
    for col in [col for col in worker_df.columns if col.startswith('exec_time_')]:
        logger.info(f'\n>>> {col}:\n{worker_df.groupby("scenario")[col].agg(["mean", "std", "min", "max"])}')

    # Write to file
    out_file = output_directory / 'log_summary'
    if output_hdf:
        out_file = out_file.with_suffix(".hdf")
        worker_df.to_hdf(out_file, key='worker_data')
    else:
        out_file = out_file.with_suffix(".csv")
        worker_df.to_csv(out_file)
    logger.info(f'All log files successfully parsed. Summary {"hdf" if output_hdf else "csv"} can be found at {out_file}.')
