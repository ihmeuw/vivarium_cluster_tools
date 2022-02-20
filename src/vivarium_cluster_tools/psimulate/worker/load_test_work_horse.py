"""
================
Load Test Worker
================

RQ worker executable for doing load testing.

"""
import time
from traceback import format_exc

import numpy as np
import pandas as pd
from loguru import logger
from rq import get_current_job
from vivarium.framework.randomness import get_hash

from vivarium_cluster_tools.psimulate.environment import ENV_VARIABLES
from vivarium_cluster_tools.psimulate.jobs import JobParameters

LOAD_TEST_WORK_HORSE_IMPORT_PATH = f"{__name__}.work_horse"


def work_horse(job_parameters: dict) -> pd.DataFrame:
    node = f"{ENV_VARIABLES.HOSTNAME.value}"
    job = f"{ENV_VARIABLES.JOB_ID.value}:{ENV_VARIABLES.TASK_ID.value}"

    job_parameters = JobParameters(**job_parameters)

    test_type = job_parameters.extras["test_type"]
    test_runner = {
        "sleep": sleep_test,
    }[test_type]

    logger.info(f"Launching new job {job} on {node}")
    logger.info(f"Starting job: {job_parameters}")
    try:
        return test_runner(job_parameters)
    except Exception:
        logger.exception("Unhandled exception in worker")
        job = get_current_job()
        job.meta["root_exception"] = format_exc()
        job.save_meta()
        raise
    finally:
        logger.info(f"Exiting job: {job_parameters}")


def sleep_test(job_parameters: JobParameters) -> pd.DataFrame:
    min_sleep_time = 5
    max_sleep_time = 60
    rs = np.random.RandomState(seed=get_hash(f"sleep_test_{job_parameters.random_seed}"))
    sleep_time = rs.randint(min_sleep_time, max_sleep_time)

    logger.info(f"Sleeping for {sleep_time}s.")
    time.sleep(sleep_time)
    logger.info(f"Sleep test successful.")
    return pd.DataFrame(
        {"sleep_time": sleep_time}, index=pd.Index([job_parameters.random_seed], name="seed")
    )
