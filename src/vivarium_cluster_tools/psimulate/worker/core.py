"""
==================
Distributed Worker
==================

RQ worker with custom retry handling.

"""
from __future__ import annotations

import atexit
import os
import random
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, TextIO

import redis
from loguru import logger
from rq import Queue
from rq.job import Job, JobStatus
from rq.registry import FailedJobRegistry
from rq.worker import Worker

from vivarium_cluster_tools.psimulate.environment import ENV_VARIABLES


def build_launch_script(
    worker_settings_file: Path,
    worker_log_directory: Path,
) -> tempfile._TemporaryFileWrapper[str]:
    """Generates a shell file that, on execution, spins up an RQ worker."""
    launcher = tempfile.NamedTemporaryFile(
        mode="w",
        dir=".",
        prefix="vivarium_cluster_tools_launcher_",
        suffix=".sh",
        delete=False,
    )

    output_dir = str(worker_settings_file.resolve().parent)
    launcher.write(
        f"""
    export {ENV_VARIABLES.VIVARIUM_LOGGING_DIRECTORY.name}={worker_log_directory}
    export {ENV_VARIABLES.PYTHONPATH.name}={output_dir}:${ENV_VARIABLES.PYTHONPATH.name}

    {shutil.which('rq')} worker -c {worker_settings_file.stem} \
        --name ${{{ENV_VARIABLES.JOB_ID.name}}}.${{{ENV_VARIABLES.TASK_ID.name}}} \
        --burst \
        -w "{__name__}._ResilientWorker" \
        --exception-handler "{__name__}._retry_handler" vivarium

    """
    )
    launcher.close()

    atexit.register(lambda: os.remove(launcher.name))
    return launcher


def _retry_handler(job: Job, *exc_info: Any) -> bool:
    retries = job.meta.get("remaining_retries", 2)

    if retries > 0:
        retries -= 1
        job.meta["remaining_retries"] = retries
        job.set_status(JobStatus.QUEUED)
        # Note: Cannot directly assign to read-only exc_info property, but RQ will handle this
        job.save()
        q = Queue(name=job.origin, connection=job.connection)
        q.enqueue_job(job)
        logger.info(f"Retrying job {job.id}")
    else:
        logger.error(f"Failing job {job.id}")
        q = Queue(name=job.origin, connection=job.connection)
        failed_queue = FailedJobRegistry(queue=q)
        # Convert exc_info to string for the failed queue
        exc_string = str(exc_info) if exc_info else "Unknown error"
        failed_queue.add(job, exc_string=exc_string)
    return False


class _ResilientWorker(Worker):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.acceptable_failure_count = 3
        log_name = f"{ENV_VARIABLES.JOB_ID.value}.{ENV_VARIABLES.TASK_ID.value}.log"
        logging_directory = Path(ENV_VARIABLES.VIVARIUM_LOGGING_DIRECTORY.value)
        logger.add(logging_directory / log_name, level="DEBUG")

    def work(
        self,
        burst: bool = True,
        logging_level: str | None = "INFO",
        date_format: str = "%H:%M:%S",
        log_format: str = "%(asctime)s %(message)s",
        max_jobs: int | None = None,
        max_idle_time: int | None = None,
        with_scheduler: bool = False,
        dequeue_strategy: Any = None,  # DequeueStrategy type is not available
    ) -> bool:
        kwargs = {
            "burst": burst,
            "logging_level": "DEBUG",  # Override to DEBUG
            "date_format": date_format,
            "log_format": log_format,
            "max_jobs": max_jobs,
            "max_idle_time": max_idle_time,
            "with_scheduler": with_scheduler,
            "dequeue_strategy": dequeue_strategy,
        }

        retries = 0
        while retries < 10:
            try:
                result = super(_ResilientWorker, self).work(**kwargs)
                return result
            except redis.exceptions.ConnectionError:
                backoff = random.random() * 60
                logger.error(f"Couldn't connect to redis. Retrying in {backoff}...")
                retries += 1
                time.sleep(backoff)
        logger.error(f"Ran out of retries. Killing worker")
        return False

    def main_work_horse(self, job: Job, queue: Queue) -> None:
        retries = 0
        while retries < 10:
            try:
                super(_ResilientWorker, self).main_work_horse(job, queue)
                return
            except redis.exceptions.ConnectionError:
                backoff = random.random() * 60
                logger.error(f"Couldn't connect to redis. Retrying in {backoff}...")
                retries += 1
                time.sleep(backoff)
        logger.error(f"Ran out of retries. Killing work horse")

    def fork_work_horse(self, job: Job, queue: Queue) -> None:
        """Spawns a work horse to perform the actual work and passes it a job."""
        child_pid = os.fork()
        ENV_VARIABLES.RQ_WORKER_ID.update(self.name)
        ENV_VARIABLES.RQ_JOB_ID.update(job.id)
        if child_pid == 0:
            self.main_work_horse(job, queue)
            os._exit(0)  # just in case
        else:
            self._horse_pid = child_pid
            self.procline("Forked {0} at {1}".format(child_pid, time.time()))  # type: ignore[no-untyped-call] # procline is not typed in rq
