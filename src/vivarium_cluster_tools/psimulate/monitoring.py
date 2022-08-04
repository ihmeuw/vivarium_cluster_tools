"""
Monitoring Module for Psimulate
"""
# this will become a subpackage in the future with added functionality

import atexit
import subprocess
from pathlib import Path

from loguru import logger


def run_rq_dashboard(redis_urls: list, logging_root: Path) -> None:
    # Get generic hostname url
    url = redis_urls[0].split("//")[1]
    hostname = url.split(":")[0]

    # Set up log file
    rq_dashboard_log = (logging_root / "rq_dashboard.log").open("a")
    logger.info("Fetching redis urls and starting RQ-Dashboard")
    url_flags = " ".join([f"-u {url}" for url in redis_urls])
    command = "rq-dashboard " + url_flags + " --debug"

    proc = subprocess.Popen(
        command, shell=True, stdout=rq_dashboard_log, stderr=rq_dashboard_log
    )
    rq_dashboard_log.write(f"Dashboard running at http://{hostname}:9181\n")
    logger.info(f"Dashboard running at http://{hostname}:9181")
    atexit.register(proc.kill)
