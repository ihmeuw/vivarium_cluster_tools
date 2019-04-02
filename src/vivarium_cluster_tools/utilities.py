from datetime import datetime
from pathlib import Path
import sys

from loguru import logger

from vivarium_cluster_tools import globals


def configure_master_process_logging_to_terminal():
    logger.add(sys.stdout, colorize=True, level="INFO")
    logger.add(sys.stdout, colorize=True, level="DEBUG")
    logger.add(sys.stderr, colorize=True, level="WARNING")
    logger.add(sys.stderr, colorize=True, level="ERROR")


def configure_master_process_logging_to_file(output_directory):
    master_log = output_directory / 'master.log'
    logger.add(master_log, colorize=True, level="INFO", serialize=True)
    logger.add(master_log, colorize=True, level="DEBUG", serialize=True)
    logger.add(master_log, colorize=True, level="WARNING", serialize=True)
    logger.add(master_log, colorize=True, level="ERROR", serialize=True)


def get_output_directory(model_specification_file=None, output_directory=None, restart=False):
    if restart:
        output_directory = Path(output_directory)
    else:
        root = Path(output_directory) if output_directory else Path(globals.DEFAULT_OUTPUT_DIRECTORY)
        launch_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        model_specification_name = Path(model_specification_file).stem
        output_directory = root / model_specification_name / launch_time
    return output_directory

