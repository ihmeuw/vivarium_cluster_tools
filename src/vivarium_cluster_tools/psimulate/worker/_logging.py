"""
=========================
Worker Logging Helper
=========================

Shared loguru configuration for worker-side entry points.

Worker tasks write to two SLURM streams: stdout (mirrored to the workflow's
stdout log file) and stderr (mirrored to the SLURM stderr file, which the
Jobmon GUI surfaces in its "Task Instance stderr" pane). Routing INFO+
messages to stdout and WARNING+ messages to stderr means the GUI shows
problems while routine progress stays out of the alarm channel.
"""

from __future__ import annotations

import sys

from loguru import logger


def configure_dual_sink() -> None:
    """Route INFO+ to stdout and WARNING+ to stderr.

    Called once at the top of each worker entry point so warnings and
    errors land in the SLURM stderr file and the Jobmon GUI surfaces them.
    """
    logger.add(sys.stdout, level="INFO")
    logger.add(sys.stderr, level="WARNING")
