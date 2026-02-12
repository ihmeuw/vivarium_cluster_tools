"""
==================
Results Staging
==================

Staging area for task results before periodic aggregation into final output files.

Workers write results to the staging directory as individual files per task.
The main process periodically scans for completed tasks, reads their results,
and aggregates them into the final output using ``write_results_batch()``.

File naming convention per task_id:
- ``{task_id}_metadata.csv`` -- finished simulation metadata
- ``{task_id}_{metric}.parquet`` -- results per metric
- ``{task_id}.done`` -- marker file written LAST to signal completion

"""

import threading
from pathlib import Path

import pandas as pd
from loguru import logger

from vivarium_cluster_tools.psimulate.paths import OutputPaths
from vivarium_cluster_tools.psimulate.results.processing import (
    OutputFileMap,
    write_results_batch,
)


def write_staging_results(
    staging_dir: Path,
    task_id: str,
    metadata_df: pd.DataFrame,
    results_dict: dict[str, pd.DataFrame],
) -> None:
    """Write task results to the staging directory.

    Writes metadata CSV, metric parquets, then a ``.done`` marker file LAST
    to signal that all data files are complete.

    Parameters
    ----------
    staging_dir
        The staging directory.
    task_id
        The deterministic task ID.
    metadata_df
        The finished simulation metadata DataFrame.
    results_dict
        Dictionary mapping metric names to results DataFrames.
    """
    metadata_df.to_csv(staging_dir / f"{task_id}_metadata.csv", index=False)
    for metric, df in results_dict.items():
        df.to_parquet(staging_dir / f"{task_id}_{metric}.parquet")
    # Write the done marker LAST
    (staging_dir / f"{task_id}.done").touch()


def read_staging_result(
    staging_dir: Path, task_id: str
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Read a single task's staged results.

    Parameters
    ----------
    staging_dir
        The staging directory.
    task_id
        The deterministic task ID.

    Returns
    -------
        A tuple of (metadata_df, results_dict).
    """
    metadata_df = pd.read_csv(staging_dir / f"{task_id}_metadata.csv")
    results_dict: dict[str, pd.DataFrame] = {}

    for path in staging_dir.glob(f"{task_id}_*.parquet"):
        # Extract metric name: "{task_id}_{metric}.parquet" -> "metric"
        metric = path.stem[len(task_id) + 1 :]
        results_dict[metric] = pd.read_parquet(path)

    return metadata_df, results_dict


def scan_done_markers(staging_dir: Path, already_processed: set[str]) -> list[str]:
    """Scan the staging directory for new completed tasks.

    Parameters
    ----------
    staging_dir
        The staging directory.
    already_processed
        Set of task IDs that have already been processed.

    Returns
    -------
        List of newly completed task IDs.
    """
    new_task_ids = []
    for done_file in staging_dir.glob("*.done"):
        task_id = done_file.stem
        if task_id not in already_processed:
            new_task_ids.append(task_id)
    return new_task_ids


def cleanup_staging_files(staging_dir: Path, task_id: str) -> None:
    """Remove all staging files for a given task.

    Parameters
    ----------
    staging_dir
        The staging directory.
    task_id
        The deterministic task ID.
    """
    for path in staging_dir.glob(f"{task_id}_*"):
        path.unlink(missing_ok=True)
    (staging_dir / f"{task_id}.done").unlink(missing_ok=True)


def aggregate_pending_staging(
    staging_dir: Path,
    output_paths: OutputPaths,
    existing_metadata: pd.DataFrame,
    output_file_size: int,
) -> pd.DataFrame:
    """Aggregate any pending staged results on startup (crash recovery).

    Scans for ``.done`` markers and flushes their results via
    ``write_results_batch()``, then cleans up the staging files.

    Parameters
    ----------
    staging_dir
        The staging directory.
    output_paths
        The output paths container.
    existing_metadata
        The existing metadata DataFrame from prior runs.
    output_file_size
        Maximum file size in bytes for output files.

    Returns
    -------
        The updated metadata DataFrame after flushing any pending results.
    """
    if not staging_dir.exists():
        return existing_metadata

    task_ids = scan_done_markers(staging_dir, already_processed=set())
    if not task_ids:
        return existing_metadata

    logger.info(
        f"Found {len(task_ids)} pending staged results from a previous run. " "Aggregating..."
    )

    output_file_map = OutputFileMap.from_existing_results(
        output_paths.results_dir, output_file_size
    )

    unwritten_metadata: list[pd.DataFrame] = []
    unwritten_results: list[dict[str, pd.DataFrame]] = []
    for task_id in task_ids:
        metadata_df, results_dict = read_staging_result(staging_dir, task_id)
        unwritten_metadata.append(metadata_df)
        unwritten_results.append(results_dict)

    existing_metadata, _, _ = write_results_batch(
        output_paths=output_paths,
        existing_metadata=existing_metadata,
        unwritten_metadata=unwritten_metadata,
        unwritten_results=unwritten_results,
        batch_size=len(unwritten_results),
        output_file_map=output_file_map,
        output_file_size=output_file_size,
    )

    for task_id in task_ids:
        cleanup_staging_files(staging_dir, task_id)

    logger.info(f"Aggregated {len(task_ids)} pending staged results.")
    return existing_metadata


class PeriodicAggregator(threading.Thread):
    """Background thread that periodically scans the staging directory
    and flushes completed task results to the final output files.

    Parameters
    ----------
    staging_dir
        The staging directory to scan.
    output_paths
        The output paths container.
    existing_metadata
        The current metadata DataFrame (will be updated in place via reference).
    output_file_size
        Maximum file size in bytes for output files.
    batch_size
        Number of results to accumulate before flushing.
    interval
        Seconds between scans. Defaults to 30.
    """

    def __init__(
        self,
        staging_dir: Path,
        output_paths: OutputPaths,
        existing_metadata: pd.DataFrame,
        output_file_size: int,
        batch_size: int,
        interval: float = 30.0,
    ) -> None:
        super().__init__(daemon=True)
        self._staging_dir = staging_dir
        self._output_paths = output_paths
        self._existing_metadata = existing_metadata
        self._output_file_size = output_file_size
        self._batch_size = batch_size
        self._interval = interval

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._processed: set[str] = set()
        self._buffer_metadata: list[pd.DataFrame] = []
        self._buffer_results: list[dict[str, pd.DataFrame]] = []
        self._output_file_map = OutputFileMap.from_existing_results(
            output_paths.results_dir, output_file_size
        )
        self._total_flushed = 0

    @property
    def existing_metadata(self) -> pd.DataFrame:
        """Thread-safe access to the current metadata."""
        with self._lock:
            return self._existing_metadata

    @property
    def total_flushed(self) -> int:
        """Total number of results flushed to disk."""
        with self._lock:
            return self._total_flushed

    def run(self) -> None:
        """Main loop: scan, buffer, and flush on interval."""
        while not self._stop_event.is_set():
            self._scan_and_buffer()
            self._maybe_flush()
            self._stop_event.wait(self._interval)

    def stop(self) -> None:
        """Signal the thread to stop."""
        self._stop_event.set()

    def final_flush(self) -> pd.DataFrame:
        """Flush all remaining buffered results. Call after ``stop()`` and ``join()``.

        Returns
        -------
            The final metadata DataFrame.
        """
        # One last scan to pick up any stragglers
        self._scan_and_buffer()
        self._flush_buffer(force=True)
        return self._existing_metadata

    def _scan_and_buffer(self) -> None:
        """Scan for new .done markers and buffer their results."""
        new_task_ids = scan_done_markers(self._staging_dir, self._processed)
        for task_id in new_task_ids:
            try:
                metadata_df, results_dict = read_staging_result(self._staging_dir, task_id)
                with self._lock:
                    self._buffer_metadata.append(metadata_df)
                    self._buffer_results.append(results_dict)
                    self._processed.add(task_id)
            except Exception:
                logger.exception(f"Failed to read staging result for task {task_id}")

    def _maybe_flush(self) -> None:
        """Flush buffer if it exceeds batch_size."""
        with self._lock:
            should_flush = len(self._buffer_results) >= self._batch_size
        if should_flush:
            self._flush_buffer()

    def _flush_buffer(self, force: bool = False) -> None:
        """Write buffered results to disk."""
        with self._lock:
            if not self._buffer_results and not force:
                return
            metadata_to_write = self._buffer_metadata
            results_to_write = self._buffer_results
            self._buffer_metadata = []
            self._buffer_results = []

        if not results_to_write:
            return

        count = len(results_to_write)
        logger.info(f"Flushing {count} buffered results to disk.")

        self._existing_metadata, _, _ = write_results_batch(
            output_paths=self._output_paths,
            existing_metadata=self._existing_metadata,
            unwritten_metadata=metadata_to_write,
            unwritten_results=results_to_write,
            batch_size=count,
            output_file_map=self._output_file_map,
            output_file_size=self._output_file_size,
        )

        # Clean up staging files for flushed tasks
        with self._lock:
            self._total_flushed += count

        # We can get the task_ids from the processed set, but we only
        # want to clean up the ones we just flushed. Since scan_done_markers
        # returns task_ids in order and we process them in order, we can
        # track which ones to clean up based on the metadata we just wrote.
        # For simplicity, clean up all processed tasks that have been flushed.
        for task_id in list(self._processed):
            cleanup_staging_files(self._staging_dir, task_id)
