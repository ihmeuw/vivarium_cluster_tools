"""
==================
Results Processing
==================

Tools for processing and writing results.

"""

import re
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from loguru import logger

from vivarium_cluster_tools import utilities as vct_utils
from vivarium_cluster_tools.psimulate.paths import OutputPaths

DEFAULT_BYTES_PER_ROW_ESTIMATE = 500.0


class ChunkMap:
    """Tracks current chunk number per metric during results writing.

    This is mutable state that gets updated in place as chunks rotate.

    Attributes
    ----------
    results_dir
        Directory containing metric subdirectories with chunk files.
    metrics
        Dictionary mapping metric name to current chunk number. Defaults to 0 for new metrics.
    """

    def __init__(self, results_dir: Path, metrics: dict[str, int] | None = None) -> None:
        self.results_dir = results_dir
        self.metrics: defaultdict[str, int] = defaultdict(int)
        if metrics is not None:
            self.metrics.update(metrics)

    @classmethod
    def from_existing_results(cls, results_dir: Path) -> "ChunkMap":
        """Create ChunkMap by scanning existing chunk files.

        Parameters
        ----------
        results_dir
            Directory containing metric subdirectories with chunk files.

        Returns
        -------
            ChunkMap initialized with current chunk numbers per metric.
        """
        metrics: dict[str, int] = {}

        if not results_dir.exists():
            return cls(results_dir, metrics)

        for metric_dir in results_dir.iterdir():
            if not metric_dir.is_dir():
                continue

            chunk_files = list(metric_dir.glob("chunk_*.parquet"))
            if not chunk_files:
                continue

            # Find highest chunk number
            chunk_nums = []
            for f in chunk_files:
                match = re.match(r"chunk_(\d+)\.parquet", f.name)
                if match:
                    chunk_nums.append(int(match.group(1)))

            if chunk_nums:
                metrics[metric_dir.name] = max(chunk_nums)

        return cls(results_dir, metrics)

    def get_path(self, metric: str) -> Path:
        """Get full path to current chunk file for a metric."""
        chunk_num = self.metrics[metric]
        return self.results_dir / metric / f"chunk_{chunk_num:04d}.parquet"

    def __getitem__(self, metric: str) -> int:
        """Get current chunk number for a metric."""
        return self.metrics[metric]

    def __setitem__(self, metric: str, chunk_num: int) -> None:
        """Set chunk number for a metric."""
        self.metrics[metric] = chunk_num

    def __contains__(self, metric: str) -> bool:
        """Check if metric is tracked."""
        return metric in self.metrics

    def bytes_per_row(self, metric: str) -> float:
        """Get estimated bytes per row for a metric.

        Uses the initial chunk file (chunk_0000.parquet) if it exists,
        otherwise returns the default estimate.

        Parameters
        ----------
        metric
            The metric name.

        Returns
        -------
            Estimated bytes per row.
        """
        chunk_0_path = self.results_dir / metric / "chunk_0000.parquet"
        if chunk_0_path.exists():
            rows: int = pq.read_metadata(chunk_0_path).num_rows
            if rows > 0:
                return chunk_0_path.stat().st_size / rows
        return DEFAULT_BYTES_PER_ROW_ESTIMATE


def write_results_batch(
    output_paths: OutputPaths,
    existing_metadata: pd.DataFrame,
    unwritten_metadata: list[pd.DataFrame],
    unwritten_results: list[dict[str, pd.DataFrame]],
    batch_size: int,
    chunk_map: ChunkMap,
    output_file_size: int,
) -> tuple[pd.DataFrame, list[pd.DataFrame], list[dict[str, pd.DataFrame]]]:
    """Write batch of results and finished simulation metadata to disk.

    Results are written to chunked parquet files within per-metric directories
    (e.g., ``results_dir/metric_name/chunk_0000.parquet``). When a chunk file
    exceeds ``output_file_size`` bytes, a new chunk file is started.

    To read all results for a metric, use ``pd.read_parquet(results_dir / metric_name)``,
    which automatically combines all parquet files in the directory.

    Parameters
    ----------
    output_paths
        Container class with output filepaths as attributes.
    existing_metadata
        Metadata for finished simulations that has already been written to disk.
    unwritten_metadata
        Metadata for finished simulations that has not yet been written to disk.
    unwritten_results
        Results for finished simulations that have not yet been written to disk.
    batch_size
        Number of results to write in this batch.
    chunk_map
        Dictionary mapping metric names to current chunk numbers. Updated in place.
    output_file_size
        Maximum file size in bytes per chunk. When exceeded, a new chunk is started.

    Returns
    -------
        A tuple of:
        - The updated metadata that was written to disk as part of this batch.
        - The unwritten metadata remaining to write in subsequent batches.
        - The unwritten results remaining to write in subsequent batches.
    """
    logger.info(f"Writing batch of {batch_size} results.")
    new_metadata_to_write, unwritten_metadata = (
        unwritten_metadata[:batch_size],
        unwritten_metadata[batch_size:],
    )
    new_results_to_write, unwritten_results = (
        unwritten_results[:batch_size],
        unwritten_results[batch_size:],
    )
    metadata_to_write = _concat_metadata(existing_metadata, new_metadata_to_write)
    results_to_write = _combine_batch_results(new_results_to_write)

    start = time.time()
    # Write results to chunked files per metric
    for metric, new_df in results_to_write.items():
        (output_paths.results_dir / metric).mkdir(exist_ok=True)
        _write_metric_chunk(
            new_data=new_df,
            chunk_map=chunk_map,
            metric=metric,
            output_file_size=output_file_size,
        )
    # Metadata is small enough to overwrite entirely each time
    _safe_write(metadata_to_write, output_paths.finished_sim_metadata)
    end = time.time()
    logger.info(f"Updated results in {end - start:.4f}s.")
    return metadata_to_write, unwritten_metadata, unwritten_results


def _write_metric_chunk(
    new_data: pd.DataFrame,
    chunk_map: ChunkMap,
    metric: str,
    output_file_size: int,
) -> None:
    """Write new data to chunk file(s), splitting across chunks if needed.

    Data may be split across multiple chunk files to respect output_file_size limits.
    Each chunk file will be approximately output_file_size bytes or smaller.
    """
    remaining = new_data.reset_index(drop=True)

    while not remaining.empty:
        chunk_path = chunk_map.get_path(metric)

        if chunk_path.exists():
            current_size = chunk_path.stat().st_size
            remaining_space = max(0, output_file_size - current_size)
        else:
            remaining_space = output_file_size

        rows_that_fit = int(remaining_space / chunk_map.bytes_per_row(metric))
        if rows_that_fit < 1:
            if chunk_map.bytes_per_row(metric) > output_file_size:
                # If the file size is smaller than estimated bytes per row,
                # we have no choice but to write one row per chunk
                logger.warning(
                    f"Estimated bytes per row for metric '{metric}' "
                    f"({chunk_map.bytes_per_row(metric):.2f} bytes) "
                    f"exceeds chunk size ({output_file_size} bytes). "
                    f"Writing one row per chunk file."
                )
                rows_that_fit = 1
            else:
                # No space left in current chunk, rotate to next chunk
                chunk_map[metric] += 1
                continue

        to_write = remaining.iloc[:rows_that_fit].reset_index(drop=True)
        remaining = remaining.iloc[rows_that_fit:].reset_index(drop=True)

        # Combine with existing data if present
        if chunk_path.exists():
            combined = _concat_preserve_types(
                [pd.read_parquet(chunk_path).reset_index(drop=True), to_write]
            )
        else:
            combined = to_write

        _safe_write(combined, chunk_path)

        # If more data remains, rotate to next chunk for next iteration
        if not remaining.empty:
            chunk_map[metric] += 1


def _concat_metadata(
    old_metadata: pd.DataFrame, new_metadata: list[pd.DataFrame]
) -> pd.DataFrame:
    """Concatenate the new metadata to the old."""
    start = time.time()

    to_concat_old = [old_metadata.reset_index(drop=True)] if not old_metadata.empty else []
    to_concat = to_concat_old + [df.reset_index(drop=True) for df in new_metadata]

    updated = _concat_preserve_types(to_concat)

    end = time.time()
    logger.info(f"Concatenated {len(new_metadata)} metadata in {end - start:.2f}s.")
    return updated


def _combine_batch_results(
    new_results: list[dict[str, pd.DataFrame]],
) -> dict[str, pd.DataFrame]:
    """Concatenate only the new batch results (not existing results on disk).

    This function combines the results from the current batch into a single
    DataFrame per metric, without loading any existing results from disk.
    """
    start = time.time()
    results = {}
    metrics = {key for new in new_results for key in new.keys()}
    for metric in metrics:
        to_concat = [
            df.reset_index(drop=True)
            for result in new_results
            for met, df in result.items()
            if met == metric
        ]
        results[metric] = _concat_preserve_types(to_concat)

    end = time.time()
    logger.info(f"Concatenated {len(new_results)} results in {end - start:.2f}s.")
    return results


def _concat_preserve_types(df_list: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate datasets and preserve all ``numpy`` dtypes.

    This does not preserve any pandas-specific dtypes, e.g. categories become
    objects). We assume that all dataframes in the list have identical columns
    and dtypes to the first.
    """
    dtype_mapping = {col: df_list[0][col].dtype for col in df_list[0].columns}
    for df in df_list:
        for col, dtype in dtype_mapping.items():
            df[col] = df[col].astype(dtype)
    return pd.concat(df_list, axis=0).reset_index(drop=True)


@vct_utils.backoff_and_retry(backoff_seconds=30, num_retries=3, log_function=logger.warning)
def _safe_write(df: pd.DataFrame, output_path: Path) -> None:
    """Write a dataframe to disk, retrying if there are any issues.

    Parameters
    ----------
    df
        DataFrame to write to disk.
    output_path
        Path to write the DataFrame to.

    Raises
    ------
    NotImplementedError
        If the file extension is not supported.
    """
    # Writing to some file types over and over balloons the file size so
    # write to new file and move it over to avoid
    temp_output_path = output_path.with_name(
        output_path.stem + "_update" + output_path.suffix
    )
    if output_path.suffix == ".parquet":
        df.to_parquet(temp_output_path)
    elif output_path.suffix == ".csv":
        df.to_csv(temp_output_path, index=False)
    else:
        raise NotImplementedError(f"Writing to {output_path.suffix} is not supported.")
    temp_output_path.replace(output_path)
