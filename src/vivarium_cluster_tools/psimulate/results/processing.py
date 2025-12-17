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
    metrics
        Dictionary mapping metric name to current chunk number. Defaults to 0 for new metrics.
    """

    def __init__(self, metrics: dict[str, int] | None = None) -> None:
        self.metrics = defaultdict(int)
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
            return cls(metrics)

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

        return cls(metrics)

    def get_path(self, metric: str) -> Path:
        """Get current chunk path for a metric."""
        chunk_num = self.metrics[metric]
        return Path(f"chunk_{chunk_num:04d}.parquet")

    def __getitem__(self, metric: str) -> int:
        """Get current chunk number for a metric."""
        return self.metrics[metric]

    def __setitem__(self, metric: str, chunk_num: int) -> None:
        """Set chunk number for a metric."""
        self.metrics[metric] = chunk_num

    def __contains__(self, metric: str) -> bool:
        """Check if metric is tracked."""
        return metric in self.metrics


def write_results_batch(
    output_paths: OutputPaths,
    existing_metadata: pd.DataFrame,
    unwritten_metadata: list[pd.DataFrame],
    unwritten_results: list[dict[str, pd.DataFrame]],
    batch_size: int,
    chunk_map: ChunkMap,
    chunk_size: int,
) -> tuple[pd.DataFrame, list[pd.DataFrame], list[dict[str, pd.DataFrame]]]:
    """Write batch of results and finished simulation metadata to disk.

    Results are written to chunked parquet files within per-metric directories
    (e.g., ``results_dir/metric_name/chunk_0000.parquet``). When a chunk file
    exceeds ``chunk_size`` bytes, a new chunk file is started.

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
    chunk_size
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
    results_to_write = _concat_batch_results(new_results_to_write)

    start = time.time()
    # Write results to chunked files per metric
    for metric, new_df in results_to_write.items():
        _write_metric_chunk(
            metric_dir=output_paths.results_dir / metric,
            new_data=new_df,
            chunk_map=chunk_map,
            metric=metric,
            chunk_size=chunk_size,
        )
    # Metadata is small enough to overwrite entirely each time
    _safe_write(metadata_to_write, output_paths.finished_sim_metadata)
    end = time.time()
    logger.info(f"Updated results in {end - start:.4f}s.")
    return metadata_to_write, unwritten_metadata, unwritten_results


def _estimate_bytes_per_row(
    chunk_path: Path | None, sample_df: pd.DataFrame | None = None
) -> float:
    """Estimate bytes per row from existing file or by sampling the data.

    Parameters
    ----------
    chunk_path
        Path to existing parquet file to estimate from.
    sample_df
        DataFrame to sample if no existing file available.

    Returns
    -------
        Estimated bytes per row.
    """
    # Try to estimate from existing file
    if chunk_path and chunk_path.exists():
        rows = pq.read_metadata(chunk_path).num_rows
        if rows > 0:
            return chunk_path.stat().st_size / rows

    # Estimate by serializing a small sample of the data
    # Use a small sample to get a conservative (larger) estimate that accounts
    # for per-file overhead, which ensures we don't overfill chunks
    if sample_df is not None and not sample_df.empty:
        import io

        buf = io.BytesIO()
        # Use a small sample (10 rows) to get conservative overhead estimate
        sample_rows = min(10, len(sample_df))
        sample_df.head(sample_rows).to_parquet(buf)
        return buf.tell() / sample_rows

    # Fallback default (conservative estimate)
    return DEFAULT_BYTES_PER_ROW_ESTIMATE


def _write_metric_chunk(
    metric_dir: Path,
    new_data: pd.DataFrame,
    chunk_map: ChunkMap,
    metric: str,
    chunk_size: int,
) -> None:
    """Write new data to chunk file(s), splitting across chunks if needed.

    Data may be split across multiple chunk files to respect chunk_size limits.
    Each chunk file will be approximately chunk_size bytes or smaller.
    """
    metric_dir.mkdir(exist_ok=True)

    remaining = new_data.reset_index(drop=True)

    while not remaining.empty:
        chunk_path = metric_dir / chunk_map.get_path(metric)

        # If current chunk is already at/over limit, rotate first
        if chunk_path.exists() and chunk_path.stat().st_size >= chunk_size:
            chunk_map[metric] += 1
            chunk_path = metric_dir / chunk_map.get_path(metric)

        # Estimate how many rows will fit
        bytes_per_row = _estimate_bytes_per_row(chunk_path, remaining)

        if chunk_path.exists():
            current_size = chunk_path.stat().st_size
            remaining_space = max(0, chunk_size - current_size)
        else:
            remaining_space = chunk_size

        # Calculate rows that fit, but always write at least 1 row to make progress
        rows_that_fit = max(1, int(remaining_space / bytes_per_row))
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


def _concat_batch_results(
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
