"""
==================
Results Processing
==================

Tools for processing and writing results.

"""

import time
from pathlib import Path

import pandas as pd
from loguru import logger

from vivarium_cluster_tools import utilities as vct_utils
from vivarium_cluster_tools.psimulate.paths import OutputPaths


def write_results_batch(
    output_paths: OutputPaths,
    existing_metadata: pd.DataFrame,
    unwritten_metadata: list[pd.DataFrame],
    unwritten_results: list[dict[str, pd.DataFrame]],
    batch_size: int,
) -> tuple[pd.DataFrame, list[pd.DataFrame], list[dict[str, pd.DataFrame]]]:
    """Write batch of results and finished simulation metadata to disk.

    Results are written incrementally as separate parquet files within per-metric
    directories (e.g., ``results_dir/metric_name/batch_<timestamp>.parquet``).
    This approach avoids loading all existing results into memory, preventing OOM
    errors for large-scale simulations.

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
    # Write results incrementally as separate batch files per metric
    batch_timestamp = int(time.time() * 1000)
    for metric, df in results_to_write.items():
        metric_dir = output_paths.results_dir / metric
        metric_dir.mkdir(exist_ok=True)
        batch_path = metric_dir / f"batch_{batch_timestamp}.parquet"
        _safe_write_batch(df, batch_path)
    # Metadata is small enough to overwrite entirely each time
    _safe_write(metadata_to_write, output_paths.finished_sim_metadata)
    end = time.time()
    logger.info(f"Updated results in {end - start:.4f}s.")
    return metadata_to_write, unwritten_metadata, unwritten_results


def _concat_metadata(
    old_metadata: pd.DataFrame, new_metadata: list[pd.DataFrame]
) -> pd.DataFrame:
    """Concatenate the new metadata to the old."""
    # Skips all the pandas index checking because columns are in the same order.
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
def _safe_write_batch(df: pd.DataFrame, output_path: Path) -> None:
    """Write a batch dataframe to disk as a new parquet file.

    This function writes a new parquet file without overwriting existing files,
    supporting incremental batch writes for large-scale simulations.

    Parameters
    ----------
    df
        DataFrame to write to disk.
    output_path
        Path to write the DataFrame to. Should be a unique path (e.g., with timestamp).
    """
    # Write to temp file first, then move to final location for atomicity
    temp_output_path = output_path.with_name(output_path.stem + "_temp" + output_path.suffix)
    df.to_parquet(temp_output_path)
    temp_output_path.replace(output_path)


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
