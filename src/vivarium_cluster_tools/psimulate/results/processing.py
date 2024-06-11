"""
==================
Results Processing
==================

Tools for processing and writing results.

"""

import time
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from loguru import logger

from vivarium_cluster_tools import utilities as vct_utils
from vivarium_cluster_tools.psimulate.paths import OutputPaths


def write_results_batch(
    output_paths: OutputPaths,
    existing_metadata: pd.DataFrame,
    existing_results: Dict[str, pd.DataFrame],
    unwritten_metadata: List[pd.DataFrame],
    unwritten_results: List[Dict[str, pd.DataFrame]],
    batch_size: int = 50,
) -> Tuple[
    pd.DataFrame, List[pd.DataFrame], Dict[str, pd.DataFrame], List[Dict[str, pd.DataFrame]]
]:
    """Write batch of results and finished simulation metadata to disk."""
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
    results_to_write = _concat_results(existing_results, new_results_to_write)

    start = time.time()
    # write out updated metadata and results
    for metric, df in results_to_write.items():
        _safe_write(df, output_paths.results_dir / f"{metric}.parquet")
    _safe_write(metadata_to_write, output_paths.finished_sim_metadata)
    end = time.time()
    logger.info(f"Updated results in {end - start:.4f}s.")
    return metadata_to_write, unwritten_metadata, results_to_write, unwritten_results


def _concat_metadata(old: pd.DataFrame, new: List[pd.DataFrame]) -> pd.DataFrame:
    # Skips all the pandas index checking because columns are in the same order.
    start = time.time()

    to_concat = [df.reset_index(drop=True) for df in new]
    if not old.empty:
        to_concat += [old.reset_index(drop=True)]

    updated = _concat_preserve_types(to_concat)

    end = time.time()
    logger.info(f"Concatenated {len(new)} metadata in {end - start:.2f}s.")
    return updated


def _concat_results(
    old: Dict[str, pd.DataFrame], new: List[Dict[str, pd.DataFrame]]
) -> Dict[str, pd.DataFrame]:
    # Skips all the pandas index checking because columns are in the same order.
    start = time.time()
    results = {}
    metrics = {key for new_results in new for key in new_results.keys()}
    for metric in metrics:
        to_concat = [
            df.reset_index(drop=True)
            for result in new
            for met, df in result.items()
            if met == metric
        ]

        if metric in old:
            to_concat += [old[metric].reset_index(drop=True)]

        results[metric] = _concat_preserve_types(to_concat)

    end = time.time()
    logger.info(f"Concatenated {len(new)} results in {end - start:.2f}s.")
    return results


def _concat_preserve_types(df_list: List[pd.DataFrame]) -> pd.DataFrame:
    """Concatenation preserves all ``numpy`` dtypes but does not preserve any
    pandas specific dtypes (e.g., categories become objects."""
    # We assume that all dataframes in the list have identical dtypes to the first
    # Also, we convert dtypes to their string representations to avoid comparison
    # issues (especially with CategoricalDtype)
    col_order = df_list[0].columns
    dtypes = df_list[0].dtypes.astype(str)
    columns_by_dtype = [list(dtype_group.index) for _, dtype_group in dtypes.groupby(dtypes)]

    splits = []
    for columns in columns_by_dtype:
        original_dtypes = {col: df_list[0][col].dtype for col in columns}
        slices = [df.filter(columns) for df in df_list]
        slice_df = pd.DataFrame(data=np.concatenate(slices), columns=columns)
        for col, dtype in original_dtypes.items():
            slice_df[col] = slice_df[col].astype(dtype)
        splits.append(slice_df)
    return pd.concat(splits, axis=1)[col_order]


@vct_utils.backoff_and_retry(backoff_seconds=30, num_retries=3, log_function=logger.warning)
def _safe_write(results: pd.DataFrame, output_path: Path) -> None:
    # Writing to some file types over and over balloons the file size so
    # write to new file and move it over to avoid
    temp_output_path = output_path.with_name(output_path.name + "update")
    if output_path.suffix == ".parquet":
        results.to_parquet(temp_output_path)
    elif output_path.suffix == ".csv":
        results.to_csv(temp_output_path, index=False)
    else:
        raise NotImplementedError(f"Writing to {output_path.suffix} is not supported.")
    temp_output_path.replace(output_path)
