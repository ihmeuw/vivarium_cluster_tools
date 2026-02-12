"""
================
Results Writing
================

Simple per-task result writing. Each worker writes one parquet file per metric
and one metadata CSV directly to the results directory.

Directory structure::

    results/
        metadata/
            {task_id}_metadata.csv
        {metric_name}/
            {task_id}.parquet

Reading all results for a metric is simply ``pd.read_parquet(results_dir / metric_name)``,
which automatically combines all parquet files in the directory.

"""

from pathlib import Path

import pandas as pd
from loguru import logger


def write_task_results(
    results_dir: Path,
    task_id: str,
    metadata_df: pd.DataFrame,
    results_dict: dict[str, pd.DataFrame],
) -> None:
    """Write a single task's results directly to the results directory.

    Parameters
    ----------
    results_dir
        The results directory (e.g., ``output_root/results``).
    task_id
        The deterministic task ID.
    metadata_df
        The finished simulation metadata DataFrame.
    results_dict
        Dictionary mapping metric names to results DataFrames.
    """
    # Write metadata
    metadata_dir = results_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    metadata_df.to_csv(metadata_dir / f"{task_id}_metadata.csv", index=False)

    # Write one parquet per metric
    for metric, df in results_dict.items():
        metric_dir = results_dir / metric
        metric_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(metric_dir / f"{task_id}.parquet")


def collect_metadata(results_dir: Path) -> pd.DataFrame:
    """Collect all per-task metadata CSVs into a single DataFrame.

    Parameters
    ----------
    results_dir
        The results directory containing a ``metadata/`` subdirectory.

    Returns
    -------
        Combined metadata DataFrame, or an empty DataFrame if no metadata exists.
    """
    metadata_dir = results_dir / "metadata"
    if not metadata_dir.exists():
        return pd.DataFrame()

    csv_files = sorted(metadata_dir.glob("*_metadata.csv"))
    if not csv_files:
        return pd.DataFrame()

    dfs = [pd.read_csv(f) for f in csv_files]
    return pd.concat(dfs, ignore_index=True)


def count_completed_tasks(results_dir: Path) -> int:
    """Count completed tasks by counting metadata files.

    Parameters
    ----------
    results_dir
        The results directory containing a ``metadata/`` subdirectory.

    Returns
    -------
        Number of completed tasks.
    """
    metadata_dir = results_dir / "metadata"
    if not metadata_dir.exists():
        return 0
    return len(list(metadata_dir.glob("*_metadata.csv")))
