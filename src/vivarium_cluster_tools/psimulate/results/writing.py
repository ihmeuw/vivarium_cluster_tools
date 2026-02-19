"""
================
Results Writing
================

Simple per-task result writing. Each worker writes one parquet file per metric
and one metadata JSON file directly to the results directory.

Directory structure::

    results/
        metadata/
            {task_id}_metadata.json
        {metric_name}/
            {task_id}.parquet

Reading all results for a metric is simply ``pd.read_parquet(results_dir / metric_name)``,
which automatically combines all parquet files in the directory.

"""

import json
from pathlib import Path
from typing import Any

import pandas as pd
from vivarium.framework.utilities import collapse_nested_dict

from vivarium_cluster_tools.psimulate.jobs import JobParameters


def write_task_results(
    results_dir: Path,
    task_id: str,
    job_parameters: JobParameters,
    results_dict: dict[str, pd.DataFrame],
) -> None:
    """Write a single task's results directly to the results directory.

    Parameters
    ----------
    results_dir
        The results directory (e.g., ``output_root/results``).
    task_id
        The deterministic task ID.
    job_parameters
        The job parameters for this task.
    results_dict
        Dictionary mapping metric names to results DataFrames.
    """
    # Write metadata as JSON from job-specific parameters
    metadata_dir = results_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    metadata: dict[str, Any] = {}
    for key, val in collapse_nested_dict(job_parameters.job_specific):
        metadata[key] = val
    with open(metadata_dir / f"{task_id}_metadata.json", "w") as f:
        json.dump(metadata, f)

    # Write one parquet per metric, injecting job-specific columns
    for metric, df in results_dict.items():
        metric_dir = results_dir / metric
        metric_dir.mkdir(parents=True, exist_ok=True)
        for key, val in collapse_nested_dict(job_parameters.job_specific):
            col_name = key.split(".")[-1]
            df.insert(df.shape[1] - 1, col_name, val)
        df.to_parquet(metric_dir / f"{task_id}.parquet")


def collect_metadata(results_dir: Path) -> pd.DataFrame:
    """Collect all per-task metadata JSON files into a single DataFrame.

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

    json_files = sorted(metadata_dir.glob("*_metadata.json"))
    if not json_files:
        return pd.DataFrame()

    rows = []
    for f in json_files:
        with open(f) as fh:
            rows.append(json.load(fh))
    return pd.DataFrame(rows)


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
    return len(list(metadata_dir.glob("*_metadata.json")))
