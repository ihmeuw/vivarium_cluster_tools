from pathlib import Path

import pandas as pd
import pytest

from vivarium_cluster_tools.psimulate.paths import OutputPaths
from vivarium_cluster_tools.psimulate.results.processing import (
    _concat_batch_results,
    _concat_metadata,
    _concat_preserve_types,
    write_results_batch,
)

_DATA_TYPES = (
    list[int] | list[float] | list[str] | list[int | float] | list[int | float | str]
)


@pytest.mark.parametrize(
    "data",
    [
        [0, 1, 2],
        [0, 0.0, 1, 2, 3],
        [0.0, 1.0, 1.987987],
        [0, "a", 1, "b", 1.2],
        ["a", "b", "c", "d"],
    ],
    ids=[
        "just ints",
        "ints and floats",
        "just floats",
        "ints, floats, and strings",
        "just strings",
    ],
)
def test_concat_preserve_types(data: _DATA_TYPES) -> None:
    df = pd.DataFrame([data])
    df2 = pd.DataFrame([[d * 2 for d in data]])

    result = pd.DataFrame(_concat_preserve_types([df, df2]))

    expected_dtypes = df.dtypes

    expected_shape = (df.shape[0] + df2.shape[0], df.shape[1])

    for c in df:
        assert (result[c] == pd.concat([df[c], df2[c]]).reset_index(drop=True)).all()

    assert result.shape == expected_shape
    assert result.dtypes.sort_index().equals(expected_dtypes)


@pytest.mark.parametrize(
    "data",
    [
        [0, 1, 2],
        [0, 0.0, 1, 2, 3],
        [0.0, 1.0, 1.987987],
        [0, "a", 1, "b", 1.2],
        ["a", "b", "c", "d"],
    ],
    ids=[
        "just ints",
        "ints and floats",
        "just floats",
        "ints, floats, and strings",
        "just strings",
    ],
)
def test_concat_results(data: _DATA_TYPES) -> None:
    columns = [chr(i) for i in range(ord("a"), ord("a") + len(data))]
    old = pd.DataFrame([data], columns=columns)
    new = pd.DataFrame([[d * 2 for d in data]], columns=columns)

    old["input_draw"] = old["random_seed"] = 0.0
    new["input_draw"] = new["random_seed"] = 1.0

    combined = _concat_metadata(old, [new])

    expected_dtypes = old.dtypes

    expected_shape = (old.shape[0] + new.shape[0], old.shape[1])

    for c in old:
        assert (combined[c] == pd.concat([old[c], new[c]]).reset_index(drop=True)).all()

    assert combined.shape == expected_shape
    assert combined.dtypes.sort_index().equals(expected_dtypes.sort_index())

    # now no existing results
    no_old_combined = _concat_metadata(pd.DataFrame(), [old, new])

    assert no_old_combined.equals(combined)


def test_write_results_batch(tmp_path: Path) -> None:
    """Test that one call writes exactly batch_size results to one batch file."""
    output_paths = OutputPaths.from_entry_point_args(
        command="foo",
        input_artifact_path=Path("some/artifact/path"),
        result_directory=tmp_path,
        input_model_spec_path=Path("some/model/spec/path"),
    )
    output_paths.results_dir.mkdir()

    # Start with existing metadata (simulating a restart scenario)
    existing_metadata_orig = pd.DataFrame({"rows": [1, 2], "batch": [1, 1]})
    existing_metadata_orig.to_csv(output_paths.finished_sim_metadata, index=False)

    # Create 3 unwritten results with batch_size=2
    # This means 2 should be written, 1 should remain unwritten
    batch_size = 2
    unwritten_metadata_orig = [
        pd.DataFrame({"rows": [3], "batch": [2]}),
        pd.DataFrame({"rows": [4], "batch": [2]}),
        pd.DataFrame({"rows": [5], "batch": [3]}),  # This one should NOT be written
    ]
    unwritten_results_orig = [
        {"results": pd.DataFrame({"rows": [3], "batch": [2], "value": [30]})},
        {"results": pd.DataFrame({"rows": [4], "batch": [2], "value": [40]})},
        {"results": pd.DataFrame({"rows": [5], "batch": [3], "value": [50]})},
    ]

    (existing_metadata, unwritten_metadata, unwritten_results,) = write_results_batch(
        output_paths,
        existing_metadata_orig,
        unwritten_metadata_orig,
        unwritten_results_orig,
        batch_size,
    )

    # Check metadata was updated with only the batch_size items
    expected_metadata = pd.concat(
        [existing_metadata_orig] + unwritten_metadata_orig[:batch_size], axis=0
    ).reset_index(drop=True)
    assert existing_metadata.equals(expected_metadata)
    assert existing_metadata.equals(pd.read_csv(output_paths.finished_sim_metadata))

    # Check exactly one batch file was created
    results_metric_dir = output_paths.results_dir / "results"
    assert results_metric_dir.is_dir()
    batch_files = list(results_metric_dir.glob("batch_*.parquet"))
    assert len(batch_files) == 1

    # Check the batch file contains exactly batch_size results (not all 3)
    batch_results = pd.read_parquet(batch_files[0])
    assert len(batch_results) == batch_size
    assert batch_results["value"].tolist() == [30, 40]

    # Check that the remaining item is returned for future batches
    assert len(unwritten_metadata) == 1
    assert len(unwritten_results) == 1
    assert unwritten_results[0]["results"]["value"].tolist() == [50]


def test_write_results_batch_multiple_calls(tmp_path: Path) -> None:
    """Test that multiple calls create multiple batch files that pd.read_parquet() combines."""
    import time

    output_paths = OutputPaths.from_entry_point_args(
        command="foo",
        input_artifact_path=Path("some/artifact/path"),
        result_directory=tmp_path,
        input_model_spec_path=Path("some/model/spec/path"),
    )
    output_paths.results_dir.mkdir()

    existing_metadata = pd.DataFrame()
    batch_size = 2

    # First call: write 2 results
    unwritten_metadata_1 = [
        pd.DataFrame({"rows": [1], "batch": [1]}),
        pd.DataFrame({"rows": [2], "batch": [1]}),
    ]
    unwritten_results_1 = [
        {"results": pd.DataFrame({"rows": [1], "batch": [1], "value": [10]})},
        {"results": pd.DataFrame({"rows": [2], "batch": [1], "value": [20]})},
    ]

    existing_metadata, _, _ = write_results_batch(
        output_paths,
        existing_metadata,
        unwritten_metadata_1,
        unwritten_results_1,
        batch_size=batch_size,
    )

    # Ensure different timestamp for second batch file
    time.sleep(0.002)

    # Second call: write 2 more results
    unwritten_metadata_2 = [
        pd.DataFrame({"rows": [3], "batch": [2]}),
        pd.DataFrame({"rows": [4], "batch": [2]}),
    ]
    unwritten_results_2 = [
        {"results": pd.DataFrame({"rows": [3], "batch": [2], "value": [30]})},
        {"results": pd.DataFrame({"rows": [4], "batch": [2], "value": [40]})},
    ]

    existing_metadata, _, _ = write_results_batch(
        output_paths,
        existing_metadata,
        unwritten_metadata_2,
        unwritten_results_2,
        batch_size=batch_size,
    )

    # Check that 2 separate batch files were created
    results_metric_dir = output_paths.results_dir / "results"
    batch_files = sorted(results_metric_dir.glob("batch_*.parquet"))
    assert len(batch_files) == 2

    # Check each batch file has exactly batch_size rows
    for batch_file in batch_files:
        df = pd.read_parquet(batch_file)
        assert len(df) == batch_size

    # KEY TEST: pd.read_parquet() on the directory combines all batch files
    all_results = pd.read_parquet(results_metric_dir)
    assert len(all_results) == 4
    assert set(all_results["value"].tolist()) == {10, 20, 30, 40}

    # Check metadata accumulated correctly across calls
    assert len(existing_metadata) == 4
