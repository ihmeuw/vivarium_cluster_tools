from pathlib import Path

import pandas as pd
import pytest

from vivarium_cluster_tools.psimulate.paths import OutputPaths
from vivarium_cluster_tools.psimulate.results.processing import (
    ChunkMap,
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
    """Test that one call writes exactly batch_size results to chunk files."""
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
    chunk_size = 100 * 1024 * 1024  # 100 MB
    chunk_map = ChunkMap(output_paths.results_dir)

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
        chunk_map,
        chunk_size,
    )

    # Check metadata was updated with only the batch_size items
    expected_metadata = pd.concat(
        [existing_metadata_orig] + unwritten_metadata_orig[:batch_size], axis=0
    ).reset_index(drop=True)
    assert existing_metadata.equals(expected_metadata)
    assert existing_metadata.equals(pd.read_csv(output_paths.finished_sim_metadata))

    # Check results were written to chunk files in metric directories
    results_metric_dir = output_paths.results_dir / "results"
    assert results_metric_dir.is_dir()
    chunk_files = list(results_metric_dir.glob("chunk_*.parquet"))
    assert len(chunk_files) == 1

    # Check the chunk file contains exactly batch_size results
    chunk_results = pd.read_parquet(chunk_files[0])
    assert len(chunk_results) == batch_size
    assert chunk_results["value"].tolist() == [30, 40]

    # Check that the remaining item is returned for future batches
    assert len(unwritten_metadata) == 1
    assert len(unwritten_results) == 1
    assert unwritten_results[0]["results"]["value"].tolist() == [50]

    # Check chunk_map was updated
    assert chunk_map.metrics == {"results": 0}


def test_write_results_batch_multiple_calls(tmp_path: Path) -> None:
    """Test that multiple calls accumulate data in chunk files and pd.read_parquet() combines them."""
    output_paths = OutputPaths.from_entry_point_args(
        command="foo",
        input_artifact_path=Path("some/artifact/path"),
        result_directory=tmp_path,
        input_model_spec_path=Path("some/model/spec/path"),
    )
    output_paths.results_dir.mkdir()

    existing_metadata = pd.DataFrame()
    batch_size = 2
    chunk_size = 100 * 1024 * 1024  # 100 MB
    chunk_map = ChunkMap(output_paths.results_dir)

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
        chunk_map=chunk_map,
        chunk_size=chunk_size,
    )

    # Second call: write 2 more results (should append to same chunk)
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
        chunk_map=chunk_map,
        chunk_size=chunk_size,
    )

    # Check that there's still just 1 chunk file (all data fits)
    results_metric_dir = output_paths.results_dir / "results"
    chunk_files = sorted(results_metric_dir.glob("chunk_*.parquet"))
    assert len(chunk_files) == 1

    # pd.read_parquet() on the directory returns all data
    all_results = pd.read_parquet(results_metric_dir)
    assert len(all_results) == 4
    assert set(all_results["value"].tolist()) == {10, 20, 30, 40}

    # Check metadata accumulated correctly across calls
    assert len(existing_metadata) == 4


def test_chunk_rotation_between_batches(tmp_path: Path) -> None:
    """Test that chunks rotate between batches when file size exceeds chunk_size."""
    output_paths = OutputPaths.from_entry_point_args(
        command="foo",
        input_artifact_path=Path("some/artifact/path"),
        result_directory=tmp_path,
        input_model_spec_path=Path("some/model/spec/path"),
    )
    output_paths.results_dir.mkdir()

    existing_metadata = pd.DataFrame()
    batch_size = 2
    chunk_map = ChunkMap(output_paths.results_dir)

    # First batch: write 2 results - creates chunk_0000
    unwritten_metadata_1 = [pd.DataFrame({"rows": [i], "batch": [1]}) for i in range(2)]
    unwritten_results_1 = [
        {"results": pd.DataFrame({"rows": [i], "batch": [1], "value": [i * 10]})}
        for i in range(2)
    ]

    existing_metadata, _, _ = write_results_batch(
        output_paths,
        existing_metadata,
        unwritten_metadata_1,
        unwritten_results_1,
        batch_size=batch_size,
        chunk_map=chunk_map,
        chunk_size=100 * 1024 * 1024,  # Large - no rotation within batch
    )

    # Get the size of the first chunk to use as chunk_size for second batch
    results_metric_dir = output_paths.results_dir / "results"
    first_chunk_size = (results_metric_dir / "chunk_0000.parquet").stat().st_size

    # Second batch with chunk_size = first_chunk_size means rotation should happen
    unwritten_metadata_2 = [pd.DataFrame({"rows": [i], "batch": [2]}) for i in range(2, 4)]
    unwritten_results_2 = [
        {"results": pd.DataFrame({"rows": [i], "batch": [2], "value": [i * 10]})}
        for i in range(2, 4)
    ]

    existing_metadata, _, _ = write_results_batch(
        output_paths,
        existing_metadata,
        unwritten_metadata_2,
        unwritten_results_2,
        batch_size=batch_size,
        chunk_map=chunk_map,
        chunk_size=first_chunk_size,  # This will force rotation
    )

    # Check that 2 chunk files were created
    chunk_files = sorted(results_metric_dir.glob("chunk_*.parquet"))
    assert len(chunk_files) == 2

    # Check chunk_map was updated
    assert chunk_map.metrics["results"] == 1

    # Check reading directory combines all chunks
    all_results = pd.read_parquet(results_metric_dir)
    assert len(all_results) == 4
    assert set(all_results["value"].tolist()) == {0, 10, 20, 30}


def test_batch_split_across_old_and_new_chunk(tmp_path: Path) -> None:
    """Test that a single batch can be split between an existing chunk and a new one."""
    output_paths = OutputPaths.from_entry_point_args(
        command="foo",
        input_artifact_path=Path("some/artifact/path"),
        result_directory=tmp_path,
        input_model_spec_path=Path("some/model/spec/path"),
    )
    output_paths.results_dir.mkdir()

    existing_metadata = pd.DataFrame()
    chunk_map = ChunkMap(output_paths.results_dir)

    # First batch: write 1 result to establish chunk_0000
    unwritten_metadata_1 = [pd.DataFrame({"rows": [0], "batch": [1]})]
    unwritten_results_1 = [
        {"results": pd.DataFrame({"rows": [0], "batch": [1], "value": [0]})}
    ]

    existing_metadata, _, _ = write_results_batch(
        output_paths,
        existing_metadata,
        unwritten_metadata_1,
        unwritten_results_1,
        batch_size=1,
        chunk_map=chunk_map,
        chunk_size=100 * 1024 * 1024,  # Large - no rotation yet
    )

    results_metric_dir = output_paths.results_dir / "results"
    first_chunk_size = (results_metric_dir / "chunk_0000.parquet").stat().st_size

    # Second batch: write 4 results with chunk_size allowing ~2 rows per chunk
    # This should split: some rows append to chunk_0000, rest go to chunk_0001
    unwritten_metadata_2 = [pd.DataFrame({"rows": [i], "batch": [2]}) for i in range(1, 5)]
    unwritten_results_2 = [
        {"results": pd.DataFrame({"rows": [i], "batch": [2], "value": [i * 10]})}
        for i in range(1, 5)
    ]

    # Set chunk_size to ~2x the single-row file size
    chunk_size = int(first_chunk_size * 2.5)

    existing_metadata, _, _ = write_results_batch(
        output_paths,
        existing_metadata,
        unwritten_metadata_2,
        unwritten_results_2,
        batch_size=4,
        chunk_map=chunk_map,
        chunk_size=chunk_size,
    )

    # Check that multiple chunk files were created
    chunk_files = sorted(results_metric_dir.glob("chunk_*.parquet"))
    assert len(chunk_files) >= 2, f"Expected at least 2 chunks, got {len(chunk_files)}"

    # Check that chunk_0000 has more than 1 row (original + some from batch 2)
    chunk_0 = pd.read_parquet(results_metric_dir / "chunk_0000.parquet")
    assert len(chunk_0) > 1, "Chunk 0 should have data from both batches"

    # Check reading directory combines all chunks correctly
    all_results = pd.read_parquet(results_metric_dir)
    assert len(all_results) == 5
    assert set(all_results["value"].tolist()) == {0, 10, 20, 30, 40}


def test_large_batch_splits_into_multiple_new_chunks(tmp_path: Path) -> None:
    """Test that a single large batch can be split across multiple new chunk files."""
    output_paths = OutputPaths.from_entry_point_args(
        command="foo",
        input_artifact_path=Path("some/artifact/path"),
        result_directory=tmp_path,
        input_model_spec_path=Path("some/model/spec/path"),
    )
    output_paths.results_dir.mkdir()

    existing_metadata = pd.DataFrame()
    chunk_map = ChunkMap(output_paths.results_dir)

    # Create a batch with many results - use larger data to minimize parquet overhead effects
    num_results = 100
    unwritten_metadata = [
        pd.DataFrame({"rows": [i], "batch": [1]}) for i in range(num_results)
    ]
    # Create results with more columns to make each row larger
    unwritten_results = [
        {
            "results": pd.DataFrame(
                {
                    "rows": [i],
                    "batch": [1],
                    "value": [i * 10],
                    "extra_col_1": ["x" * 100],  # Add some bulk
                    "extra_col_2": [float(i) * 1.23456],
                    "extra_col_3": [i * 1000],
                }
            )
        }
        for i in range(num_results)
    ]

    # Write a sample batch to estimate file sizes
    sample_results = unwritten_results[:10]
    sample_combined = pd.concat([r["results"] for r in sample_results], ignore_index=True)
    sample_path = tmp_path / "sample.parquet"
    sample_combined.to_parquet(sample_path)
    ten_row_size = sample_path.stat().st_size

    # Set chunk_size to hold approximately 20 rows (twice the sample)
    # This should result in ~5 chunks for 100 rows
    chunk_size = ten_row_size * 2

    existing_metadata, _, _ = write_results_batch(
        output_paths,
        existing_metadata,
        unwritten_metadata,
        unwritten_results,
        batch_size=num_results,
        chunk_map=chunk_map,
        chunk_size=chunk_size,
    )

    # Check that multiple chunk files were created
    results_metric_dir = output_paths.results_dir / "results"
    chunk_files = sorted(results_metric_dir.glob("chunk_*.parquet"))
    assert len(chunk_files) >= 3, f"Expected at least 3 chunks, got {len(chunk_files)}"

    # Check reading directory combines all chunks correctly
    all_results = pd.read_parquet(results_metric_dir)
    assert len(all_results) == num_results
    assert set(all_results["value"].tolist()) == {i * 10 for i in range(num_results)}


def test_chunk_map_from_existing_results(tmp_path: Path) -> None:
    """Test that ChunkMap.from_existing_results correctly scans existing chunk files."""
    # Create existing chunk files for two metrics
    metric_a_dir = tmp_path / "results" / "metric_a"
    metric_a_dir.mkdir(parents=True)
    metric_b_dir = tmp_path / "results" / "metric_b"
    metric_b_dir.mkdir(parents=True)

    # Write chunks for metric_a (two chunks)
    pd.DataFrame({"a": [1, 2, 3]}).to_parquet(metric_a_dir / "chunk_0000.parquet")
    pd.DataFrame({"a": [4, 5]}).to_parquet(metric_a_dir / "chunk_0001.parquet")

    # Write chunks for metric_b (one chunk)
    pd.DataFrame({"b": [10, 20]}).to_parquet(metric_b_dir / "chunk_0000.parquet")

    # Load chunk map
    chunk_map = ChunkMap.from_existing_results(tmp_path / "results")

    # Should detect highest chunk number for each metric
    assert chunk_map.metrics == {"metric_a": 1, "metric_b": 0}
