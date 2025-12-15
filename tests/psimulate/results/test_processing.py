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
    output_paths = OutputPaths.from_entry_point_args(
        command="foo",
        input_artifact_path=Path("some/artifact/path"),
        result_directory=tmp_path,
        input_model_spec_path=Path("some/model/spec/path"),
    )
    output_paths.results_dir.mkdir()

    # create and save out already-existing metadata file
    existing_metadata_orig = pd.DataFrame({"rows": [1, 2], "batch": [1, 1]})
    existing_metadata_orig.to_csv(output_paths.finished_sim_metadata, index=False)

    # create new metadata and results to write (larger than batch size of 2)
    unwritten_metadata_orig = [
        pd.DataFrame({"rows": [3], "batch": [2]}),
        pd.DataFrame({"rows": [4], "batch": [2]}),
        pd.DataFrame({"rows": [5], "batch": [3]}),
    ]
    unwritten_results_orig = []
    batch_size = 2
    for i in range(batch_size + 1):
        unwritten_results_orig += [
            {
                "results": pd.concat(
                    [unwritten_metadata_orig[i], pd.DataFrame({"value": [(i + 3) * 10]})],
                    axis=1,
                )
            }
        ]

    (
        existing_metadata,
        unwritten_metadata,
        unwritten_results,
    ) = write_results_batch(
        output_paths,
        existing_metadata_orig,
        unwritten_metadata_orig,
        unwritten_results_orig,
        batch_size,
    )

    # check that updated existing metadata has been updated w/ a new batch of 2
    assert existing_metadata.equals(
        pd.concat(
            [existing_metadata_orig] + unwritten_metadata_orig[:batch_size], axis=0
        ).reset_index(drop=True)
    )

    # check that the metadata has been written out
    assert existing_metadata.equals(pd.read_csv(output_paths.finished_sim_metadata))

    # check that results were written as batch files in metric directories
    results_metric_dir = output_paths.results_dir / "results"
    assert results_metric_dir.is_dir()
    batch_files = list(results_metric_dir.glob("batch_*.parquet"))
    assert len(batch_files) == 1

    # check that the batch file contains only the current batch (not all results)
    batch_results = pd.read_parquet(batch_files[0])
    expected_batch_results = pd.concat(
        [ur["results"] for ur in unwritten_results_orig[:batch_size]],
        axis=0,
    ).reset_index(drop=True)
    assert batch_results.equals(expected_batch_results)

    # check that the leftover unwritten metadata and results are returned
    assert unwritten_metadata == unwritten_metadata_orig[batch_size:]
    assert unwritten_results == unwritten_results_orig[batch_size:]


def test_write_results_batch_multiple_batches(tmp_path: Path) -> None:
    """Test that multiple batches write separate files that can be read together."""
    output_paths = OutputPaths.from_entry_point_args(
        command="foo",
        input_artifact_path=Path("some/artifact/path"),
        result_directory=tmp_path,
        input_model_spec_path=Path("some/model/spec/path"),
    )
    output_paths.results_dir.mkdir()

    existing_metadata = pd.DataFrame()

    # Write first batch
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
        batch_size=10,
    )

    # Write second batch (with a small delay to get different timestamp)
    import time
    time.sleep(0.01)  # Ensure different timestamp

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
        batch_size=10,
    )

    # Check that there are now 2 batch files
    results_metric_dir = output_paths.results_dir / "results"
    batch_files = list(results_metric_dir.glob("batch_*.parquet"))
    assert len(batch_files) == 2

    # Check that reading the directory combines all results
    all_results = pd.read_parquet(results_metric_dir)
    assert len(all_results) == 4
    assert set(all_results["value"].tolist()) == {10, 20, 30, 40}


def test_concat_batch_results() -> None:
    """Test that _concat_batch_results only combines new batch results."""
    new_results = [
        {"metric1": pd.DataFrame({"a": [1, 2]}), "metric2": pd.DataFrame({"b": [3, 4]})},
        {"metric1": pd.DataFrame({"a": [5, 6]}), "metric2": pd.DataFrame({"b": [7, 8]})},
    ]

    combined = _concat_batch_results(new_results)

    assert set(combined.keys()) == {"metric1", "metric2"}
    assert len(combined["metric1"]) == 4
    assert combined["metric1"]["a"].tolist() == [1, 2, 5, 6]
    assert len(combined["metric2"]) == 4
    assert combined["metric2"]["b"].tolist() == [3, 4, 7, 8]
