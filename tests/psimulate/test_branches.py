from typing import Any

import pytest
from vivarium.framework.utilities import collapse_nested_dict

from vivarium_cluster_tools.psimulate.branches import (
    Keyspace,
    calculate_input_draws,
    calculate_random_seeds,
    expand_branch_templates,
)


def test_expand_branch_template() -> None:
    source: list[dict[str, Any]] = [
        {"a": {"b": [1, 2], "c": 3, "d": [4, 5, 6], "e": [True, False]}},
        {"a": {"b": 10, "c": 30, "d": 40, "e": True}},
    ]
    result = expand_branch_templates(source)

    result_collapsed = [collapse_nested_dict(r) for r in result]

    expected = [
        collapse_nested_dict(r)
        for r in [
            {"a": {"b": 1, "c": 3, "d": 4, "e": True}},
            {"a": {"b": 2, "c": 3, "d": 5, "e": True}},
            {"a": {"b": 1, "c": 3, "d": 6, "e": True}},
            {"a": {"b": 2, "c": 3, "d": 4, "e": True}},
            {"a": {"b": 1, "c": 3, "d": 5, "e": True}},
            {"a": {"b": 2, "c": 3, "d": 6, "e": True}},
            {"a": {"b": 10, "c": 30, "d": 40, "e": True}},
            {"a": {"b": 1, "c": 3, "d": 4, "e": False}},
            {"a": {"b": 2, "c": 3, "d": 5, "e": False}},
            {"a": {"b": 1, "c": 3, "d": 6, "e": False}},
            {"a": {"b": 2, "c": 3, "d": 4, "e": False}},
            {"a": {"b": 1, "c": 3, "d": 5, "e": False}},
            {"a": {"b": 2, "c": 3, "d": 6, "e": False}},
        ]
    ]
    assert sorted(result_collapsed) == sorted(expected)


@pytest.mark.parametrize("seed_count", [0, 10, 100, 1000])
def test_calculate_random_seeds(seed_count: int) -> None:
    seeds = calculate_random_seeds(seed_count)
    assert len(seeds) == len(set(seeds))
    assert len(seeds) == seed_count


@pytest.mark.parametrize("seed_count", [0, 10, 100, 1000])
def test_calculate_random_seeds_existing(seed_count: int) -> None:
    existing = list(range(20))

    seeds = calculate_random_seeds(seed_count, existing)
    assert len(seeds) == len(set(seeds))
    assert len(seeds + existing) == len(set(seeds + existing))
    assert len(seeds) == seed_count


def test_keyspace_order() -> None:
    ## divide an integer range of 15 into five bins
    input_draws, random_seeds, foos = [list(range(i, i + 3)) for i in range(0, 9, 3)]
    branches = [{"foo": foo} for foo in foos]
    # Define the correct ordering of the keyspace:
    ordered_keys = [
        (input_draw, random_seed, branch)
        # Iterate over branches, then seeds, then draws
        for input_draw in input_draws
        for random_seed in random_seeds
        for branch in branches
    ]

    full_keyspace = Keyspace(
        branches=branches, keyspace={"input_draw": input_draws, "random_seed": random_seeds}
    )

    for test_key, reference_key in zip(full_keyspace, ordered_keys):
        assert test_key == reference_key


@pytest.mark.parametrize("draw_count", [1, 5, 50])
def test_calculate_input_draws_excludes_existing(draw_count: int) -> None:
    """New draws never overlap with existing draws."""
    existing = calculate_input_draws(draw_count)
    additional = calculate_input_draws(draw_count, existing_draws=existing)
    assert len(additional) == draw_count
    assert len(additional) == len(set(additional))
    assert set(additional).isdisjoint(set(existing))


def test_add_draws_produces_distinct_values() -> None:
    """Keyspace.add_draws appends draws that don't overlap with the originals."""
    original_draws = calculate_input_draws(5)
    ks = Keyspace(
        branches=[{}],
        keyspace={"input_draw": list(original_draws), "random_seed": [0]},
    )
    ks.add_draws(3)
    all_draws = ks._keyspace["input_draw"]
    assert len(all_draws) == 8
    assert len(set(all_draws)) == 8  # all unique
    # Original draws are preserved at the front
    assert all_draws[:5] == original_draws


def test_add_seeds_produces_distinct_values() -> None:
    """Keyspace.add_seeds appends seeds that don't overlap with the originals."""
    original_seeds = calculate_random_seeds(10)
    ks = Keyspace(
        branches=[{}],
        keyspace={"input_draw": [0], "random_seed": list(original_seeds)},
    )
    ks.add_seeds(5)
    all_seeds = ks._keyspace["random_seed"]
    assert len(all_seeds) == 15
    assert len(set(all_seeds)) == 15  # all unique
    # Original seeds are preserved at the front
    assert all_seeds[:10] == original_seeds
