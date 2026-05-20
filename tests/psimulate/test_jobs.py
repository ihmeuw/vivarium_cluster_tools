from copy import deepcopy
from pathlib import Path

import pandas as pd
import pytest

from vivarium_cluster_tools.psimulate.branches import Keyspace
from vivarium_cluster_tools.psimulate.jobs import (
    JobParameters,
    build_job_list,
    generate_task_id,
)


def test_branch_config_immutable() -> None:
    "Test that the branch_configuration doesn't get mutated in place."
    original_branch_config = {"foo": "bar", "input_data": {"spam": "eggs"}}
    params = JobParameters(
        model_specification="model_spec.yaml",
        branch_configuration=deepcopy(original_branch_config),
        input_draw=0,
        random_seed=1,
        results_path="results",
        backup_configuration={},
        extras={},
        worker_logging_root="/tmp/worker_logs",
    )
    params.sim_config  # This was previously causing branch_config to change
    assert params.branch_configuration == original_branch_config


class TestGenerateTaskId:
    @property
    def branch_config(self) -> dict[str, dict[str, str]]:
        return {"scenario": {"treatment": "A"}}

    def test_deterministic(self) -> None:
        """Same inputs always produce the same task ID."""
        id1 = generate_task_id(1, 42, self.branch_config)
        id2 = generate_task_id(1, 42, self.branch_config)
        assert id1 == id2

    def test_different_draw(self) -> None:
        """Different input draws produce different IDs."""
        id1 = generate_task_id(1, 42, self.branch_config)
        id2 = generate_task_id(2, 42, self.branch_config)
        assert id1 != id2

    def test_different_seed(self) -> None:
        """Different random seeds produce different IDs."""

        id1 = generate_task_id(1, 42, self.branch_config)
        id2 = generate_task_id(1, 43, self.branch_config)
        assert id1 != id2

    def test_different_branch(self) -> None:
        """Different branch configurations produce different IDs."""
        id1 = generate_task_id(1, 42, {"scenario": {"treatment": "A"}})
        id2 = generate_task_id(1, 42, {"scenario": {"treatment": "B"}})
        assert id1 != id2

    def test_length(self) -> None:
        """Task ID is 16 hex characters."""
        task_id = generate_task_id(0, 0, {})
        assert len(task_id) == 16
        assert all(c in "0123456789abcdef" for c in task_id)

    def test_key_order_insensitive(self) -> None:
        """Dict key order should not affect the hash (JSON sort_keys=True)."""
        branch1 = {"b": 2, "a": 1}
        branch2 = {"a": 1, "b": 2}
        assert generate_task_id(1, 42, branch1) == generate_task_id(1, 42, branch2)


class TestJobParametersTaskId:
    def test_task_id_property(self) -> None:
        """JobParameters.task_id matches generate_task_id for the same params."""
        job_parameters = JobParameters(
            model_specification="test.yaml",
            branch_configuration={"scenario": "A"},
            input_draw=5,
            random_seed=10,
            results_path="/tmp/results",
            backup_configuration={},
            extras={},
            worker_logging_root="/tmp/worker_logs",
        )
        expected = generate_task_id(5, 10, {"scenario": "A"})
        assert job_parameters.task_id == expected


class TestBuildJobList:
    def _make_keyspace(self) -> Keyspace:
        # Two draws x two seeds x one (empty) branch = 4 jobs.
        return Keyspace(
            branches=[{}],
            keyspace={"input_draw": [0, 1], "random_seed": [100, 200]},
        )

    @pytest.mark.parametrize("backup_freq", [None, 300])
    def test_backup_freq_propagates_into_job_parameters(
        self, tmp_path: Path, backup_freq: int | None
    ) -> None:
        """build_job_list forwards backup_freq into each JobParameters.

        Covers the ``backup_freq=None`` case used when running psimulate
        without backups.
        """
        backup_dir = tmp_path / "backups"
        backup_metadata_path = backup_dir / "backup_metadata.csv"
        jobs, num_completed = build_job_list(
            model_specification_path=tmp_path / "model_spec.yaml",
            output_root=tmp_path / "results",
            keyspace=self._make_keyspace(),
            finished_sim_metadata=pd.DataFrame(),
            backup_freq=backup_freq,
            backup_dir=backup_dir,
            backup_metadata_path=backup_metadata_path,
            worker_logging_root=tmp_path / "logs",
            extras={},
        )

        assert num_completed == 0
        assert len(jobs) == 4
        for job in jobs:
            assert job.backup_configuration == {
                "backup_dir": backup_dir,
                "backup_freq": backup_freq,
                "backup_metadata_path": backup_metadata_path,
            }
