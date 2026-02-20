"""Tests for the generate_task_id function and JobParameters.task_id property."""

from vivarium_cluster_tools.psimulate.jobs import JobParameters, generate_task_id


class TestGenerateTaskId:
    def test_deterministic(self) -> None:
        """Same inputs always produce the same task ID."""
        branch = {"scenario": {"treatment": "A"}}
        id1 = generate_task_id(1, 42, branch)
        id2 = generate_task_id(1, 42, branch)
        assert id1 == id2

    def test_different_draw(self) -> None:
        """Different input draws produce different IDs."""
        branch = {"scenario": {"treatment": "A"}}
        id1 = generate_task_id(1, 42, branch)
        id2 = generate_task_id(2, 42, branch)
        assert id1 != id2

    def test_different_seed(self) -> None:
        """Different random seeds produce different IDs."""
        branch = {"scenario": {"treatment": "A"}}
        id1 = generate_task_id(1, 42, branch)
        id2 = generate_task_id(1, 43, branch)
        assert id1 != id2

    def test_different_branch(self) -> None:
        """Different branch configurations produce different IDs."""
        id1 = generate_task_id(1, 42, {"scenario": {"treatment": "A"}})
        id2 = generate_task_id(1, 42, {"scenario": {"treatment": "B"}})
        assert id1 != id2

    def test_length(self) -> None:
        """Task ID is 32 hex characters."""
        task_id = generate_task_id(0, 0, {})
        assert len(task_id) == 32
        assert all(c in "0123456789abcdef" for c in task_id)

    def test_key_order_insensitive(self) -> None:
        """Dict key order should not affect the hash (JSON sort_keys=True)."""
        branch1 = {"b": 2, "a": 1}
        branch2 = {"a": 1, "b": 2}
        assert generate_task_id(1, 42, branch1) == generate_task_id(1, 42, branch2)


class TestJobParametersTaskId:
    def test_task_id_property(self) -> None:
        """JobParameters.task_id matches generate_task_id for the same params."""
        jp = JobParameters(
            model_specification="test.yaml",
            branch_configuration={"scenario": "A"},
            input_draw=5,
            random_seed=10,
            results_path="/tmp/results",
            backup_configuration={},
            extras={},
        )
        expected = generate_task_id(5, 10, {"scenario": "A"})
        assert jp.task_id == expected
