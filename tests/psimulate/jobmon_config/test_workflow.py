"""Tests for the Jobmon workflow builder utilities."""

from vivarium_cluster_tools.psimulate.jobmon_config.workflow import _runtime_to_seconds


class TestRuntimeToSeconds:
    def test_hms(self) -> None:
        assert _runtime_to_seconds("01:30:00") == 5400

    def test_ms(self) -> None:
        assert _runtime_to_seconds("05:00") == 300

    def test_seconds_only(self) -> None:
        assert _runtime_to_seconds("120") == 120

    def test_zero(self) -> None:
        assert _runtime_to_seconds("00:00:00") == 0
