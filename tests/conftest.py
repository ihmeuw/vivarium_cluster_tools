import pytest
from _pytest.config import Config, argparsing
from _pytest.python import Function


def pytest_addoption(parser: argparsing.Parser) -> None:
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")
    parser.addoption(
        "--runcluster",
        action="store_true",
        default=False,
        help="run tests that require a SLURM cluster",
    )
    parser.addoption(
        "--slurm-project",
        type=str,
        default="proj_simscience",
        help="SLURM project for cluster tests (default: proj_simscience)",
    )


def pytest_configure(config: Config) -> None:
    config.addinivalue_line("markers", "slow: mark test as slow to run")
    config.addinivalue_line(
        "markers", "cluster: mark test as requiring a SLURM cluster environment"
    )


def pytest_collection_modifyitems(config: Config, items: list[Function]) -> None:
    if not config.getoption("--runslow"):
        skip_slow = pytest.mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    if not config.getoption("--runcluster"):
        skip_cluster = pytest.mark.skip(reason="need --runcluster option to run")
        for item in items:
            if "cluster" in item.keywords:
                item.add_marker(skip_cluster)
