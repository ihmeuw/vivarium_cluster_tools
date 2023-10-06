import pytest

from vivarium_cluster_tools.psimulate.redis_dbs.registry import RegistryManager


@pytest.mark.parametrize("num_queues", [1, 7, 10, 19, 37])
@pytest.mark.parametrize("num_jobs", [1, 10, 42, 1337, 2023])
def test_allocate_jobs(mocker, num_queues, num_jobs):
    # mock the QueueManager class
    mocker.patch("vivarium_cluster_tools.psimulate.redis_dbs.registry.QueueManager")
    queues = [(f"queue_{i}", i) for i in range(num_queues)]
    manager = RegistryManager(queues, 0)
    # create a list of dummy jobs based on a list of 100 integers
    jobs = [{"job": i} for i in range(num_jobs)]
    # make a numpy array of all the jobs in every queue
    jobs_by_queue = list(manager.allocate_jobs(jobs))
    test_jobs = []
    # Test that queues are monotonically increasing
    for queue in jobs_by_queue:
        for i in range(len(queue) - 1):
            assert queue[i]["job"] < queue[i + 1]["job"]
