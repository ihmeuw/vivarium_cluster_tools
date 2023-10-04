import pytest
import numpy
from vivarium_cluster_tools.psimulate.redis_dbs.registry import RegistryManager


@pytest.mark.parametrize("num_queues", [1, 10, 100, 1000], "num_jobs", [1, 10, 100, 1000])
def test_allocate_jobs(mocker, num_queues, num_jobs):
    num_queues = 10
    # mock the QueueManager class
    mocker.patch('registry.QueueManager')
    manager = RegistryManager(num_queues,0)
    # create a list of dummy jobs based on a list of 100 integers
    jobs = [{'job': i} for i in range(num_jobs)]
    # make a 2x2 numpy array of all the jobs in every queue
    queued_jobs = [manager.allocate_jobs(jobs) for _ in range(num_queues)]
    # Make sure that jobs get allocated across queues in a round-robin fashion
    assert numpy.array(queued_jobs).T.flatten() == range(num_jobs)
    
    