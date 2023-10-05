import pytest
import numpy as np
from vivarium_cluster_tools.psimulate.redis_dbs.registry import RegistryManager


@pytest.mark.parametrize("num_queues", [1, 5, 7, 10, 25, 50])
@pytest.mark.parametrize("num_jobs", [1, 10, 1000, 2005, 10000, 20000])
def test_allocate_jobs(mocker, num_queues, num_jobs):
    # mock the QueueManager class
    mocker.patch('vivarium_cluster_tools.psimulate.redis_dbs.registry.QueueManager')
    queues = [(f'queue_{i}', i) for i in range(num_queues)]
    manager = RegistryManager(queues,0)
    # create a list of dummy jobs based on a list of 100 integers
    jobs = [{'job': i} for i in range(num_jobs)]
    # make a numpy array of all the jobs in every queue
    jobs_by_queue = list(manager.allocate_jobs(jobs))
    test_jobs = []
    # Simulate the order that a single worker would pick up jobs round-robin
    while jobs_by_queue:
        for i, queue in enumerate(jobs_by_queue):
            if queue:
                test_jobs.append(queue.pop(0))
            else:
                jobs_by_queue.pop(i)
    assert test_jobs == jobs
    
    
    