import pytest

from time import time
from vivarium.framework.utilities import collapse_nested_dict
from vivarium_cluster_tools.psimulate.jobs import JobParameters
from vivarium_cluster_tools.psimulate.worker.vivarium_work_horse import (
    setup_sim,
    parameter_update_format,
)


def test_setup_sim(mocker):
    mocker.patch(
        "vivarium_cluster_tools.psimulate.worker.vivarium_work_horse.get_current_job",
        return_value=mocker.Mock(id="test_string"),
    )
    mocker.patch(
        "vivarium_cluster_tools.psimulate.worker.vivarium_work_horse.time",
        return_value=1337,
    )
    job_parameters = JobParameters(
        model_specification=None,
        branch_configuration={
            "input_data": {"artifact_path": "~/vivarium.yaml"},
            "time": {"end": {"year": 2020}},
        },
        input_draw=1,
        random_seed=2,
        results_path="~/tmp",
        extras={},
    )

    initial_job_params = job_parameters.branch_configuration.copy()
    update_dict = parameter_update_format(job_parameters)
    sim_config = setup_sim(job_parameters).configuration.to_dict()
    job_config = job_parameters.branch_configuration

    def compare_dicts(test_dict, ref_dict):
        for k, v in ref_dict.items():
            if isinstance(v, dict):
                compare_dicts(test_dict[k], v)
            else:
                assert test_dict[k] == v

    for test_dict in [sim_config, job_config]:
        for ref_dict in [initial_job_params, update_dict]:
            compare_dicts(test_dict, ref_dict)
