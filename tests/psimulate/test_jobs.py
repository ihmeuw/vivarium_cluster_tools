from copy import deepcopy

from vivarium_cluster_tools.psimulate.jobs import JobParameters


def test_branch_config_immutable():
    original_branch_config = {"foo": "bar", "input_data": {"spam": "eggs"}}
    params = JobParameters(
        model_specification="model_spec.yaml",
        branch_configuration=deepcopy(original_branch_config),
        input_draw=0,
        random_seed=1,
        results_path="results",
        backup_configuration={},
        extras={},
    )
    params.sim_config
    assert params.branch_configuration == original_branch_config
