import dill
import pandas as pd
import pytest

from vivarium_cluster_tools.psimulate.jobs import JobParameters
from vivarium_cluster_tools.psimulate.worker.vivarium_work_horse import (
    get_backup,
    remove_backups,
    parameter_update_format,
    setup_sim,
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
        backup_configuration={},
        extras={},
    )

    initial_job_params = job_parameters.branch_configuration.copy()
    update_dict = parameter_update_format(job_parameters)
    sim_config = setup_sim(job_parameters).configuration.to_dict()
    job_config = job_parameters.branch_configuration

    ## Check that for all nested key, value pairs in ref_dict,
    ## the test dict has that key with the same value.
    def compare_dicts(test_dict, ref_dict):
        for k, v in ref_dict.items():
            if isinstance(v, dict):
                compare_dicts(test_dict[k], v)
            else:
                assert test_dict[k] == v

    for test_dict in [sim_config, job_config]:
        for ref_dict in [initial_job_params, update_dict]:
            compare_dicts(test_dict, ref_dict)


@pytest.mark.parametrize(
    "make_dir,has_metadata_file,has_backup",
    [(False, False, False), (True, False, False), (True, True, False), (True, True, True)],
)
def test_get_and_remove_backup(tmp_path, make_dir, has_metadata_file, has_backup):
    input_draw = 1
    random_seed = 2
    branch_configuration = {"branch_key": "branch_value"}
    job_id = "job_id"
    job_parameters = JobParameters(
        model_specification=None,
        branch_configuration=branch_configuration,
        input_draw=input_draw,
        random_seed=random_seed,
        results_path="~/tmp",
        backup_configuration={
            "backup_dir": tmp_path / "backups",
            "backup_metadata_path": tmp_path / "backups" / "backup_metadata.csv",
        },
        extras={},
    )
    if make_dir:
        (tmp_path / "backups").mkdir(exist_ok=False)
        if has_metadata_file:
            metadata_draw = input_draw if has_backup else 7
            metadata = pd.DataFrame(
                {
                    "input_draw": [metadata_draw],
                    "random_seed": [random_seed],
                    "job_id": job_id,
                    "branch_key": ["branch_value"],
                }
            )
            metadata.to_csv(tmp_path / "backups" / "backup_metadata.csv", index=False)

    if make_dir and has_metadata_file and has_backup:
        pickle_path = tmp_path / "backups" / f"{job_id}.pkl"
        pickle = [1, 2, 3, 4, 5]
        with open(pickle_path, "wb") as f:
            dill.dump(pickle, f)
        backup = get_backup(job_parameters)
        assert backup == pickle
        # Check we can remove the backup
        remove_backups(job_parameters)
        assert not pickle_path.exists()
        
    else:
        backup = get_backup(job_parameters)
        assert not backup
