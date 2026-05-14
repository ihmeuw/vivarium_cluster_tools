#!/bin/bash
# Standalone reproducer for tests/psimulate/test_workflow_e2e.py::test_sequential_simulation_steps.
#
# Writes a 2-step simulation workflow (kenya, ethiopia) using the same model spec
# and branches as the failing e2e test, but to a fixed output directory that is
# NOT cleaned up. After the run, inspect worker logs at:
#
#   $OUTPUT_DIR/<location>/<timestamp>/logs/<timestamp>_run/worker_logs/
#
# Run on the cluster from inside an activated conda env that has psimulate installed.
#
# Usage:
#   bash repro_sequential_sim_steps.sh [output_root] [slurm_project]

set -euo pipefail

OUTPUT_ROOT="${1:-/mnt/team/simulation_science/priv/engineering/tests/output/repro_seq_sim_steps}"
SLURM_PROJECT="${2:-proj_simscience}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${REPO_ROOT}/tests/psimulate/data"
MODEL_SPEC="${DATA_DIR}/e2e_model_spec.yaml"
BRANCHES="${DATA_DIR}/e2e_branches.yaml"

if [[ ! -f "${MODEL_SPEC}" ]] || [[ ! -f "${BRANCHES}" ]]; then
    echo "ERROR: Expected test fixtures not found:" >&2
    echo "  ${MODEL_SPEC}" >&2
    echo "  ${BRANCHES}" >&2
    exit 1
fi

# Clean slate so the build_timestamp file doesn't reuse a previous run's dir.
rm -rf "${OUTPUT_ROOT}"
mkdir -p "${OUTPUT_ROOT}"
SCRIPT_DIR="${OUTPUT_ROOT}/scripts"
mkdir -p "${SCRIPT_DIR}"

# Per-location model spec files (just copies of the shared spec, named by location)
# so each step writes to its own <location>/<timestamp>/ subdirectory.
for loc in kenya ethiopia; do
    cp "${MODEL_SPEC}" "${SCRIPT_DIR}/${loc}.yaml"
done

CONFIG_PATH="${OUTPUT_ROOT}/workflow_config.yaml"
cat > "${CONFIG_PATH}" <<EOF
workflow:
  name: repro_seq_sim_steps
  project: ${SLURM_PROJECT}
  queue: all.q
  output_directory: ${OUTPUT_ROOT}
  steps:
    - name: kenya
      type: simulation
      resources:
        memory_gb: 1
        runtime: "00:05:00"
      args:
        model_specification: ${SCRIPT_DIR}/kenya.yaml
        branch_configuration: ${BRANCHES}
    - name: ethiopia
      type: simulation
      resources:
        memory_gb: 1
        runtime: "00:05:00"
      args:
        model_specification: ${SCRIPT_DIR}/ethiopia.yaml
        branch_configuration: ${BRANCHES}
EOF

echo "=========================================="
echo "Output dir : ${OUTPUT_ROOT}"
echo "Config     : ${CONFIG_PATH}"
echo "Project    : ${SLURM_PROJECT}"
echo "=========================================="
echo

set +e
psimulate workflow -c "${CONFIG_PATH}" -P "${SLURM_PROJECT}" -o "${OUTPUT_ROOT}"
RC=$?
set -e

echo
echo "=========================================="
echo "psimulate exit code: ${RC}"
echo
echo "Worker logs (if any) — inspect for failures:"
for loc in kenya ethiopia; do
    LOG_GLOB="${OUTPUT_ROOT}/${loc}/*/logs/*/worker_logs"
    for d in ${LOG_GLOB}; do
        if [[ -d "${d}" ]]; then
            echo "  ${d}"
            ls -la "${d}" | head -20
            echo
        fi
    done
done
echo "=========================================="
exit ${RC}
