#!/usr/bin/env bash
#SBATCH --job-name=daily_rag_ingest
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=02:30:00
#SBATCH --output=logs/slurm/%x-%j.out
#SBATCH --error=logs/slurm/%x-%j.err

set -euo pipefail

if [[ -n "${SLURM_SUBMIT_DIR:-}" && -d "${SLURM_SUBMIT_DIR}/scripts" ]]; then
  REPO_ROOT="${SLURM_SUBMIT_DIR}"
else
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
fi
cd "$REPO_ROOT"

mkdir -p logs/slurm logs/daily_ingest state data/incoming
echo "[slurm_daily_ingest] repo_root=${REPO_ROOT}"
echo "[slurm_daily_ingest] submit_dir=${SLURM_SUBMIT_DIR:-unknown}"

ENV_FILE="${REPO_ROOT}/state/daily_ingest.env"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
else
  echo "Missing credentials file: $ENV_FILE" >&2
  exit 1
fi

bash scripts/main_daily_ingest.sh "$@"
