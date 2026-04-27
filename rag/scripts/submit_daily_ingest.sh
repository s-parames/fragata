#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

MODE="${DAILY_INGEST_SUBMIT_MODE:-slurm}"
FORWARD_ARGS=()

usage() {
  cat <<'EOF'
Usage:
  bash scripts/submit_daily_ingest.sh [--mode slurm|compute] [main_daily_ingest options]

Modes:
  slurm    submit sbatch job (default)
  compute  run main_daily_ingest.sh requesting resources with `compute`
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --slurm)
      MODE="slurm"
      shift
      ;;
    --compute)
      MODE="compute"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      FORWARD_ARGS+=("$1")
      shift
      ;;
  esac
done

mkdir -p logs/slurm logs/daily_ingest state data/incoming

MODE="$(echo "$MODE" | tr '[:upper:]' '[:lower:]')"
case "$MODE" in
  slurm)
    exec sbatch --chdir "$REPO_ROOT" "${SCRIPT_DIR}/slurm_daily_ingest.sbatch" "${FORWARD_ARGS[@]}"
    ;;
  compute)
    exec bash scripts/main_daily_ingest.sh --request-supercompute "${FORWARD_ARGS[@]}"
    ;;
  *)
    echo "Invalid mode: $MODE (allowed: slurm, compute)" >&2
    exit 1
    ;;
esac
