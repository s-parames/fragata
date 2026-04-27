#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

DAYS="${DAYS:-30}"
RUN_TIME="${RUN_TIME:-11:10:00}"
START="${START:-tomorrow}"
WEEKDAY="${WEEKDAY:-tuesday}"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/schedule_daily_ingest_slurm.sh [--days <n>] [--time HH:MM[:SS]] [--weekday <0-6|name>] [--start <date_expr>]

Options:
  --days <n>         Number of weekly runs to schedule (default: 30)
  --time <hh:mm:ss>  Server-local launch time (default: 11:10:00)
  --weekday <value>  Target weekday (0-6, sun..sat; default: tuesday)
  --start <expr>     GNU date expression lower bound for first week (default: tomorrow)
                     Example: "today", "tomorrow", "2026-03-24"
  -h, --help         Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --days)
      DAYS="${2:-}"
      shift 2
      ;;
    --time)
      RUN_TIME="${2:-}"
      shift 2
      ;;
    --weekday)
      WEEKDAY="${2:-}"
      shift 2
      ;;
    --start)
      START="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if ! [[ "$DAYS" =~ ^[0-9]+$ ]] || [[ "$DAYS" -lt 1 ]]; then
  echo "--days must be a positive integer" >&2
  exit 1
fi

if ! command -v sbatch >/dev/null 2>&1; then
  echo "sbatch not found in PATH" >&2
  exit 1
fi

if [[ ! -f "state/daily_ingest.env" ]]; then
  echo "Missing state/daily_ingest.env. Create it first with DAILY_DB_* variables." >&2
  exit 1
fi

START_DAY="$(date -d "$START" +%Y-%m-%d 2>/dev/null || true)"
if [[ -z "$START_DAY" ]]; then
  echo "Invalid --start expression: $START" >&2
  exit 1
fi

normalize_weekday() {
  local raw="${1,,}"
  case "$raw" in
    0|sun|sunday) echo 0 ;;
    1|mon|monday) echo 1 ;;
    2|tue|tuesday) echo 2 ;;
    3|wed|wednesday) echo 3 ;;
    4|thu|thursday) echo 4 ;;
    5|fri|friday) echo 5 ;;
    6|sat|saturday) echo 6 ;;
    *) return 1 ;;
  esac
}

TARGET_WEEKDAY="$(normalize_weekday "$WEEKDAY" || true)"
if [[ -z "$TARGET_WEEKDAY" ]]; then
  echo "--weekday must be 0-6 or sun..sat" >&2
  exit 1
fi

start_weekday="$(date -d "$START_DAY" +%w)"
delta_days=$(( (TARGET_WEEKDAY - start_weekday + 7) % 7 ))
FIRST_RUN_DAY="$(date -d "$START_DAY +${delta_days} day" +%Y-%m-%d)"

echo "Scheduling $DAYS weekly run(s) from $FIRST_RUN_DAY on weekday=${TARGET_WEEKDAY} at $RUN_TIME (server local time)..."

for ((i = 0; i < DAYS; i++)); do
  run_day="$(date -d "$FIRST_RUN_DAY +$((i * 7)) day" +%Y-%m-%d)"
  begin_ts="${run_day}T${RUN_TIME}"
  job_id="$(sbatch --parsable --begin="$begin_ts" scripts/slurm_daily_ingest.sbatch)"
  echo "Scheduled job_id=${job_id} begin=${begin_ts}"
done

echo "Done."
