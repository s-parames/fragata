#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

PY="${PYTHON_BIN:-./RAG/bin/python}"
CONFIG_DAILY="config/daily_ingest.yaml"
ENV_FILE="state/daily_ingest.env"
RUN_ONCE=0

usage() {
  cat <<'EOF'
Usage:
  bash scripts/run_interval_scheduler.sh [options]

Options:
  --config-daily <path>   Daily ingest config (default: config/daily_ingest.yaml)
  --env-file <path>       Env file with DAILY_DB_* vars (default: state/daily_ingest.env)
  --once                  Run exactly one cycle and exit
  -h, --help              Show this help

Config keys read from schedule block in config-daily:
  schedule.mode                 (interval | daily_at | weekly_at)
  schedule.interval_minutes
  schedule.daily_time_local     (HH:MM or HH:MM:SS)
  schedule.weekly_day_of_week   (0-6 or sun..sat, used by weekly_at)
  schedule.run_on_startup
  schedule.failure_retry_minutes
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config-daily)
      CONFIG_DAILY="${2:-}"
      shift 2
      ;;
    --env-file)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --once)
      RUN_ONCE=1
      shift
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

if [[ ! -x "$PY" ]]; then
  echo "Python interpreter not found or not executable: $PY" >&2
  exit 1
fi
if [[ ! -f "$CONFIG_DAILY" ]]; then
  echo "Config file not found: $CONFIG_DAILY" >&2
  exit 1
fi
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Env file not found: $ENV_FILE" >&2
  exit 1
fi

read_schedule() {
  "$PY" - "$CONFIG_DAILY" <<'PY'
import sys
import yaml

cfg_path = sys.argv[1]
with open(cfg_path, "r", encoding="utf-8") as fh:
    cfg = yaml.safe_load(fh) or {}
s = cfg.get("schedule") or {}

mode = str(s.get("mode", "interval")).strip().lower()
interval_minutes = int(s.get("interval_minutes", 1440))
daily_time_local = str(s.get("daily_time_local", "02:00:00")).strip()
weekly_day_raw = s.get("weekly_day_of_week", "sunday")
failure_retry_minutes = int(s.get("failure_retry_minutes", 30))
run_on_startup = bool(s.get("run_on_startup", True))

if mode not in {"interval", "daily_at", "weekly_at"}:
    raise SystemExit("schedule.mode must be 'interval', 'daily_at', or 'weekly_at'")
if interval_minutes < 1:
    raise SystemExit("schedule.interval_minutes must be >= 1")
if failure_retry_minutes < 1:
    raise SystemExit("schedule.failure_retry_minutes must be >= 1")
parts = daily_time_local.split(":")
if len(parts) not in (2, 3):
    raise SystemExit("schedule.daily_time_local must be HH:MM or HH:MM:SS")
try:
    hh = int(parts[0]); mm = int(parts[1]); ss = int(parts[2]) if len(parts) == 3 else 0
except ValueError as exc:
    raise SystemExit("schedule.daily_time_local contains non-numeric values") from exc
if not (0 <= hh <= 23 and 0 <= mm <= 59 and 0 <= ss <= 59):
    raise SystemExit("schedule.daily_time_local out of range")
if len(parts) == 2:
    daily_time_local = f"{hh:02d}:{mm:02d}:00"
else:
    daily_time_local = f"{hh:02d}:{mm:02d}:{ss:02d}"

if isinstance(weekly_day_raw, int):
    weekly_day_of_week = weekly_day_raw
else:
    weekly_text = str(weekly_day_raw).strip().lower()
    aliases = {
        "0": 0, "sun": 0, "sunday": 0,
        "1": 1, "mon": 1, "monday": 1,
        "2": 2, "tue": 2, "tuesday": 2,
        "3": 3, "wed": 3, "wednesday": 3,
        "4": 4, "thu": 4, "thursday": 4,
        "5": 5, "fri": 5, "friday": 5,
        "6": 6, "sat": 6, "saturday": 6,
    }
    if weekly_text not in aliases:
        raise SystemExit("schedule.weekly_day_of_week must be 0-6 or sun..sat")
    weekly_day_of_week = aliases[weekly_text]

if not (0 <= int(weekly_day_of_week) <= 6):
    raise SystemExit("schedule.weekly_day_of_week must be in range 0..6")

print(mode)
print(interval_minutes)
print(daily_time_local)
print(failure_retry_minutes)
print("1" if run_on_startup else "0")
print(int(weekly_day_of_week))
PY
}

load_env() {
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
}

seconds_until_next_local_daily_time() {
  local target_time="$1"
  local now_epoch
  local today
  local target_epoch
  now_epoch="$(date +%s)"
  today="$(date +%Y-%m-%d)"
  target_epoch="$(date -d "${today} ${target_time}" +%s)"
  if (( target_epoch <= now_epoch )); then
    target_epoch="$(date -d "tomorrow ${target_time}" +%s)"
  fi
  echo $((target_epoch - now_epoch))
}

seconds_until_next_local_weekly_time() {
  local target_weekday="$1"
  local target_time="$2"
  local now_epoch
  local today
  local current_weekday
  local delta_days
  local target_epoch

  now_epoch="$(date +%s)"
  today="$(date +%Y-%m-%d)"
  current_weekday="$(date +%w)"
  delta_days=$(( (target_weekday - current_weekday + 7) % 7 ))

  target_epoch="$(date -d "${today} +${delta_days} day ${target_time}" +%s)"
  if (( target_epoch <= now_epoch )); then
    target_epoch="$(date -d "${today} +$((delta_days + 7)) day ${target_time}" +%s)"
  fi
  echo $((target_epoch - now_epoch))
}

mkdir -p logs/daily_ingest

cycle=0
while true; do
  mapfile -t SCHEDULE_VALUES < <(read_schedule)
  mode="${SCHEDULE_VALUES[0]}"
  interval_minutes="${SCHEDULE_VALUES[1]}"
  daily_time_local="${SCHEDULE_VALUES[2]}"
  failure_retry_minutes="${SCHEDULE_VALUES[3]}"
  run_on_startup="${SCHEDULE_VALUES[4]}"
  weekly_day_of_week="${SCHEDULE_VALUES[5]}"

  if [[ "$cycle" -eq 0 && "$run_on_startup" -eq 0 ]]; then
    if [[ "$mode" == "daily_at" ]]; then
      sleep_sec="$(seconds_until_next_local_daily_time "$daily_time_local")"
      echo "[scheduler] Startup wait until next daily slot ${daily_time_local} (sleep=${sleep_sec}s)."
    elif [[ "$mode" == "weekly_at" ]]; then
      sleep_sec="$(seconds_until_next_local_weekly_time "$weekly_day_of_week" "$daily_time_local")"
      echo "[scheduler] Startup wait until next weekly slot weekday=${weekly_day_of_week} at ${daily_time_local} (sleep=${sleep_sec}s)."
    else
      sleep_sec=$((interval_minutes * 60))
      echo "[scheduler] Startup wait: ${interval_minutes}m before first run."
    fi
    sleep "$sleep_sec"
  fi

  start_ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if [[ "$mode" == "daily_at" ]]; then
    echo "[scheduler] Cycle ${cycle} started at ${start_ts} (daily_at=${daily_time_local})"
  elif [[ "$mode" == "weekly_at" ]]; then
    echo "[scheduler] Cycle ${cycle} started at ${start_ts} (weekly_at=weekday:${weekly_day_of_week} time:${daily_time_local})"
  else
    echo "[scheduler] Cycle ${cycle} started at ${start_ts} (interval=${interval_minutes}m)"
  fi

  set +e
  load_env
  bash scripts/main_daily_ingest.sh --config-daily "$CONFIG_DAILY"
  run_exit=$?
  set -e

  if [[ "$RUN_ONCE" -eq 1 ]]; then
    exit "$run_exit"
  fi

  if [[ "$run_exit" -eq 0 ]]; then
    if [[ "$mode" == "daily_at" ]]; then
      sleep_sec="$(seconds_until_next_local_daily_time "$daily_time_local")"
      echo "[scheduler] Cycle ${cycle} succeeded. Next run at ${daily_time_local} local (sleep=${sleep_sec}s)."
    elif [[ "$mode" == "weekly_at" ]]; then
      sleep_sec="$(seconds_until_next_local_weekly_time "$weekly_day_of_week" "$daily_time_local")"
      echo "[scheduler] Cycle ${cycle} succeeded. Next weekly run weekday=${weekly_day_of_week} at ${daily_time_local} local (sleep=${sleep_sec}s)."
    else
      sleep_sec=$((interval_minutes * 60))
      echo "[scheduler] Cycle ${cycle} succeeded. Sleeping ${interval_minutes}m."
    fi
  else
    sleep_sec=$((failure_retry_minutes * 60))
    echo "[scheduler] Cycle ${cycle} failed (exit=${run_exit}). Sleeping ${failure_retry_minutes}m."
  fi

  cycle=$((cycle + 1))
  sleep "$sleep_sec"
done
