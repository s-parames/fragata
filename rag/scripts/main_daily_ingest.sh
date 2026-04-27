#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

PY="${PYTHON_BIN:-./RAG/bin/python}"
CONFIG_DAILY="config/daily_ingest.yaml"
CONFIG_PREPROCESS="config/preprocess.yaml"
CONFIG_RAG="config/rag.yaml"
STATE_DIR="state"
INCOMING_ROOT="data/incoming"
ENV_FILE=""
WORK_ID=""
RETRY_MAX=1
RETRY_DELAY_SEC=300
OVERLAP_HOURS=""
ENGINE_RELOAD_ENDPOINT="${RAG_ENGINE_RELOAD_URL:-}"
ENGINE_RELOAD_TIMEOUT_SEC="${RAG_ENGINE_RELOAD_TIMEOUT_SEC:-20}"
ENGINE_RELOAD_RETRIES="${RAG_ENGINE_RELOAD_RETRIES:-3}"
ENGINE_RELOAD_REQUIRED="${RAG_ENGINE_RELOAD_REQUIRED:-0}"
REQUEST_SUPERCOMPUTE="${DAILY_INGEST_REQUEST_SUPERCOMPUTE:-0}"
COMPUTE_BIN="${DAILY_INGEST_COMPUTE_BIN:-compute}"
COMPUTE_CPUS="${DAILY_INGEST_COMPUTE_CPUS:-${RAG_HPC_CORES:-32}}"
COMPUTE_MEM="${DAILY_INGEST_COMPUTE_MEM:-${RAG_HPC_MEM:-32G}}"
COMPUTE_GPU="${DAILY_INGEST_COMPUTE_GPU:-${RAG_HPC_GPU:-1}}"
FAISS_APPEND_ENABLE="${DAILY_INGEST_FAISS_APPEND_ENABLE:-1}"
FAISS_APPEND_FALLBACK_REBUILD="${DAILY_INGEST_FAISS_APPEND_FALLBACK_REBUILD:-0}"
PENDING_WATERMARK=""
COMPLETION_REASON=""
LAST_RELOAD_STATUS=""

usage() {
  cat <<'EOF'
Usage:
  bash scripts/main_daily_ingest.sh [options]

Options:
  --config-daily <path>       Daily ingest config (default: config/daily_ingest.yaml)
  --config-preprocess <path>  Preprocess config (default: config/preprocess.yaml)
  --config-rag <path>         RAG config (default: config/rag.yaml)
  --state-dir <path>          State directory (default: state)
  --incoming-root <path>      Root directory for run artifacts (default: data/incoming)
  --env-file <path>           Optional env file with DAILY_DB_* vars (default: <state-dir>/daily_ingest.env)
  --work-id <id>              Optional fixed run id
  --retry-max <n>             Automatic retries on failure (default: 1)
  --retry-delay-sec <n>       Delay between retries in seconds (default: 300)
  --overlap-hours <n>         Override extraction overlap window (hours)
  --request-supercompute      Request resources with `compute` and run payload remotely
  --no-request-supercompute   Force local execution (disable supercomputer request)
  --compute-bin <path>        compute binary path/name (default: compute)
  --compute-cpus <n>          Cores requested to compute (default: 32 or RAG_HPC_CORES)
  --compute-mem <value>       Memory requested to compute (default: 32G or RAG_HPC_MEM)
  --compute-gpu <0|1>         Request GPU on compute (default: 1 or RAG_HPC_GPU)
  --reload-endpoint <url>     POST endpoint to reload API engine after success
  --reload-timeout-sec <n>    Timeout per reload request in seconds (default: 20)
  --reload-retries <n>        Reload retry attempts (default: 3)
  --reload-required           Mark run as failed if engine reload fails
  --enable-faiss-append       Use incremental FAISS append (default)
  --disable-faiss-append      Disable incremental append and keep full rebuild path
  --faiss-append-fallback-rebuild
                              Allow full rebuild fallback if append fails (default: disabled)
  --no-faiss-append-fallback-rebuild
                              Do not fallback to full rebuild when append fails
  -h, --help                  Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config-daily)
      CONFIG_DAILY="${2:-}"
      shift 2
      ;;
    --config-preprocess)
      CONFIG_PREPROCESS="${2:-}"
      shift 2
      ;;
    --config-rag)
      CONFIG_RAG="${2:-}"
      shift 2
      ;;
    --state-dir)
      STATE_DIR="${2:-}"
      shift 2
      ;;
    --incoming-root)
      INCOMING_ROOT="${2:-}"
      shift 2
      ;;
    --env-file)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --work-id)
      WORK_ID="${2:-}"
      shift 2
      ;;
    --retry-max)
      RETRY_MAX="${2:-}"
      shift 2
      ;;
    --retry-delay-sec)
      RETRY_DELAY_SEC="${2:-}"
      shift 2
      ;;
    --overlap-hours)
      OVERLAP_HOURS="${2:-}"
      shift 2
      ;;
    --request-supercompute)
      REQUEST_SUPERCOMPUTE="1"
      shift
      ;;
    --no-request-supercompute)
      REQUEST_SUPERCOMPUTE="0"
      shift
      ;;
    --compute-bin)
      COMPUTE_BIN="${2:-}"
      shift 2
      ;;
    --compute-cpus)
      COMPUTE_CPUS="${2:-}"
      shift 2
      ;;
    --compute-mem)
      COMPUTE_MEM="${2:-}"
      shift 2
      ;;
    --compute-gpu)
      COMPUTE_GPU="${2:-}"
      shift 2
      ;;
    --reload-endpoint)
      ENGINE_RELOAD_ENDPOINT="${2:-}"
      shift 2
      ;;
    --reload-timeout-sec)
      ENGINE_RELOAD_TIMEOUT_SEC="${2:-}"
      shift 2
      ;;
    --reload-retries)
      ENGINE_RELOAD_RETRIES="${2:-}"
      shift 2
      ;;
    --reload-required)
      ENGINE_RELOAD_REQUIRED="1"
      shift
      ;;
    --enable-faiss-append)
      FAISS_APPEND_ENABLE="1"
      shift
      ;;
    --disable-faiss-append)
      FAISS_APPEND_ENABLE="0"
      shift
      ;;
    --faiss-append-fallback-rebuild)
      FAISS_APPEND_FALLBACK_REBUILD="1"
      shift
      ;;
    --no-faiss-append-fallback-rebuild)
      FAISS_APPEND_FALLBACK_REBUILD="0"
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

normalize_bool() {
  local raw="${1:-}"
  case "${raw,,}" in
    1|true|yes|y|on)
      echo "1"
      ;;
    *)
      echo "0"
      ;;
  esac
}

to_payload_path() {
  local raw="${1:-}"
  if [[ -z "$raw" ]]; then
    echo ""
    return
  fi
  if [[ "$raw" == "$REPO_ROOT" ]]; then
    echo "."
    return
  fi
  local repo_prefix="${REPO_ROOT%/}/"
  if [[ "$raw" == "$repo_prefix"* ]]; then
    echo "${raw#"$repo_prefix"}"
    return
  fi
  echo "$raw"
}

REQUEST_SUPERCOMPUTE="$(normalize_bool "$REQUEST_SUPERCOMPUTE")"
COMPUTE_GPU="$(normalize_bool "$COMPUTE_GPU")"
FAISS_APPEND_ENABLE="$(normalize_bool "$FAISS_APPEND_ENABLE")"
FAISS_APPEND_FALLBACK_REBUILD="$(normalize_bool "$FAISS_APPEND_FALLBACK_REBUILD")"

if [[ ! -x "$PY" ]]; then
  echo "Python interpreter not found or not executable: $PY" >&2
  exit 1
fi
for cfg in "$CONFIG_DAILY" "$CONFIG_PREPROCESS" "$CONFIG_RAG"; do
  if [[ ! -f "$cfg" ]]; then
    echo "Config file not found: $cfg" >&2
    exit 1
  fi
done

if [[ "$REQUEST_SUPERCOMPUTE" == "1" && "${MAIN_DAILY_INGEST_IN_COMPUTE:-0}" != "1" ]]; then
  if ! command -v "$COMPUTE_BIN" >/dev/null 2>&1; then
    echo "compute binary not found: $COMPUTE_BIN" >&2
    exit 1
  fi
  if ! [[ "$COMPUTE_CPUS" =~ ^[0-9]+$ ]] || (( COMPUTE_CPUS < 1 )); then
    echo "Invalid --compute-cpus value: $COMPUTE_CPUS" >&2
    exit 1
  fi

  payload_config_daily="$(to_payload_path "$CONFIG_DAILY")"
  payload_config_preprocess="$(to_payload_path "$CONFIG_PREPROCESS")"
  payload_config_rag="$(to_payload_path "$CONFIG_RAG")"
  payload_state_dir="$(to_payload_path "$STATE_DIR")"
  payload_incoming_root="$(to_payload_path "$INCOMING_ROOT")"
  payload_env_file="$(to_payload_path "$ENV_FILE")"

  payload_cmd=(
    bash
    scripts/main_daily_ingest.sh
    --config-daily "$payload_config_daily"
    --config-preprocess "$payload_config_preprocess"
    --config-rag "$payload_config_rag"
    --state-dir "$payload_state_dir"
    --incoming-root "$payload_incoming_root"
    --retry-max "$RETRY_MAX"
    --retry-delay-sec "$RETRY_DELAY_SEC"
    --reload-timeout-sec "$ENGINE_RELOAD_TIMEOUT_SEC"
    --reload-retries "$ENGINE_RELOAD_RETRIES"
    --no-request-supercompute
  )
  if [[ -n "$payload_env_file" ]]; then
    payload_cmd+=(--env-file "$payload_env_file")
  fi
  if [[ -n "$WORK_ID" ]]; then
    payload_cmd+=(--work-id "$WORK_ID")
  fi
  if [[ -n "$OVERLAP_HOURS" ]]; then
    payload_cmd+=(--overlap-hours "$OVERLAP_HOURS")
  fi
  if [[ -n "$ENGINE_RELOAD_ENDPOINT" ]]; then
    payload_cmd+=(--reload-endpoint "$ENGINE_RELOAD_ENDPOINT")
  fi
  if [[ "$ENGINE_RELOAD_REQUIRED" == "1" ]]; then
    payload_cmd+=(--reload-required)
  fi
  if [[ "$FAISS_APPEND_ENABLE" == "1" ]]; then
    payload_cmd+=(--enable-faiss-append)
  else
    payload_cmd+=(--disable-faiss-append)
  fi
  if [[ "$FAISS_APPEND_FALLBACK_REBUILD" == "1" ]]; then
    payload_cmd+=(--faiss-append-fallback-rebuild)
  else
    payload_cmd+=(--no-faiss-append-fallback-rebuild)
  fi

  payload_cmd_quoted="$(printf '%q ' "${payload_cmd[@]}")"
  payload_cmd_quoted="${payload_cmd_quoted% }"

  compute_cmd=("$COMPUTE_BIN" -c "$COMPUTE_CPUS" --mem "$COMPUTE_MEM")
  if [[ "$COMPUTE_GPU" == "1" ]]; then
    compute_cmd+=(--gpu)
  fi
  compute_cmd+=(-- env MAIN_DAILY_INGEST_IN_COMPUTE=1 bash -lc "$payload_cmd_quoted")

  echo "[supercompute] Requesting resources: cores=${COMPUTE_CPUS} mem=${COMPUTE_MEM} gpu=${COMPUTE_GPU}"
  exec "${compute_cmd[@]}"
fi

mkdir -p "$STATE_DIR" "$INCOMING_ROOT" logs/daily_ingest logs/slurm

WATERMARK_FILE="${STATE_DIR}/last_success_ts.txt"
LOCK_FILE="${STATE_DIR}/main_daily_ingest.lock"
if [[ -z "$ENV_FILE" ]]; then
  ENV_FILE="${STATE_DIR}/daily_ingest.env"
fi
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
else
  echo "[env] Env file not found at $ENV_FILE. Continuing with current process environment."
fi

if [[ -z "$WORK_ID" ]]; then
  WORK_ID="$(date -u +%Y%m%d_%H%M%SZ)_daily_ingest"
fi
RUN_ROOT="${INCOMING_ROOT}/${WORK_ID}"
mkdir -p "$RUN_ROOT"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "Another daily ingestion run is already active (lock: $LOCK_FILE)." >&2
  exit 1
fi

json_get() {
  local json_path="$1"
  local key_path="$2"
  "$PY" - "$json_path" "$key_path" <<'PY'
import json
import sys

path, key_path = sys.argv[1], sys.argv[2]
with open(path, "r", encoding="utf-8") as src:
    data = json.load(src)

value = data
for token in key_path.split("."):
    if isinstance(value, dict):
        value = value.get(token)
    else:
        value = None
        break

if value is None:
    print("")
elif isinstance(value, (dict, list)):
    print(json.dumps(value, ensure_ascii=False))
else:
    print(value)
PY
}

write_watermark() {
  local watermark="$1"
  if [[ -z "$watermark" ]]; then
    echo "Refusing to write empty watermark." >&2
    exit 1
  fi
  local tmp_file="${WATERMARK_FILE}.tmp.$$"
  printf '%s\n' "$watermark" > "$tmp_file"
  mv "$tmp_file" "$WATERMARK_FILE"
}

write_minimal_faiss_append_summary() {
  local summary_path="$1"
  local reason="$2"
  local config_path="$3"
  local faiss_dir="$4"
  local delta_path="$5"
  local delta_input_rows="${6:-0}"
  "$PY" - "$summary_path" "$reason" "$config_path" "$faiss_dir" "$delta_path" "$delta_input_rows" <<'PY'
import json
import sys
from pathlib import Path

summary_path = Path(sys.argv[1])
reason = sys.argv[2]
config_path = sys.argv[3]
faiss_dir = sys.argv[4]
delta_path = sys.argv[5]
delta_input_rows = int(sys.argv[6])

payload = {
    "applied": False,
    "reason": reason,
    "config_path": config_path,
    "faiss_dir": faiss_dir,
    "delta_path": delta_path,
    "delta_input_rows": delta_input_rows,
    "delta_docs_appended": 0,
    "index_count_before": 0,
    "index_count_after": 0,
    "docstore_count_before": 0,
    "docstore_count_after": 0,
    "fallback_used": False,
    "append_error": None,
    "fallback_error": None,
    "rebuilt_doc_count": 0,
    "staging_dir": None,
    "backup_dir": None,
}

summary_path.parent.mkdir(parents=True, exist_ok=True)
with summary_path.open("w", encoding="utf-8") as dst:
    json.dump(payload, dst, ensure_ascii=False, indent=2)
    dst.write("\n")
PY
}

log_completion_checkpoint() {
  local checkpoint="$1"
  local status="$2"
  local attempt_idx="$3"
  local watermark_value="${4:-}"
  local detail="${5:-}"
  local reload_status="${6:-}"
  "$PY" - "$checkpoint" "$status" "$attempt_idx" "$watermark_value" "$detail" "$reload_status" "$WORK_ID" <<'PY'
import json
import sys
from datetime import datetime, timezone

checkpoint = sys.argv[1]
status = sys.argv[2]
attempt = int(sys.argv[3])
watermark = sys.argv[4]
detail = sys.argv[5]
reload_status = sys.argv[6]
work_id = sys.argv[7]

payload = {
    "ts_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    "event": "daily_ingest_completion_checkpoint",
    "work_id": work_id,
    "attempt": attempt,
    "checkpoint": checkpoint,
    "status": status,
}
if watermark:
    payload["watermark_candidate_utc"] = watermark
if detail:
    payload["detail"] = detail
if reload_status:
    payload["reload_status"] = reload_status

print(json.dumps(payload, ensure_ascii=False))
PY
}

reload_engine_after_success() {
  LAST_RELOAD_STATUS=""
  if [[ -z "$ENGINE_RELOAD_ENDPOINT" ]]; then
    LAST_RELOAD_STATUS="skipped_no_endpoint"
    echo "[Reload] Skipped (no endpoint configured)."
    return 0
  fi

  if ! command -v curl >/dev/null 2>&1; then
    echo "[Reload] curl not found; cannot call ${ENGINE_RELOAD_ENDPOINT}." >&2
    if [[ "$ENGINE_RELOAD_REQUIRED" == "1" ]]; then
      LAST_RELOAD_STATUS="failed_required_curl_missing"
      return 1
    fi
    LAST_RELOAD_STATUS="failed_best_effort_curl_missing"
    return 0
  fi

  local attempt=1
  while (( attempt <= ENGINE_RELOAD_RETRIES )); do
    local tmp_body
    tmp_body="$(mktemp)"
    set +e
    local http_code
    http_code="$(curl -sS -m "$ENGINE_RELOAD_TIMEOUT_SEC" -o "$tmp_body" -w "%{http_code}" -X POST "$ENGINE_RELOAD_ENDPOINT")"
    local curl_rc=$?
    set -e
    local body=""
    if [[ -f "$tmp_body" ]]; then
      body="$(cat "$tmp_body" 2>/dev/null || true)"
      rm -f "$tmp_body"
    fi

    if [[ "$curl_rc" -eq 0 && "$http_code" =~ ^2 ]]; then
      LAST_RELOAD_STATUS="success"
      echo "[Reload] Engine reload OK (http=${http_code}) endpoint=${ENGINE_RELOAD_ENDPOINT}"
      if [[ -n "$body" ]]; then
        echo "[Reload] Response: ${body}"
      fi
      return 0
    fi

    echo "[Reload] Attempt ${attempt}/${ENGINE_RELOAD_RETRIES} failed (curl_rc=${curl_rc} http=${http_code}) endpoint=${ENGINE_RELOAD_ENDPOINT}" >&2
    if [[ -n "$body" ]]; then
      echo "[Reload] Response: ${body}" >&2
    fi
    attempt=$((attempt + 1))
    if (( attempt <= ENGINE_RELOAD_RETRIES )); then
      sleep 5
    fi
  done

  if [[ "$ENGINE_RELOAD_REQUIRED" == "1" ]]; then
    LAST_RELOAD_STATUS="failed_required_http_or_curl"
    echo "[Reload] Engine reload failed and is required. Marking run as failed." >&2
    return 1
  fi
  LAST_RELOAD_STATUS="failed_best_effort_http_or_curl"
  echo "[Reload] Engine reload failed, but run remains successful (best effort mode)." >&2
  return 0
}

run_once() {
  local attempt="$1"
  PENDING_WATERMARK=""
  COMPLETION_REASON=""
  local attempt_dir="${RUN_ROOT}/attempt_${attempt}"
  local raw_dir="${attempt_dir}/raw"
  local prepared_dir="${attempt_dir}/prepared"
  local extract_summary="${attempt_dir}/extract_summary.json"
  local prepare_summary="${attempt_dir}/prepare_summary.json"
  mkdir -p "$raw_dir" "$prepared_dir"

  echo "[Attempt ${attempt}] Extracting resolved tickets..."
  extract_cmd=(
    "$PY" scripts/07_extract_resolved_tickets.py
    --config "$CONFIG_DAILY"
    --out-dir "$raw_dir"
    --watermark-file "$WATERMARK_FILE"
    --summary-out "$extract_summary"
  )
  if [[ -n "$OVERLAP_HOURS" ]]; then
    extract_cmd+=(--overlap-hours "$OVERLAP_HOURS")
  fi
  if ! "${extract_cmd[@]}"; then
    echo "[Attempt ${attempt}] Extraction failed." >&2
    return 1
  fi

  local extracted_rows
  extracted_rows="$(json_get "$extract_summary" "total_rows")"
  extracted_rows="${extracted_rows:-0}"
  local next_watermark
  next_watermark="$(json_get "$extract_summary" "next_watermark_utc")"

  if [[ -z "$next_watermark" ]]; then
    echo "Extraction summary missing next_watermark_utc: $extract_summary" >&2
    exit 1
  fi

  if [[ "$extracted_rows" == "0" ]]; then
    echo "[Attempt ${attempt}] No new resolved tickets. Deferring watermark commit until completion checkpoints."
    PENDING_WATERMARK="$next_watermark"
    COMPLETION_REASON="no_new_resolved_rows"
    return 0
  fi

  echo "[Attempt ${attempt}] Preparing onboarding JSONL..."
  if ! "$PY" scripts/08_prepare_onboard_input.py \
    --input-dir "$raw_dir" \
    --out-dir "$prepared_dir" \
    --summary-out "$prepare_summary"; then
    echo "[Attempt ${attempt}] Preparation failed." >&2
    return 1
  fi

  local prepared_rows
  prepared_rows="$(json_get "$prepare_summary" "total_rows_out")"
  prepared_rows="${prepared_rows:-0}"

  if [[ "$prepared_rows" == "0" ]]; then
    echo "[Attempt ${attempt}] No valid rows after preparation. Deferring watermark commit until completion checkpoints."
    PENDING_WATERMARK="$next_watermark"
    COMPLETION_REASON="no_valid_rows_after_prepare"
    return 0
  fi

  local route_summary
  route_summary="${attempt_dir}/route_summary.json"
  echo "[Attempt ${attempt}] Routing files and running onboarding..."
  if ! bash scripts/09_route_and_onboard.sh \
    --input-dir "$prepared_dir" \
    --config-preprocess "$CONFIG_PREPROCESS" \
    --config-rag "$CONFIG_RAG" \
    --work-id "${WORK_ID}_a${attempt}" \
    --route-summary-out "$route_summary" \
    --skip-rebuild-index; then
    echo "[Attempt ${attempt}] Onboarding failed." >&2
    return 1
  fi

  if [[ ! -f "$route_summary" ]]; then
    echo "[Attempt ${attempt}] Missing route summary after onboarding: $route_summary" >&2
    return 1
  fi

  local faiss_dir
  local append_delta
  local append_stdout
  local append_summary
  faiss_dir="$("$PY" -c 'import sys,yaml; cfg=yaml.safe_load(open(sys.argv[1], "r", encoding="utf-8")) or {}; print(cfg.get("retrieval", {}).get("faiss_dir", "data/index/faiss_v2"))' "$CONFIG_RAG")"
  append_delta="${attempt_dir}/global_delta_for_append.jsonl"
  append_stdout="${attempt_dir}/faiss_append_stdout.json"
  append_summary="${attempt_dir}/faiss_append_summary.json"

  if [[ "$FAISS_APPEND_ENABLE" == "1" ]]; then
    local global_delta_rows
    global_delta_rows="$(json_get "$route_summary" "aggregate.global_delta_rows_total")"
    global_delta_rows="${global_delta_rows:-0}"

    if [[ "$global_delta_rows" == "0" ]]; then
      echo "[Attempt ${attempt}] Delta is empty (global_delta_rows_total=0). Skipping FAISS append/rebuild."
      : > "$append_delta"
      write_minimal_faiss_append_summary \
        "$append_summary" \
        "delta_rows_zero_route_summary" \
        "$CONFIG_RAG" \
        "$faiss_dir" \
        "$append_delta" \
        "0"
      echo "[Attempt ${attempt}] FAISS append summary: ${append_summary}"
    else
      if ! "$PY" - "$route_summary" "$append_delta" <<'PY'
import json
import sys
from pathlib import Path

route_summary_path = Path(sys.argv[1])
out_delta_path = Path(sys.argv[2])

payload = json.loads(route_summary_path.read_text(encoding="utf-8"))
entries = payload.get("entries") or []
out_delta_path.parent.mkdir(parents=True, exist_ok=True)
count = 0
with out_delta_path.open("w", encoding="utf-8") as out:
    for entry in entries:
        merge = entry.get("merge") or {}
        global_merge = merge.get("global_merge") or {}
        delta_path_raw = global_merge.get("delta_path")
        if not delta_path_raw:
            continue
        delta_path = Path(str(delta_path_raw))
        if not delta_path.exists():
            continue
        with delta_path.open("r", encoding="utf-8") as src:
            for line in src:
                if not line.strip():
                    continue
                out.write(line)
                count += 1
print(count)
PY
      then
        echo "[Attempt ${attempt}] Failed to build run-level global delta file for FAISS append." >&2
        return 1
      fi

      append_cmd=(
        "$PY" scripts/10_incremental_faiss_append.py
        --config "$CONFIG_RAG"
        --faiss-dir "$faiss_dir"
        --delta "$append_delta"
        --summary-out "$append_summary"
      )
      if [[ "$FAISS_APPEND_FALLBACK_REBUILD" == "0" ]]; then
        append_cmd+=(--disable-fallback-rebuild)
      fi

      echo "[Attempt ${attempt}] Running incremental FAISS append (delta_rows=${global_delta_rows})..."
      if ! "${append_cmd[@]}" >"$append_stdout"; then
        echo "[Attempt ${attempt}] FAISS append failed. fallback_rebuild=${FAISS_APPEND_FALLBACK_REBUILD}" >&2
        if [[ -f "$append_stdout" ]]; then
          cat "$append_stdout" >&2 || true
        fi
        return 1
      fi

      local append_reason
      local append_fallback_used
      local append_applied
      append_reason="$(json_get "$append_summary" "reason")"
      append_fallback_used="$(json_get "$append_summary" "fallback_used")"
      append_applied="$(json_get "$append_summary" "applied")"

      echo "[Attempt ${attempt}] FAISS append summary: ${append_summary}"
      echo "[Attempt ${attempt}] FAISS append result: applied=${append_applied} reason=${append_reason} fallback_used=${append_fallback_used}"

      if [[ "$append_fallback_used" == "True" || "$append_fallback_used" == "true" ]]; then
        echo "[Attempt ${attempt}] Append failed and fallback full rebuild was used (policy enabled)."
      fi
    fi
  else
    echo "[Attempt ${attempt}] Incremental append disabled. Rebuilding FAISS index..."
    if ! "$PY" RAG_v1.py --config "$CONFIG_RAG" --rebuild-index --query "health check"; then
      echo "[Attempt ${attempt}] FAISS rebuild failed." >&2
      return 1
    fi
    : > "$append_delta"
    write_minimal_faiss_append_summary \
      "$append_summary" \
      "append_disabled_full_rebuild_policy" \
      "$CONFIG_RAG" \
      "$faiss_dir" \
      "$append_delta" \
      "0"
    echo "[Attempt ${attempt}] FAISS append summary: ${append_summary}"
  fi

  PENDING_WATERMARK="$next_watermark"
  COMPLETION_REASON="index_update_completed"
  echo "[Attempt ${attempt}] Data/index update completed. Deferring watermark commit until reload checkpoint."
}

attempt=0
while true; do
  set +e
  run_once "$attempt"
  exit_code=$?
  set -e

  if [[ "$exit_code" -eq 0 ]]; then
    if [[ -z "$PENDING_WATERMARK" ]]; then
      echo "Completion flow invariant violated: pending watermark is empty after successful run." >&2
      exit 1
    fi
    log_completion_checkpoint \
      "data_index_update" \
      "ok" \
      "$attempt" \
      "$PENDING_WATERMARK" \
      "$COMPLETION_REASON" \
      ""

    if ! reload_engine_after_success; then
      log_completion_checkpoint \
        "engine_reload" \
        "error" \
        "$attempt" \
        "$PENDING_WATERMARK" \
        "reload_failed_required_path" \
        "$LAST_RELOAD_STATUS"
      log_completion_checkpoint \
        "watermark_commit" \
        "skipped" \
        "$attempt" \
        "$PENDING_WATERMARK" \
        "reload_failed_watermark_not_committed" \
        "$LAST_RELOAD_STATUS"
      echo "Daily ingestion completed but engine reload failed in required mode." >&2
      exit 1
    fi
    log_completion_checkpoint \
      "engine_reload" \
      "ok" \
      "$attempt" \
      "$PENDING_WATERMARK" \
      "reload_completed_or_skipped" \
      "$LAST_RELOAD_STATUS"
    log_completion_checkpoint \
      "watermark_commit" \
      "start" \
      "$attempt" \
      "$PENDING_WATERMARK" \
      "writing_last_success_ts" \
      "$LAST_RELOAD_STATUS"
    write_watermark "$PENDING_WATERMARK"
    log_completion_checkpoint \
      "watermark_commit" \
      "ok" \
      "$attempt" \
      "$PENDING_WATERMARK" \
      "last_success_ts_written" \
      "$LAST_RELOAD_STATUS"
    echo "Daily ingestion completed (work_id=$WORK_ID, attempts=$((attempt + 1)))."
    break
  fi

  if (( attempt >= RETRY_MAX )); then
    echo "Daily ingestion failed after $((attempt + 1)) attempt(s)." >&2
    exit "$exit_code"
  fi

  attempt=$((attempt + 1))
  echo "Run failed. Retrying in ${RETRY_DELAY_SEC}s (retry ${attempt}/${RETRY_MAX})..." >&2
  sleep "$RETRY_DELAY_SEC"
done
