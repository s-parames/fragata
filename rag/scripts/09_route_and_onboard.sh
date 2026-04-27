#!/usr/bin/env bash
set -euo pipefail

INPUT_DIR=""
CONFIG_PREPROCESS="config/preprocess.yaml"
CONFIG_RAG="config/rag.yaml"
WORK_ID=""
REPORT_ROOT="data/reports/onboard"
DEPARTMENT_DATASET_ROOT="data/datasets"
ROUTE_SUMMARY_OUT=""
SKIP_REBUILD_INDEX=0

usage() {
  cat <<'EOF'
Usage:
  bash scripts/09_route_and_onboard.sh --input-dir <prepared_jsonl_dir> [options]

Options:
  --config-preprocess <path>  Preprocess config (default: config/preprocess.yaml)
  --config-rag <path>         RAG config (default: config/rag.yaml)
  --work-id <id>              Work id prefix for reports
  --report-root <path>        Root folder for onboarding reports (default: data/reports/onboard)
  --department-dataset-root <path>
                              Root folder for department datasets (default: data/datasets)
  --route-summary-out <path>  Optional route summary JSON output path
  --skip-rebuild-index        Forward --skip-rebuild-index to onboard wrapper
  -h, --help                  Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input-dir)
      INPUT_DIR="${2:-}"
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
    --work-id)
      WORK_ID="${2:-}"
      shift 2
      ;;
    --report-root)
      REPORT_ROOT="${2:-}"
      shift 2
      ;;
    --department-dataset-root)
      DEPARTMENT_DATASET_ROOT="${2:-}"
      shift 2
      ;;
    --route-summary-out)
      ROUTE_SUMMARY_OUT="${2:-}"
      shift 2
      ;;
    --skip-rebuild-index)
      SKIP_REBUILD_INDEX=1
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

if [[ -z "$INPUT_DIR" ]]; then
  usage >&2
  exit 1
fi

if [[ ! -d "$INPUT_DIR" ]]; then
  echo "Input directory not found: $INPUT_DIR" >&2
  exit 1
fi

if [[ ! -f "$CONFIG_PREPROCESS" ]]; then
  echo "Config file not found: $CONFIG_PREPROCESS" >&2
  exit 1
fi
if [[ ! -f "$CONFIG_RAG" ]]; then
  echo "Config file not found: $CONFIG_RAG" >&2
  exit 1
fi

if [[ -z "$WORK_ID" ]]; then
  WORK_ID="$(date -u +%Y%m%d_%H%M%SZ)_daily_route"
fi
if [[ -z "$ROUTE_SUMMARY_OUT" ]]; then
  ROUTE_SUMMARY_OUT="${REPORT_ROOT}/${WORK_ID}_route_summary.json"
fi

mkdir -p "$REPORT_ROOT"
ROUTE_ENTRIES_TMP="$(mktemp /tmp/route_onboard_entries.XXXXXX.jsonl)"
cleanup_route_tmp() {
  rm -f "$ROUTE_ENTRIES_TMP"
}
trap cleanup_route_tmp EXIT

shopt -s nullglob
FILES=("$INPUT_DIR"/*.jsonl)
if [[ ${#FILES[@]} -eq 0 ]]; then
  echo "No prepared JSONL files found under $INPUT_DIR. Nothing to onboard."
  exit 0
fi

processed=0
skipped_empty=0

for input_file in "${FILES[@]}"; do
  if [[ ! -s "$input_file" ]]; then
    echo "Skipping empty file: $input_file"
    skipped_empty=$((skipped_empty + 1))
    continue
  fi

  file_name="$(basename "$input_file" | tr '[:upper:]' '[:lower:]')"
  wrapper=""
  department=""

  if [[ "$file_name" == *"aplicaciones"* ]]; then
    wrapper="scripts/onboard_aplicaciones.sh"
    department="aplicaciones"
  elif [[ "$file_name" == *"sistemas"* ]]; then
    wrapper="scripts/onboard_sistemas.sh"
    department="sistemas"
  elif [[ "$file_name" == *"bigdata"* || "$file_name" == *"big_data"* || "$file_name" == *"big-data"* ]]; then
    wrapper="scripts/onboard_bigdata.sh"
    department="bigdata"
  elif [[ "$file_name" == *"general"* ]]; then
    wrapper="scripts/onboard_general.sh"
    department="general"
  elif [[ "$file_name" == *"comunicaciones"* || "$file_name" == *"comunicacion"* ]]; then
    wrapper="scripts/onboard_comunicaciones.sh"
    department="comunicaciones"
  else
    echo "Cannot infer department from filename: $file_name" >&2
    echo "Expected one of tokens: aplicaciones, sistemas, bigdata, general, comunicaciones" >&2
    exit 1
  fi

  work_id_file="${WORK_ID}_${department}_$((processed + 1))"
  cmd=(
    bash "$wrapper"
    --input "$input_file"
    --config-preprocess "$CONFIG_PREPROCESS"
    --config-rag "$CONFIG_RAG"
    --work-id "$work_id_file"
    --report-root "$REPORT_ROOT"
    --department-dataset-root "$DEPARTMENT_DATASET_ROOT"
  )
  if [[ "$SKIP_REBUILD_INDEX" -eq 1 ]]; then
    cmd+=(--skip-rebuild-index)
  fi

  echo "Onboarding file: $input_file (department=$department, work_id=$work_id_file)"
  "${cmd[@]}"

  merge_summary_path="${REPORT_ROOT}/${work_id_file}/merge_summary.json"
  if [[ ! -f "$merge_summary_path" ]]; then
    echo "Missing merge summary for work_id=$work_id_file at $merge_summary_path" >&2
    exit 1
  fi

  ./RAG/bin/python - "$ROUTE_ENTRIES_TMP" "$input_file" "$department" "$work_id_file" \
    "$REPORT_ROOT/${work_id_file}" "$merge_summary_path" <<'PY'
import json
import sys
from pathlib import Path

entries_path = Path(sys.argv[1])
record = {
    "input_file": sys.argv[2],
    "department": sys.argv[3],
    "work_id": sys.argv[4],
    "report_dir": sys.argv[5],
    "merge_summary_path": sys.argv[6],
}
with entries_path.open("a", encoding="utf-8") as dst:
    dst.write(json.dumps(record, ensure_ascii=False) + "\n")
PY

  processed=$((processed + 1))
done

./RAG/bin/python - "$ROUTE_ENTRIES_TMP" "$ROUTE_SUMMARY_OUT" "$WORK_ID" "$INPUT_DIR" "$processed" "$skipped_empty" <<'PY'
import json
import sys
from pathlib import Path

entries_path = Path(sys.argv[1])
out_path = Path(sys.argv[2])
work_id = sys.argv[3]
input_dir = sys.argv[4]
processed = int(sys.argv[5])
skipped_empty = int(sys.argv[6])

entries = []
for line in entries_path.read_text(encoding="utf-8").splitlines():
    if not line.strip():
        continue
    row = json.loads(line)
    merge_path = Path(row["merge_summary_path"])
    merge_payload = {}
    if merge_path.exists():
        merge_payload = json.loads(merge_path.read_text(encoding="utf-8"))
    row["merge"] = merge_payload
    entries.append(row)

department_delta_total = 0
global_delta_total = 0
new_total = 0
updated_total = 0
unchanged_total = 0
no_op_files = 0
for row in entries:
    merge = row.get("merge") or {}
    dept_merge = merge.get("department_merge") or {}
    global_merge = merge.get("global_merge") or {}
    department_delta_total += int(dept_merge.get("delta_rows", 0) or 0)
    global_delta_total += int(global_merge.get("delta_rows", 0) or 0)
    new_total += int(global_merge.get("new_count", 0) or 0)
    updated_total += int(global_merge.get("updated_count", 0) or 0)
    unchanged_total += int(global_merge.get("unchanged_count", 0) or 0)
    if int(global_merge.get("delta_rows", 0) or 0) == 0:
        no_op_files += 1

summary = {
    "work_id": work_id,
    "input_dir": input_dir,
    "processed_files": processed,
    "skipped_empty_files": skipped_empty,
    "entries": entries,
    "aggregate": {
        "department_delta_rows_total": department_delta_total,
        "global_delta_rows_total": global_delta_total,
        "new_count_total": new_total,
        "updated_count_total": updated_total,
        "unchanged_count_total": unchanged_total,
        "no_op_files": no_op_files,
    },
}

out_path.parent.mkdir(parents=True, exist_ok=True)
with out_path.open("w", encoding="utf-8") as dst:
    json.dump(summary, dst, ensure_ascii=False, indent=2)
    dst.write("\n")
print(str(out_path))
PY

echo "Routing completed. processed=$processed skipped_empty=$skipped_empty input_dir=$INPUT_DIR route_summary=$ROUTE_SUMMARY_OUT"
