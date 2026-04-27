#!/usr/bin/env bash
set -euo pipefail

PY="./RAG/bin/python"
CONFIG_PREPROCESS="config/preprocess.yaml"
CONFIG_RAG="config/rag.yaml"
INPUT=""
DEPARTMENT=""
WORK_ID=""
REPORT_ROOT="data/reports/onboard"
DEPARTMENT_DATASET_ROOT="data/datasets"
SKIP_REBUILD_INDEX=0

usage() {
  cat <<'EOF'
Usage:
  bash scripts/onboard_db.sh --input <path.jsonl> --department <aplicaciones|sistemas|bigdata> [options]

Options:
  --config-preprocess <path>  Preprocess config (default: config/preprocess.yaml)
  --config-rag <path>         RAG config used to resolve dataset_path and rebuild FAISS
  --work-id <id>              Custom execution id for report folder
  --report-root <path>        Root folder for onboarding reports (default: data/reports/onboard)
  --department-dataset-root <path>
                              Root folder for department datasets (default: data/datasets)
  --skip-rebuild-index        Skip FAISS rebuild step
  -h, --help                  Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)
      INPUT="${2:-}"
      shift 2
      ;;
    --department)
      DEPARTMENT="${2:-}"
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

if [[ -z "$INPUT" || -z "$DEPARTMENT" ]]; then
  usage >&2
  exit 1
fi

if [[ ! -f "$INPUT" ]]; then
  echo "Input file not found: $INPUT" >&2
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

DEPARTMENT="$(echo "$DEPARTMENT" | tr '[:upper:]' '[:lower:]')"
case "$DEPARTMENT" in
  aplicacion|aplicaciones|aplicaiones)
    DEPARTMENT="aplicaciones"
    ;;
  sistema|sistemas)
    DEPARTMENT="sistemas"
    ;;
  bigdata|big_data|big-data|"big data"|bd)
    DEPARTMENT="bigdata"
    ;;
  *)
    echo "Invalid department: $DEPARTMENT. Use aplicaciones, sistemas or bigdata." >&2
    exit 1
    ;;
esac

if [[ -z "$WORK_ID" ]]; then
  WORK_ID="$(date +%Y%m%d_%H%M%S)_${DEPARTMENT}"
fi

GLOBAL_DATASET="$($PY -c 'import sys,yaml; cfg=yaml.safe_load(open(sys.argv[1], "r", encoding="utf-8")) or {}; print(cfg.get("retrieval", {}).get("dataset_path", "data/datasetFinal.jsonl"))' "$CONFIG_RAG")"
DEPT_DATASET="${DEPARTMENT_DATASET_ROOT}/dataset_${DEPARTMENT}.jsonl"
REPORT_DIR="${REPORT_ROOT}/${WORK_ID}"
MERGE_ARTIFACT_DIR="${REPORT_DIR}/merge"
DEPT_MERGE_DELTA="${MERGE_ARTIFACT_DIR}/department_delta.jsonl"
DEPT_MERGE_SUMMARY="${MERGE_ARTIFACT_DIR}/department_summary.json"
GLOBAL_MERGE_DELTA="${MERGE_ARTIFACT_DIR}/global_delta.jsonl"
GLOBAL_MERGE_SUMMARY="${MERGE_ARTIFACT_DIR}/global_summary.json"
MERGE_SUMMARY="${REPORT_DIR}/merge_summary.json"

TMP_DIR="$(mktemp -d /tmp/rag_onboard.${DEPARTMENT}.XXXXXX)"
RAW_JSON="${REPORT_DIR}/raw_profile_${DEPARTMENT}.json"
RAW_MD="${REPORT_DIR}/raw_profile_${DEPARTMENT}.md"
CHUNK_JSON="${REPORT_DIR}/chunk_quality_${DEPARTMENT}.json"
CHUNK_MD="${REPORT_DIR}/chunk_quality_${DEPARTMENT}.md"
CLEANED_TMP="${TMP_DIR}/conversations_clean.jsonl"
CHUNKED_TMP="${TMP_DIR}/output_chunked.jsonl"
READY_TMP="${TMP_DIR}/dataset_ready_${DEPARTMENT}.jsonl"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

mkdir -p "$REPORT_DIR" "$MERGE_ARTIFACT_DIR" "$DEPARTMENT_DATASET_ROOT" "$(dirname "$GLOBAL_DATASET")"

echo "[1/8] Validating input contract..."
$PY scripts/00_validate_input_contract.py --input "$INPUT"

echo "[2/8] Profiling raw dataset..."
$PY scripts/01_profile_raw.py \
  --input "$INPUT" \
  --out-json "$RAW_JSON" \
  --out-md "$RAW_MD"

echo "[3/8] Cleaning and normalizing..."
$PY scripts/02_clean_anonymize.py \
  --config "$CONFIG_PREPROCESS" \
  --input "$INPUT" \
  --out "$CLEANED_TMP" \
  --department "$DEPARTMENT"

echo "[4/8] Chunking conversations..."
$PY scripts/03_chunk_conversations.py \
  --config "$CONFIG_PREPROCESS" \
  --input "$CLEANED_TMP" \
  --out "$CHUNKED_TMP"

echo "[5/8] Validating chunk quality..."
$PY scripts/04_validate_chunks.py \
  --config "$CONFIG_PREPROCESS" \
  --input "$CHUNKED_TMP" \
  --out-json "$CHUNK_JSON" \
  --out-md "$CHUNK_MD"

echo "[6/8] Building ready dataset..."
$PY scripts/05_build_ready_dataset.py \
  --input "$CHUNKED_TMP" \
  --out "$READY_TMP"

echo "[7/8] Merging datasets..."
$PY scripts/06_merge_datasets.py \
  --base "$DEPT_DATASET" \
  --new "$READY_TMP" \
  --out "$DEPT_DATASET" \
  --out-delta "$DEPT_MERGE_DELTA" \
  --summary-out "$DEPT_MERGE_SUMMARY"
$PY scripts/06_merge_datasets.py \
  --base "$GLOBAL_DATASET" \
  --new "$READY_TMP" \
  --out "$GLOBAL_DATASET" \
  --out-delta "$GLOBAL_MERGE_DELTA" \
  --summary-out "$GLOBAL_MERGE_SUMMARY"

# Ensure delta artifacts always exist for deterministic no-op behavior.
for delta_file in "$DEPT_MERGE_DELTA" "$GLOBAL_MERGE_DELTA"; do
  if [[ ! -f "$delta_file" ]]; then
    : > "$delta_file"
  fi
done

$PY - "$DEPT_MERGE_SUMMARY" "$GLOBAL_MERGE_SUMMARY" "$MERGE_SUMMARY" \
  "$WORK_ID" "$DEPARTMENT" "$INPUT" "$DEPT_DATASET" "$GLOBAL_DATASET" \
  "$DEPT_MERGE_DELTA" "$GLOBAL_MERGE_DELTA" <<'PY'
import json
import sys
from pathlib import Path


dept_summary_path = Path(sys.argv[1])
global_summary_path = Path(sys.argv[2])
merge_summary_path = Path(sys.argv[3])
work_id = sys.argv[4]
department = sys.argv[5]
input_path = sys.argv[6]
department_dataset = sys.argv[7]
global_dataset = sys.argv[8]
department_delta = sys.argv[9]
global_delta = sys.argv[10]

with dept_summary_path.open("r", encoding="utf-8") as src:
    dept_summary = json.load(src)
with global_summary_path.open("r", encoding="utf-8") as src:
    global_summary = json.load(src)

payload = {
    "work_id": work_id,
    "department": department,
    "input_path": input_path,
    "department_dataset_path": department_dataset,
    "global_dataset_path": global_dataset,
    "department_merge": {
        **dept_summary,
        "summary_path": str(dept_summary_path),
        "delta_path": department_delta,
    },
    "global_merge": {
        **global_summary,
        "summary_path": str(global_summary_path),
        "delta_path": global_delta,
    },
    "delta_rows_effective": int(global_summary.get("delta_rows", 0)),
    "no_op": int(global_summary.get("delta_rows", 0)) == 0,
}
merge_summary_path.parent.mkdir(parents=True, exist_ok=True)
with merge_summary_path.open("w", encoding="utf-8") as dst:
    json.dump(payload, dst, ensure_ascii=False, indent=2)
    dst.write("\n")
print(str(merge_summary_path))
PY

if [[ "$SKIP_REBUILD_INDEX" -eq 1 ]]; then
  echo "[8/8] Skipping FAISS rebuild (--skip-rebuild-index)."
else
  echo "[8/8] Rebuilding FAISS index..."
  $PY RAG_v1.py --config "$CONFIG_RAG" --rebuild-index --query "health check"
fi

echo ""
echo "Onboarding completed."
echo "Department:       $DEPARTMENT"
echo "Input:            $INPUT"
echo "Department data:  $DEPT_DATASET"
echo "Global data:      $GLOBAL_DATASET"
echo "Reports:          $REPORT_DIR"
echo "Merge summary:    $MERGE_SUMMARY"
echo "Dept delta:       $DEPT_MERGE_DELTA"
echo "Global delta:     $GLOBAL_MERGE_DELTA"
