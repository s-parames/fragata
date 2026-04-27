#!/usr/bin/env bash
set -euo pipefail

PY="./RAG/bin/python"
INPUT="/mnt/netapp1/Store_CESGA/home/cesga/tec_app2/data/ChatsAplicacions.jsonl"
CONFIG="config/preprocess.yaml"
FINAL_OUTPUT="data/dataset_final.jsonl"
TMP_DIR="$(mktemp -d /tmp/rag_pipeline.XXXXXX)"
CLEANED_TMP="$TMP_DIR/conversations_clean.jsonl"
CHUNKED_TMP="$TMP_DIR/output_chunked_v2.jsonl"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

$PY scripts/01_profile_raw.py \
  --input "$INPUT" \
  --out-json data/reports/raw_profile.json \
  --out-md data/reports/raw_profile.md

$PY scripts/02_clean_anonymize.py \
  --config "$CONFIG" \
  --input "$INPUT" \
  --out "$CLEANED_TMP"

$PY scripts/03_chunk_conversations.py \
  --config "$CONFIG" \
  --input "$CLEANED_TMP" \
  --out "$CHUNKED_TMP"

$PY scripts/04_validate_chunks.py \
  --config "$CONFIG" \
  --input "$CHUNKED_TMP" \
  --out-json data/reports/chunk_quality.json \
  --out-md data/reports/chunk_quality.md

$PY scripts/05_build_ready_dataset.py \
  --input "$CHUNKED_TMP" \
  --out "$FINAL_OUTPUT"

echo "Pipeline finished. Output: $FINAL_OUTPUT"
