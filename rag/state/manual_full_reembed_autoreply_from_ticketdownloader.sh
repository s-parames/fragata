#!/usr/bin/env bash
set -euo pipefail

cd /mnt/netapp1/Store_CESGA/home/cesga/tec_app2/rag

RUN_ID="full_autoreply_reembed_$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="data/incoming/${RUN_ID}"
RAW_DIR="${RUN_DIR}/raw"
PREP_DIR="${RUN_DIR}/prepared"
BACKUP_DIR="data/backups/${RUN_ID}"
SUMMARY_PATH="${RUN_DIR}/manual_autoreply_reembed_summary.json"

mkdir -p "$RUN_DIR" "$RAW_DIR" "$PREP_DIR" "$BACKUP_DIR" data/datasets data/index/faiss_v2

echo "[info] run_id=${RUN_ID}"
echo "[info] run_dir=${RUN_DIR}"

if [[ -f data/datasetFinalV2.jsonl ]]; then
  cp -f data/datasetFinalV2.jsonl "${BACKUP_DIR}/datasetFinalV2.before.jsonl"
fi
if [[ -f data/index/faiss_v2/index.faiss ]]; then
  cp -f data/index/faiss_v2/index.faiss "${BACKUP_DIR}/index.faiss.before"
fi
if [[ -f data/index/faiss_v2/index.pkl ]]; then
  cp -f data/index/faiss_v2/index.pkl "${BACKUP_DIR}/index.pkl.before"
fi
if [[ -f state/last_success_ts.txt ]]; then
  cp -f state/last_success_ts.txt "${BACKUP_DIR}/last_success_ts.before.txt"
fi

echo "[step] Downloading resolved tickets from RT using scriptsDescargaRAG/ticketDownloader.py ..."
(
  cd "$RAW_DIR"
  ../../../../RAG/bin/python ../../../../scriptsDescargaRAG/ticketDownloader.py
)

if [[ -f "${RAW_DIR}/ChatsAplicacionsDates_subject_resolved_comments.jsonl" ]]; then
  mv \
    "${RAW_DIR}/ChatsAplicacionsDates_subject_resolved_comments.jsonl" \
    "${RAW_DIR}/ChatsAplicacionesDates_subject_resolved_comments.jsonl"
fi

echo "[step] Reset datasets and FAISS artifacts for full rebuild ..."
: > data/datasetFinalV2.jsonl
: > data/datasets/dataset_aplicaciones.jsonl
: > data/datasets/dataset_sistemas.jsonl
: > data/datasets/dataset_bigdata.jsonl
: > data/datasets/dataset_general.jsonl
: > data/datasets/dataset_comunicaciones.jsonl
rm -f data/index/faiss_v2/index.faiss data/index/faiss_v2/index.pkl

echo "[step] Preparing onboarding input ..."
./RAG/bin/python scripts/08_prepare_onboard_input.py \
  --input-dir "$RAW_DIR" \
  --out-dir "$PREP_DIR" \
  --summary-out "${RUN_DIR}/prepare_summary.json"

echo "[step] Routing and onboarding by department (autoreply_only cleaning) ..."
bash scripts/09_route_and_onboard.sh \
  --input-dir "$PREP_DIR" \
  --config-preprocess config/preprocess_autoreply_only.yaml \
  --config-rag config/rag.yaml \
  --work-id "$RUN_ID" \
  --route-summary-out "${RUN_DIR}/route_summary.json" \
  --skip-rebuild-index

echo "[step] Full FAISS rebuild ..."
./RAG/bin/python RAG_v1.py --config config/rag.yaml --rebuild-index

echo "[step] Rebuild source catalog ..."
./RAG/bin/python scripts/14_rebuild_source_catalog.py --config config/rag.yaml --json > "${RUN_DIR}/source_catalog_summary.json"

echo "[step] Compute watermark from downloaded raw files ..."
MAX_WATERMARK_UTC="$(
  ./RAG/bin/python - <<'PY' "$RAW_DIR"
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

raw_dir = Path(sys.argv[1])
best = None
for path in sorted(raw_dir.glob("*.jsonl")):
    with path.open("r", encoding="utf-8") as src:
        for line in src:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            raw = str(row.get("lastUpdated") or "").strip()
            if not raw:
                continue
            try:
                dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if best is None or dt > best:
                best = dt
if best is None:
    print("")
else:
    print(best.replace(microsecond=0).isoformat().replace("+00:00", "Z"))
PY
)"
if [[ -z "$MAX_WATERMARK_UTC" ]]; then
  MAX_WATERMARK_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
fi
echo "$MAX_WATERMARK_UTC" > state/last_success_ts.txt

./RAG/bin/python - <<'PY' "$RUN_ID" "$RUN_DIR" "$MAX_WATERMARK_UTC" "$SUMMARY_PATH"
import hashlib
import json
import sys
from pathlib import Path

run_id, run_dir, watermark_utc, summary_path = sys.argv[1:5]
root = Path('.')

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

dataset = root / 'data' / 'datasetFinalV2.jsonl'
faiss = root / 'data' / 'index' / 'faiss_v2' / 'index.faiss'
pkl = root / 'data' / 'index' / 'faiss_v2' / 'index.pkl'
watermark = root / 'state' / 'last_success_ts.txt'

payload = {
    'run_id': run_id,
    'run_dir': run_dir,
    'cleaning_mode': 'autoreply_only',
    'watermark_utc': watermark_utc,
    'artifacts': {
        'dataset_path': str(dataset),
        'dataset_size_bytes': dataset.stat().st_size if dataset.exists() else None,
        'dataset_sha256': sha256(dataset) if dataset.exists() else None,
        'index_faiss_path': str(faiss),
        'index_faiss_size_bytes': faiss.stat().st_size if faiss.exists() else None,
        'index_faiss_sha256': sha256(faiss) if faiss.exists() else None,
        'index_pkl_path': str(pkl),
        'index_pkl_size_bytes': pkl.stat().st_size if pkl.exists() else None,
        'index_pkl_sha256': sha256(pkl) if pkl.exists() else None,
        'watermark_path': str(watermark),
        'watermark_value': watermark.read_text(encoding='utf-8').strip() if watermark.exists() else None,
    },
}

summary_file = Path(summary_path)
summary_file.parent.mkdir(parents=True, exist_ok=True)
summary_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print(summary_file)
PY

echo "[done] Full autoreply-only re-embed completed. Summary: ${SUMMARY_PATH}"
