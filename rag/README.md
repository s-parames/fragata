# rag — backend, retrieval engine, and ingestion pipeline

Part of [FRAGATA](../README.md). This component runs on two machines:

- **FT3 supercomputer** — weekly ticket extraction, embedding, FAISS index build (GPU).
- **VM** — FastAPI search API, async ingestion jobs, live engine hot-swap.

## Requirements

- Python 3.11+
- A local virtualenv at `RAG/` (created during setup, gitignored)
- GPU access for embedding and FAISS build (FT3 side only)

## Setup

```bash
# 1. Create virtualenv
python3 -m venv RAG
./RAG/bin/pip install -r requirements.txt

# 2. Fill in credentials
cp state/daily_ingest.env.example state/daily_ingest.env
$EDITOR state/daily_ingest.env   # DB host/user/password, FT3 SSH key, workdir

# 3. (Optional) set bootstrap date so a fresh deploy does not pull all tickets since 2000
#    Edit config/daily_ingest.yaml → extract.bootstrap_watermark_utc
```

The Python interpreter for all commands below is `./RAG/bin/python`.

## Running the API server

```bash
./RAG/bin/python -m uvicorn app:app --host 0.0.0.0 --port 8010
```

Health check:

```bash
curl -s http://127.0.0.1:8010/health
```

For production, a systemd unit is provided at `deploy/cloud_srv_cesga/systemd/`.

## Weekly ticket ingestion

The weekly pipeline extracts newly resolved tickets from the RT MySQL database, embeds them on FT3, and syncs the updated FAISS index back to the VM.

**Start the scheduler** (runs every Sunday at 02:00, keeps itself alive):

```bash
bash scripts/run_interval_scheduler.sh
```

**Manual one-shot run:**

```bash
bash scripts/main_daily_ingest.sh
```

**What it does:**

1. Reads watermark from `state/last_success_ts.txt` (falls back to `bootstrap_watermark_utc` if missing — downloads everything from that date).
2. Extracts resolved tickets from the RT database since the last watermark.
3. Cleans, chunks, and routes rows by department.
4. Offloads FAISS build to FT3 via `scripts/compute_via_ft3.sh`.
5. Syncs updated index back to the VM and triggers a live engine reload.
6. Advances the watermark only on full success.

## Configuration files

| File | Purpose |
|---|---|
| `config/rag.yaml` | Retrieval models, FAISS paths, fusion weights |
| `config/daily_ingest.yaml` | DB env-var mapping, SQL template, schedule, bootstrap watermark |
| `config/preprocess.yaml` | Chunking and text-cleaning settings |
| `state/daily_ingest.env` | **Gitignored.** Real credentials and SSH paths. Copy from `daily_ingest.env.example`. |
| `deploy/cloud_srv_cesga/env/rag-backend.env.example` | Template for VM runtime environment variables |

## Key portability notes

If you change the FT3 username or the project path, update these files:

1. `state/daily_ingest.env` — `DAILY_INGEST_REMOTE_USER`, `DAILY_INGEST_REMOTE_WORKDIR`, `DAILY_INGEST_REMOTE_SSH_KEY`.
2. `config/preprocess*.yaml` — `input_path` still references an absolute path for the raw ticket JSONL. Pass `--input` on the CLI instead when running one-off pipelines.
3. `scripts/run_pipeline.sh` — the `INPUT` variable is hardcoded; override it with a CLI argument for portability.

See `bitacora_vm_frontend_rag_prod_02_2026_03_27.md` (section 9.4) for a full checklist.

## Running tests

```bash
./RAG/bin/python -m unittest -v
```

## Onboarding a new ticket database

```bash
bash scripts/onboard_aplicaciones.sh --input /path/to/new_db.jsonl
# or onboard_sistemas.sh / onboard_bigdata.sh depending on department
```

This validates, cleans, chunks, merges into the global dataset, and rebuilds the FAISS index.

## Building the static frontend

If you need to rebuild the frontend for a different backend URL:

```bash
scripts/build_frontend_bundle.sh \
  --api-base-url http://<VM_IP>:8010 \
  --output-dir ../rag-frontend-build
```

## Deployment (VM side)

Sync from FT3:

```bash
rsync -aP --delete -e "ssh -i ~/.ssh/<KEY>" ./ <vm_user>@<vm_ip>:/opt/rag/
```

Nginx config, systemd unit, and environment template are in `deploy/cloud_srv_cesga/`.
