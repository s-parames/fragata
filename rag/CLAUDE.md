# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment

The Python interpreter and all dependencies live in a local virtualenv:

```bash
./RAG/bin/python   # interpreter
./RAG/bin/pip      # package manager
```

Always use `./RAG/bin/python` instead of the system `python3`.

## Common Commands

**Start the API server:**
```bash
./RAG/bin/python -m uvicorn app:app --host 0.0.0.0 --port 8000
```

**Run all tests:**
```bash
./RAG/bin/python -m unittest -v
```

**Run a single test file:**
```bash
./RAG/bin/python -m unittest tests.test_ingest_api -v
```

**Run a single test case:**
```bash
./RAG/bin/python -m unittest tests.test_ingest_api.TestIngestApi.test_web_enqueue -v
```

**Syntax check key modules:**
```bash
./RAG/bin/python -m py_compile app.py RAG_v1.py ingest/runner.py ingest/contracts.py
```

**Rebuild FAISS index from CLI:**
```bash
./RAG/bin/python RAG_v1.py --config config/rag.yaml --rebuild-index
```

**Query retrieval from CLI:**
```bash
./RAG/bin/python RAG_v1.py --config config/rag.yaml --query "your query"
```

**Rebuild source catalog manually:**
```bash
./RAG/bin/python scripts/14_rebuild_source_catalog.py --config config/rag.yaml
```

**Build static frontend bundle (split deployment):**
```bash
scripts/build_frontend_bundle.sh --api-base-url http://<BACKEND_HOST>:8010 --output-dir /tmp/rag-frontend
```

## Architecture

### Retrieval Engine (`RAG_v1.py`)

The core retrieval engine uses **hybrid retrieval + cross-encoder reranking**:
1. **Semantic** — FAISS + multilingual sentence-transformers (`paraphrase-multilingual-MiniLM-L12-v2`)
2. **Lexical** — BM25 over the same corpus
3. **Fusion** — Reciprocal Rank Fusion (RRF) to merge the two ranked lists
4. **Reranking** — cross-encoder (`mmarco-mMiniLMv2-L12-H384-v1`) refines the top-N fused results

All retrieval parameters (model paths, `semantic_k`, `lexical_k`, `rerank_top_n`, `final_k`, RRF weights) are in `config/rag.yaml`. The active dataset is `retrieval.dataset_path`; the FAISS index lives at `retrieval.faiss_dir`.

`build_engine(config)` is the main entry point that loads or builds the index. `RAGEngine` is the stateful object held in memory at runtime.

### API Server (`app.py`)

FastAPI app. Key responsibilities:
- Serves the web UI from `templates/` + `static/`
- `POST /search` — calls the retrieval engine
- `POST /ingest/{web,pdf,repo-docs,rt-weekly}` — creates async ingestion jobs
- `POST /admin/purge-department` — creates async purge jobs
- `GET /ingest/jobs/{job_id}` — polls job status
- `GET /catalog/sources` — source inventory
- `POST /admin/engine/reload` — hot-swap the in-memory engine without restart

CORS for split deployments is configured via `RAG_CORS_ALLOW_ORIGINS` (see `deploy/cloud_srv_cesga/env/rag-backend.env.example`).

### Ingestion Package (`ingest/`)

Each module has a single responsibility:

| Module | Role |
|---|---|
| `contracts.py` | Pydantic request/response models, `IngestSourceType`, `IngestState` |
| `storage.py` | `IngestionJobStore` — reads/writes per-job `manifest.json` under `data/reports/ingest_jobs/` |
| `orchestrator.py` | `IngestionOrchestrator` — enqueues jobs into the store |
| `runner.py` | `IngestionJobRunner` — executes jobs asynchronously in a `ThreadPoolExecutor`; coordinates all pipeline stages |
| `engine_manager.py` | `EngineManager` — thread-safe holder for the live `RAGEngine`; handles generation tracking and hot-swap |
| `faiss_incremental.py` | Delta append to FAISS without full rebuild; falls back to full rebuild on failure |
| `web_pipeline.py` | Website mirror → extract → prepare → chunk stages |
| `pdf_pipeline.py` | PDF upload → extract → prepare → chunk stages |
| `repo_docs_pipeline.py` | GitHub/GitLab README/wiki → prepare → chunk stages |
| `hpc_executor.py` | `HpcExecutor` — SSH-based offload of index stages to FT3 supercomputer (Slurm) |
| `source_catalog.py` | Builds/queries logical source inventory from the dataset |
| `index_backup_retention.py` | Prunes old FAISS backup snapshots per `index_backups.keep_last` |
| `web_job_cleanup.py` | Deletes raw web job artifacts after successful merge |
| `security.py` | URL validation for public HTTP ingestion inputs |

**Ingestion stage flow (online web/PDF path):**
`dispatch → acquire_source → extract → prepare → chunk → merge → index_append → reload`

When HPC offload is active for a source type, `index_append` expands to:
`resource_requested → waiting_resources → running_remote → sync_back`

**Purge flow:**
`dispatch → purge_dataset → [full_rebuild → reload]` (dry-run skips rebuild and reload)

### Data Pipeline Scripts (`scripts/`)

Numbered `00`–`14`, designed to run sequentially for offline onboarding of new ticket databases:

- `00` validates input JSONL contract
- `02` cleans/anonymizes text (PII preserved by choice)
- `03` chunks conversations
- `05`–`06` build and merge ready datasets
- `07_*` extract from HTML, PDF, or RT ticket sources
- `09` routes chunks to the correct department onboarding wrapper
- `10` incremental FAISS append
- `11` purges a department dataset
- `12`–`13` prune index backups and web job artifacts
- `14` rebuilds the source catalog

`scripts/main_daily_ingest.sh` is the production orchestrator for the weekly RT ticket ingestion cycle (watermark, lock, retry). It runs as a persistent scheduler via `scripts/run_interval_scheduler.sh` (Sunday 02:00, configured in `config/daily_ingest.yaml`).

### Department Normalization

Department values are normalized consistently everywhere: lowercase ASCII, spaces→`_`, must match `[a-z0-9][a-z0-9_-]{2,31}`. The canonical function is in `scripts/common_department.py` (imported with a fallback for when `scripts/` is not on `sys.path`). The same pattern appears in `app.py`, `RAG_v1.py`, and ingestion contracts — always use `normalize_department` / `normalize_search_department_filter` rather than inline logic.

### Deployment Layout

- **FT3 HPC** (`ft3.cesga.es`, user `tec_app2`): runs heavy compute (embedding, FAISS build) via Slurm. Project path: `/mnt/netapp1/Store_CESGA/home/cesga/tec_app2/rag`
- **VM `rag-prod-02`** (IP `10.38.29.165`, user `cesgaxuser`): serves the FastAPI backend + Nginx static frontend. Project path: `/opt/rag/`
- SSH jump: `ssh -J tec_app2@ft3.cesga.es -i "~/.ssh/rag_cesga" cesgaxuser@10.38.29.165`
- Code sync to VM: `rsync -aP --delete -e "ssh -i ~/.ssh/rag_cesga" ./ cesgaxuser@10.38.29.165:/opt/rag/`

Nginx config and systemd units live in `deploy/cloud_srv_cesga/`. Backend env vars template is at `deploy/cloud_srv_cesga/env/rag-backend.env.example`.

## Key Configuration Files

| File | Purpose |
|---|---|
| `config/rag.yaml` | Retrieval models, index paths, fusion weights, backup retention |
| `config/preprocess.yaml` | Chunking and preprocessing settings |
| `config/daily_ingest.yaml` | DB connection env mapping, SQL template, schedule |
| `deploy/cloud_srv_cesga/env/rag-backend.env.example` | All runtime env vars for production |

## Testing

Tests are in `tests/`. Fixtures (including `withcomments_sample.jsonl`) live in `tests/fixtures/`. The test suite exercises ingestion contracts, FAISS incremental append, engine hot-swap, pipeline stages, source catalog, and search department filtering — all without requiring a running server.
