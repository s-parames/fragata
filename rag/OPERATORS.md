# RAG Ticket Search Web App

This repository provides a web-based RAG search system for CESGA ticket conversations.

It combines:
- semantic retrieval (FAISS + embeddings),
- lexical retrieval (BM25),
- hybrid fusion (RRF),
- and cross-encoder reranking.

The web UI and API allow operators to query tickets with department and date filters, and the data pipeline includes onboarding scripts to ingest new databases safely.

## Main Features

- Web UI at `/` for interactive ticket search.
- Web UI ingestion panel to submit website, repository-docs, and PDF jobs with an optional ingest label.
- Web UI admin purge panel to run department purge jobs with explicit confirmation.
- Web UI catalog panel to browse websites, PDFs, and ticket sources already represented in the RAG.
- REST API with typed request/response validation.
- Hybrid retrieval + reranking for better precision.
- Search department filter supports `all`, canonical values (`aplicaciones`, `sistemas`, `bigdata`), and custom sanitized values via API.
- Date range filters (`date_from`, `date_to`).
- Date-only search mode (empty query + date range).
- `Top K = 0` support for date-only mode to return all tickets in range.
- End-to-end dataset onboarding flow for new JSONL sources.
- Async ingestion jobs with persisted manifests and stage logs.
- Incremental FAISS append using merge delta, with full rebuild fallback.
- Async department purge jobs with atomic dataset update + full FAISS rebuild.
- Runtime engine hot-swap (no `uvicorn` restart required after successful ingestion).

## Web UI Functionality

The UI at `/` provides:
- Query textarea for natural language or keyword-heavy technical questions.
- Department selector (`All`, `Aplicaciones`, `Sistemas`, `Bigdata`).
- Date range filters (`From`, `To`).
- `Top K` selector.
- Client-side pagination (25 results per page) for large result sets.
- Ingestion panel:
  - source selector (`Website URL` / `Repo docs URL` / `PDF upload`),
  - required field (`department`) with suggestions for known values plus custom typing,
  - optional `ingest_label` field for batch/operator tagging,
  - async job status polling with stage and progress rendering.
- Purge panel:
  - target `department` input,
  - `dry_run` toggle,
  - mandatory explicit confirmation before API submission,
  - async purge status with stage/progress/summary details.
- Catalog panel:
  - server-backed source inventory (`ticket`, `web`, `pdf`),
  - free-text filter (`title`, `host`, `source`, `ingest_job_id`),
  - source-type and department filters,
  - API-backed pagination for larger inventories.
- Ranked result cards with:
  - rerank score,
  - fused score,
  - conversation ID,
  - ticket ID,
  - chunk ID,
  - last update date,
  - department,
  - source ticket link.

## Project Structure

- `app.py`: FastAPI server, UI routes, and `/search` endpoint.
- `RAG_v1.py`: retrieval engine, index loading/building, reranking.
- `ingest/`: ingestion contracts, orchestration, runner, FAISS incremental append, engine manager.
- `templates/`, `static/`: web frontend.
- `config/preprocess.yaml`: preprocessing and chunking settings.
- `config/rag.yaml`: retrieval settings and active dataset path.
- `scripts/`: data pipeline and onboarding scripts.
- `data/`: datasets, indexes, and reports.

## Preprocessing Notes

The dataset rebuild flow strips high-noise ticket boilerplate during preprocessing in `scripts/02_clean_anonymize.py` before chunking runs in `scripts/03_chunk_conversations.py`.
PII anonymization is disabled in this pipeline: names, phone numbers, and emails are preserved in chunk text.
This ordering is the same in both `scripts/run_pipeline.sh` and `scripts/onboard_db.sh`, so standard rebuilds and department onboarding automatically pick up the same cleaning behavior.

Supported ticket-conversation shapes:
- legacy rows with `link`, `messages`, and `lastUpdated`/`last_updated`
- new `withcomments` rows that additionally provide top-level `subject`, `status`, and `messages[].role == "comment"`

Field handling in the new format:
- `subject` is promoted into retrieval text as `Subject: ...` because it usually condenses the real problem better than the body boilerplate
- `status` is preserved as metadata only because current datasets are effectively constant (`resolved`) and adding it to retrieval text would only inject noise
- `comment` messages are cleaned more aggressively than `user` or `assistant` messages because they often contain HTML fragments, long signatures, or closure-only follow-up

Comment-role cleanup keeps useful operator follow-up such as diagnostics, parameter changes, and code/log snippets, while removing:
- HTML wrappers and mail-formatting artifacts
- signature/contact blocks
- closure-only fragments such as `Pecho este ticket.`, `Cierro este ticket.`, `Cerramos este ticket.`

Targeted noise families include:
- automatic ticket prologues such as `This message has been automatically generated...`, `a summary of which appears below.`, `There is no need to reply...`, `Your ticket has been assigned an ID...`, `Please include the string...`, `To do so, you may reply...`
- external notification fragments such as `This ticket has been created by ...`, `We are sorry if you got notified about this twice.`, `To access this ticket, click: ...`
- placeholder-only content such as `This transaction appears to have no content` and `Output other`

Preservation rule:
- if the automatic header shares a line with useful ticket content, preprocessing keeps the useful subject/problem text and removes only the boilerplate wrapper
- ordinary human text outside that ticket-header context, including normal uses of `Thank you,`, is preserved

Regression coverage:
- `tests/test_clean_anonymize_boilerplate.py` validates cleaner behavior
- `tests/test_chunk_conversations.py` validates that chunking and ready-dataset build do not reintroduce stripped boilerplate and preserve the new-format metadata contract

Sample fixture:
- `tests/fixtures/withcomments_sample.jsonl` provides a minimal new-format example with `subject`, `status`, and `comment` messages for future manual checks

## Quick Start (Web)

1. Go to the repo:

```bash
cd /mnt/netapp1/Store_CESGA/home/cesga/tec_app2/rag
```

2. Install dependencies (if needed):

```bash
./RAG/bin/pip install -r requirements.txt
```

3. Start the API server:

```bash
./RAG/bin/python -m uvicorn app:app --host 0.0.0.0 --port 8000
```

4. Health check:

```bash
curl -s http://127.0.0.1:8000/health
```

5. Open UI:

- `http://127.0.0.1:8000/`

## Split Frontend/Backend Deployment

The web client now supports split deployments:

- Frontend calls can target a remote API host using `<meta name="rag-api-base-url" ...>` in `index.html`.
- `scripts/build_frontend_bundle.sh` creates a static frontend bundle ready to serve from Nginx.
- Backend CORS can be configured with `RAG_CORS_ALLOW_ORIGINS`.

Build static frontend (example):

```bash
scripts/build_frontend_bundle.sh \
  --api-base-url https://rag-api.example.cesga.es \
  --output-dir dist/frontend
```

Backend CORS example:

```bash
export RAG_CORS_ALLOW_ORIGINS="https://rag-front.example.cesga.es"
export RAG_CORS_ALLOW_METHODS="GET,POST,OPTIONS"
export RAG_CORS_ALLOW_HEADERS="*"
```

Deployment templates and a CESGA cloud runbook are available at:

- `deploy/cloud_srv_cesga/`
- `doc/deploy_cloud_srv_cesga.md`

## API Usage

### `GET /health`

Returns backend status and engine state:
- `status`
- `engine_loaded`
- `engine_loading`
- `engine_error`

### `POST /ingest/web`

Creates an async website ingestion job and returns a `job_id` immediately.

Request body:

```json
{
  "url": "https://example.com/docs/",
  "department": "Platform Ops Team",
  "ingest_label": "optional batch"
}
```

Runtime behavior (automatic online path):
- stages: `dispatch -> acquire_source -> extract -> prepare -> chunk -> merge -> index_append -> reload`
- when HPC mode is enabled, index append introduces resource lifecycle stages:
  `resource_requested -> waiting_resources -> running_remote -> sync_back`
- merges chunk output into global dataset (`retrieval.dataset_path`)
- generates per-job artifacts under `data/reports/ingest_jobs/<job_id>/artifacts`:
  - `delta.jsonl`
  - `merge_summary.json`
  - `faiss_append_summary.json`
- appends only delta embeddings into FAISS (`retrieval.faiss_dir`) without full-rebuild fallback in online path
- triggers runtime hot-swap reload when previous stages succeed
- if `delta_rows=0`, `index_append` is marked as no-op candidate and reload still runs

### `POST /ingest/pdf`

Creates an async PDF ingestion job from multipart upload and returns a `job_id` immediately.

Multipart fields:
- `department` (required)
- `ingest_label` (optional)
- `source_url` (optional)
- `file` (required, PDF)

Example:

```bash
curl -s -X POST http://127.0.0.1:8000/ingest/pdf \
  -F 'department=Data Science Team' \
  -F 'ingest_label=optional batch' \
  -F 'file=@/tmp/manual.pdf;type=application/pdf'
```

### `POST /ingest/repo-docs`

Creates an async repository-documentation ingestion job and returns a `job_id` immediately.

Request body:

```json
{
  "url": "https://github.com/ACEsuit/mace/blob/main/README.md",
  "department": "Platform Ops Team",
  "ingest_label": "optional batch"
}
```

Runtime behavior (automatic online path):
- stages: `dispatch -> acquire_source -> prepare -> chunk -> merge -> index_append -> reload`
- when HPC mode is enabled, index append introduces resource lifecycle stages:
  `resource_requested -> waiting_resources -> running_remote -> sync_back`
- fetches only targeted repository docs instead of using the website mirroring path
- writes acquisition artifacts under `data/raw_site/jobs/<job_id>/repo_docs`
- preserves repo-docs metadata such as `original_url`, `canonical_url`, `acquisition_url`, provider, and repository identity
- reuses the same merge, FAISS append, catalog refresh, and runtime reload flow as the existing online ingestion paths

Supported public URL families:
- GitHub README:
  - `https://github.com/<owner>/<repo>/blob/<ref>/README.md`
- GitHub wiki:
  - `https://github.com/<owner>/<repo>/wiki`
  - `https://github.com/<owner>/<repo>/wiki/<page>`
- GitLab README:
  - `https://gitlab.com/<group>/<repo>/-/blob/<ref>/README.md`
- GitLab wiki:
  - `https://gitlab.com/<group>/<repo>/-/wikis`
  - `https://gitlab.com/<group>/<repo>/-/wikis/<page>`

Current non-goals:
- full repository crawling outside README/wikis
- ingestion of issues, pull requests, commits, release notes, or arbitrary source files
- support for non-GitHub/non-GitLab providers

Current limitations:
- only public repository docs URLs are supported
- wiki discovery is bounded to the repository wiki scope and page limit (default `20` pages); it does not crawl the full host or repository site
- unsupported or provider-mismatched URLs fail fast with explicit validation errors
- repo docs continue to appear as `html`/web sources in downstream search and catalog views so existing retrieval and catalog flows remain compatible

### `POST /ingest/rt-weekly`

Creates an async weekly RT ingestion job and returns a `job_id` immediately.

Request body:

```json
{
  "overlap_hours": 48,
  "ingest_label": "optional weekly batch label"
}
```

Runtime behavior:
- source type: `rt_weekly`
- command payload: `scripts/main_daily_ingest.sh` (with optional `--overlap-hours`)
- when HPC mode is enabled for `rt_weekly`, stages include:
  `dispatch -> resource_requested -> waiting_resources -> running_remote -> sync_back -> reload`
- when HPC mode is disabled, it runs locally and keeps the same job contract.

### HPC Offload Mode

The backend supports optional VM-to-supercomputer execution for selected source types.

Main environment variables:
- `RAG_HPC_ENABLED` (`0` or `1`)
- `RAG_HPC_SOURCE_TYPES` (CSV, e.g. `web,pdf,repo_docs,rt_weekly`)
- `RAG_HPC_CORES` (default `32`)
- `RAG_HPC_MEM` (default `32G`)
- `RAG_HPC_GPU` (`0` or `1`)
- `RAG_HPC_REMOTE_HOST`
- `RAG_HPC_REMOTE_USER`
- `RAG_HPC_SSH_KEY_PATH`
- `RAG_HPC_REMOTE_WORKDIR`
- `RAG_HPC_PYTHON_BIN`
- `RAG_HPC_RELEASE_POLICY` (`auto` default, or `explicit_cancel`)
- optional: `RAG_HPC_SUBMIT_TEMPLATE`, `RAG_HPC_CANCEL_TEMPLATE`, `RAG_HPC_TIMEOUT_SEC`

Release semantics:
- `auto`: success path relies on remote command completion (`release_status=auto_release_after_completion`)
- `explicit_cancel`: success path requires explicit `scancel` on tracked `allocation_id`
- failure path always attempts `scancel` when an `allocation_id` exists

See:
- `deploy/cloud_srv_cesga/env/rag-backend.env.example`
- `doc/vm_hpc_resource_orchestration.md`

### `GET /ingest/jobs/{job_id}`

Returns current ingestion manifest snapshot:
- `state` (`queued`, `running`, `succeeded`, `failed`)
- `stage`
- `progress` (`0.0..1.0`)
- timestamps
- `result` and `error` payloads, including stage observability fields:
  - `output_delta_path`
  - `merge_summary_path`
  - `index_append_summary_path`
  - `purge_summary_path`
  - `full_rebuild_summary_path`
  - `reload_metadata`
  - `backup_prune_metadata`
  - `source_catalog_refresh_metadata`
  - `stage_metrics`:
    - online ingest jobs: `merge`, `source_catalog_refresh`, `index_append`, `reload`, `backup_prune`
    - purge jobs:
      - dry-run: `purge_dataset`, `source_catalog_refresh` skipped, `full_rebuild` skipped, `reload` skipped
      - confirmed execution: `purge_dataset`, `source_catalog_refresh`, `full_rebuild`, `reload`, `backup_prune`

### `GET /catalog/sources`

Returns the logical source catalog used by the web UI.

Query params:
- `source_type` (`ticket`, `web`, `pdf`)
- `department`
- `q`
- `page`
- `page_size` (default `25`, max `100`)

Response behavior:
- returns logical source/document entries, not raw dataset chunks
- reads from `data/reports/catalog/source_catalog.json`
- rebuilds the artifact automatically if it is missing or older than the configured dataset path

### `GET /catalog/sources/{catalog_id}`

Returns one logical catalog entry by id.

### `POST /admin/engine/reload`

Forces runtime reload and returns generation/health metadata.
If reload fails, active engine remains unchanged and endpoint returns `500`.

### `POST /admin/purge-department`

Creates an async purge job and returns a `job_id` immediately.

Request body:

```json
{
  "department": "Data Science Team",
  "confirm": true,
  "dry_run": false
}
```

Validation behavior:
- `department` is required and normalized using the same ingestion rules (`3..32`, pattern `[a-z0-9][a-z0-9_-]{2,31}` after normalization).
- `confirm` is required and must be `true`.
- `dry_run` is optional and defaults to `false`.

Runtime behavior (purge path):
- `dry_run=true`: `dispatch -> purge_dataset` only; rebuild and reload are skipped explicitly and no FAISS mutation happens
- `dry_run=false`: `dispatch -> purge_dataset -> full_rebuild -> reload`
- on success, terminal stage is `completed` with `state=succeeded`
- artifacts under `data/reports/ingest_jobs/<job_id>/artifacts`:
  - `purge_summary.json`
  - `full_rebuild_summary.json` (only for non-dry-run execution)
  - `dataset_before_purge.jsonl` (only when rows were actually removed in non-dry-run mode)
- result observability fields include:
  - `purge_summary_path`
  - `full_rebuild_summary_path`
  - `output_index_path`
  - `backup_dataset_path`
  - `backup_prune_metadata`
  - `stage_metrics.purge_dataset`
  - `stage_metrics.full_rebuild`
  - `stage_metrics.reload`
  - `stage_metrics.backup_prune` (only after successful index mutation + successful reload)

Dry-run example (recommended first):

```bash
curl -s -X POST http://127.0.0.1:8000/admin/purge-department \
  -H 'Content-Type: application/json' \
  -d '{"department":"Data Science Team","confirm":true,"dry_run":true}'
```

Confirmed execution example:

```bash
curl -s -X POST http://127.0.0.1:8000/admin/purge-department \
  -H 'Content-Type: application/json' \
  -d '{"department":"Data Science Team","confirm":true,"dry_run":false}'
```

Dry-run then confirmed workflow (operator-safe):

```bash
# 1) Launch dry-run and capture job id
DRY_JOB_ID=$(curl -s -X POST http://127.0.0.1:8000/admin/purge-department \
  -H 'Content-Type: application/json' \
  -d '{"department":"Data Science Team","confirm":true,"dry_run":true}' \
  | ./RAG/bin/python -c 'import json,sys; print(json.load(sys.stdin)["job_id"])')

# 2) Poll dry-run job until terminal state
while true; do
  RESP=$(curl -s "http://127.0.0.1:8000/ingest/jobs/${DRY_JOB_ID}")
  echo "$RESP"
  echo "$RESP" | grep -Eq '"state":"(succeeded|failed)"' && break
  sleep 2
done

# 3) If dry-run impact is expected, launch confirmed purge
RUN_JOB_ID=$(curl -s -X POST http://127.0.0.1:8000/admin/purge-department \
  -H 'Content-Type: application/json' \
  -d '{"department":"Data Science Team","confirm":true,"dry_run":false}' \
  | ./RAG/bin/python -c 'import json,sys; print(json.load(sys.stdin)["job_id"])')

# 4) Poll confirmed job
while true; do
  RESP=$(curl -s "http://127.0.0.1:8000/ingest/jobs/${RUN_JOB_ID}")
  echo "$RESP"
  echo "$RESP" | grep -Eq '"state":"(succeeded|failed)"' && break
  sleep 2
done
```

### `POST /search`

Request body:

```json
{
  "query": "cannot compile TDEP gfortran -fpp -qopenmp",
  "k": 8,
  "department": "data_science_team",
  "date_from": "2024-01-01",
  "date_to": "2024-12-31"
}
```

Behavior notes:
- `query` can be empty only if at least one date filter is provided.
- Accepted date formats for `date_from` / `date_to`: `YYYY-MM-DD`, `DD/MM/YYYY`, or ISO datetime.
- Empty `query` + date filters triggers **date-only mode** (no semantic reranking).
- `department` accepts:
  - `all`
  - canonical aliases/values (normalized internally)
  - custom values that normalize to `3..32` chars, lowercase ASCII, pattern `[a-z0-9][a-z0-9_-]{2,31}`.
- In date-only mode:
  - `k = 0` returns all tickets in the date range.
  - `k > 0` returns the latest updated tickets in that range.
- If `query` is non-empty, `k` must be `>= 1`.

Minimal example:

```bash
curl -s -X POST http://127.0.0.1:8000/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"cannot compile TDEP gfortran -fpp -qopenmp","k":8}'
```

## Which Database Is Being Used?

The active global dataset is configured in:

- `config/rag.yaml` -> `retrieval.dataset_path`

Current value:

- `data/datasetFinalV2.jsonl`

If you change this value, the retrieval engine will read that dataset path when loading/rebuilding the index.

FAISS index path is also configured in:

- `config/rag.yaml` -> `retrieval.faiss_dir`

## Onboarding a New Database

When a new input DB arrives and department is unknown in the raw data, choose the operational script based on where you want to classify it.

### Option A: Force tag to Aplicaciones

```bash
bash scripts/onboard_aplicaciones.sh --input /path/to/new_db.jsonl
```

### Option B: Force tag to Sistemas

```bash
bash scripts/onboard_sistemas.sh --input /path/to/new_db.jsonl
```

### Option C: Force tag to Bigdata

```bash
bash scripts/onboard_bigdata.sh --input /path/to/new_db.jsonl
```

Important behavior:
- The wrapper script decides the department label.
- Raw input `department` is not trusted.
- Passing `--department` to wrappers is intentionally blocked.

### What the onboarding flow does

1. Validates input contract.
2. Profiles raw content.
3. Cleans and normalizes text.
4. Chunks conversations.
5. Runs chunk quality checks.
6. Builds ready dataset.
7. Merges into:
   - departmental dataset (`data/datasets/dataset_aplicaciones.jsonl`, `data/datasets/dataset_sistemas.jsonl`, or `data/datasets/dataset_bigdata.jsonl`)
   - global dataset (`retrieval.dataset_path` from `config/rag.yaml`)
8. Rebuilds FAISS index automatically.

## Ingestion Job Storage (Operational)

Per-job status and logs:

- `data/reports/ingest_jobs/<job_id>/manifest.json`
- `data/reports/ingest_jobs/<job_id>/logs/job.log`

Per-job raw/intermediate data:

- `data/raw_site/jobs/<job_id>/...`

Web-ingest raw data lifecycle:

- For web jobs, `mirror`, `extract`, `prepared`, and `chunked` live under `data/raw_site/jobs/<job_id>/` while the job is running.
- After successful `merge` into the configured global dataset (`retrieval.dataset_path`, currently `data/datasetFinalV2.jsonl`), the web raw job tree is deleted automatically when `web_job_cleanup.enabled=true`.
- Logs and report artifacts remain under `data/reports/ingest_jobs/<job_id>/` and are not touched by cleanup.
- The cleanup metadata is exposed in job status as `result.web_job_cleanup_metadata` and `result.stage_metrics.web_job_cleanup`.
- This automatic cleanup is scoped to web-ingest raw trees only; it does not prune FAISS, datasets, or report artifacts.

## Source Catalog (Operational)

The source catalog is a logical inventory of what the RAG currently contains. It is not a chunk browser.

- Source of truth for content remains the configured dataset (`retrieval.dataset_path`, currently `data/datasetFinalV2.jsonl`).
- The catalog groups dataset rows into logical source/document records:
  - tickets
  - website pages/documents
  - PDFs
- The persisted artifact lives at `data/reports/catalog/source_catalog.json`.
- The web UI and `/catalog/sources` API read this artifact instead of scanning raw chunk rows on every request.
- If the artifact is missing or stale relative to the dataset file, the backend rebuilds it automatically on the next catalog request.

Refresh lifecycle:

- online web/PDF ingest:
  - after successful `merge`, if `delta_rows > 0`, `source_catalog_refresh` rebuilds the catalog
  - if `delta_rows = 0`, the catalog refresh is skipped with explicit reason
- purge:
  - after successful `purge_dataset`, if `rows_removed > 0`, `source_catalog_refresh` rebuilds the catalog
  - `dry_run=true` skips catalog refresh explicitly
- observability:
  - `result.source_catalog_refresh_metadata`
  - `result.stage_metrics.source_catalog_refresh`
  - `logs/job.log` entries for completed/skipped/failed refreshes

Manual backfill cleanup for already accumulated web jobs:

```bash
./RAG/bin/python scripts/13_prune_web_job_artifacts.py --all-web-jobs --dry-run
./RAG/bin/python scripts/13_prune_web_job_artifacts.py --job-id <job_id> --apply
```

### Input JSONL contract

Each JSONL row must include:
- `link`
- `messages` (array)
- `lastUpdated` or `last_updated`

Row acceptance rule:
- the row must contain at least one non-empty `messages[].content`, or a non-empty `subject`
- empty `messages[].content` entries are ignored by preprocessing instead of failing the whole row
- `messages[].role` is still required for messages that do carry non-empty content

### Main outputs

- Department dataset: `data/datasets/dataset_aplicaciones.jsonl`, `data/datasets/dataset_sistemas.jsonl`, or `data/datasets/dataset_bigdata.jsonl`
- Global dataset: path configured in `config/rag.yaml` (`retrieval.dataset_path`)
- Reports:
  - `data/reports/onboard/<work_id>/raw_profile_<department>.md`
  - `data/reports/onboard/<work_id>/chunk_quality_<department>.md`

## Department Normalization

### Ingestion (`/ingest/web`, `/ingest/pdf`)

- Department is required and normalized to lowercase ASCII.
- Allowed raw chars before normalization: letters, numbers, spaces, `_`, `-`.
- Unsafe chars (for example `$`, `@`, `%`) are rejected with `422`.
- Normalized value must satisfy `3..32` chars and pattern `[a-z0-9][a-z0-9_-]{2,31}`.

Examples:
- `Data Science Team` -> `data_science_team`
- `Platform/Ops` -> `platform_ops`
- `BIG DATA` -> `bigdata` (legacy alias compatibility)

### Search (`/search`)

- `department=all` keeps existing behavior (no department restriction).
- Canonical values and legacy aliases remain compatible.
- Custom sanitized department values are accepted and matched against normalized metadata.
- Invalid department filters return `400` with actionable validation details.

## Daily Automated Ingestion (MySQL/MariaDB + Slurm)

The repo includes a daily orchestration flow:

1. Extract newly resolved tickets from DB with overlap window.
2. Prepare rows to onboarding input contract.
3. Route by filename token (`aplicaciones`, `sistemas`, `bigdata`) and onboard.
4. Rebuild FAISS once per successful run.
5. Advance watermark only on full success.

Main scripts:

- `scripts/main_daily_ingest.sh`: principal orchestrator, retry, lock, watermark management.
- `scripts/07_extract_resolved_tickets.py`: incremental DB extraction.
- `scripts/08_prepare_onboard_input.py`: conversion to onboarding contract.
- `scripts/09_route_and_onboard.sh`: routes each file to the correct onboarding wrapper.
- `scripts/slurm_daily_ingest.sbatch`: Slurm entrypoint.
- `scripts/submit_daily_ingest.sh`: creates required directories and submits sbatch.
- `config/daily_ingest.yaml`: DB env mapping + SQL template.

Environment variables required by extractor:

- `DAILY_DB_HOST`
- `DAILY_DB_PORT`
- `DAILY_DB_USER`
- `DAILY_DB_PASSWORD`
- `DAILY_DB_NAME`

Manual run:

```bash
bash scripts/main_daily_ingest.sh
```

Submit with Slurm:

```bash
bash scripts/submit_daily_ingest.sh
```

Request supercomputer resources directly with `compute`:

```bash
bash scripts/submit_daily_ingest.sh --mode compute
```

Equivalent direct launch:

```bash
bash scripts/main_daily_ingest.sh --request-supercompute
```

Authoritative production scheduler (weekly Sunday at 02:00, server local time):

```bash
bash scripts/run_interval_scheduler.sh
```

The scheduler reads `config/daily_ingest.yaml` -> `schedule` and should run as a persistent service process.

Operational checks (planned slot + installed process):

```bash
./RAG/bin/python scripts/check_next_scheduler_run.py --config config/daily_ingest.yaml
ps -ef | grep -E "run_interval_scheduler\\.sh" | grep -v grep
```

Legacy fallback (optional) with cron + Slurm submit:

```bash
0 2 * * 0 cd /mnt/netapp1/Store_CESGA/home/cesga/tec_app2/rag && bash scripts/submit_daily_ingest.sh
```

## Manual Utilities

Run complete pipeline on a default input:

```bash
bash scripts/run_pipeline.sh
```

Run retrieval from CLI:

```bash
./RAG/bin/python RAG_v1.py --config config/rag.yaml --query "your query"
```

Evaluate retrieval:

```bash
./RAG/bin/python scripts/eval_retrieval.py \
  --config config/rag.yaml \
  --eval-set data/eval_queries.jsonl \
  --out data/reports/retrieval_eval.json
```

Rebuild the persisted source catalog manually:

```bash
./RAG/bin/python scripts/14_rebuild_source_catalog.py --config config/rag.yaml
```

## Post-Operation Verification (Purge Flow)

1. Confirm final job status:
   - `state=succeeded`
   - `stage=completed`
   - `result.stage_metrics.purge_dataset.status=ok`
   - `result.stage_metrics.full_rebuild.status=ok`
2. Confirm expected artifacts exist:
   - `data/reports/ingest_jobs/<job_id>/manifest.json`
   - `data/reports/ingest_jobs/<job_id>/logs/job.log`
   - `data/reports/ingest_jobs/<job_id>/artifacts/purge_summary.json`
   - `data/reports/ingest_jobs/<job_id>/artifacts/full_rebuild_summary.json`
3. Verify dataset counts and target department impact:

```bash
export DATASET_PATH=$(./RAG/bin/python -c 'import yaml; print((yaml.safe_load(open("config/rag.yaml",encoding="utf-8")) or {}).get("retrieval",{}).get("dataset_path","data/datasetFinalV2.jsonl"))')
export TARGET_DEPT=data_science_team
./RAG/bin/python - <<'PY'
import json
import os

dataset_path = os.environ["DATASET_PATH"]
target_dept = os.environ["TARGET_DEPT"]
rows_total = 0
rows_target_department = 0

with open(dataset_path, encoding="utf-8") as src:
    for raw in src:
        line = raw.strip()
        if not line:
            continue
        row = json.loads(line)
        rows_total += 1
        if str(row.get("department", "")).strip().lower() == target_dept:
            rows_target_department += 1

print(f"dataset_path={dataset_path}")
print(f"rows_total={rows_total}")
print(f"rows_target_department={rows_target_department}")
PY
```

4. Run search sanity checks:
   - target department should return expected reduced/empty results.
   - non-target departments should still return expected data.

```bash
curl -s -X POST http://127.0.0.1:8000/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"cluster issue","department":"data_science_team","k":5}'

curl -s -X POST http://127.0.0.1:8000/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"cluster issue","department":"sistemas","k":5}'
```

## Troubleshooting

- First query is slow: model/index warmup is expected at startup.
- Invalid custom department in ingestion/search:
  - allowed input chars: letters, numbers, spaces, `_`, `-`
  - normalized result must match `[a-z0-9][a-z0-9_-]{2,31}` (length `3..32`)
- Ingestion job failed: inspect `manifest.json` and `logs/job.log` under `data/reports/ingest_jobs/<job_id>/`.
- API returns stale results after manual data/index changes: trigger `POST /admin/engine/reload`.
- Catalog inventory looks stale or incomplete:
  - inspect `result.source_catalog_refresh_metadata` and `stage_metrics.source_catalog_refresh` on the last successful ingest/purge job
  - if the catalog artifact was removed or is older than the dataset, the next `GET /catalog/sources` request rebuilds it automatically
  - if drift is still suspected after that, regenerate the catalog manually before investigating dataset correctness further:
    - `./RAG/bin/python scripts/14_rebuild_source_catalog.py --config config/rag.yaml`
- `index_append` with `delta_rows=0`:
  - expected behavior (no-op candidate); no FAISS mutation is needed.
  - verify `stage_metrics.index_append.status=no_op_candidate`.
- `index_append` failed:
  - inspect `manifest.json` and `logs/job.log` for stage=`index_append`.
  - inspect `data/reports/ingest_jobs/<job_id>/artifacts/faiss_append_summary.json`.
  - for manual recovery with fallback rebuild enabled:
    - `./RAG/bin/python scripts/10_incremental_faiss_append.py --config config/rag.yaml --delta data/reports/ingest_jobs/<job_id>/artifacts/delta.jsonl --summary-out data/reports/ingest_jobs/<job_id>/artifacts/faiss_append_summary.manual.json`
- Reload failure (`/admin/engine/reload` = 500):
  - active engine is still serving traffic.
  - inspect `detail.extra.health` and retry only after fixing root cause.
- Purge removed zero rows (`result.stage_metrics.purge_dataset.rows_removed=0`):
  - verify requested department normalization (`request.department` and `stage_metrics.purge_dataset.target_department`).
  - verify whether the department had already been purged previously.
  - if impact is unexpectedly zero, do not run confirmed purge blindly; inspect dataset distribution first.
- Purge full rebuild failed (`stage=full_rebuild`, `state=failed`):
  - inspect `data/reports/ingest_jobs/<job_id>/artifacts/full_rebuild_summary.json`.
  - run managed manual rebuild: `./RAG/bin/python RAG_v1.py --config config/rag.yaml --rebuild-index`
  - then retry: `POST /admin/engine/reload`
- Purge reload failed (`stage=reload`, `state=failed`):
  - rebuilt artifacts may exist, but active in-memory engine is still previous generation.
  - inspect job log + manifest, then retry `POST /admin/engine/reload` after fixing cause.
- Purge rollback/recovery (non-dry-run, rows removed):
  - restore backup from `result.backup_dataset_path` (or `purge_summary.json.backup_dataset_path`),
  - rebuild index (`--rebuild-index`),
  - reload engine (`POST /admin/engine/reload`),
  - rerun purge with dry-run first.
- FAISS artifact mismatch or corruption (cache enabled):
  - check `retrieval.faiss_dir` for both `index.faiss` and `index.pkl`.
  - if artifacts are missing/corrupt, recover with managed full rebuild:
  - `./RAG/bin/python RAG_v1.py --config config/rag.yaml --rebuild-index`
  - the command rebuilds through staging/promotion and prunes old backups according to `index_backups.keep_last`
  - then `POST /admin/engine/reload`
- Input contract validation fails: fix JSONL schema issues before rerun.

## Release Checklist

1. `./RAG/bin/python -m unittest -v`
2. `./RAG/bin/python -m py_compile app.py RAG_v1.py ingest/runner.py ingest/contracts.py scripts/06_merge_datasets.py scripts/10_incremental_faiss_append.py`
3. Launch one real end-to-end ingestion job (`/ingest/web` or `/ingest/pdf`) with a custom department (example: `Platform Ops Team`) and wait until `state=succeeded`.
4. Confirm online stage path reaches `merge`, `index_append`, and `reload` (or `index_append` no-op candidate if `delta_rows=0`).
5. Execute one purge dry-run (`POST /admin/purge-department`, `dry_run=true`) and verify:
   - `state=succeeded`
   - `stage_metrics.purge_dataset.rows_removed` is coherent
   - `purge_summary_path` exists
   - `stage_metrics.full_rebuild.status=skipped`
   - `stage_metrics.reload.status=skipped`
6. If dry-run impact is correct, execute confirmed purge (`dry_run=false`) and verify:
   - stage path reaches `purge_dataset`, `full_rebuild`, `reload`
   - post-check search sanity for target/non-target departments
   - backup path is recorded when rows were removed
   - `stage_metrics.backup_prune.status=ok` when rebuild and reload succeed
7. Confirm `engine_generation` increments after successful ingestion/purge hot-swap and check job traceability files:
   - `data/reports/ingest_jobs/<job_id>/manifest.json`
   - `data/reports/ingest_jobs/<job_id>/logs/job.log`

## Additional Docs

- `doc/alta_nueva_bd.md`: Spanish operational guide for onboarding.
- `doc/ingestion_hot_swap_runbook.md`: ingestion jobs + hot-swap operations runbook.
