from __future__ import annotations

import asyncio
import io
import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException
import fastapi.dependencies.utils as fastapi_dep_utils
from fastapi.testclient import TestClient
from starlette.datastructures import Headers, UploadFile

from ingest.source_catalog import rebuild_source_catalog
from ingest import (
    IngestJobResultSummary,
    IngestSourceType,
    IngestionJobRunner,
    IngestionJobStore,
    IngestionOrchestrator,
    PurgeDepartmentRequest,
    RtWeeklyIngestRequest,
)
from ingest.faiss_incremental import IncrementalAppendResult
from ingest.index_backup_retention import IndexBackupPruneResult

os.environ["RAG_SKIP_WARMUP"] = "1"
fastapi_dep_utils.ensure_multipart_is_installed = lambda: None  # type: ignore[assignment]
import app as app_module  # noqa: E402


class _FakeEngineManager:
    def health(self):
        return {
            "engine_loaded": True,
            "engine_loading": False,
            "engine_error": None,
        }


class _FakeEngineManagerLoading:
    def health(self):
        return {
            "engine_loaded": False,
            "engine_loading": True,
            "engine_error": None,
        }


class _FakeDoc:
    def __init__(self, metadata: dict, page_content: str = "dummy snippet"):
        self.metadata = metadata
        self.page_content = page_content


class _FakeSearchEngine:
    def __init__(self) -> None:
        self.last_retrieve_kwargs = None
        self.last_retrieve_by_date_kwargs = None
        self.documents = []

    def retrieve(self, query, date_from=None, date_to=None, department="all", k=None):
        self.last_retrieve_kwargs = {
            "query": query,
            "date_from": date_from,
            "date_to": date_to,
            "department": department,
            "k": k,
        }
        return [
            {
                "doc": _FakeDoc(
                    {
                        "conversation_id": "conv_1",
                        "chunk_id": "chunk_1",
                        "ticket_id": 101,
                        "source_type": "ticket",
                        "last_updated": "2026-02-26 12:00:00",
                        "department": department,
                        "source": "https://example.com/doc",
                    },
                    page_content="Search result for custom department",
                ),
                "fused_score": 0.5,
                "rerank_score": 1.2,
            }
        ]

    def retrieve_by_date(self, date_from=None, date_to=None, department="all", k=None):
        self.last_retrieve_by_date_kwargs = {
            "date_from": date_from,
            "date_to": date_to,
            "department": department,
            "k": k,
        }
        return []


class _FakeRepoDocsSearchEngine(_FakeSearchEngine):
    def retrieve(self, query, date_from=None, date_to=None, department="all", k=None):
        self.last_retrieve_kwargs = {
            "query": query,
            "date_from": date_from,
            "date_to": date_to,
            "department": department,
            "k": k,
        }
        return [
            {
                "doc": _FakeDoc(
                    {
                        "conversation_id": "doc_repo",
                        "chunk_id": "doc_repo_page_001_chunk_001",
                        "ticket_id": None,
                        "source_type": "html",
                        "last_updated": "2026-03-04 12:00:00",
                        "department": department,
                        "source": "https://github.com/ACEsuit/mace/blob/main/README.md",
                        "page_title": "MACE README",
                    },
                    page_content="Fast atomic simulations and installation steps",
                ),
                "fused_score": 0.91,
                "rerank_score": 7.5,
            }
        ]


class _FakeDuplicateTicketSearchEngine(_FakeSearchEngine):
    def retrieve(self, query, date_from=None, date_to=None, department="all", k=None):
        self.last_retrieve_kwargs = {
            "query": query,
            "date_from": date_from,
            "date_to": date_to,
            "department": department,
            "k": k,
        }
        return [
            {
                "doc": _FakeDoc(
                    {
                        "conversation_id": "conv_501",
                        "chunk_id": "chunk_501_best",
                        "ticket_id": 501,
                        "source_type": "ticket",
                        "last_updated": "2026-03-02 11:00:00",
                        "department": department,
                        "source": "ticket://501",
                    },
                    page_content="Best ranked chunk for ticket 501",
                ),
                "fused_score": 0.42,
                "rerank_score": 9.1,
            },
            {
                "doc": _FakeDoc(
                    {
                        "conversation_id": "conv_501",
                        "chunk_id": "chunk_501_worse",
                        "ticket_id": 501,
                        "source_type": "ticket",
                        "last_updated": "2026-03-02 11:00:00",
                        "department": department,
                        "source": "ticket://501",
                    },
                    page_content="Lower ranked chunk for ticket 501",
                ),
                "fused_score": 0.40,
                "rerank_score": 2.0,
            },
            {
                "doc": _FakeDoc(
                    {
                        "conversation_id": "conv_777",
                        "chunk_id": "chunk_777_best",
                        "ticket_id": 777,
                        "source_type": "ticket",
                        "last_updated": "2026-03-03 08:30:00",
                        "department": department,
                        "source": "ticket://777",
                    },
                    page_content="Best ranked chunk for ticket 777",
                ),
                "fused_score": 0.33,
                "rerank_score": 8.4,
            },
        ]


class IngestApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp.name)
        self.store = IngestionJobStore(root_dir=str(self.tmp_path / "reports"))
        self.orchestrator = IngestionOrchestrator(job_store=self.store)
        self.runner = IngestionJobRunner(orchestrator=self.orchestrator, max_workers=1)

        def tmp_pdf_input(job_id: str) -> Path:
            return self.tmp_path / "raw_site" / "jobs" / job_id / "pdf_input"

        self.patches = [
            patch.object(app_module, "_ingest_store", self.store),
            patch.object(app_module, "_ingest_orchestrator", self.orchestrator),
            patch.object(app_module, "_ingest_runner", self.runner),
            patch.object(app_module, "_pdf_job_input_dir", tmp_pdf_input),
        ]
        for p in self.patches:
            p.start()

        self.start_job_patch = patch.object(app_module._ingest_runner, "start_job", return_value=None)
        self.start_job_mock = self.start_job_patch.start()
        self.client = TestClient(app_module.app)

    def tearDown(self) -> None:
        self.client.close()
        self.start_job_patch.stop()
        for p in reversed(self.patches):
            p.stop()
        self.tmp.cleanup()

    def _job_logs(self, job_id: str) -> list[dict]:
        path = self.store.log_path(job_id)
        if not path.exists():
            return []
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        return [json.loads(line) for line in lines]

    def _write_catalog_config(self) -> Path:
        config_dir = self.tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "rag.yaml"
        config_path.write_text(
            "\n".join(
                [
                    "retrieval:",
                    "  dataset_path: data/datasetFinalV2.jsonl",
                    "  faiss_dir: data/index/faiss_v2",
                    "fusion:",
                    "  retrieval_k: 30",
                    "  final_k: 10",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        return config_path

    def _write_catalog_dataset(self, rows: list[dict]) -> Path:
        dataset_path = self.tmp_path / "data" / "datasetFinalV2.jsonl"
        dataset_path.parent.mkdir(parents=True, exist_ok=True)
        with dataset_path.open("w", encoding="utf-8") as dst:
            for row in rows:
                dst.write(json.dumps(row, ensure_ascii=False))
                dst.write("\n")
        return dataset_path

    def _prepare_catalog_fixture(self, rows: list[dict]) -> Path:
        config_path = self._write_catalog_config()
        self._write_catalog_dataset(rows)
        rebuild_source_catalog(config_path=config_path)
        return config_path

    def _catalog_snapshot(self) -> SimpleNamespace:
        return SimpleNamespace(
            dataset_path=str(self.tmp_path / "datasetFinal.jsonl"),
            catalog_path=str(self.tmp_path / "data" / "reports" / "catalog" / "source_catalog.json"),
            generated_at="2026-03-03T11:00:00+00:00",
            total_entries=3,
        )

    def _ui_script(self) -> str:
        return (app_module.APP_ROOT / "static" / "app.js").read_text(encoding="utf-8")

    def test_web_ingest_job_creation(self) -> None:
        resp = self.client.post(
            "/ingest/web",
            json={
                "url": "https://example.com/docs",
                "department": "sistemas",
            },
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["state"], "queued")
        job_id = body["job_id"]
        self.assertTrue(self.store.manifest_path(job_id).exists())
        self.assertEqual(body["source_type"], "web")
        self.assertEqual(body["stage"], "queued")
        self.assertEqual(body["progress"], 0.0)
        self.assertEqual(body["request"]["department"], "sistemas")
        self.assertIsNone(body["request"].get("ingest_label"))
        self.assertEqual(body["request"]["url"], "https://example.com/docs")
        datetime.fromisoformat(body["created_at"].replace("Z", "+00:00"))

    def test_web_ingest_accepts_custom_department(self) -> None:
        resp = self.client.post(
            "/ingest/web",
            json={
                "url": "https://example.com/docs",
                "department": "Data Science Team",
            },
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["request"]["department"], "data_science_team")

    def test_web_ingest_preserves_existing_optional_fields(self) -> None:
        resp = self.client.post(
            "/ingest/web",
            json={
                "url": "https://example.com/docs",
                "department": "sistemas",
                "ingest_label": "  web batch  ",
                "depth_limit": 4,
            },
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["request"]["department"], "sistemas")
        self.assertEqual(body["request"]["ingest_label"], "web batch")
        self.assertEqual(body["request"]["depth_limit"], 4)

    def test_web_ingest_invalid_request(self) -> None:
        resp = self.client.post(
            "/ingest/web",
            json={
                "url": "https://example.com/docs",
                "department": "invalid$dept",
            },
        )
        self.assertEqual(resp.status_code, 422)

    def test_web_ingest_rejects_private_url(self) -> None:
        resp = self.client.post(
            "/ingest/web",
            json={
                "url": "http://127.0.0.1/internal",
                "department": "sistemas",
            },
        )
        self.assertEqual(resp.status_code, 422)

    def test_repo_docs_ingest_job_creation(self) -> None:
        resp = self.client.post(
            "/ingest/repo-docs",
            json={
                "url": "https://github.com/ACEsuit/mace/blob/main/README.md",
                "department": "sistemas",
            },
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["state"], "queued")
        self.assertEqual(body["source_type"], "repo_docs")
        self.assertEqual(body["stage"], "queued")
        self.assertEqual(body["request"]["department"], "sistemas")
        self.assertEqual(body["request"]["url"], "https://github.com/ACEsuit/mace/blob/main/README.md")
        job_id = body["job_id"]
        self.assertTrue(self.store.manifest_path(job_id).exists())
        self.start_job_mock.assert_any_call(
            job_id=job_id,
            source_type=app_module.IngestSourceType.REPO_DOCS,
        )

    def test_repo_docs_ingest_accepts_custom_department(self) -> None:
        resp = self.client.post(
            "/ingest/repo-docs",
            json={
                "url": "https://github.com/trinityrnaseq/trinityrnaseq/wiki",
                "department": "Platform Ops Team",
            },
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["request"]["department"], "platform_ops_team")

    def test_repo_docs_ingest_accepts_ingest_label(self) -> None:
        resp = self.client.post(
            "/ingest/repo-docs",
            json={
                "url": "https://github.com/trinityrnaseq/trinityrnaseq/wiki",
                "department": "sistemas",
                "ingest_label": "  repo batch  ",
            },
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["request"]["ingest_label"], "repo batch")

    def test_repo_docs_ingest_rejects_unsupported_repo_docs_url(self) -> None:
        resp = self.client.post(
            "/ingest/repo-docs",
            json={
                "url": "https://github.com/ACEsuit/mace/issues/1",
                "department": "sistemas",
            },
        )
        self.assertEqual(resp.status_code, 422)
        self.start_job_mock.assert_not_called()

    def test_repo_docs_ingest_rejects_private_url(self) -> None:
        resp = self.client.post(
            "/ingest/repo-docs",
            json={
                "url": "http://127.0.0.1/repo/wiki",
                "department": "sistemas",
            },
        )
        self.assertEqual(resp.status_code, 422)
        self.start_job_mock.assert_not_called()

    def test_rt_weekly_ingest_job_creation(self) -> None:
        resp = self.client.post(
            "/ingest/rt-weekly",
            json={
                "overlap_hours": 24,
                "ingest_label": "  weekly rt  ",
            },
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["state"], "queued")
        self.assertEqual(body["source_type"], "rt_weekly")
        self.assertEqual(body["stage"], "queued")
        self.assertEqual(body["request"]["overlap_hours"], 24)
        self.assertEqual(body["request"]["ingest_label"], "weekly rt")
        job_id = body["job_id"]
        self.assertTrue(self.store.manifest_path(job_id).exists())
        self.start_job_mock.assert_any_call(
            job_id=job_id,
            source_type=app_module.IngestSourceType.RT_WEEKLY,
        )

    def test_rt_weekly_ingest_rejects_invalid_overlap(self) -> None:
        resp = self.client.post(
            "/ingest/rt-weekly",
            json={
                "overlap_hours": 999,
            },
        )
        self.assertEqual(resp.status_code, 422)
        self.start_job_mock.assert_not_called()

    def test_runner_rt_weekly_hpc_flow_succeeds_and_persists_metadata(self) -> None:
        class _FakeHpcExecutor:
            def __init__(self) -> None:
                self.config = SimpleNamespace(
                    remote_host="ft3.cesga.es",
                    remote_workdir="/mnt/netapp1/Store_CESGA/home/cesga/tec_app2/rag",
                )
                self.calls: list[str] = []

            def run(self, *, payload_command, spec, timeout_sec, submit_template):
                self.calls.append(payload_command)
                return SimpleNamespace(
                    return_code=0,
                    stdout="allocation_id=555",
                    stderr="",
                    request_command="compute -c 32 --mem 32G --gpu",
                    remote_command=f"compute -c 32 --mem 32G --gpu -- bash -lc '{payload_command}'",
                    allocation_id="555",
                )

            def cancel(self, allocation_id, timeout_sec=30):  # pragma: no cover - not expected
                return SimpleNamespace(return_code=0, stdout="", stderr="", allocation_id=allocation_id)

        status = self.orchestrator.enqueue_rt_weekly(
            RtWeeklyIngestRequest(overlap_hours=12, ingest_label="weekly run")
        )
        fake_hpc = _FakeHpcExecutor()
        self.runner._hpc_enabled = True
        self.runner._hpc_source_types = {IngestSourceType.RT_WEEKLY}
        self.runner._hpc_executor = fake_hpc

        self.runner._run_job(status.job_id, IngestSourceType.RT_WEEKLY, None)

        final = self.orchestrator.get_job(status.job_id)
        self.assertIsNotNone(final)
        assert final is not None and final.result is not None
        self.assertEqual(final.state.value, "succeeded")
        self.assertEqual(final.source_type.value, "rt_weekly")
        self.assertIn("rt_weekly", final.result.stage_metrics)
        self.assertIn("hpc_execution", final.result.stage_metrics)
        self.assertEqual(final.result.stage_metrics["hpc_execution"]["status"], "ok")
        self.assertEqual(final.result.hpc_execution.mode, "hpc")
        self.assertEqual(final.result.hpc_execution.allocation_id, "555")
        self.assertTrue(final.result.hpc_execution.released)
        self.assertEqual(final.result.hpc_execution.release_policy, "auto")
        self.assertEqual(final.result.hpc_execution.release_status, "auto_release_after_completion")
        self.assertTrue(fake_hpc.calls)
        self.assertIn("--state-dir state", fake_hpc.calls[0])
        self.assertIn("--env-file state/daily_ingest.env", fake_hpc.calls[0])

    def test_runner_rt_weekly_marks_failed_when_reload_callback_fails(self) -> None:
        status = self.orchestrator.enqueue_rt_weekly(RtWeeklyIngestRequest(overlap_hours=12))
        result = IngestJobResultSummary(
            merged_rows=0,
            delta_rows=0,
            chunk_rows=0,
            index_updated=True,
            stage_metrics={"rt_weekly": {"status": "ok"}},
        )

        def failing_reload(_job_id, _source_type, _result):
            raise RuntimeError("reload boom")

        self.runner.on_job_success = failing_reload
        with patch.object(self.runner, "_run_rt_weekly_job", return_value=result):
            self.runner._run_job(status.job_id, IngestSourceType.RT_WEEKLY, None)

        final = self.orchestrator.get_job(status.job_id)
        self.assertIsNotNone(final)
        assert final is not None
        self.assertEqual(final.state.value, "failed")
        self.assertEqual(final.stage, "reload")
        self.assertIn("reload boom", final.error or "")
        self.assertIsNotNone(final.result)
        assert final.result is not None
        self.assertIn("rt_weekly", final.result.stage_metrics)

    def test_runner_rt_weekly_hpc_explicit_cancel_policy_releases_with_scancel(self) -> None:
        class _FakeHpcExecutor:
            def __init__(self) -> None:
                self.config = SimpleNamespace(
                    remote_host="ft3.cesga.es",
                    remote_workdir="/mnt/netapp1/Store_CESGA/home/cesga/tec_app2/rag",
                )
                self.calls: list[str] = []
                self.cancel_calls: list[str] = []

            def run(self, *, payload_command, spec, timeout_sec, submit_template):
                self.calls.append(payload_command)
                return SimpleNamespace(
                    return_code=0,
                    stdout="allocation_id=991",
                    stderr="",
                    request_command="compute -c 32 --mem 32G --gpu",
                    remote_command=f"compute -c 32 --mem 32G --gpu -- bash -lc '{payload_command}'",
                    allocation_id="991",
                )

            def cancel(self, allocation_id, timeout_sec=30):
                self.cancel_calls.append(str(allocation_id))
                return SimpleNamespace(return_code=0, stdout="cancelled", stderr="", allocation_id=allocation_id)

        status = self.orchestrator.enqueue_rt_weekly(RtWeeklyIngestRequest(overlap_hours=12))
        fake_hpc = _FakeHpcExecutor()
        self.runner._hpc_enabled = True
        self.runner._hpc_source_types = {IngestSourceType.RT_WEEKLY}
        self.runner._hpc_executor = fake_hpc
        self.runner._hpc_release_policy = "explicit_cancel"

        self.runner._run_job(status.job_id, IngestSourceType.RT_WEEKLY, None)

        final = self.orchestrator.get_job(status.job_id)
        self.assertIsNotNone(final)
        assert final is not None and final.result is not None
        self.assertEqual(final.state.value, "succeeded")
        self.assertEqual(final.result.hpc_execution.release_policy, "explicit_cancel")
        self.assertTrue(final.result.hpc_execution.released)
        self.assertEqual(final.result.hpc_execution.release_status, "cancel_ok_after_completion")
        self.assertEqual(final.result.stage_metrics["hpc_execution"]["release_status"], "cancel_ok_after_completion")
        self.assertEqual(fake_hpc.cancel_calls, ["991"])

    def test_hpc_failure_path_attempts_cancel_when_allocation_id_exists(self) -> None:
        class _FakeHpcExecutor:
            def __init__(self) -> None:
                self.config = SimpleNamespace(
                    remote_host="ft3.cesga.es",
                    remote_workdir="/mnt/netapp1/Store_CESGA/home/cesga/tec_app2/rag",
                )
                self.cancel_calls: list[str] = []

            def run(self, *, payload_command, spec, timeout_sec, submit_template):
                return SimpleNamespace(
                    return_code=7,
                    stdout="allocation_id=222",
                    stderr="runtime failed",
                    request_command="compute -c 32 --mem 32G --gpu",
                    remote_command=f"compute -c 32 --mem 32G --gpu -- bash -lc '{payload_command}'",
                    allocation_id="222",
                )

            def cancel(self, allocation_id, timeout_sec=30):
                self.cancel_calls.append(str(allocation_id))
                return SimpleNamespace(return_code=0, stdout="cancelled", stderr="", allocation_id=allocation_id)

        status = self.store.create_job(source_type=IngestSourceType.RT_WEEKLY, request_payload={})
        result = IngestJobResultSummary(stage_metrics={})
        fake_hpc = _FakeHpcExecutor()
        self.runner._hpc_enabled = True
        self.runner._hpc_source_types = {IngestSourceType.RT_WEEKLY}
        self.runner._hpc_executor = fake_hpc
        self.runner._hpc_release_policy = "auto"

        with self.assertRaises(RuntimeError):
            self.runner._run_hpc_command_stage(
                job_id=status.job_id,
                source_type=IngestSourceType.RT_WEEKLY,
                payload_command="bash scripts/main_daily_ingest.sh",
                result=result,
                context_stage="rt_weekly",
            )

        self.assertEqual(fake_hpc.cancel_calls, ["222"])
        self.assertIsNotNone(result.hpc_execution)
        assert result.hpc_execution is not None
        self.assertEqual(result.hpc_execution.release_status, "cancel_ok_after_failure")
        self.assertTrue(result.hpc_execution.released)
        self.assertEqual(result.hpc_execution.release_policy, "auto")
        self.assertEqual(result.stage_metrics["hpc_execution"]["status"], "error")

    def test_runner_index_append_uses_hpc_stages_when_enabled(self) -> None:
        class _FakeHpcExecutor:
            def __init__(self) -> None:
                self.config = SimpleNamespace(
                    remote_host="ft3.cesga.es",
                    remote_workdir="/mnt/netapp1/Store_CESGA/home/cesga/tec_app2/rag",
                )
                self.calls: list[str] = []

            def run(self, *, payload_command, spec, timeout_sec, submit_template):
                self.calls.append(payload_command)
                return SimpleNamespace(
                    return_code=0,
                    stdout="allocation_id=777",
                    stderr="",
                    request_command="compute -c 32 --mem 32G --gpu",
                    remote_command=f"compute -c 32 --mem 32G --gpu -- bash -lc '{payload_command}'",
                    allocation_id="777",
                )

            def cancel(self, allocation_id, timeout_sec=30):  # pragma: no cover - not expected
                return SimpleNamespace(return_code=0, stdout="", stderr="", allocation_id=allocation_id)

        status = self.store.create_job(source_type=IngestSourceType.WEB, request_payload={})
        artifacts = self.store.job_dir(status.job_id) / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        delta_path = artifacts / "delta.jsonl"
        delta_path.write_text('{"id":"x"}\n', encoding="utf-8")
        append_summary_path = artifacts / "faiss_append_summary.json"
        append_summary_path.write_text(
            json.dumps(
                {
                    "applied": True,
                    "reason": "ok",
                    "faiss_dir": str(self.tmp_path / "index" / "faiss"),
                    "fallback_used": False,
                },
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )
        result = IngestJobResultSummary(
            delta_rows=1,
            output_delta_path=str(delta_path),
            artifacts_dir=str(artifacts),
            index_append_summary_path=str(append_summary_path),
        )
        fake_hpc = _FakeHpcExecutor()
        self.runner._hpc_enabled = True
        self.runner._hpc_source_types = {IngestSourceType.WEB}
        self.runner._hpc_executor = fake_hpc

        self.runner._run_index_append_stage(status.job_id, IngestSourceType.WEB, result)

        self.assertTrue(result.index_updated)
        self.assertEqual(result.output_index_path, str(self.tmp_path / "index" / "faiss"))
        self.assertEqual(result.stage_metrics["index_append"]["status"], "ok")
        self.assertEqual(result.stage_metrics["hpc_execution"]["status"], "ok")
        self.assertEqual(result.hpc_execution.mode, "hpc")
        self.assertEqual(result.hpc_execution.allocation_id, "777")
        persisted = self.orchestrator.get_job(status.job_id)
        self.assertIsNotNone(persisted)
        assert persisted is not None
        self.assertEqual(persisted.stage, "sync_back")
        self.assertTrue(fake_hpc.calls)

    def test_admin_purge_department_job_creation(self) -> None:
        resp = self.client.post(
            "/admin/purge-department",
            json={
                "department": "Data Science Team",
                "confirm": True,
            },
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["source_type"], "purge_department")
        self.assertEqual(body["state"], "queued")
        self.assertEqual(body["stage"], "queued")
        self.assertEqual(body["request"]["department"], "data_science_team")
        self.assertTrue(body["request"]["confirm"])
        self.assertFalse(body["request"]["dry_run"])
        job_id = body["job_id"]
        self.assertTrue(self.store.manifest_path(job_id).exists())
        self.start_job_mock.assert_any_call(
            job_id=job_id,
            source_type=app_module.IngestSourceType.PURGE_DEPARTMENT,
        )

    def test_admin_purge_department_accepts_dry_run_true(self) -> None:
        resp = self.client.post(
            "/admin/purge-department",
            json={
                "department": "sistemas",
                "confirm": True,
                "dry_run": True,
            },
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body["request"]["dry_run"])

    def test_admin_purge_department_rejects_confirm_false(self) -> None:
        resp = self.client.post(
            "/admin/purge-department",
            json={
                "department": "sistemas",
                "confirm": False,
            },
        )
        self.assertEqual(resp.status_code, 422)
        detail = resp.json()["detail"]
        self.assertEqual(detail["code"], "invalid_request")
        self.assertIn("errors", detail["extra"])
        self.assertTrue(any(item.get("loc", [])[-1:] == ["confirm"] for item in detail["extra"]["errors"]))
        self.start_job_mock.assert_not_called()

    def test_admin_purge_department_rejects_missing_confirm(self) -> None:
        resp = self.client.post(
            "/admin/purge-department",
            json={
                "department": "sistemas",
            },
        )
        self.assertEqual(resp.status_code, 422)
        detail = resp.json()["detail"]
        self.assertEqual(detail["code"], "invalid_request")
        self.assertIn("errors", detail["extra"])
        self.assertTrue(any(item.get("loc", [])[-1:] == ["confirm"] for item in detail["extra"]["errors"]))
        self.start_job_mock.assert_not_called()

    def test_admin_purge_department_rejects_unsafe_department(self) -> None:
        resp = self.client.post(
            "/admin/purge-department",
            json={
                "department": "ops$team",
                "confirm": True,
            },
        )
        self.assertEqual(resp.status_code, 422)
        detail = resp.json()["detail"]
        self.assertEqual(detail["code"], "invalid_request")
        self.assertIn("errors", detail["extra"])
        self.assertTrue(any(item.get("loc", [])[-1:] == ["department"] for item in detail["extra"]["errors"]))
        self.start_job_mock.assert_not_called()

    def test_purge_job_runner_applies_dataset_and_writes_summary_artifact(self) -> None:
        dataset_path = self.tmp_path / "datasetFinal.jsonl"
        dataset_rows = [
            {"conversation_id": "1", "department": "sistemas", "text": "remove-me"},
            {"conversation_id": "2", "department": "aplicaciones", "text": "keep-me"},
            {"conversation_id": "3", "department": "Data Science Team", "text": "remove-me-2"},
        ]
        dataset_path.write_text(
            "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in dataset_rows),
            encoding="utf-8",
        )
        request = PurgeDepartmentRequest(
            department="Data Science Team",
            confirm=True,
            dry_run=False,
        )
        status = self.orchestrator.enqueue_purge_department(request)

        fake_rebuild = IncrementalAppendResult(
            applied=True,
            reason="fallback_full_rebuild",
            config_path="cfg.yaml",
            faiss_dir=str(self.tmp_path / "index" / "faiss"),
            delta_path="unused",
            delta_input_rows=0,
            delta_docs_appended=0,
            index_count_before=3,
            index_count_after=2,
            docstore_count_before=3,
            docstore_count_after=2,
            rebuilt_doc_count=2,
        )
        with (
            patch.object(self.runner, "_resolve_global_dataset_path", return_value=dataset_path),
            patch("ingest.runner.rebuild_source_catalog", return_value=self._catalog_snapshot()) as catalog_refresh_mock,
            patch("ingest.runner.rebuild_full_faiss", return_value=fake_rebuild) as rebuild_mock,
        ):
            self.runner._run_job(status.job_id, IngestSourceType.PURGE_DEPARTMENT, None)

        final = self.orchestrator.get_job(status.job_id)
        self.assertIsNotNone(final)
        assert final is not None
        self.assertEqual(final.state.value, "succeeded")
        self.assertEqual(final.stage, "completed")
        self.assertIsNotNone(final.result)
        assert final.result is not None
        self.assertEqual(final.result.delta_rows, 1)
        self.assertEqual(final.result.merged_rows, 2)
        self.assertEqual(final.result.output_dataset_path, str(dataset_path))
        self.assertIsNotNone(final.result.purge_summary_path)
        self.assertEqual(final.result.output_index_path, str(self.tmp_path / "index" / "faiss"))
        self.assertTrue(final.result.index_updated)
        self.assertIsNotNone(final.result.full_rebuild_summary_path)
        self.assertEqual(final.result.source_catalog_refresh_metadata["status"], "ok")
        self.assertEqual(final.result.source_catalog_refresh_metadata["trigger"], "after_purge_dataset")
        self.assertIn("reload", final.result.stage_metrics)
        self.assertIn("source_catalog_refresh", final.result.stage_metrics)
        self.assertEqual(final.result.stage_metrics["reload"]["status"], "skipped")
        self.assertEqual(final.result.stage_metrics["reload"]["reason"], "no_reload_callback")
        self.assertEqual(final.result.reload_metadata, final.result.stage_metrics["reload"])

        summary_path = Path(final.result.purge_summary_path or "")
        self.assertTrue(summary_path.exists())
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        self.assertEqual(summary["rows_before"], 3)
        self.assertEqual(summary["rows_removed"], 1)
        self.assertEqual(summary["rows_after"], 2)
        self.assertEqual(summary["target_department"], "data_science_team")
        self.assertFalse(summary["dry_run"])
        self.assertTrue(summary["dataset_modified"])
        self.assertTrue(summary.get("backup_dataset_path"))
        self.assertTrue(Path(summary["backup_dataset_path"]).exists())

        rebuild_summary_path = Path(final.result.full_rebuild_summary_path or "")
        self.assertTrue(rebuild_summary_path.exists())
        rebuild_summary = json.loads(rebuild_summary_path.read_text(encoding="utf-8"))
        self.assertEqual(rebuild_summary["status"], "ok")
        self.assertEqual(rebuild_summary["index_path"], str(self.tmp_path / "index" / "faiss"))
        self.assertEqual(rebuild_summary["vector_count"], 2)
        self.assertEqual(rebuild_summary["doc_count"], 2)
        self.assertIn("duration_seconds", rebuild_summary)
        self.assertIn("full_rebuild", final.result.stage_metrics)
        catalog_refresh_mock.assert_called_once()
        rebuild_mock.assert_called_once_with(
            config_path=str(self.runner.project_root / "config" / "rag.yaml"),
            delta_path=str(self.store.job_dir(status.job_id) / "artifacts" / "purge_delta.jsonl"),
            dataset_path_override=str(dataset_path),
        )

        out_rows = [
            json.loads(line)
            for line in dataset_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.assertEqual(len(out_rows), 2)
        self.assertEqual([row["conversation_id"] for row in out_rows], ["1", "2"])

    def test_purge_job_runner_preserves_non_target_rows(self) -> None:
        dataset_path = self.tmp_path / "datasetFinal.jsonl"
        dataset_rows = [
            {"conversation_id": "10", "department": "sistemas", "text": "keep-1", "meta": {"priority": 1}},
            {"conversation_id": "11", "department": "Data Science Team", "text": "remove-1", "meta": {"priority": 2}},
            {"conversation_id": "12", "department": "bigdata", "text": "keep-2", "meta": {"priority": 3}},
            {"conversation_id": "13", "department": "data_science_team", "text": "remove-2", "meta": {"priority": 4}},
        ]
        dataset_path.write_text(
            "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in dataset_rows),
            encoding="utf-8",
        )
        request = PurgeDepartmentRequest(
            department="Data Science Team",
            confirm=True,
            dry_run=False,
        )
        status = self.orchestrator.enqueue_purge_department(request)

        fake_rebuild = IncrementalAppendResult(
            applied=True,
            reason="fallback_full_rebuild",
            config_path="cfg.yaml",
            faiss_dir=str(self.tmp_path / "index" / "faiss"),
            delta_path="unused",
            delta_input_rows=0,
            delta_docs_appended=0,
            index_count_before=4,
            index_count_after=2,
            docstore_count_before=4,
            docstore_count_after=2,
            rebuilt_doc_count=2,
        )
        with (
            patch.object(self.runner, "_resolve_global_dataset_path", return_value=dataset_path),
            patch("ingest.runner.rebuild_full_faiss", return_value=fake_rebuild),
        ):
            self.runner._run_job(status.job_id, IngestSourceType.PURGE_DEPARTMENT, None)

        final = self.orchestrator.get_job(status.job_id)
        self.assertIsNotNone(final)
        assert final is not None and final.result is not None
        self.assertEqual(final.state.value, "succeeded")
        self.assertEqual(final.result.delta_rows, 2)
        self.assertEqual(final.result.merged_rows, 2)

        out_rows = [
            json.loads(line)
            for line in dataset_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.assertEqual(
            out_rows,
            [
                {"conversation_id": "10", "department": "sistemas", "text": "keep-1", "meta": {"priority": 1}},
                {"conversation_id": "12", "department": "bigdata", "text": "keep-2", "meta": {"priority": 3}},
            ],
        )

    def test_purge_job_runner_dry_run_does_not_modify_dataset(self) -> None:
        dataset_path = self.tmp_path / "datasetFinal.jsonl"
        dataset_rows = [
            {"conversation_id": "1", "department": "sistemas", "text": "remove-me"},
            {"conversation_id": "2", "department": "aplicaciones", "text": "keep-me"},
        ]
        original = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in dataset_rows)
        dataset_path.write_text(original, encoding="utf-8")
        request = PurgeDepartmentRequest(
            department="sistemas",
            confirm=True,
            dry_run=True,
        )
        status = self.orchestrator.enqueue_purge_department(request)

        with (
            patch.object(self.runner, "_resolve_global_dataset_path", return_value=dataset_path),
            patch("ingest.runner.rebuild_source_catalog") as catalog_refresh_mock,
            patch("ingest.runner.rebuild_full_faiss") as rebuild_mock,
        ):
            self.runner._run_job(status.job_id, IngestSourceType.PURGE_DEPARTMENT, None)

        final = self.orchestrator.get_job(status.job_id)
        self.assertIsNotNone(final)
        assert final is not None
        self.assertEqual(final.state.value, "succeeded")
        self.assertIsNotNone(final.result)
        assert final.result is not None
        self.assertEqual(final.result.delta_rows, 1)
        self.assertEqual(final.result.merged_rows, 1)
        self.assertEqual(dataset_path.read_text(encoding="utf-8"), original)
        rebuild_mock.assert_not_called()
        self.assertIn("full_rebuild", final.result.stage_metrics)
        self.assertEqual(final.result.stage_metrics["full_rebuild"]["status"], "skipped")
        self.assertEqual(final.result.stage_metrics["full_rebuild"]["reason"], "dry_run_no_mutation")
        self.assertIn("reload", final.result.stage_metrics)
        self.assertEqual(final.result.stage_metrics["reload"]["status"], "skipped")
        self.assertEqual(final.result.stage_metrics["reload"]["reason"], "dry_run_no_mutation")
        self.assertEqual(final.result.reload_metadata, final.result.stage_metrics["reload"])
        self.assertEqual(final.result.source_catalog_refresh_metadata["status"], "skipped")
        self.assertEqual(final.result.source_catalog_refresh_metadata["reason"], "dry_run_no_mutation")
        catalog_refresh_mock.assert_not_called()

        summary_path = Path(final.result.purge_summary_path or "")
        self.assertTrue(summary_path.exists())
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        self.assertTrue(summary["dry_run"])
        self.assertFalse(summary["dataset_modified"])
        self.assertIsNone(summary.get("backup_dataset_path"))

        self.assertIsNone(final.result.full_rebuild_summary_path)

    def test_purge_job_runner_dry_run_does_not_call_reload_callback(self) -> None:
        dataset_path = self.tmp_path / "datasetFinal.jsonl"
        dataset_rows = [
            {"conversation_id": "1", "department": "sistemas", "text": "remove-me"},
            {"conversation_id": "2", "department": "aplicaciones", "text": "keep-me"},
        ]
        dataset_path.write_text(
            "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in dataset_rows),
            encoding="utf-8",
        )
        request = PurgeDepartmentRequest(
            department="sistemas",
            confirm=True,
            dry_run=True,
        )
        status = self.orchestrator.enqueue_purge_department(request)

        reload_calls = {"n": 0}

        def reload_callback(_job_id, _source_type, _result):
            reload_calls["n"] += 1
            return {"status": "ok"}

        self.runner.on_job_success = reload_callback
        with patch.object(self.runner, "_resolve_global_dataset_path", return_value=dataset_path):
            self.runner._run_job(status.job_id, IngestSourceType.PURGE_DEPARTMENT, None)

        final = self.orchestrator.get_job(status.job_id)
        self.assertIsNotNone(final)
        assert final is not None and final.result is not None
        self.assertEqual(final.state.value, "succeeded")
        self.assertEqual(reload_calls["n"], 0)
        self.assertEqual(final.result.stage_metrics["reload"]["status"], "skipped")
        self.assertEqual(final.result.stage_metrics["reload"]["reason"], "dry_run_no_mutation")

    def test_purge_job_runner_logs_operational_summary_and_zero_rows_warning(self) -> None:
        dataset_path = self.tmp_path / "datasetFinal.jsonl"
        dataset_rows = [
            {"conversation_id": "1", "department": "aplicaciones", "text": "keep-me"},
            {"conversation_id": "2", "department": "bigdata", "text": "keep-me-too"},
        ]
        dataset_path.write_text(
            "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in dataset_rows),
            encoding="utf-8",
        )
        request = PurgeDepartmentRequest(
            department="sistemas",
            confirm=True,
            dry_run=False,
        )
        status = self.orchestrator.enqueue_purge_department(request)

        fake_rebuild = IncrementalAppendResult(
            applied=True,
            reason="fallback_full_rebuild",
            config_path="cfg.yaml",
            faiss_dir=str(self.tmp_path / "index" / "faiss"),
            delta_path="unused",
            delta_input_rows=0,
            delta_docs_appended=0,
            index_count_before=2,
            index_count_after=2,
            docstore_count_before=2,
            docstore_count_after=2,
            rebuilt_doc_count=2,
        )
        with (
            patch.object(self.runner, "_resolve_global_dataset_path", return_value=dataset_path),
            patch("ingest.runner.rebuild_source_catalog", return_value=self._catalog_snapshot()),
            patch("ingest.runner.rebuild_full_faiss", return_value=fake_rebuild),
        ):
            self.runner._run_job(status.job_id, IngestSourceType.PURGE_DEPARTMENT, None)

        final = self.orchestrator.get_job(status.job_id)
        self.assertIsNotNone(final)
        assert final is not None
        self.assertEqual(final.state.value, "succeeded")
        self.assertIsNotNone(final.result)
        assert final.result is not None
        self.assertEqual(final.result.delta_rows, 0)
        self.assertTrue(final.result.purge_summary_path)
        self.assertTrue(final.result.full_rebuild_summary_path)
        self.assertEqual(final.result.output_index_path, str(self.tmp_path / "index" / "faiss"))

        logs = self._job_logs(status.job_id)
        self.assertTrue(logs)

        loaded = next((entry for entry in logs if entry.get("message") == "Purge request loaded"), None)
        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded["extra"]["requested_department"], "sistemas")
        self.assertFalse(loaded["extra"]["dry_run"])

        warning = next((entry for entry in logs if entry.get("message") == "Purge removed zero rows"), None)
        self.assertIsNotNone(warning)
        assert warning is not None
        self.assertEqual(warning.get("level"), "warning")
        self.assertEqual(warning["extra"]["rows_removed"], 0)
        self.assertEqual(warning["extra"]["requested_department"], "sistemas")

        summary = next((entry for entry in logs if entry.get("message") == "Purge flow completed"), None)
        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertEqual(summary["extra"]["requested_department"], "sistemas")
        self.assertEqual(summary["extra"]["rows_removed"], 0)
        self.assertEqual(summary["extra"]["source_catalog_refresh_status"], "skipped")
        self.assertEqual(summary["extra"]["rebuild_status"], "ok")
        self.assertEqual(summary["extra"]["reload_status"], "skipped")
        self.assertEqual(summary["extra"]["output_index_path"], str(self.tmp_path / "index" / "faiss"))
        self.assertTrue(summary["extra"]["purge_summary_path"])
        self.assertTrue(summary["extra"]["full_rebuild_summary_path"])

    def test_purge_job_runner_calls_reload_on_success(self) -> None:
        dataset_path = self.tmp_path / "datasetFinal.jsonl"
        dataset_rows = [
            {"conversation_id": "1", "department": "sistemas", "text": "remove-me"},
            {"conversation_id": "2", "department": "aplicaciones", "text": "keep-me"},
        ]
        dataset_path.write_text(
            "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in dataset_rows),
            encoding="utf-8",
        )
        request = PurgeDepartmentRequest(
            department="sistemas",
            confirm=True,
            dry_run=False,
        )
        status = self.orchestrator.enqueue_purge_department(request)
        reload_calls = {"n": 0}

        def reload_callback(_job_id, _source_type, _result):
            reload_calls["n"] += 1
            return {
                "status": "ok",
                "engine_generation": 17,
                "engine_loaded_at": "2026-02-27T12:00:00+00:00",
            }

        fake_rebuild = IncrementalAppendResult(
            applied=True,
            reason="fallback_full_rebuild",
            config_path="cfg.yaml",
            faiss_dir=str(self.tmp_path / "index" / "faiss"),
            delta_path="unused",
            delta_input_rows=0,
            delta_docs_appended=0,
            index_count_before=2,
            index_count_after=1,
            docstore_count_before=2,
            docstore_count_after=1,
            rebuilt_doc_count=1,
        )
        self.runner.on_job_success = reload_callback
        try:
            with (
                patch.object(self.runner, "_resolve_global_dataset_path", return_value=dataset_path),
                patch("ingest.runner.rebuild_full_faiss", return_value=fake_rebuild),
                patch(
                    "ingest.runner.prune_index_backups",
                    return_value=IndexBackupPruneResult(
                        active_dir=str(self.tmp_path / "index" / "faiss"),
                        keep_last=1,
                        retained=[str(self.tmp_path / "index" / "faiss.backup.20260303_071457")],
                        prunable=[str(self.tmp_path / "index" / "faiss.backup.20260302_120531")],
                        deleted=[str(self.tmp_path / "index" / "faiss.backup.20260302_120531")],
                        mode="apply",
                    ),
                ) as prune_mock,
            ):
                self.runner._run_job(status.job_id, IngestSourceType.PURGE_DEPARTMENT, None)
        finally:
            self.runner.on_job_success = None

        final = self.orchestrator.get_job(status.job_id)
        self.assertIsNotNone(final)
        assert final is not None and final.result is not None
        self.assertEqual(final.state.value, "succeeded")
        self.assertEqual(reload_calls["n"], 1)
        self.assertEqual(final.result.reload_metadata["status"], "ok")
        self.assertEqual(final.result.reload_metadata["engine_generation"], 17)
        prune_mock.assert_called_once()
        self.assertIn("backup_prune", final.result.stage_metrics)
        self.assertEqual(final.result.backup_prune_metadata["status"], "ok")
        self.assertEqual(final.result.backup_prune_metadata["keep_last"], 1)
        self.assertEqual(
            final.result.backup_prune_metadata["deleted"],
            [str(self.tmp_path / "index" / "faiss.backup.20260302_120531")],
        )

    def test_purge_job_runner_marks_failed_when_full_rebuild_fails(self) -> None:
        dataset_path = self.tmp_path / "datasetFinal.jsonl"
        dataset_rows = [
            {"conversation_id": "1", "department": "sistemas", "text": "remove-me"},
            {"conversation_id": "2", "department": "aplicaciones", "text": "keep-me"},
        ]
        dataset_path.write_text(
            "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in dataset_rows),
            encoding="utf-8",
        )
        request = PurgeDepartmentRequest(
            department="sistemas",
            confirm=True,
            dry_run=False,
        )
        status = self.orchestrator.enqueue_purge_department(request)

        with (
            patch.object(self.runner, "_resolve_global_dataset_path", return_value=dataset_path),
            patch("ingest.runner.rebuild_full_faiss", side_effect=RuntimeError("faiss rebuild boom")),
        ):
            self.runner._run_job(status.job_id, IngestSourceType.PURGE_DEPARTMENT, None)

        final = self.orchestrator.get_job(status.job_id)
        self.assertIsNotNone(final)
        assert final is not None
        self.assertEqual(final.state.value, "failed")
        self.assertEqual(final.stage, "full_rebuild")
        self.assertIn("Full rebuild failed", final.error or "")
        self.assertIsNotNone(final.result)
        assert final.result is not None
        self.assertTrue(final.result.purge_summary_path)
        self.assertTrue(final.result.full_rebuild_summary_path)
        self.assertIn("full_rebuild", final.result.stage_metrics)
        self.assertEqual(final.result.stage_metrics["full_rebuild"]["status"], "error")
        self.assertIn("faiss rebuild boom", str(final.result.stage_metrics["full_rebuild"]["error"]))

        rebuild_summary_path = self.store.job_dir(status.job_id) / "artifacts" / "full_rebuild_summary.json"
        self.assertTrue(rebuild_summary_path.exists())
        rebuild_summary = json.loads(rebuild_summary_path.read_text(encoding="utf-8"))
        self.assertEqual(rebuild_summary["status"], "error")
        self.assertIn("faiss rebuild boom", rebuild_summary["error"])

    def test_purge_job_runner_does_not_call_reload_when_full_rebuild_fails(self) -> None:
        dataset_path = self.tmp_path / "datasetFinal.jsonl"
        dataset_rows = [
            {"conversation_id": "1", "department": "sistemas", "text": "remove-me"},
            {"conversation_id": "2", "department": "aplicaciones", "text": "keep-me"},
        ]
        dataset_path.write_text(
            "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in dataset_rows),
            encoding="utf-8",
        )
        request = PurgeDepartmentRequest(
            department="sistemas",
            confirm=True,
            dry_run=False,
        )
        status = self.orchestrator.enqueue_purge_department(request)
        reload_calls = {"n": 0}

        def reload_callback(_job_id, _source_type, _result):
            reload_calls["n"] += 1
            return {"status": "ok"}

        self.runner.on_job_success = reload_callback
        try:
            with (
                patch.object(self.runner, "_resolve_global_dataset_path", return_value=dataset_path),
                patch("ingest.runner.rebuild_full_faiss", side_effect=RuntimeError("faiss rebuild boom")),
            ):
                self.runner._run_job(status.job_id, IngestSourceType.PURGE_DEPARTMENT, None)
        finally:
            self.runner.on_job_success = None

        final = self.orchestrator.get_job(status.job_id)
        self.assertIsNotNone(final)
        assert final is not None
        self.assertEqual(final.state.value, "failed")
        self.assertEqual(final.stage, "full_rebuild")
        self.assertEqual(reload_calls["n"], 0)

    def test_purge_job_runner_marks_failed_when_reload_fails(self) -> None:
        dataset_path = self.tmp_path / "datasetFinal.jsonl"
        dataset_rows = [
            {"conversation_id": "1", "department": "sistemas", "text": "remove-me"},
            {"conversation_id": "2", "department": "aplicaciones", "text": "keep-me"},
        ]
        dataset_path.write_text(
            "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in dataset_rows),
            encoding="utf-8",
        )
        request = PurgeDepartmentRequest(
            department="sistemas",
            confirm=True,
            dry_run=False,
        )
        status = self.orchestrator.enqueue_purge_department(request)

        fake_rebuild = IncrementalAppendResult(
            applied=True,
            reason="fallback_full_rebuild",
            config_path="cfg.yaml",
            faiss_dir=str(self.tmp_path / "index" / "faiss"),
            delta_path="unused",
            delta_input_rows=0,
            delta_docs_appended=0,
            index_count_before=2,
            index_count_after=1,
            docstore_count_before=2,
            docstore_count_after=1,
            rebuilt_doc_count=1,
        )

        def failing_reload(_job_id, _source_type, _result):
            raise RuntimeError("reload boom")

        self.runner.on_job_success = failing_reload
        with (
            patch.object(self.runner, "_resolve_global_dataset_path", return_value=dataset_path),
            patch("ingest.runner.rebuild_full_faiss", return_value=fake_rebuild),
            patch("ingest.runner.prune_index_backups") as prune_mock,
        ):
            self.runner._run_job(status.job_id, IngestSourceType.PURGE_DEPARTMENT, None)
        self.runner.on_job_success = None

        final = self.orchestrator.get_job(status.job_id)
        self.assertIsNotNone(final)
        assert final is not None
        self.assertEqual(final.state.value, "failed")
        self.assertEqual(final.stage, "reload")
        self.assertIn("reload boom", final.error or "")
        self.assertIsNotNone(final.result)
        assert final.result is not None
        self.assertTrue(final.result.purge_summary_path)
        self.assertTrue(final.result.full_rebuild_summary_path)
        self.assertEqual(final.result.output_index_path, str(self.tmp_path / "index" / "faiss"))
        prune_mock.assert_not_called()
        rebuild_summary_path = self.store.job_dir(status.job_id) / "artifacts" / "full_rebuild_summary.json"
        self.assertTrue(rebuild_summary_path.exists())

    def test_search_accepts_custom_department_filter(self) -> None:
        engine = _FakeSearchEngine()
        with (
            patch.object(app_module, "_engine_manager", _FakeEngineManager()),
            patch.object(app_module, "get_engine", return_value=engine),
        ):
            resp = self.client.post(
                "/search",
                json={
                    "query": "compile issue",
                    "department": "Data Science Team",
                    "k": 5,
                },
            )
        self.assertEqual(resp.status_code, 200)
        self.assertIsNotNone(engine.last_retrieve_kwargs)
        self.assertEqual(engine.last_retrieve_kwargs["department"], "data_science_team")
        self.assertEqual(engine.last_retrieve_kwargs["k"], 20)
        self.assertEqual(resp.json()["total"], 1)
        self.assertEqual(resp.json()["results"][0]["source_type"], "ticket")

    def test_search_accepts_slurm_department_filter(self) -> None:
        engine = _FakeSearchEngine()
        with (
            patch.object(app_module, "_engine_manager", _FakeEngineManager()),
            patch.object(app_module, "get_engine", return_value=engine),
        ):
            resp = self.client.post(
                "/search",
                json={
                    "query": "scheduler",
                    "department": "SLURM",
                    "k": 5,
                },
            )
        self.assertEqual(resp.status_code, 200)
        self.assertIsNotNone(engine.last_retrieve_kwargs)
        self.assertEqual(engine.last_retrieve_kwargs["department"], "slurm")

    def test_search_returns_repo_docs_results_coherently(self) -> None:
        engine = _FakeRepoDocsSearchEngine()
        with (
            patch.object(app_module, "_engine_manager", _FakeEngineManager()),
            patch.object(app_module, "get_engine", return_value=engine),
        ):
            resp = self.client.post(
                "/search",
                json={
                    "query": "mace install",
                    "department": "sistemas",
                    "k": 5,
                },
            )

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["results"][0]["source_type"], "html")
        self.assertEqual(body["results"][0]["department"], "sistemas")
        self.assertEqual(body["results"][0]["source"], "https://github.com/ACEsuit/mace/blob/main/README.md")
        self.assertIn("MACE README", body["results"][0]["snippet"])

    def test_search_returns_one_result_per_ticket_using_best_chunk(self) -> None:
        engine = _FakeDuplicateTicketSearchEngine()
        with (
            patch.object(app_module, "_engine_manager", _FakeEngineManager()),
            patch.object(app_module, "get_engine", return_value=engine),
        ):
            resp = self.client.post(
                "/search",
                json={
                    "query": "swan netcdf",
                    "department": "sistemas",
                    "k": 2,
                },
            )

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["total"], 2)
        self.assertEqual([row["ticket_id"] for row in body["results"]], [501, 777])
        self.assertEqual(body["results"][0]["chunk_id"], "chunk_501_best")
        self.assertEqual(engine.last_retrieve_kwargs["k"], 8)

    def test_search_auto_reloads_engine_when_artifacts_change(self) -> None:
        class _ReloadingEngineManager:
            def __init__(self) -> None:
                self.reload_calls = 0

            def health(self):
                return {
                    "engine_loaded": True,
                    "engine_loading": False,
                    "engine_error": None,
                }

            def reload_engine(self):
                self.reload_calls += 1
                return {
                    "engine_loaded": True,
                    "engine_loading": False,
                    "engine_error": None,
                    "engine_generation": self.reload_calls,
                }

        manager = _ReloadingEngineManager()
        engine = _FakeSearchEngine()
        with (
            patch.object(app_module, "_engine_manager", manager),
            patch.object(app_module, "get_engine", return_value=engine),
            patch.object(app_module, "_compute_active_artifact_signature", return_value="sig_new"),
            patch.object(app_module, "_engine_artifact_signature", "sig_old"),
        ):
            resp = self.client.post(
                "/search",
                json={
                    "query": "compile issue",
                    "department": "sistemas",
                    "k": 5,
                },
            )

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(manager.reload_calls, 1)

    def test_search_rejects_unsafe_department_filter(self) -> None:
        engine = _FakeSearchEngine()
        with (
            patch.object(app_module, "_engine_manager", _FakeEngineManager()),
            patch.object(app_module, "get_engine", return_value=engine),
        ):
            resp = self.client.post(
                "/search",
                json={
                    "query": "compile issue",
                    "department": "ops$team",
                },
            )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("Invalid department.", resp.json().get("detail", ""))
        self.assertIsNone(engine.last_retrieve_kwargs)

    def test_search_departments_includes_custom_sorted_values(self) -> None:
        engine = _FakeSearchEngine()
        engine.documents = [
            _FakeDoc({"department": "sistemas"}),
            _FakeDoc({"department": "Qiskit"}),
            _FakeDoc({"department": "Data Science Team"}),
            _FakeDoc({"department": "ops$team"}),
            _FakeDoc({"department": "qiskit"}),
        ]
        with (
            patch.object(app_module, "_engine_manager", _FakeEngineManager()),
            patch.object(app_module, "get_engine", return_value=engine),
        ):
            resp = self.client.get("/search/departments")

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertIn("departments", body)
        self.assertEqual(
            body["departments"],
            ["all", "sistemas", "data_science_team", "qiskit"],
        )

    def test_search_departments_fallback_when_engine_loading(self) -> None:
        with (
            patch.object(app_module, "_engine_manager", _FakeEngineManagerLoading()),
            patch.object(
                app_module,
                "_search_departments_from_active_dataset",
                return_value=["all", "aplicaciones", "sistemas", "bigdata"],
            ),
            patch.object(app_module, "get_engine", side_effect=RuntimeError("engine loading")),
        ):
            resp = self.client.get("/search/departments")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["departments"], ["all", "aplicaciones", "sistemas", "bigdata"])

    def test_search_departments_includes_slurm_only_when_present(self) -> None:
        engine = _FakeSearchEngine()
        engine.documents = [
            _FakeDoc({"department": "slurm"}),
            _FakeDoc({"department": "bigdata"}),
        ]
        with (
            patch.object(app_module, "_engine_manager", _FakeEngineManager()),
            patch.object(app_module, "get_engine", return_value=engine),
        ):
            resp = self.client.get("/search/departments")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["departments"], ["all", "bigdata", "slurm"])

    def test_index_initial_department_menu_does_not_hardcode_slurm(self) -> None:
        resp = self.client.get("/")

        self.assertEqual(resp.status_code, 200)
        self.assertIn('data-value="all"', resp.text)
        self.assertNotIn('data-value="slurm"', resp.text)

    def test_catalog_sources_default_list_response(self) -> None:
        config_path = self._prepare_catalog_fixture(
            [
                {
                    "chunk_id": "ticket-1-a",
                    "ticket_id": 501,
                    "conversation_id": "conv-ticket-1",
                    "source_type": "ticket",
                    "department": "sistemas",
                    "subject": "Slurm incident",
                    "source": "ticket://501",
                    "last_updated": "2026-03-01T11:00:00+00:00",
                    "ingest_job_id": "ing_ticket_1",
                    "ingested_at": "2026-03-01T11:05:00+00:00",
                },
                {
                    "chunk_id": "ticket-1-b",
                    "ticket_id": 501,
                    "conversation_id": "conv-ticket-1",
                    "source_type": "ticket",
                    "department": "sistemas",
                    "subject": "Slurm incident",
                    "source": "ticket://501",
                    "last_updated": "2026-03-01T11:00:00+00:00",
                    "ingest_job_id": "ing_ticket_1",
                    "ingested_at": "2026-03-01T11:05:00+00:00",
                },
                {
                    "chunk_id": "web-1-a",
                    "doc_id": "doc-web-1",
                    "conversation_id": "conv-web-1",
                    "source_type": "html",
                    "department": "slurm",
                    "page_title": "Slurm Quick Start",
                    "source": "https://slurm.schedmd.com/quickstart.html",
                    "last_updated": "2026-03-02T08:30:00+00:00",
                    "ingest_job_id": "ing_web_1",
                    "ingested_at": "2026-03-02T08:35:00+00:00",
                },
                {
                    "chunk_id": "pdf-1-a",
                    "doc_id": "doc-pdf-1",
                    "source_type": "pdf",
                    "department": "aplicaciones",
                    "page_title": "Cluster Manual",
                    "source": "https://docs.example.com/cluster-manual.pdf",
                    "last_updated": "2026-03-02T09:00:00+00:00",
                    "ingest_job_id": "ing_pdf_1",
                    "ingested_at": "2026-03-02T09:05:00+00:00",
                },
            ]
        )

        with patch.object(app_module, "CONFIG_PATH", config_path):
            resp = self.client.get("/catalog/sources")

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["total"], 3)
        self.assertEqual(body["page"], 1)
        self.assertEqual(body["page_size"], 25)
        self.assertFalse(body["has_more"])
        self.assertEqual(len(body["items"]), 3)
        self.assertEqual(
            set(body["items"][0].keys()),
            {
                "catalog_id",
                "source_type",
                "department",
                "source",
                "host",
                "title",
                "chunk_count",
                "last_updated",
                "ingest_job_id",
                "ingested_at",
            },
        )

    def test_catalog_sources_filters_by_source_type(self) -> None:
        config_path = self._prepare_catalog_fixture(
            [
                {
                    "chunk_id": "ticket-1-a",
                    "ticket_id": 501,
                    "source_type": "ticket",
                    "department": "sistemas",
                    "subject": "Slurm incident",
                    "source": "ticket://501",
                },
                {
                    "chunk_id": "web-1-a",
                    "doc_id": "doc-web-1",
                    "source_type": "html",
                    "department": "slurm",
                    "page_title": "Slurm Quick Start",
                    "source": "https://slurm.schedmd.com/quickstart.html",
                },
            ]
        )

        with patch.object(app_module, "CONFIG_PATH", config_path):
            resp = self.client.get("/catalog/sources", params={"source_type": "html"})

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["items"][0]["source_type"], "web")
        self.assertEqual(body["items"][0]["department"], "slurm")

    def test_catalog_sources_filters_by_department(self) -> None:
        config_path = self._prepare_catalog_fixture(
            [
                {
                    "chunk_id": "ticket-1-a",
                    "ticket_id": 501,
                    "source_type": "ticket",
                    "department": "sistemas",
                    "subject": "Slurm incident",
                    "source": "ticket://501",
                },
                {
                    "chunk_id": "pdf-1-a",
                    "doc_id": "doc-pdf-1",
                    "source_type": "pdf",
                    "department": "data_science_team",
                    "page_title": "Cluster Manual",
                    "source": "https://docs.example.com/cluster-manual.pdf",
                },
            ]
        )

        with patch.object(app_module, "CONFIG_PATH", config_path):
            resp = self.client.get("/catalog/sources", params={"department": "Data Science Team"})

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["items"][0]["department"], "data_science_team")

    def test_catalog_sources_lists_repo_docs_as_web_entries(self) -> None:
        config_path = self._prepare_catalog_fixture(
            [
                {
                    "chunk_id": "repo-1-a",
                    "doc_id": "doc-repo-1",
                    "conversation_id": "doc_repo",
                    "source_type": "html",
                    "department": "sistemas",
                    "page_title": "MACE README",
                    "source": "https://github.com/ACEsuit/mace/blob/main/README.md",
                    "canonical_url": "https://github.com/ACEsuit/mace/blob/main/README.md",
                    "repo_docs_provider": "github",
                    "repo_docs_kind": "readme",
                    "repo_slug": "ACEsuit/mace",
                    "last_updated": "2026-03-04T12:00:00+00:00",
                    "ingest_job_id": "ing_repo_1",
                    "ingested_at": "2026-03-04T12:05:00+00:00",
                },
                {
                    "chunk_id": "repo-1-b",
                    "doc_id": "doc-repo-1",
                    "conversation_id": "doc_repo",
                    "source_type": "html",
                    "department": "sistemas",
                    "page_title": "MACE README",
                    "source": "https://github.com/ACEsuit/mace/blob/main/README.md",
                    "canonical_url": "https://github.com/ACEsuit/mace/blob/main/README.md",
                    "repo_docs_provider": "github",
                    "repo_docs_kind": "readme",
                    "repo_slug": "ACEsuit/mace",
                    "last_updated": "2026-03-04T12:00:00+00:00",
                    "ingest_job_id": "ing_repo_1",
                    "ingested_at": "2026-03-04T12:05:00+00:00",
                },
            ]
        )

        with patch.object(app_module, "CONFIG_PATH", config_path):
            resp = self.client.get("/catalog/sources", params={"q": "mace"})

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["items"][0]["source_type"], "web")
        self.assertEqual(body["items"][0]["host"], "github.com")
        self.assertEqual(body["items"][0]["title"], "MACE README")
        self.assertEqual(body["items"][0]["source"], "https://github.com/ACEsuit/mace/blob/main/README.md")
        self.assertEqual(body["items"][0]["chunk_count"], 2)
        self.assertEqual(body["items"][0]["department"], "sistemas")

    def test_catalog_sources_filters_by_free_text(self) -> None:
        config_path = self._prepare_catalog_fixture(
            [
                {
                    "chunk_id": "ticket-1-a",
                    "ticket_id": 501,
                    "source_type": "ticket",
                    "department": "sistemas",
                    "subject": "Storage incident",
                    "source": "ticket://501",
                },
                {
                    "chunk_id": "web-1-a",
                    "doc_id": "doc-web-1",
                    "source_type": "html",
                    "department": "slurm",
                    "page_title": "Slurm Quick Start",
                    "source": "https://slurm.schedmd.com/quickstart.html",
                },
            ]
        )

        with patch.object(app_module, "CONFIG_PATH", config_path):
            resp = self.client.get("/catalog/sources", params={"q": "slurm"})

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["items"][0]["source_type"], "web")
        self.assertEqual(body["items"][0]["title"], "Slurm Quick Start")

    def test_catalog_sources_pagination_boundaries(self) -> None:
        config_path = self._prepare_catalog_fixture(
            [
                {
                    "chunk_id": "ticket-1-a",
                    "ticket_id": 501,
                    "source_type": "ticket",
                    "department": "sistemas",
                    "subject": "Alpha ticket",
                    "source": "ticket://501",
                },
                {
                    "chunk_id": "web-1-a",
                    "doc_id": "doc-web-1",
                    "source_type": "html",
                    "department": "slurm",
                    "page_title": "Beta page",
                    "source": "https://slurm.schedmd.com/beta.html",
                },
                {
                    "chunk_id": "pdf-1-a",
                    "doc_id": "doc-pdf-1",
                    "source_type": "pdf",
                    "department": "aplicaciones",
                    "page_title": "Gamma manual",
                    "source": "https://docs.example.com/gamma.pdf",
                },
            ]
        )

        with patch.object(app_module, "CONFIG_PATH", config_path):
            resp = self.client.get("/catalog/sources", params={"page": 2, "page_size": 2})

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["total"], 3)
        self.assertEqual(body["page"], 2)
        self.assertEqual(body["page_size"], 2)
        self.assertFalse(body["has_more"])
        self.assertEqual(len(body["items"]), 1)

    def test_catalog_overview_returns_grouped_websites_pdfs_and_tickets(self) -> None:
        config_path = self._prepare_catalog_fixture(
            [
                {
                    "chunk_id": "web-1-a",
                    "doc_id": "doc-web-1",
                    "source_type": "html",
                    "department": "slurm",
                    "page_title": "Quick Start",
                    "source": "https://slurm.schedmd.com/quickstart.html",
                    "last_updated": "2026-03-02T08:30:00+00:00",
                    "ingested_at": "2026-03-02T08:35:00+00:00",
                },
                {
                    "chunk_id": "web-1-b",
                    "doc_id": "doc-web-2",
                    "source_type": "html",
                    "department": "slurm",
                    "page_title": "Accounting",
                    "source": "https://slurm.schedmd.com/accounting.html",
                    "last_updated": "2026-03-02T08:31:00+00:00",
                    "ingested_at": "2026-03-02T08:36:00+00:00",
                },
                {
                    "chunk_id": "pdf-1-a",
                    "doc_id": "doc-pdf-1",
                    "source_type": "pdf",
                    "department": "aplicaciones",
                    "page_title": "Cluster Manual",
                    "source": "https://docs.example.com/cluster-manual.pdf",
                    "last_updated": "2026-03-02T09:00:00+00:00",
                    "ingested_at": "2026-03-02T09:05:00+00:00",
                },
                {
                    "chunk_id": "ticket-1-a",
                    "ticket_id": 501,
                    "source_type": "ticket",
                    "department": "sistemas",
                    "subject": "Storage incident",
                    "source": "ticket://501",
                    "last_updated": "2026-03-01T11:00:00+00:00",
                    "ingested_at": "2026-03-01T11:05:00+00:00",
                },
            ]
        )

        with patch.object(app_module, "CONFIG_PATH", config_path):
            resp = self.client.get("/catalog/overview")

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["limit_per_type"], 6)
        self.assertEqual(body["total"], 3)
        buckets = {bucket["source_type"]: bucket for bucket in body["buckets"]}
        self.assertEqual(buckets["web"]["total"], 1)
        self.assertEqual(buckets["pdf"]["total"], 1)
        self.assertEqual(buckets["ticket"]["total"], 1)
        self.assertEqual(buckets["web"]["items"][0]["label"], "slurm.schedmd.com")
        self.assertEqual(buckets["web"]["items"][0]["total_documents"], 2)
        self.assertEqual(buckets["web"]["items"][0]["total_chunks"], 2)

    def test_catalog_tree_returns_groups_with_children(self) -> None:
        config_path = self._prepare_catalog_fixture(
            [
                {
                    "chunk_id": "web-1-a",
                    "doc_id": "doc-web-1",
                    "source_type": "html",
                    "department": "slurm",
                    "page_title": "Quick Start",
                    "source": "https://slurm.schedmd.com/quickstart.html",
                    "last_updated": "2026-03-02T08:30:00+00:00",
                    "ingested_at": "2026-03-02T08:35:00+00:00",
                },
                {
                    "chunk_id": "pdf-1-a",
                    "doc_id": "doc-pdf-1",
                    "source_type": "pdf",
                    "department": "slurm",
                    "page_title": "Slurm Manual",
                    "source": "https://slurm.schedmd.com/pdfs/manual.pdf",
                    "last_updated": "2026-03-02T09:00:00+00:00",
                    "ingested_at": "2026-03-02T09:05:00+00:00",
                },
                {
                    "chunk_id": "ticket-1-a",
                    "ticket_id": 501,
                    "source_type": "ticket",
                    "department": "sistemas",
                    "subject": "Storage incident",
                    "source": "ticket://501",
                    "last_updated": "2026-03-01T11:00:00+00:00",
                    "ingested_at": "2026-03-01T11:05:00+00:00",
                },
            ]
        )

        with patch.object(app_module, "CONFIG_PATH", config_path):
            resp = self.client.get("/catalog/tree")

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["total_groups"], 2)
        self.assertEqual(body["total_items"], 3)
        self.assertEqual(body["groups"][0]["label"], "Slurm")
        self.assertEqual(body["groups"][0]["web_count"], 1)
        self.assertEqual(body["groups"][0]["pdf_count"], 1)
        self.assertEqual(len(body["groups"][0]["children"]), 2)
        self.assertEqual(body["groups"][1]["label"], "Tickets")
        self.assertEqual(body["groups"][1]["ticket_count"], 1)

    def test_catalog_sources_rebuilds_when_catalog_artifact_is_missing(self) -> None:
        config_path = self._prepare_catalog_fixture(
            [
                {
                    "chunk_id": "web-1-a",
                    "doc_id": "doc-web-1",
                    "source_type": "html",
                    "department": "slurm",
                    "page_title": "Slurm Quick Start",
                    "source": "https://slurm.schedmd.com/quickstart.html",
                },
            ]
        )
        catalog_path = self.tmp_path / "data" / "reports" / "catalog" / "source_catalog.json"
        self.assertTrue(catalog_path.exists())
        catalog_path.unlink()

        with patch.object(app_module, "CONFIG_PATH", config_path):
            resp = self.client.get("/catalog/sources")

        self.assertEqual(resp.status_code, 200)
        self.assertTrue(catalog_path.exists())
        self.assertEqual(resp.json()["total"], 1)

    def test_catalog_sources_rebuilds_when_catalog_artifact_is_stale(self) -> None:
        config_path = self._prepare_catalog_fixture(
            [
                {
                    "chunk_id": "web-1-a",
                    "doc_id": "doc-web-1",
                    "source_type": "html",
                    "department": "slurm",
                    "page_title": "Slurm Quick Start",
                    "source": "https://slurm.schedmd.com/quickstart.html",
                },
            ]
        )
        dataset_path = self.tmp_path / "data" / "datasetFinalV2.jsonl"
        with dataset_path.open("a", encoding="utf-8") as dst:
            dst.write(
                json.dumps(
                    {
                        "chunk_id": "pdf-1-a",
                        "doc_id": "doc-pdf-1",
                        "source_type": "pdf",
                        "department": "aplicaciones",
                        "page_title": "Cluster Manual",
                        "source": "https://docs.example.com/cluster-manual.pdf",
                    },
                    ensure_ascii=False,
                )
            )
            dst.write("\n")

        with patch.object(app_module, "CONFIG_PATH", config_path):
            resp = self.client.get("/catalog/sources")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["total"], 2)

    def test_index_renders_catalog_panel_shell(self) -> None:
        resp = self.client.get("/")

        self.assertEqual(resp.status_code, 200)
        self.assertIn('id="workspace-tabs"', resp.text)
        self.assertIn('id="workspace-tabs" class="workspace-tabs" role="tablist" aria-label="Workspace tabs" hidden', resp.text)
        self.assertIn('id="catalog-title"', resp.text)
        self.assertIn('id="catalog-filters-toggle"', resp.text)
        self.assertIn('id="catalog-filters-panel"', resp.text)
        self.assertIn('aria-controls="catalog-filters-panel"', resp.text)
        self.assertIn('aria-expanded="false"', resp.text)
        self.assertIn('id="catalog-filters-panel" class="catalog-filters-panel" hidden', resp.text)
        self.assertIn('id="catalog-form"', resp.text)
        self.assertIn('id="catalog-q"', resp.text)
        self.assertIn('id="catalog-source-type-select"', resp.text)
        self.assertIn('id="catalog-source-type-trigger"', resp.text)
        self.assertIn('id="catalog-source-type-menu"', resp.text)
        self.assertIn('id="catalog-source-type"', resp.text)
        self.assertIn('id="catalog-department"', resp.text)
        self.assertIn('id="catalog-status"', resp.text)
        self.assertIn('id="catalog-overview-results"', resp.text)
        self.assertIn('id="catalog-view-tab-overview"', resp.text)
        self.assertIn('id="catalog-view-tab-browse"', resp.text)
        self.assertIn('id="results-sort"', resp.text)
        self.assertIn('data-i18n="results.sortLabel"', resp.text)
        self.assertIn('id="catalog-results"', resp.text)
        self.assertIn('id="catalog-pagination"', resp.text)
        self.assertIn('id="catalog-total-chip"', resp.text)

    def test_catalog_ui_script_covers_empty_state_and_initial_load(self) -> None:
        script = self._ui_script()

        self.assertIn("catalog.empty", script)
        self.assertIn("catalog.filters.toggleAriaExpand", script)
        self.assertIn("catalog.filters.toggleAriaCollapse", script)
        self.assertIn("function catalogEmptyMarkup(message)", script)
        self.assertIn("function renderCatalogFiltersDrawer()", script)
        self.assertIn("function renderCatalogOverview()", script)
        self.assertIn("setCatalogStatusKey('catalog.status.loading'", script)
        self.assertIn("setCatalogFiltersExpanded(false);", script)
        self.assertIn("void loadCatalogPage(1);", script)

    def test_catalog_ui_script_wires_filter_changes_and_reset(self) -> None:
        script = self._ui_script()

        self.assertIn("catalogForm.addEventListener('submit'", script)
        self.assertIn("catalogResetBtn.addEventListener('click'", script)
        self.assertIn("catalogFiltersToggleEl.addEventListener('click'", script)
        self.assertIn("function setCatalogSourceTypeValue(value)", script)
        self.assertIn("function initCatalogSourceTypeSelect()", script)
        self.assertIn("catalogSourceTypeMenuEl.addEventListener('click'", script)
        self.assertIn("setCatalogFiltersExpanded(!catalogFiltersExpanded", script)
        self.assertIn("catalogQueryEl?.value", script)
        self.assertIn("catalogSourceTypeEl?.value", script)
        self.assertIn("catalogDepartmentEl?.value", script)
        self.assertIn("renderCatalogFiltersDrawer();", script)
        self.assertIn("fetch(apiUrl(`/catalog/tree?", script)
        self.assertIn("fetch(apiUrl(`/catalog/sources?", script)

    def test_catalog_ui_script_syncs_filter_drawer_accessibility(self) -> None:
        script = self._ui_script()

        self.assertIn("catalogFiltersToggleEl.setAttribute('aria-controls', catalogFiltersPanelEl.id)", script)
        self.assertIn("catalogFiltersToggleEl.setAttribute('aria-expanded', catalogFiltersExpanded ? 'true' : 'false')", script)
        self.assertIn("catalogFiltersToggleEl.setAttribute(", script)
        self.assertIn("catalogFiltersPanelEl.hidden = !catalogFiltersExpanded", script)
        self.assertIn("if (ev.key === 'Escape' && inCatalogFilters && catalogFiltersExpanded)", script)
        self.assertIn("setCatalogFiltersExpanded(false, { focusTrigger: true });", script)

    def test_catalog_ui_script_wires_pagination_interactions(self) -> None:
        script = self._ui_script()

        self.assertIn("if (catalogPaginationEl)", script)
        self.assertIn("catalogPaginationEl.addEventListener('click'", script)
        self.assertIn("void loadCatalogPage(targetPage);", script)
        self.assertIn("setCatalogView('browse');", script)

    def test_search_ui_script_supports_result_sorting_modes(self) -> None:
        script = self._ui_script()

        self.assertIn("const resultSortEl = document.getElementById('results-sort');", script)
        self.assertIn("function normalizeResultSortMode(value)", script)
        self.assertIn("function parseResultLastUpdatedMs(value)", script)
        self.assertIn("function sortSearchResults(rows, sortMode)", script)
        self.assertIn("currentResults = sortSearchResults(dataset, currentResultSort);", script)
        self.assertIn("function applyResultSort(sortMode, { resetPage = true } = {})", script)
        self.assertIn("resultSortEl.addEventListener('change'", script)
        self.assertIn("applyResultSort(resultSortEl.value, { resetPage: true });", script)

    def test_web_ingest_missing_required_fields(self) -> None:
        resp = self.client.post(
            "/ingest/web",
            json={
                "department": "sistemas",
            },
        )
        self.assertEqual(resp.status_code, 422)
        detail = resp.json().get("detail", [])
        self.assertTrue(any("url" in str(item.get("loc", "")) for item in detail if isinstance(item, dict)))

    def test_job_status_retrieval(self) -> None:
        created = self.client.post(
            "/ingest/web",
            json={
                "url": "https://example.com",
                "department": "sistemas",
            },
        )
        self.assertEqual(created.status_code, 200)
        job_id = created.json()["job_id"]

        status = self.client.get(f"/ingest/jobs/{job_id}")
        self.assertEqual(status.status_code, 200)
        self.assertEqual(status.json()["job_id"], job_id)

    def test_job_status_retrieval_truncates_oversized_manifest_error(self) -> None:
        created = self.client.post(
            "/ingest/web",
            json={
                "url": "https://example.com",
                "department": "sistemas",
            },
        )
        self.assertEqual(created.status_code, 200)
        job_id = created.json()["job_id"]

        self.orchestrator.mark_failed(
            job_id,
            stage="acquire_source",
            error="RuntimeError: " + ("x" * 5000),
        )

        status = self.client.get(f"/ingest/jobs/{job_id}")

        self.assertEqual(status.status_code, 200)
        body = status.json()
        self.assertEqual(body["job_id"], job_id)
        self.assertEqual(body["state"], "failed")
        self.assertEqual(body["stage"], "acquire_source")
        self.assertLessEqual(len(body["error"]), 4096)
        self.assertIn("[truncated; see job.log]", body["error"])

    def test_job_status_exposes_extended_result_fields(self) -> None:
        created = self.client.post(
            "/ingest/web",
            json={
                "url": "https://example.com/docs",
                "department": "sistemas",
            },
        )
        self.assertEqual(created.status_code, 200)
        job_id = created.json()["job_id"]
        self.orchestrator.mark_succeeded(
            job_id,
            result=IngestJobResultSummary(
                merged_rows=10,
                delta_rows=2,
                chunk_rows=3,
                index_updated=True,
                output_dataset_path="data/datasetFinal.jsonl",
                output_delta_path=f"data/reports/ingest_jobs/{job_id}/artifacts/delta.jsonl",
                merge_summary_path=f"data/reports/ingest_jobs/{job_id}/artifacts/merge_summary.json",
                index_append_summary_path=f"data/reports/ingest_jobs/{job_id}/artifacts/faiss_append_summary.json",
                output_index_path="data/index/faiss",
                reload_metadata={"engine_generation": 4, "engine_loaded_at": "2026-02-27T12:00:00+00:00"},
                backup_prune_metadata={
                    "status": "ok",
                    "keep_last": 1,
                    "deleted": ["data/index/faiss.backup.20260227_115145"],
                },
                source_catalog_refresh_metadata={
                    "status": "ok",
                    "trigger": "after_merge",
                    "catalog_path": "data/reports/catalog/source_catalog.json",
                    "total_entries": 3,
                },
                web_job_cleanup_metadata={
                    "status": "ok",
                    "trigger": "after_merge",
                    "deleted_job_root": f"data/raw_site/jobs/{job_id}",
                    "reclaimed_bytes": 2048,
                },
                stage_metrics={
                    "merge": {"status": "ok", "delta_rows": 2},
                    "source_catalog_refresh": {"status": "ok", "catalog_path": "data/reports/catalog/source_catalog.json"},
                    "index_append": {"status": "ok", "applied": True},
                    "reload": {"status": "ok", "engine_generation": 4},
                    "backup_prune": {"status": "ok", "deleted": ["data/index/faiss.backup.20260227_115145"]},
                    "web_job_cleanup": {"status": "ok", "deleted_job_root": f"data/raw_site/jobs/{job_id}"},
                },
            ),
        )

        status = self.client.get(f"/ingest/jobs/{job_id}")
        self.assertEqual(status.status_code, 200)
        body = status.json()
        self.assertEqual(body["state"], "succeeded")
        self.assertEqual(body["result"]["merged_rows"], 10)
        self.assertEqual(body["result"]["delta_rows"], 2)
        self.assertEqual(body["result"]["output_dataset_path"], "data/datasetFinal.jsonl")
        self.assertEqual(body["result"]["output_index_path"], "data/index/faiss")
        self.assertEqual(body["result"]["reload_metadata"]["engine_generation"], 4)
        self.assertEqual(body["result"]["backup_prune_metadata"]["keep_last"], 1)
        self.assertEqual(body["result"]["source_catalog_refresh_metadata"]["catalog_path"], "data/reports/catalog/source_catalog.json")
        self.assertEqual(body["result"]["web_job_cleanup_metadata"]["trigger"], "after_merge")
        self.assertIn("merge", body["result"]["stage_metrics"])
        self.assertIn("source_catalog_refresh", body["result"]["stage_metrics"])
        self.assertIn("web_job_cleanup", body["result"]["stage_metrics"])

    def test_job_status_not_found(self) -> None:
        resp = self.client.get("/ingest/jobs/ing_missing")
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json()["detail"]["code"], "job_not_found")
        self.assertIn("message", resp.json()["detail"])

    def test_pdf_ingest_rejects_invalid_file_type(self) -> None:
        upload = UploadFile(
            filename="notes.txt",
            file=io.BytesIO(b"hello"),
            headers=Headers({"content-type": "text/plain"}),
        )
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(
                app_module.ingest_pdf(
                    department="sistemas",
                    source_url=None,
                    file=upload,
                )
            )
        self.assertEqual(ctx.exception.status_code, 415)
        self.assertEqual(ctx.exception.detail["code"], "invalid_file_type")

    def test_pdf_ingest_invalid_request_shape(self) -> None:
        upload = UploadFile(
            filename="tiny.pdf",
            file=io.BytesIO(b"%PDF-1.4\n%%EOF\n"),
            headers=Headers({"content-type": "application/pdf"}),
        )
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(
                app_module.ingest_pdf(
                    department="platform$ops",
                    source_url=None,
                    file=upload,
                )
            )
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertEqual(ctx.exception.detail["code"], "invalid_request")
        self.assertIn("message", ctx.exception.detail)
        self.assertIn("extra", ctx.exception.detail)
        self.assertIsInstance(ctx.exception.detail["extra"].get("errors"), list)

    def test_pdf_ingest_rejects_oversized_file(self) -> None:
        big_content = b"%PDF-1.4\n" + (b"A" * 2048)
        with patch.dict(os.environ, {"RAG_MAX_PDF_UPLOAD_BYTES": "256"}):
            upload = UploadFile(
                filename="big.pdf",
                file=io.BytesIO(big_content),
                headers=Headers({"content-type": "application/pdf"}),
            )
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(
                    app_module.ingest_pdf(
                        department="sistemas",
                        source_url=None,
                        file=upload,
                    )
                )
        self.assertEqual(ctx.exception.status_code, 413)
        self.assertEqual(ctx.exception.detail["code"], "file_too_large")

    def test_pdf_ingest_response_contract(self) -> None:
        upload = UploadFile(
            filename="tiny.pdf",
            file=io.BytesIO(b"%PDF-1.4\n%%EOF\n"),
            headers=Headers({"content-type": "application/pdf"}),
        )
        status = asyncio.run(
            app_module.ingest_pdf(
                department="Platform Ops Team",
                source_url="https://example.com/origin",
                file=upload,
            )
        )
        self.assertEqual(status.source_type.value, "pdf")
        self.assertEqual(status.state.value, "queued")
        self.assertEqual(status.stage, "queued")
        self.assertEqual(status.progress, 0.0)
        self.assertEqual(status.request["department"], "platform_ops_team")
        self.assertIsNone(status.request.get("ingest_label"))
        self.assertEqual(status.request["source_url"], "https://example.com/origin")
        self.assertEqual(status.request["original_filename"], "tiny.pdf")
        datetime.fromisoformat(status.created_at.replace("Z", "+00:00"))

    def test_pdf_ingest_preserves_existing_optional_fields(self) -> None:
        upload = UploadFile(
            filename="tiny.pdf",
            file=io.BytesIO(b"%PDF-1.4\n%%EOF\n"),
            headers=Headers({"content-type": "application/pdf"}),
        )
        status = asyncio.run(
            app_module.ingest_pdf(
                department="Platform Ops Team",
                ingest_label="  pdf batch  ",
                source_url="https://example.com/manual",
                file=upload,
            )
        )
        self.assertEqual(status.request["department"], "platform_ops_team")
        self.assertEqual(status.request["ingest_label"], "pdf batch")
        self.assertEqual(status.request["source_url"], "https://example.com/manual")
        self.assertEqual(status.request["original_filename"], "tiny.pdf")

    def test_pdf_ingest_successful_small_fixture(self) -> None:
        pdf_bytes = b"%PDF-1.4\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\n"
        upload = UploadFile(
            filename="tiny.pdf",
            file=io.BytesIO(pdf_bytes),
            headers=Headers({"content-type": "application/pdf"}),
        )
        body = asyncio.run(
            app_module.ingest_pdf(
                department="sistemas",
                source_url=None,
                file=upload,
            )
        )
        self.assertEqual(body.state.value, "queued")
        job_id = body.job_id
        pdf_input_dir = self.tmp_path / "raw_site" / "jobs" / job_id / "pdf_input"
        self.assertTrue(pdf_input_dir.exists())
        pdf_files = list(pdf_input_dir.glob("*.pdf"))
        self.assertEqual(len(pdf_files), 1)
        self.assertGreater(pdf_files[0].stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
