from __future__ import annotations

import json
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ingest import (
    IngestJobResultSummary,
    IngestionJobRunner,
    IngestionJobStore,
    IngestionOrchestrator,
    IngestSourceType,
    IngestState,
    PdfIngestRequest,
    WebIngestRequest,
)
from ingest.index_backup_retention import IndexBackupPruneResult


class RunnerPdfPipelineTests(unittest.TestCase):
    def test_pdf_pipeline_path_with_small_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            store = IngestionJobStore(root_dir=str(tmp_path / "reports"))
            orchestrator = IngestionOrchestrator(job_store=store)
            runner = IngestionJobRunner(orchestrator=orchestrator, max_workers=1)
            runner.project_root = tmp_path
            runner.scripts_root = tmp_path / "scripts"
            runner.preprocess_config = tmp_path / "config" / "preprocess.yaml"
            runner.scripts_root.mkdir(parents=True, exist_ok=True)
            runner.preprocess_config.parent.mkdir(parents=True, exist_ok=True)
            runner.preprocess_config.write_text("chunking: {}\n", encoding="utf-8")

            request = PdfIngestRequest(
                department="Data Science Team",
                ingest_label="fixture_pdf",
                original_filename="tiny.pdf",
            )
            status = orchestrator.enqueue_pdf(request)
            self.assertEqual(status.request["department"], "data_science_team")
            staged = tmp_path / "data" / "raw_site" / "jobs" / status.job_id / "pdf_input" / "tiny.pdf"
            staged.parent.mkdir(parents=True, exist_ok=True)
            staged.write_bytes(b"%PDF-1.4\n%%EOF\n")

            with (
                patch("ingest.runner.run_pdf_extract_stage", return_value={"input_pdf_files": 1, "extracted_pdf_pages": 1}),
                patch(
                    "ingest.runner.run_pdf_prepare_stage",
                    return_value={"prepared_page_rows": 1},
                ) as prepare_mock,
                patch("ingest.runner.run_pdf_chunk_stage", return_value={"chunk_rows": 2, "chunked_output_path": "x"}),
            ):
                result = runner._run_pdf_job(status.job_id, str(staged))

            self.assertEqual(result.chunk_rows, 2)
            self.assertIn("extract", result.stage_metrics)
            self.assertIn("prepare", result.stage_metrics)
            self.assertIn("chunk", result.stage_metrics)
            self.assertTrue((result.artifacts_dir or "").endswith(f"/{status.job_id}/artifacts"))
            self.assertTrue((result.output_delta_path or "").endswith(f"/{status.job_id}/artifacts/delta.jsonl"))
            self.assertTrue((result.merge_summary_path or "").endswith(f"/{status.job_id}/artifacts/merge_summary.json"))
            self.assertTrue(
                (result.index_append_summary_path or "").endswith(
                    f"/{status.job_id}/artifacts/faiss_append_summary.json"
                )
            )
            self.assertEqual(prepare_mock.call_args.kwargs["department"], "data_science_team")

    def test_successful_job_triggers_auto_reload_callback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            store = IngestionJobStore(root_dir=str(tmp_path / "reports"))
            orchestrator = IngestionOrchestrator(job_store=store)
            calls = {"n": 0}

            def on_success(job_id, source_type, _result):
                calls["n"] += 1
                return {"reloaded": True, "job_id": job_id, "source_type": source_type.value}

            runner = IngestionJobRunner(
                orchestrator=orchestrator,
                max_workers=1,
                on_job_success=on_success,
            )
            runner.project_root = tmp_path
            runner.scripts_root = tmp_path / "scripts"
            runner.preprocess_config = tmp_path / "config" / "preprocess.yaml"
            runner.scripts_root.mkdir(parents=True, exist_ok=True)
            runner.preprocess_config.parent.mkdir(parents=True, exist_ok=True)
            runner.preprocess_config.write_text("chunking: {}\n", encoding="utf-8")

            request = PdfIngestRequest(
                department="sistemas",
                ingest_label="fixture_pdf",
                original_filename="tiny.pdf",
            )
            status = orchestrator.enqueue_pdf(request)
            staged = tmp_path / "data" / "raw_site" / "jobs" / status.job_id / "pdf_input" / "tiny.pdf"
            staged.parent.mkdir(parents=True, exist_ok=True)
            staged.write_bytes(b"%PDF-1.4\n%%EOF\n")

            with (
                patch("ingest.runner.run_pdf_extract_stage", return_value={"input_pdf_files": 1, "extracted_pdf_pages": 1}),
                patch("ingest.runner.run_pdf_prepare_stage", return_value={"prepared_page_rows": 1}),
                patch("ingest.runner.run_pdf_chunk_stage", return_value={"chunk_rows": 2, "chunked_output_path": "x"}),
                patch.object(runner, "_run_merge_stage", return_value=None),
            ):
                runner._run_job(status.job_id, IngestSourceType.PDF, str(staged))

            final = orchestrator.get_job(status.job_id)
            assert final is not None
            self.assertEqual(final.state, IngestState.SUCCEEDED)
            self.assertEqual(calls["n"], 1)
            assert final.result is not None
            self.assertTrue(final.result.stage_metrics["reload"]["reloaded"])

class RunnerWebPipelineTests(unittest.TestCase):
    def test_web_pipeline_uses_normalized_custom_department_in_prepare_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            store = IngestionJobStore(root_dir=str(tmp_path / "reports"))
            orchestrator = IngestionOrchestrator(job_store=store)
            runner = IngestionJobRunner(orchestrator=orchestrator, max_workers=1)
            runner.project_root = tmp_path
            runner.scripts_root = tmp_path / "scripts"
            runner.preprocess_config = tmp_path / "config" / "preprocess.yaml"
            runner.scripts_root.mkdir(parents=True, exist_ok=True)
            runner.preprocess_config.parent.mkdir(parents=True, exist_ok=True)
            runner.preprocess_config.write_text("chunking: {}\n", encoding="utf-8")

            request = WebIngestRequest(
                url="https://example.com/docs",
                department="Platform Ops Team",
                ingest_label="fixture_web",
            )
            status = orchestrator.enqueue_web(request)
            self.assertEqual(status.request["department"], "platform_ops_team")

            with (
                patch(
                    "ingest.runner.mirror_website",
                    return_value={"domain": "example.com", "mirrored_html_files": 1, "mirrored_pdf_files": 0},
                ),
                patch(
                    "ingest.runner.run_extract_stage",
                    return_value={"mirrored_html_files": 1, "mirrored_pdf_files": 0, "extracted_html_docs": 1, "extracted_pdf_pages": 0},
                ),
                patch("ingest.runner.run_prepare_stage", return_value={"prepared_page_rows": 1}) as prepare_mock,
                patch("ingest.runner.run_chunk_stage", return_value={"chunk_rows": 1, "chunked_output_path": "x"}),
            ):
                result = runner._run_web_job(status.job_id)

            self.assertEqual(result.chunk_rows, 1)
            self.assertIn("prepare", result.stage_metrics)
            self.assertTrue((result.artifacts_dir or "").endswith(f"/{status.job_id}/artifacts"))
            self.assertTrue((result.output_chunked_path or "").endswith("/chunked/raw_site_pages_chunked.jsonl"))
            self.assertEqual(prepare_mock.call_args.kwargs["department"], "platform_ops_team")


class RunnerOnlineIncrementalPathTests(unittest.TestCase):
    @staticmethod
    def _catalog_snapshot(tmp_path: Path) -> SimpleNamespace:
        return SimpleNamespace(
            dataset_path=str(tmp_path / "data" / "datasetFinalV2.jsonl"),
            catalog_path=str(tmp_path / "data" / "reports" / "catalog" / "source_catalog.json"),
            generated_at="2026-03-03T10:15:00+00:00",
            total_entries=4,
        )

    def _build_runner(self, tmp_path: Path, *, on_success=None) -> tuple[IngestionJobStore, IngestionOrchestrator, IngestionJobRunner]:
        store = IngestionJobStore(root_dir=str(tmp_path / "reports"))
        orchestrator = IngestionOrchestrator(job_store=store)
        runner = IngestionJobRunner(orchestrator=orchestrator, max_workers=2, on_job_success=on_success)
        runner.project_root = tmp_path
        runner.scripts_root = tmp_path / "scripts"
        runner.preprocess_config = tmp_path / "config" / "preprocess.yaml"
        runner.scripts_root.mkdir(parents=True, exist_ok=True)
        runner.preprocess_config.parent.mkdir(parents=True, exist_ok=True)
        runner.preprocess_config.write_text("chunking: {}\n", encoding="utf-8")
        (tmp_path / "config" / "rag.yaml").write_text(
            "\n".join(
                [
                    "retrieval:",
                    "  dataset_path: data/datasetFinalV2.jsonl",
                    "  faiss_dir: data/index/faiss",
                    "fusion:",
                    "  semantic_weight: 0.8",
                    "  lexical_weight: 0.2",
                    "  rrf_k: 60",
                    "index_backups:",
                    "  enabled: true",
                    "  keep_last: 1",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        return store, orchestrator, runner

    def _stub_result(self, tmp_path: Path, job_id: str) -> IngestJobResultSummary:
        chunked = tmp_path / "data" / "raw_site" / "jobs" / job_id / "chunked" / "raw_site_pages_chunked.jsonl"
        chunked.parent.mkdir(parents=True, exist_ok=True)
        chunked.write_text('{"text":"demo"}\n', encoding="utf-8")
        artifacts = tmp_path / "reports" / job_id / "artifacts"
        return IngestJobResultSummary(
            chunk_rows=1,
            artifacts_dir=str(artifacts),
            output_chunked_path=str(chunked),
            output_dataset_path=str(chunked),
            output_delta_path=str(artifacts / "delta.jsonl"),
            merge_summary_path=str(artifacts / "merge_summary.json"),
            index_append_summary_path=str(artifacts / "faiss_append_summary.json"),
        )

    @staticmethod
    def _arg_value(args: list[str], name: str) -> str:
        idx = args.index(name)
        return args[idx + 1]

    @staticmethod
    def _job_logs(store: IngestionJobStore, job_id: str) -> list[dict]:
        path = store.log_path(job_id)
        if not path.exists():
            return []
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        return [json.loads(line) for line in lines]

    def test_online_pdf_path_success_runs_merge_append_reload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            reload_calls = {"n": 0}

            def on_success(_job_id, _source_type, _result):
                reload_calls["n"] += 1
                return {
                    "status": "ok",
                    "engine_generation": 7,
                    "engine_loaded_at": "2026-02-27T10:00:00+00:00",
                }

            store, orchestrator, runner = self._build_runner(tmp_path, on_success=on_success)
            status = orchestrator.enqueue_pdf(
                PdfIngestRequest(department="sistemas", ingest_label="online_pdf", original_filename="doc.pdf")
            )
            stub = self._stub_result(tmp_path, status.job_id)
            script_calls: list[str] = []

            def fake_run_command(args: list[str], **kwargs):
                script_name = Path(args[1]).name
                script_calls.append(script_name)
                if script_name == "06_merge_datasets.py":
                    out_path = Path(self._arg_value(args, "--out"))
                    delta_path = Path(self._arg_value(args, "--out-delta"))
                    summary_path = Path(self._arg_value(args, "--summary-out"))
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    out_path.write_text('{"text":"merged"}\n', encoding="utf-8")
                    delta_path.parent.mkdir(parents=True, exist_ok=True)
                    delta_path.write_text('{"text":"delta"}\n', encoding="utf-8")
                    summary_path.parent.mkdir(parents=True, exist_ok=True)
                    summary_path.write_text(json.dumps({"out_rows": 12, "delta_rows": 1}), encoding="utf-8")
                    return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
                if script_name == "10_incremental_faiss_append.py":
                    self.assertEqual(kwargs.get("cwd"), str(tmp_path))
                    env = kwargs.get("env") or {}
                    self.assertIn("PYTHONPATH", env)
                    self.assertIn(str(tmp_path), env.get("PYTHONPATH", ""))
                    summary_path = Path(self._arg_value(args, "--summary-out"))
                    summary_path.parent.mkdir(parents=True, exist_ok=True)
                    summary_path.write_text(
                        json.dumps(
                            {
                                "applied": True,
                                "reason": "ok",
                                "faiss_dir": str(tmp_path / "data" / "index" / "faiss"),
                                "fallback_used": False,
                            }
                        ),
                        encoding="utf-8",
                    )
                    return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
                raise AssertionError(f"Unexpected command: {args}")

            with (
                patch.object(runner, "_run_pdf_job", return_value=stub),
                patch("ingest.runner.run_command", side_effect=fake_run_command),
                patch(
                    "ingest.runner.rebuild_source_catalog",
                    return_value=self._catalog_snapshot(tmp_path),
                ) as catalog_refresh_mock,
                patch(
                    "ingest.runner.prune_index_backups",
                    return_value=IndexBackupPruneResult(
                        active_dir=str(tmp_path / "data" / "index" / "faiss"),
                        keep_last=1,
                        retained=[str(tmp_path / "data" / "index" / "faiss.backup.20260303_071457")],
                        prunable=[str(tmp_path / "data" / "index" / "faiss.backup.20260302_120531")],
                        deleted=[str(tmp_path / "data" / "index" / "faiss.backup.20260302_120531")],
                        mode="apply",
                    ),
                ) as prune_mock,
            ):
                runner._run_job(status.job_id, IngestSourceType.PDF, None)

            final = orchestrator.get_job(status.job_id)
            assert final is not None and final.result is not None
            self.assertEqual(final.state, IngestState.SUCCEEDED)
            self.assertEqual(reload_calls["n"], 1)
            self.assertEqual(final.result.merged_rows, 12)
            self.assertEqual(final.result.delta_rows, 1)
            self.assertTrue(final.result.index_updated)
            self.assertEqual(final.result.output_index_path, str(tmp_path / "data" / "index" / "faiss"))
            self.assertEqual(final.result.reload_metadata["engine_generation"], 7)
            self.assertEqual(final.result.backup_prune_metadata["status"], "ok")
            self.assertEqual(final.result.source_catalog_refresh_metadata["status"], "ok")
            self.assertEqual(final.result.source_catalog_refresh_metadata["trigger"], "after_merge")
            self.assertIsNone(final.result.web_job_cleanup_metadata)
            self.assertEqual(
                final.result.backup_prune_metadata["deleted"],
                [str(tmp_path / "data" / "index" / "faiss.backup.20260302_120531")],
            )
            self.assertIn("merge", final.result.stage_metrics)
            self.assertIn("source_catalog_refresh", final.result.stage_metrics)
            self.assertIn("index_append", final.result.stage_metrics)
            self.assertIn("reload", final.result.stage_metrics)
            self.assertIn("backup_prune", final.result.stage_metrics)
            self.assertNotIn("web_job_cleanup", final.result.stage_metrics)
            self.assertEqual(script_calls.count("06_merge_datasets.py"), 1)
            self.assertEqual(script_calls.count("10_incremental_faiss_append.py"), 1)
            catalog_refresh_mock.assert_called_once()
            prune_mock.assert_called_once()

    def test_online_web_path_success_runs_merge_append_reload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            reload_calls = {"n": 0}

            def on_success(_job_id, _source_type, _result):
                reload_calls["n"] += 1
                return {
                    "status": "ok",
                    "engine_generation": 8,
                    "engine_loaded_at": "2026-02-27T10:02:00+00:00",
                }

            store, orchestrator, runner = self._build_runner(tmp_path, on_success=on_success)
            status = orchestrator.enqueue_web(
                WebIngestRequest(url="https://example.com/docs", department="sistemas", ingest_label="online_web")
            )
            stub = self._stub_result(tmp_path, status.job_id)
            script_calls: list[str] = []

            def fake_run_command(args: list[str], **kwargs):
                script_name = Path(args[1]).name
                script_calls.append(script_name)
                if script_name == "06_merge_datasets.py":
                    out_path = Path(self._arg_value(args, "--out"))
                    delta_path = Path(self._arg_value(args, "--out-delta"))
                    summary_path = Path(self._arg_value(args, "--summary-out"))
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    out_path.write_text('{"text":"merged"}\n', encoding="utf-8")
                    delta_path.parent.mkdir(parents=True, exist_ok=True)
                    delta_path.write_text('{"text":"delta"}\n', encoding="utf-8")
                    summary_path.parent.mkdir(parents=True, exist_ok=True)
                    summary_path.write_text(json.dumps({"out_rows": 22, "delta_rows": 2}), encoding="utf-8")
                    return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
                if script_name == "10_incremental_faiss_append.py":
                    self.assertEqual(kwargs.get("cwd"), str(tmp_path))
                    env = kwargs.get("env") or {}
                    self.assertIn("PYTHONPATH", env)
                    self.assertIn(str(tmp_path), env.get("PYTHONPATH", ""))
                    summary_path = Path(self._arg_value(args, "--summary-out"))
                    summary_path.parent.mkdir(parents=True, exist_ok=True)
                    summary_path.write_text(
                        json.dumps(
                            {
                                "applied": True,
                                "reason": "ok",
                                "faiss_dir": str(tmp_path / "data" / "index" / "faiss"),
                                "fallback_used": False,
                            }
                        ),
                        encoding="utf-8",
                    )
                    return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
                raise AssertionError(f"Unexpected command: {args}")

            with (
                patch.object(runner, "_run_web_job", return_value=stub),
                patch("ingest.runner.run_command", side_effect=fake_run_command),
                patch(
                    "ingest.runner.rebuild_source_catalog",
                    return_value=self._catalog_snapshot(tmp_path),
                ) as catalog_refresh_mock,
            ):
                runner._run_job(status.job_id, IngestSourceType.WEB, None)

            final = orchestrator.get_job(status.job_id)
            assert final is not None and final.result is not None
            self.assertEqual(final.state, IngestState.SUCCEEDED)
            self.assertEqual(reload_calls["n"], 1)
            self.assertEqual(final.result.merged_rows, 22)
            self.assertEqual(final.result.delta_rows, 2)
            self.assertTrue(final.result.index_updated)
            self.assertEqual(final.result.output_index_path, str(tmp_path / "data" / "index" / "faiss"))
            self.assertEqual(final.result.reload_metadata["engine_generation"], 8)
            self.assertEqual(final.result.source_catalog_refresh_metadata["status"], "ok")
            self.assertEqual(final.result.source_catalog_refresh_metadata["trigger"], "after_merge")
            self.assertEqual(final.result.web_job_cleanup_metadata["status"], "ok")
            self.assertEqual(final.result.web_job_cleanup_metadata["trigger"], "after_merge")
            self.assertEqual(
                final.result.web_job_cleanup_metadata["deleted_job_root"],
                str(tmp_path / "data" / "raw_site" / "jobs" / status.job_id),
            )
            self.assertFalse((tmp_path / "data" / "raw_site" / "jobs" / status.job_id).exists())
            self.assertTrue((tmp_path / "reports" / status.job_id / "artifacts" / "merge_summary.json").exists())
            self.assertEqual(script_calls.count("06_merge_datasets.py"), 1)
            self.assertEqual(script_calls.count("10_incremental_faiss_append.py"), 1)
            catalog_refresh_mock.assert_called_once()
            logs = self._job_logs(store, status.job_id)
            refresh_log = next((entry for entry in logs if entry.get("message") == "Source catalog refresh stage completed"), None)
            self.assertIsNotNone(refresh_log)
            assert refresh_log is not None
            self.assertEqual(refresh_log["extra"]["stage"], "source_catalog_refresh")
            self.assertEqual(refresh_log["extra"]["status"], "ok")
            cleanup_log = next((entry for entry in logs if entry.get("message") == "Web job cleanup stage completed"), None)
            self.assertIsNotNone(cleanup_log)
            assert cleanup_log is not None
            self.assertEqual(cleanup_log["extra"]["stage"], "web_job_cleanup")
            self.assertEqual(cleanup_log["extra"]["status"], "ok")
            self.assertEqual(cleanup_log["extra"]["trigger"], "after_merge")

    def test_online_web_path_merge_failure_does_not_cleanup_raw_job_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            _store, orchestrator, runner = self._build_runner(tmp_path, on_success=None)
            status = orchestrator.enqueue_web(
                WebIngestRequest(url="https://example.com/docs", department="sistemas", ingest_label="online_web")
            )
            stub = self._stub_result(tmp_path, status.job_id)
            raw_job_root = tmp_path / "data" / "raw_site" / "jobs" / status.job_id

            def fake_run_command(args: list[str], **kwargs):
                script_name = Path(args[1]).name
                if script_name == "06_merge_datasets.py":
                    raise RuntimeError("merge boom")
                raise AssertionError(f"Unexpected command: {args}")

            with (
                patch.object(runner, "_run_web_job", return_value=stub),
                patch("ingest.runner.run_command", side_effect=fake_run_command),
                patch("ingest.runner.rebuild_source_catalog") as catalog_refresh_mock,
                patch("ingest.runner.cleanup_web_job_artifacts") as cleanup_mock,
            ):
                runner._run_job(status.job_id, IngestSourceType.WEB, None)

            final = orchestrator.get_job(status.job_id)
            assert final is not None
            self.assertEqual(final.state, IngestState.FAILED)
            self.assertEqual(final.stage, "merge")
            self.assertTrue(raw_job_root.exists())
            catalog_refresh_mock.assert_not_called()
            cleanup_mock.assert_not_called()

    def test_online_web_path_cleanup_disabled_logs_skip_and_keeps_raw_job_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            store, orchestrator, runner = self._build_runner(tmp_path, on_success=None)
            status = orchestrator.enqueue_web(
                WebIngestRequest(url="https://example.com/docs", department="sistemas", ingest_label="online_web")
            )
            stub = self._stub_result(tmp_path, status.job_id)
            raw_job_root = tmp_path / "data" / "raw_site" / "jobs" / status.job_id

            def fake_run_command(args: list[str], **kwargs):
                script_name = Path(args[1]).name
                if script_name == "06_merge_datasets.py":
                    out_path = Path(self._arg_value(args, "--out"))
                    delta_path = Path(self._arg_value(args, "--out-delta"))
                    summary_path = Path(self._arg_value(args, "--summary-out"))
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    out_path.write_text('{"text":"merged"}\n', encoding="utf-8")
                    delta_path.parent.mkdir(parents=True, exist_ok=True)
                    delta_path.write_text('{"text":"delta"}\n', encoding="utf-8")
                    summary_path.parent.mkdir(parents=True, exist_ok=True)
                    summary_path.write_text(json.dumps({"out_rows": 22, "delta_rows": 2}), encoding="utf-8")
                    return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
                if script_name == "10_incremental_faiss_append.py":
                    summary_path = Path(self._arg_value(args, "--summary-out"))
                    summary_path.parent.mkdir(parents=True, exist_ok=True)
                    summary_path.write_text(
                        json.dumps(
                            {
                                "applied": True,
                                "reason": "ok",
                                "faiss_dir": str(tmp_path / "data" / "index" / "faiss"),
                                "fallback_used": False,
                            }
                        ),
                        encoding="utf-8",
                    )
                    return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
                raise AssertionError(f"Unexpected command: {args}")

            with (
                patch.object(runner, "_run_web_job", return_value=stub),
                patch.object(
                    runner,
                    "_resolve_web_job_cleanup_settings",
                    return_value=(
                        tmp_path / "data" / "raw_site" / "jobs",
                        False,
                        "after_merge",
                        tmp_path / "data" / "datasetFinalV2.jsonl",
                    ),
                ),
                patch("ingest.runner.run_command", side_effect=fake_run_command),
                patch("ingest.runner.cleanup_web_job_artifacts") as cleanup_mock,
            ):
                runner._run_job(status.job_id, IngestSourceType.WEB, None)

            final = orchestrator.get_job(status.job_id)
            assert final is not None and final.result is not None
            self.assertEqual(final.state, IngestState.SUCCEEDED)
            self.assertEqual(final.result.web_job_cleanup_metadata["status"], "skipped")
            self.assertEqual(final.result.web_job_cleanup_metadata["reason"], "cleanup_disabled")
            self.assertTrue(raw_job_root.exists())
            cleanup_mock.assert_not_called()
            logs = self._job_logs(store, status.job_id)
            cleanup_log = next((entry for entry in logs if entry.get("message") == "Web job cleanup stage skipped"), None)
            self.assertIsNotNone(cleanup_log)
            assert cleanup_log is not None
            self.assertEqual(cleanup_log["extra"]["stage"], "web_job_cleanup")
            self.assertEqual(cleanup_log["extra"]["status"], "skipped")
            self.assertEqual(cleanup_log["extra"]["reason"], "cleanup_disabled")

    def test_online_path_delta_empty_skips_append_and_still_reloads(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            reload_calls = {"n": 0}

            def on_success(_job_id, _source_type, _result):
                reload_calls["n"] += 1
                return {"status": "ok", "engine_generation": 9}

            _store, orchestrator, runner = self._build_runner(tmp_path, on_success=on_success)
            status = orchestrator.enqueue_pdf(
                PdfIngestRequest(department="sistemas", ingest_label="online_pdf", original_filename="doc.pdf")
            )
            stub = self._stub_result(tmp_path, status.job_id)
            script_calls: list[str] = []

            def fake_run_command(args: list[str], **kwargs):
                script_name = Path(args[1]).name
                script_calls.append(script_name)
                if script_name == "06_merge_datasets.py":
                    out_path = Path(self._arg_value(args, "--out"))
                    summary_path = Path(self._arg_value(args, "--summary-out"))
                    delta_path = Path(self._arg_value(args, "--out-delta"))
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    out_path.write_text("", encoding="utf-8")
                    delta_path.parent.mkdir(parents=True, exist_ok=True)
                    delta_path.write_text("", encoding="utf-8")
                    summary_path.parent.mkdir(parents=True, exist_ok=True)
                    summary_path.write_text(json.dumps({"out_rows": 12, "delta_rows": 0}), encoding="utf-8")
                    return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
                if script_name == "10_incremental_faiss_append.py":
                    raise AssertionError("Append script should not run when delta is empty")
                raise AssertionError(f"Unexpected command: {args}")

            with (
                patch.object(runner, "_run_pdf_job", return_value=stub),
                patch("ingest.runner.run_command", side_effect=fake_run_command),
                patch("ingest.runner.rebuild_source_catalog") as catalog_refresh_mock,
            ):
                runner._run_job(status.job_id, IngestSourceType.PDF, None)

            final = orchestrator.get_job(status.job_id)
            assert final is not None and final.result is not None
            self.assertEqual(final.state, IngestState.SUCCEEDED)
            self.assertFalse(final.result.index_updated)
            self.assertEqual(final.result.delta_rows, 0)
            self.assertEqual(final.result.source_catalog_refresh_metadata["status"], "skipped")
            self.assertEqual(final.result.source_catalog_refresh_metadata["reason"], "delta_empty_no_mutation")
            self.assertEqual(final.result.stage_metrics["index_append"]["status"], "no_op_candidate")
            self.assertEqual(reload_calls["n"], 1)
            self.assertEqual(script_calls.count("06_merge_datasets.py"), 1)
            self.assertEqual(script_calls.count("10_incremental_faiss_append.py"), 0)
            catalog_refresh_mock.assert_not_called()

    def test_online_path_append_failure_marks_job_failed_without_reload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            reload_calls = {"n": 0}

            def on_success(_job_id, _source_type, _result):
                reload_calls["n"] += 1
                return {"status": "ok"}

            _store, orchestrator, runner = self._build_runner(tmp_path, on_success=on_success)
            status = orchestrator.enqueue_pdf(
                PdfIngestRequest(department="sistemas", ingest_label="online_pdf", original_filename="doc.pdf")
            )
            stub = self._stub_result(tmp_path, status.job_id)

            def fake_run_command(args: list[str], **kwargs):
                script_name = Path(args[1]).name
                if script_name == "06_merge_datasets.py":
                    out_path = Path(self._arg_value(args, "--out"))
                    summary_path = Path(self._arg_value(args, "--summary-out"))
                    delta_path = Path(self._arg_value(args, "--out-delta"))
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    out_path.write_text("", encoding="utf-8")
                    delta_path.parent.mkdir(parents=True, exist_ok=True)
                    delta_path.write_text('{"text":"delta"}\n', encoding="utf-8")
                    summary_path.parent.mkdir(parents=True, exist_ok=True)
                    summary_path.write_text(json.dumps({"out_rows": 20, "delta_rows": 1}), encoding="utf-8")
                    return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
                if script_name == "10_incremental_faiss_append.py":
                    raise RuntimeError("append failed")
                raise AssertionError(f"Unexpected command: {args}")

            with (
                patch.object(runner, "_run_pdf_job", return_value=stub),
                patch("ingest.runner.run_command", side_effect=fake_run_command),
                patch("ingest.runner.prune_index_backups") as prune_mock,
            ):
                runner._run_job(status.job_id, IngestSourceType.PDF, None)

            final = orchestrator.get_job(status.job_id)
            assert final is not None
            self.assertEqual(final.state, IngestState.FAILED)
            self.assertEqual(final.stage, "index_append")
            self.assertIn("RuntimeError", final.error or "")
            self.assertIn("append failed", final.error or "")
            self.assertEqual(reload_calls["n"], 0)
            prune_mock.assert_not_called()

    def test_online_path_reload_failure_skips_backup_prune(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)

            def failing_reload(_job_id, _source_type, _result):
                raise RuntimeError("reload boom")

            _store, orchestrator, runner = self._build_runner(tmp_path, on_success=failing_reload)
            status = orchestrator.enqueue_pdf(
                PdfIngestRequest(department="sistemas", ingest_label="online_pdf", original_filename="doc.pdf")
            )
            stub = self._stub_result(tmp_path, status.job_id)

            def fake_run_command(args: list[str], **kwargs):
                script_name = Path(args[1]).name
                if script_name == "06_merge_datasets.py":
                    out_path = Path(self._arg_value(args, "--out"))
                    summary_path = Path(self._arg_value(args, "--summary-out"))
                    delta_path = Path(self._arg_value(args, "--out-delta"))
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    out_path.write_text("", encoding="utf-8")
                    delta_path.parent.mkdir(parents=True, exist_ok=True)
                    delta_path.write_text('{"text":"delta"}\n', encoding="utf-8")
                    summary_path.parent.mkdir(parents=True, exist_ok=True)
                    summary_path.write_text(json.dumps({"out_rows": 20, "delta_rows": 1}), encoding="utf-8")
                    return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
                if script_name == "10_incremental_faiss_append.py":
                    summary_path = Path(self._arg_value(args, "--summary-out"))
                    summary_path.parent.mkdir(parents=True, exist_ok=True)
                    summary_path.write_text(
                        json.dumps(
                            {
                                "applied": True,
                                "reason": "ok",
                                "faiss_dir": str(tmp_path / "data" / "index" / "faiss"),
                                "fallback_used": False,
                            }
                        ),
                        encoding="utf-8",
                    )
                    return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
                raise AssertionError(f"Unexpected command: {args}")

            with (
                patch.object(runner, "_run_pdf_job", return_value=stub),
                patch("ingest.runner.run_command", side_effect=fake_run_command),
                patch("ingest.runner.prune_index_backups") as prune_mock,
            ):
                runner._run_job(status.job_id, IngestSourceType.PDF, None)

            final = orchestrator.get_job(status.job_id)
            assert final is not None
            self.assertEqual(final.state, IngestState.FAILED)
            self.assertEqual(final.stage, "reload")
            self.assertIn("reload boom", final.error or "")
            prune_mock.assert_not_called()

    def test_online_path_serializes_merge_append_reload_between_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            active = {"count": 0, "max": 0}
            tracker_lock = threading.Lock()

            def track_critical(_label: str) -> None:
                with tracker_lock:
                    active["count"] += 1
                    active["max"] = max(active["max"], active["count"])
                time.sleep(0.03)
                with tracker_lock:
                    active["count"] -= 1

            def on_success(_job_id, _source_type, _result):
                track_critical("reload")
                return {"status": "ok", "engine_generation": 10}

            _store, orchestrator, runner = self._build_runner(tmp_path, on_success=on_success)
            status_a = orchestrator.enqueue_pdf(
                PdfIngestRequest(department="sistemas", ingest_label="batch_a", original_filename="a.pdf")
            )
            status_b = orchestrator.enqueue_pdf(
                PdfIngestRequest(department="sistemas", ingest_label="batch_b", original_filename="b.pdf")
            )

            result_map = {
                status_a.job_id: self._stub_result(tmp_path, status_a.job_id),
                status_b.job_id: self._stub_result(tmp_path, status_b.job_id),
            }

            def fake_run_pdf_job(job_id: str, _staged_path):
                return result_map[job_id]

            def fake_run_command(args: list[str], **kwargs):
                script_name = Path(args[1]).name
                if script_name == "06_merge_datasets.py":
                    out_path = Path(self._arg_value(args, "--out"))
                    summary_path = Path(self._arg_value(args, "--summary-out"))
                    delta_path = Path(self._arg_value(args, "--out-delta"))
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    out_path.write_text("", encoding="utf-8")
                    delta_path.parent.mkdir(parents=True, exist_ok=True)
                    delta_path.write_text('{"text":"delta"}\n', encoding="utf-8")
                    summary_path.parent.mkdir(parents=True, exist_ok=True)
                    summary_path.write_text(json.dumps({"out_rows": 30, "delta_rows": 1}), encoding="utf-8")
                    track_critical("merge")
                    return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
                if script_name == "10_incremental_faiss_append.py":
                    summary_path = Path(self._arg_value(args, "--summary-out"))
                    summary_path.parent.mkdir(parents=True, exist_ok=True)
                    summary_path.write_text(
                        json.dumps(
                            {
                                "applied": True,
                                "reason": "ok",
                                "faiss_dir": str(tmp_path / "data" / "index" / "faiss"),
                                "fallback_used": False,
                            }
                        ),
                        encoding="utf-8",
                    )
                    track_critical("append")
                    return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")
                raise AssertionError(f"Unexpected command: {args}")

            with (
                patch.object(runner, "_run_pdf_job", side_effect=fake_run_pdf_job),
                patch("ingest.runner.run_command", side_effect=fake_run_command),
            ):
                t1 = threading.Thread(target=runner._run_job, args=(status_a.job_id, IngestSourceType.PDF, None))
                t2 = threading.Thread(target=runner._run_job, args=(status_b.job_id, IngestSourceType.PDF, None))
                t1.start()
                t2.start()
                t1.join(timeout=3)
                t2.join(timeout=3)

            final_a = orchestrator.get_job(status_a.job_id)
            final_b = orchestrator.get_job(status_b.job_id)
            assert final_a is not None and final_b is not None
            self.assertEqual(final_a.state, IngestState.SUCCEEDED)
            self.assertEqual(final_b.state, IngestState.SUCCEEDED)
            self.assertEqual(active["max"], 1)


if __name__ == "__main__":
    unittest.main()
