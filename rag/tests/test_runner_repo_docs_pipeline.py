from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ingest import (
    IngestJobResultSummary,
    IngestSourceType,
    IngestState,
    IngestionJobRunner,
    IngestionJobStore,
    IngestionOrchestrator,
    RepoDocsIngestRequest,
)


class RunnerRepoDocsPipelineTests(unittest.TestCase):
    def _build_runner(self, tmp_path: Path, *, on_success=None) -> tuple[IngestionJobStore, IngestionOrchestrator, IngestionJobRunner]:
        store = IngestionJobStore(root_dir=str(tmp_path / "reports"))
        orchestrator = IngestionOrchestrator(job_store=store)
        runner = IngestionJobRunner(orchestrator=orchestrator, max_workers=1, on_job_success=on_success)
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
                    "  retrieval_k: 30",
                    "  final_k: 10",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        return store, orchestrator, runner

    @staticmethod
    def _job_logs(store: IngestionJobStore, job_id: str) -> list[dict]:
        path = store.log_path(job_id)
        if not path.exists():
            return []
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        return [json.loads(line) for line in lines]

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

    def test_repo_docs_pipeline_uses_normalized_custom_department_in_prepare_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            store, orchestrator, runner = self._build_runner(tmp_path)
            status = orchestrator.enqueue_repo_docs(
                RepoDocsIngestRequest(
                    url="https://github.com/ACEsuit/mace/blob/main/README.md",
                    department="Platform Ops Team",
                    ingest_label="repo_fixture",
                )
            )
            self.assertEqual(status.request["department"], "platform_ops_team")

            with (
                patch(
                    "ingest.runner.acquire_repo_docs",
                    return_value={"provider": "github", "fetched_page_count": 1},
                ),
                patch(
                    "ingest.runner.run_repo_docs_prepare_stage",
                    return_value={"prepared_page_rows": 1},
                ) as prepare_mock,
                patch(
                    "ingest.runner.run_repo_docs_chunk_stage",
                    return_value={"chunk_rows": 2, "chunked_output_path": "x"},
                ),
            ):
                result = runner._run_repo_docs_job(status.job_id)

            self.assertEqual(result.chunk_rows, 2)
            self.assertIn("acquire_source", result.stage_metrics)
            self.assertIn("prepare", result.stage_metrics)
            self.assertIn("chunk", result.stage_metrics)
            self.assertTrue((result.artifacts_dir or "").endswith(f"/{status.job_id}/artifacts"))
            self.assertEqual(prepare_mock.call_args.kwargs["department"], "platform_ops_team")
            logs = self._job_logs(store, status.job_id)
            messages = [entry.get("message") for entry in logs]
            self.assertIn("Acquire source stage completed", messages)
            self.assertIn("Prepare stage completed", messages)
            self.assertIn("Chunk stage completed", messages)

    def test_online_repo_docs_path_success_runs_merge_append_reload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            reload_calls = {"n": 0}

            def on_success(_job_id, _source_type, _result):
                reload_calls["n"] += 1
                return {
                    "status": "ok",
                    "engine_generation": 11,
                    "engine_loaded_at": "2026-03-04T11:00:00+00:00",
                }

            store, orchestrator, runner = self._build_runner(tmp_path, on_success=on_success)
            status = orchestrator.enqueue_repo_docs(
                RepoDocsIngestRequest(
                    url="https://github.com/trinityrnaseq/trinityrnaseq/wiki",
                    department="sistemas",
                    ingest_label="repo_online",
                )
            )
            stub = self._stub_result(tmp_path, status.job_id)

            def fake_merge(_job_id: str, result: IngestJobResultSummary) -> None:
                result.merged_rows = 12
                result.delta_rows = 2
                result.stage_metrics["merge"] = {"status": "ok", "delta_rows": 2}

            def fake_index_append(_job_id: str, result: IngestJobResultSummary) -> None:
                result.index_updated = True
                result.stage_metrics["index_append"] = {"status": "ok", "applied": True}

            with (
                patch.object(runner, "_run_repo_docs_job", return_value=stub) as repo_job_mock,
                patch.object(runner, "_run_merge_stage", side_effect=fake_merge),
                patch.object(runner, "_run_source_catalog_refresh_stage", return_value=None),
                patch.object(runner, "_run_index_append_stage", side_effect=fake_index_append),
                patch.object(runner, "_run_backup_prune_stage", return_value=None),
            ):
                runner._run_job(status.job_id, IngestSourceType.REPO_DOCS, None)

            final = orchestrator.get_job(status.job_id)
            assert final is not None
            self.assertEqual(final.state, IngestState.SUCCEEDED)
            self.assertEqual(reload_calls["n"], 1)
            repo_job_mock.assert_called_once_with(status.job_id)
            assert final.result is not None
            self.assertEqual(final.result.stage_metrics["reload"]["status"], "ok")

    def test_repo_docs_job_failure_marks_job_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            store, orchestrator, runner = self._build_runner(tmp_path)
            status = orchestrator.enqueue_repo_docs(
                RepoDocsIngestRequest(
                    url="https://gitlab.com/example-group/demo-project/-/blob/main/README.md",
                    department="sistemas",
                )
            )

            with patch.object(runner, "_run_repo_docs_job", side_effect=RuntimeError("boom")):
                runner._run_job(status.job_id, IngestSourceType.REPO_DOCS, None)

            final = orchestrator.get_job(status.job_id)
            assert final is not None
            self.assertEqual(final.state, IngestState.FAILED)
            self.assertIn("boom", final.error or "")
            logs = self._job_logs(store, status.job_id)
            self.assertTrue(any(entry.get("message") == "Job failed" for entry in logs))


if __name__ == "__main__":
    unittest.main()
