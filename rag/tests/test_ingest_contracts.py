from __future__ import annotations

import tempfile
import unittest

from pydantic import ValidationError

from RAG_v1 import load_config
from ingest.contracts import (
    HpcExecutionMetadata,
    IngestJobResultSummary,
    IngestJobStatusResponse,
    IngestSourceType,
    IngestState,
    PdfIngestRequest,
    PurgeDepartmentRequest,
    REPO_DOCS_SUPPORT_MATRIX,
    RepoDocsDocKind,
    RepoDocsIngestRequest,
    RepoDocsProvider,
    RtWeeklyIngestRequest,
    WebIngestRequest,
    classify_repo_docs_url,
    repo_docs_support_matrix,
)
from ingest.engine_manager import EngineManager
from ingest.pdf_pipeline import build_pdf_pipeline_paths
from ingest.security import validate_public_http_url
from ingest.web_pipeline import build_web_pipeline_paths


class IngestContractsTests(unittest.TestCase):
    def test_web_request_accepts_public_url(self) -> None:
        payload = WebIngestRequest(
            url="https://example.com/docs/index.html",
            department="sistemas",
        )
        self.assertEqual(payload.department, "sistemas")
        self.assertIsNone(payload.ingest_label)

    def test_web_request_accepts_custom_department(self) -> None:
        payload = WebIngestRequest(
            url="https://example.com/docs/index.html",
            department="Data Science Team",
        )
        self.assertEqual(payload.department, "data_science_team")
        self.assertIsNone(payload.ingest_label)

    def test_web_request_accepts_custom_department_with_ascii_normalization(self) -> None:
        payload = WebIngestRequest(
            url="https://example.com/docs/index.html",
            department="Plataforma Óps Team",
        )
        self.assertEqual(payload.department, "plataforma_ops_team")
        self.assertIsNone(payload.ingest_label)

    def test_pdf_request_accepts_custom_department(self) -> None:
        payload = PdfIngestRequest(
            department="MLOps-Team",
            source_url="https://example.com/manual.pdf",
        )
        self.assertEqual(payload.department, "mlops-team")
        self.assertIsNone(payload.ingest_label)

    def test_request_accepts_optional_ingest_label(self) -> None:
        payload = WebIngestRequest(
            url="https://example.com/docs/index.html",
            department="sistemas",
            ingest_label="  batch-a  ",
        )
        self.assertEqual(payload.ingest_label, "batch-a")

    def test_web_request_rejects_empty_department(self) -> None:
        with self.assertRaises(ValidationError):
            WebIngestRequest(
                url="https://example.com",
                department="   ",
            )

    def test_web_request_rejects_unsafe_department(self) -> None:
        with self.assertRaises(ValidationError):
            WebIngestRequest(
                url="https://example.com",
                department="team$ops",
            )

    def test_pdf_request_rejects_unsafe_department(self) -> None:
        with self.assertRaises(ValidationError):
            PdfIngestRequest(
                department="team$ops",
                source_url="https://example.com/manual.pdf",
            )

    def test_purge_request_normalizes_department_and_defaults_dry_run(self) -> None:
        payload = PurgeDepartmentRequest(
            department="Data Science Team",
            confirm=True,
        )
        self.assertEqual(payload.department, "data_science_team")
        self.assertTrue(payload.confirm)
        self.assertFalse(payload.dry_run)

    def test_purge_request_requires_confirm_true(self) -> None:
        with self.assertRaises(ValidationError):
            PurgeDepartmentRequest(
                department="sistemas",
                confirm=False,
            )

    def test_purge_request_requires_confirm_field(self) -> None:
        with self.assertRaises(ValidationError):
            PurgeDepartmentRequest(
                department="sistemas",
            )

    def test_purge_request_rejects_unsafe_department(self) -> None:
        with self.assertRaises(ValidationError):
            PurgeDepartmentRequest(
                department="ops$team",
                confirm=True,
            )

    def test_rt_weekly_request_accepts_optional_values(self) -> None:
        payload = RtWeeklyIngestRequest(
            overlap_hours=12,
            ingest_label="  weekly-rt  ",
        )
        self.assertEqual(payload.overlap_hours, 12)
        self.assertEqual(payload.ingest_label, "weekly-rt")

    def test_rt_weekly_request_rejects_invalid_overlap(self) -> None:
        with self.assertRaises(ValidationError):
            RtWeeklyIngestRequest(overlap_hours=1000)

    def test_web_request_rejects_too_long_department(self) -> None:
        with self.assertRaises(ValidationError):
            WebIngestRequest(
                url="https://example.com",
                department="a" * 33,
            )

    def test_pdf_request_rejects_too_long_department(self) -> None:
        with self.assertRaises(ValidationError):
            PdfIngestRequest(
                department="a" * 33,
                source_url="https://example.com/manual.pdf",
            )

    def test_legacy_canonical_departments_stay_valid(self) -> None:
        web_payload = WebIngestRequest(
            url="https://example.com",
            department="BIG DATA",
        )
        self.assertEqual(web_payload.department, "bigdata")
        pdf_payload = PdfIngestRequest(
            department="aplicaciones",
            source_url="https://example.com/manual.pdf",
        )
        self.assertEqual(pdf_payload.department, "aplicaciones")

    def test_slurm_canonical_department_is_valid(self) -> None:
        payload = WebIngestRequest(
            url="https://example.com/slurm",
            department="SLURM",
        )
        self.assertEqual(payload.department, "slurm")

    def test_web_request_rejects_localhost_url(self) -> None:
        with self.assertRaises(ValidationError):
            WebIngestRequest(
                url="http://127.0.0.1/private",
                department="sistemas",
            )

    def test_repo_docs_request_accepts_github_readme_url(self) -> None:
        payload = RepoDocsIngestRequest(
            url="https://github.com/ACEsuit/mace/blob/main/README.md",
            department="sistemas",
            ingest_label="  repo docs  ",
        )
        self.assertEqual(payload.department, "sistemas")
        self.assertEqual(payload.ingest_label, "repo docs")

    def test_classify_repo_docs_url_accepts_github_readme(self) -> None:
        classified = classify_repo_docs_url("https://github.com/ACEsuit/mace/blob/main/README.md")
        self.assertEqual(classified.provider, RepoDocsProvider.GITHUB)
        self.assertEqual(classified.doc_kind, RepoDocsDocKind.README)
        self.assertEqual(classified.repo_slug, "ACEsuit/mace")
        self.assertEqual(classified.ref, "main")
        self.assertEqual(classified.doc_path, "README.md")
        self.assertEqual(classified.canonical_url, "https://github.com/ACEsuit/mace/blob/main/README.md")
        self.assertEqual(classified.acquisition_url, "https://raw.githubusercontent.com/ACEsuit/mace/main/README.md")

    def test_classify_repo_docs_url_accepts_github_wiki_root(self) -> None:
        classified = classify_repo_docs_url("https://github.com/trinityrnaseq/trinityrnaseq/wiki")
        self.assertEqual(classified.provider, RepoDocsProvider.GITHUB)
        self.assertEqual(classified.doc_kind, RepoDocsDocKind.WIKI)
        self.assertEqual(classified.repo_slug, "trinityrnaseq/trinityrnaseq")
        self.assertTrue(classified.is_wiki_root)
        self.assertIsNone(classified.wiki_page_slug)
        self.assertEqual(classified.canonical_url, "https://github.com/trinityrnaseq/trinityrnaseq/wiki")
        self.assertEqual(classified.acquisition_url, classified.canonical_url)

    def test_classify_repo_docs_url_accepts_github_wiki_page(self) -> None:
        classified = classify_repo_docs_url("https://github.com/trinityrnaseq/trinityrnaseq/wiki/Installing-Trinity")
        self.assertEqual(classified.provider, RepoDocsProvider.GITHUB)
        self.assertEqual(classified.doc_kind, RepoDocsDocKind.WIKI)
        self.assertEqual(classified.wiki_page_slug, "Installing-Trinity")
        self.assertFalse(classified.is_wiki_root)
        self.assertEqual(
            classified.canonical_url,
            "https://github.com/trinityrnaseq/trinityrnaseq/wiki/Installing-Trinity",
        )

    def test_classify_repo_docs_url_accepts_gitlab_readme(self) -> None:
        classified = classify_repo_docs_url("https://gitlab.com/example-group/demo-project/-/blob/main/README.md")
        self.assertEqual(classified.provider, RepoDocsProvider.GITLAB)
        self.assertEqual(classified.doc_kind, RepoDocsDocKind.README)
        self.assertEqual(classified.repo_slug, "example-group/demo-project")
        self.assertEqual(classified.ref, "main")
        self.assertEqual(classified.doc_path, "README.md")
        self.assertEqual(
            classified.canonical_url,
            "https://gitlab.com/example-group/demo-project/-/blob/main/README.md",
        )
        self.assertEqual(
            classified.acquisition_url,
            "https://gitlab.com/example-group/demo-project/-/raw/main/README.md",
        )

    def test_classify_repo_docs_url_accepts_gitlab_wiki_root(self) -> None:
        classified = classify_repo_docs_url("https://gitlab.com/example-group/demo-project/-/wikis")
        self.assertEqual(classified.provider, RepoDocsProvider.GITLAB)
        self.assertEqual(classified.doc_kind, RepoDocsDocKind.WIKI)
        self.assertEqual(classified.repo_slug, "example-group/demo-project")
        self.assertTrue(classified.is_wiki_root)
        self.assertEqual(classified.canonical_url, "https://gitlab.com/example-group/demo-project/-/wikis")

    def test_classify_repo_docs_url_accepts_gitlab_wiki_page(self) -> None:
        classified = classify_repo_docs_url("https://gitlab.com/example-group/demo-project/-/wikis/getting-started")
        self.assertEqual(classified.provider, RepoDocsProvider.GITLAB)
        self.assertEqual(classified.doc_kind, RepoDocsDocKind.WIKI)
        self.assertEqual(classified.wiki_page_slug, "getting-started")
        self.assertFalse(classified.is_wiki_root)
        self.assertEqual(
            classified.canonical_url,
            "https://gitlab.com/example-group/demo-project/-/wikis/getting-started",
        )

    def test_classify_repo_docs_url_normalizes_away_query_and_fragment(self) -> None:
        classified = classify_repo_docs_url(
            "https://github.com/ACEsuit/mace/blob/main/README.md?plain=1#intro"
        )
        self.assertEqual(classified.canonical_url, "https://github.com/ACEsuit/mace/blob/main/README.md")
        self.assertEqual(classified.acquisition_url, "https://raw.githubusercontent.com/ACEsuit/mace/main/README.md")

    def test_repo_docs_request_rejects_unsupported_repo_url(self) -> None:
        with self.assertRaises(ValidationError) as ctx:
            RepoDocsIngestRequest(
                url="https://github.com/ACEsuit/mace/issues/1",
                department="sistemas",
            )
        self.assertIn("Unsupported GitHub repository documentation URL", str(ctx.exception))

    def test_repo_docs_request_rejects_unsupported_host(self) -> None:
        with self.assertRaises(ValidationError) as ctx:
            RepoDocsIngestRequest(
                url="https://bitbucket.org/example/workspace/src/main/README.md",
                department="sistemas",
            )
        self.assertIn("Only github.com and gitlab.com repository documentation URLs are currently supported", str(ctx.exception))

    def test_repo_docs_request_preserves_public_url_safety_validation(self) -> None:
        with self.assertRaises(ValidationError) as ctx:
            RepoDocsIngestRequest(
                url="http://127.0.0.1/private/README.md",
                department="sistemas",
            )
        self.assertIn("URL host is blocked", str(ctx.exception))

    def test_repo_docs_support_matrix_lists_supported_provider_shapes(self) -> None:
        matrix = repo_docs_support_matrix()
        self.assertEqual(matrix, REPO_DOCS_SUPPORT_MATRIX)
        self.assertEqual(len(matrix), 6)
        self.assertTrue(any(item["provider"] == "github" and item["doc_kind"] == "readme" for item in matrix))
        self.assertTrue(any(item["provider"] == "github" and item["doc_kind"] == "wiki" for item in matrix))
        self.assertTrue(any(item["provider"] == "gitlab" and item["doc_kind"] == "readme" for item in matrix))
        self.assertTrue(any(item["provider"] == "gitlab" and item["doc_kind"] == "wiki" for item in matrix))

    def test_validate_public_http_url_blocks_private_ip(self) -> None:
        with self.assertRaises(ValueError):
            validate_public_http_url("http://10.0.0.8/docs")

    def test_status_schema_validates_iso_dates(self) -> None:
        status = IngestJobStatusResponse(
            job_id="ing_123",
            source_type=IngestSourceType.WEB,
            state=IngestState.QUEUED,
            created_at="2026-02-26T12:00:00+00:00",
            result=IngestJobResultSummary(),
        )
        self.assertEqual(status.stage, "queued")

    def test_status_schema_accepts_rt_weekly_source_type(self) -> None:
        status = IngestJobStatusResponse(
            job_id="ing_weekly",
            source_type=IngestSourceType.RT_WEEKLY,
            state=IngestState.QUEUED,
            created_at="2026-03-30T10:00:00+00:00",
        )
        self.assertEqual(status.source_type, IngestSourceType.RT_WEEKLY)

    def test_result_summary_accepts_hpc_execution_metadata(self) -> None:
        result = IngestJobResultSummary(
            hpc_execution=HpcExecutionMetadata(
                mode="hpc",
                request_command="compute -c 32 --mem 32G --gpu",
                state="running_remote",
                requested_at="2026-03-30T10:00:00+00:00",
            )
        )
        self.assertIsNotNone(result.hpc_execution)
        assert result.hpc_execution is not None
        self.assertEqual(result.hpc_execution.mode, "hpc")
        self.assertEqual(result.hpc_execution.state, "running_remote")

    def test_status_schema_rejects_bad_dates(self) -> None:
        with self.assertRaises(ValidationError):
            IngestJobStatusResponse(
                job_id="ing_123",
                source_type=IngestSourceType.WEB,
                state=IngestState.QUEUED,
                created_at="bad-date",
            )


class PipelinePathGenerationTests(unittest.TestCase):
    def test_web_pipeline_path_generation(self) -> None:
        paths = build_web_pipeline_paths("ing_abc", base_root="/tmp/rag_jobs")
        self.assertEqual(str(paths.job_root), "/tmp/rag_jobs/ing_abc")
        self.assertTrue(str(paths.chunked_jsonl).endswith("/ing_abc/chunked/raw_site_pages_chunked.jsonl"))

    def test_pdf_pipeline_path_generation(self) -> None:
        paths = build_pdf_pipeline_paths("ing_pdf", base_root="/tmp/rag_jobs")
        self.assertEqual(str(paths.pdf_input_root), "/tmp/rag_jobs/ing_pdf/pdf_input")
        self.assertTrue(str(paths.pdf_pages_jsonl).endswith("/ing_pdf/extract/pdf_pages.jsonl"))


class ConfigParsingTests(unittest.TestCase):
    def test_load_config_reads_web_job_cleanup_settings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = build_web_pipeline_paths("cfg", base_root=tmp).job_root / "rag.yaml"
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(
                "\n".join(
                    [
                        "retrieval:",
                        "  dataset_path: data/datasetFinalV2.jsonl",
                        "  faiss_dir: data/index/faiss_v2",
                        "fusion:",
                        "  semantic_weight: 0.8",
                        "  lexical_weight: 0.2",
                        "  rrf_k: 60",
                        "web_job_cleanup:",
                        "  enabled: false",
                        "  trigger: after_merge",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            cfg = load_config(str(config_path))

            self.assertFalse(cfg.web_job_cleanup_enabled)
            self.assertEqual(cfg.web_job_cleanup_trigger, "after_merge")


class EngineManagerTests(unittest.TestCase):
    def test_placeholder_engine_manager_lazy_load(self) -> None:
        calls = {"count": 0}

        def loader():
            calls["count"] += 1
            return {"ok": True}

        manager = EngineManager(loader=loader)
        self.assertFalse(manager.health()["engine_loaded"])
        first = manager.get_engine()
        second = manager.get_engine()
        self.assertEqual(first, {"ok": True})
        self.assertEqual(second, {"ok": True})
        self.assertEqual(calls["count"], 1)


if __name__ == "__main__":
    unittest.main()
