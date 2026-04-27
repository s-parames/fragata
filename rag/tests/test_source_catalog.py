from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from ingest.source_catalog import (
    build_source_catalog_tree,
    load_source_catalog,
    load_or_rebuild_source_catalog,
    query_source_catalog,
    query_source_catalog_entries,
    rebuild_source_catalog,
    resolve_catalog_path,
    source_catalog_is_stale,
)


class SourceCatalogTests(unittest.TestCase):
    def _write_config(self, root: Path) -> Path:
        config_path = root / "config" / "rag.yaml"
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
                    "",
                ]
            ),
            encoding="utf-8",
        )
        return config_path

    def _write_dataset(self, root: Path, rows: list[dict]) -> Path:
        dataset_path = root / "data" / "datasetFinalV2.jsonl"
        dataset_path.parent.mkdir(parents=True, exist_ok=True)
        dataset_path.write_text(
            "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
            encoding="utf-8",
        )
        return dataset_path

    @staticmethod
    def _ticket_chunk(*, chunk_id: str, text: str, last_updated: str) -> dict:
        return {
            "chunk_id": chunk_id,
            "conversation_id": "conv_101",
            "ticket_id": 101,
            "source": "https://rt.cesga.es/Ticket/Display.html?id=101",
            "department": "sistemas",
            "subject": "Slurm ticket",
            "source_type": None,
            "last_updated": last_updated,
            "text": text,
        }

    @staticmethod
    def _web_chunk(*, chunk_id: str, text: str, ingest_job_id: str) -> dict:
        return {
            "chunk_id": chunk_id,
            "conversation_id": "doc_slurm_quickstart",
            "doc_id": "html_slurm_quickstart",
            "source": "https://slurm.schedmd.com/quickstart_admin.html",
            "department": "slurm",
            "source_type": "html",
            "page_title": "Slurm Quick Start",
            "ingest_job_id": ingest_job_id,
            "ingested_at": "2026-03-03 10:00:00",
            "last_updated": "2026-03-03 10:02:00",
            "text": text,
        }

    @staticmethod
    def _pdf_chunk(*, chunk_id: str, text: str) -> dict:
        return {
            "chunk_id": chunk_id,
            "conversation_id": "doc_pdf_manual",
            "doc_id": "pdf_manual_001",
            "source": "https://docs.example.com/manual.pdf",
            "department": "aplicaciones",
            "source_type": "pdf",
            "page_title": "Cluster Manual",
            "ingest_job_id": "ing_pdf_001",
            "ingested_at": "2026-03-02 09:15:00",
            "last_updated": "2026-03-02 09:15:00",
            "text": text,
        }

    def test_rebuild_source_catalog_groups_fixture_dataset_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = self._write_config(root)
            dataset_path = self._write_dataset(
                root,
                [
                    self._ticket_chunk(chunk_id="ticket_chunk_1", text="ticket text 1", last_updated="2026-03-01 08:00:00"),
                    self._ticket_chunk(chunk_id="ticket_chunk_2", text="ticket text 2", last_updated="2026-03-01 09:00:00"),
                    self._web_chunk(chunk_id="web_chunk_1", text="web text 1", ingest_job_id="ing_web_001"),
                    self._web_chunk(chunk_id="web_chunk_2", text="web text 2", ingest_job_id="ing_web_001"),
                    self._web_chunk(chunk_id="web_chunk_3", text="web text 3", ingest_job_id="ing_web_001"),
                    self._pdf_chunk(chunk_id="pdf_chunk_1", text="pdf text 1"),
                ],
            )

            snapshot = rebuild_source_catalog(config_path=str(config_path))

            self.assertEqual(snapshot.dataset_path, str(dataset_path.resolve()))
            self.assertEqual(snapshot.total_entries, 3)
            catalog_path = root / "data" / "reports" / "catalog" / "source_catalog.json"
            self.assertTrue(catalog_path.exists())

            loaded = load_source_catalog(catalog_path)
            self.assertEqual(loaded.total_entries, 3)

            by_type = {item.source_type: item for item in loaded.items}
            self.assertEqual(by_type["ticket"].chunk_count, 2)
            self.assertEqual(by_type["ticket"].title, "Slurm ticket")
            self.assertEqual(by_type["ticket"].last_updated, "2026-03-01 09:00:00")
            self.assertEqual(by_type["web"].chunk_count, 3)
            self.assertEqual(by_type["web"].title, "Slurm Quick Start")
            self.assertEqual(by_type["web"].host, "slurm.schedmd.com")
            self.assertEqual(by_type["web"].ingest_job_id, "ing_web_001")
            self.assertEqual(by_type["pdf"].chunk_count, 1)
            self.assertEqual(by_type["pdf"].title, "Cluster Manual")
            self.assertEqual(by_type["pdf"].host, "docs.example.com")

    def test_query_source_catalog_filters_by_source_type_and_department(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = self._write_config(root)
            self._write_dataset(
                root,
                [
                    self._ticket_chunk(chunk_id="ticket_chunk_1", text="ticket text 1", last_updated="2026-03-01 08:00:00"),
                    self._web_chunk(chunk_id="web_chunk_1", text="web text 1", ingest_job_id="ing_web_001"),
                    self._pdf_chunk(chunk_id="pdf_chunk_1", text="pdf text 1"),
                ],
            )

            snapshot = rebuild_source_catalog(config_path=str(config_path))

            web_only = query_source_catalog_entries(snapshot.items, source_type="html", page=1, page_size=10)
            self.assertEqual(web_only.total, 1)
            self.assertEqual(web_only.items[0].source_type, "web")

            aplicaciones_only = query_source_catalog_entries(
                snapshot.items,
                department="aplicaciones",
                page=1,
                page_size=10,
            )
            self.assertEqual(aplicaciones_only.total, 1)
            self.assertEqual(aplicaciones_only.items[0].source_type, "pdf")
            self.assertEqual(aplicaciones_only.items[0].department, "aplicaciones")

    def test_query_source_catalog_supports_pagination(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = self._write_config(root)
            self._write_dataset(
                root,
                [
                    self._ticket_chunk(chunk_id="ticket_chunk_1", text="ticket text 1", last_updated="2026-03-01 08:00:00"),
                    self._web_chunk(chunk_id="web_chunk_1", text="web text 1", ingest_job_id="ing_web_001"),
                    self._pdf_chunk(chunk_id="pdf_chunk_1", text="pdf text 1"),
                ],
            )

            rebuild_source_catalog(config_path=str(config_path))
            catalog_path = root / "data" / "reports" / "catalog" / "source_catalog.json"

            first_page = query_source_catalog(catalog_path, page=1, page_size=2)
            second_page = query_source_catalog(catalog_path, page=2, page_size=2)

            self.assertEqual(first_page.total, 3)
            self.assertEqual(len(first_page.items), 2)
            self.assertTrue(first_page.has_more)
            self.assertEqual(first_page.page, 1)
            self.assertEqual(second_page.total, 3)
            self.assertEqual(len(second_page.items), 1)
            self.assertFalse(second_page.has_more)
            self.assertEqual(second_page.page, 2)

    def test_query_source_catalog_filters_by_text_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = self._write_config(root)
            self._write_dataset(
                root,
                [
                    self._ticket_chunk(chunk_id="ticket_chunk_1", text="ticket text 1", last_updated="2026-03-01 08:00:00"),
                    self._web_chunk(chunk_id="web_chunk_1", text="web text 1", ingest_job_id="ing_web_001"),
                    self._pdf_chunk(chunk_id="pdf_chunk_1", text="pdf text 1"),
                ],
            )

            rebuild_source_catalog(config_path=str(config_path))
            catalog_path = root / "data" / "reports" / "catalog" / "source_catalog.json"

            result = query_source_catalog(catalog_path, q="slurm", page=1, page_size=10)
            self.assertEqual(result.total, 2)
            self.assertEqual(
                sorted(item.source_type for item in result.items),
                ["ticket", "web"],
            )

    def test_query_source_catalog_large_pagination_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = self._write_config(root)
            rows: list[dict] = []
            for idx in range(15):
                rows.append(
                    {
                        "chunk_id": f"web_chunk_{idx}",
                        "conversation_id": f"doc_web_{idx}",
                        "doc_id": f"html_{idx}",
                        "source": f"https://docs.example.com/page-{idx}.html",
                        "department": "sistemas" if idx % 2 == 0 else "bigdata",
                        "source_type": "html",
                        "page_title": f"Page {idx}",
                        "ingest_job_id": f"ing_web_{idx:03d}",
                        "ingested_at": f"2026-03-03 10:{idx:02d}:00",
                        "last_updated": f"2026-03-03 10:{idx:02d}:30",
                        "text": f"page text {idx}",
                    }
                )
            self._write_dataset(root, rows)

            rebuild_source_catalog(config_path=str(config_path))
            catalog_path = resolve_catalog_path(config_path)

            first_page = query_source_catalog(catalog_path, page=1, page_size=6)
            third_page = query_source_catalog(catalog_path, page=3, page_size=6)

            self.assertEqual(first_page.total, 15)
            self.assertEqual(first_page.page, 1)
            self.assertEqual(len(first_page.items), 6)
            self.assertTrue(first_page.has_more)
            self.assertEqual(third_page.page, 3)
            self.assertEqual(len(third_page.items), 3)
            self.assertFalse(third_page.has_more)

    def test_catalog_groups_mixed_source_types_into_logical_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = self._write_config(root)
            self._write_dataset(
                root,
                [
                    self._ticket_chunk(chunk_id="ticket_a", text="ticket text a", last_updated="2026-03-01 08:00:00"),
                    self._ticket_chunk(chunk_id="ticket_b", text="ticket text b", last_updated="2026-03-01 09:00:00"),
                    self._web_chunk(chunk_id="web_a", text="web text a", ingest_job_id="ing_web_001"),
                    {
                        "chunk_id": "web_b",
                        "conversation_id": "doc_other_site",
                        "doc_id": "html_other_site",
                        "source": "https://docs.example.com/guide.html",
                        "department": "sistemas",
                        "source_type": "website",
                        "page_title": "Operator Guide",
                        "ingest_job_id": "ing_web_002",
                        "ingested_at": "2026-03-03 12:00:00",
                        "last_updated": "2026-03-03 12:00:00",
                        "text": "other site",
                    },
                    self._pdf_chunk(chunk_id="pdf_a", text="pdf text a"),
                ],
            )

            snapshot = rebuild_source_catalog(config_path=str(config_path))

            self.assertEqual(snapshot.total_entries, 4)
            counts = {item.source_type: 0 for item in snapshot.items}
            for item in snapshot.items:
                counts[item.source_type] = counts.get(item.source_type, 0) + 1
            self.assertEqual(counts["ticket"], 1)
            self.assertEqual(counts["web"], 2)
            self.assertEqual(counts["pdf"], 1)

    def test_query_source_catalog_no_result_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = self._write_config(root)
            self._write_dataset(
                root,
                [
                    self._ticket_chunk(chunk_id="ticket_chunk_1", text="ticket text 1", last_updated="2026-03-01 08:00:00"),
                    self._web_chunk(chunk_id="web_chunk_1", text="web text 1", ingest_job_id="ing_web_001"),
                ],
            )

            rebuild_source_catalog(config_path=str(config_path))
            catalog_path = resolve_catalog_path(config_path)

            result = query_source_catalog(catalog_path, source_type="pdf", department="aplicaciones", q="manual", page=1, page_size=10)
            self.assertEqual(result.total, 0)
            self.assertEqual(result.items, [])
            self.assertFalse(result.has_more)

    def test_build_source_catalog_tree_groups_hosts_and_tickets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = self._write_config(root)
            self._write_dataset(
                root,
                [
                    self._ticket_chunk(chunk_id="ticket_chunk_1", text="ticket text 1", last_updated="2026-03-01 08:00:00"),
                    self._web_chunk(chunk_id="web_chunk_1", text="web text 1", ingest_job_id="ing_web_001"),
                    {
                        "chunk_id": "pdf_chunk_2",
                        "conversation_id": "doc_pdf_slurm",
                        "doc_id": "pdf_slurm_manual",
                        "source": "https://slurm.schedmd.com/pdfs/manual.pdf",
                        "department": "slurm",
                        "source_type": "pdf",
                        "page_title": "Slurm Manual",
                        "ingest_job_id": "ing_pdf_002",
                        "ingested_at": "2026-03-03 11:00:00",
                        "last_updated": "2026-03-03 11:05:00",
                        "text": "pdf text 2",
                    },
                ],
            )

            snapshot = rebuild_source_catalog(config_path=str(config_path))
            tree = build_source_catalog_tree(snapshot.items)

            self.assertEqual(tree.total_groups, 2)
            self.assertEqual(tree.total_items, 3)
            self.assertEqual(tree.groups[0].label, "Slurm")
            self.assertEqual(tree.groups[0].web_count, 1)
            self.assertEqual(tree.groups[0].pdf_count, 1)
            self.assertEqual(len(tree.groups[0].children), 2)
            self.assertEqual(tree.groups[1].label, "Tickets")
            self.assertEqual(tree.groups[1].ticket_count, 1)

    def test_load_or_rebuild_source_catalog_when_artifact_missing_or_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = self._write_config(root)
            dataset_path = self._write_dataset(
                root,
                [
                    self._ticket_chunk(chunk_id="ticket_chunk_1", text="ticket text 1", last_updated="2026-03-01 08:00:00"),
                ],
            )
            catalog_path = resolve_catalog_path(config_path)

            snapshot = load_or_rebuild_source_catalog(config_path=str(config_path))
            self.assertTrue(catalog_path.exists())
            self.assertEqual(snapshot.total_entries, 1)

            stale_before = source_catalog_is_stale(dataset_path=dataset_path, catalog_path=catalog_path)
            self.assertFalse(stale_before)

            payload = {
                "chunk_id": "web_chunk_new",
                "conversation_id": "doc_stale_case",
                "doc_id": "html_stale_case",
                "source": "https://example.com/stale.html",
                "department": "sistemas",
                "source_type": "html",
                "page_title": "Stale Case",
                "text": "new content",
            }
            with dataset_path.open("a", encoding="utf-8") as dst:
                dst.write(json.dumps(payload, ensure_ascii=False) + "\n")

            self.assertTrue(source_catalog_is_stale(dataset_path=dataset_path, catalog_path=catalog_path))
            refreshed = load_or_rebuild_source_catalog(config_path=str(config_path))
            self.assertEqual(refreshed.total_entries, 2)
            self.assertFalse(source_catalog_is_stale(dataset_path=dataset_path, catalog_path=catalog_path))

    def test_rebuild_source_catalog_script_outputs_json_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = self._write_config(root)
            self._write_dataset(
                root,
                [
                    self._ticket_chunk(chunk_id="ticket_chunk_1", text="ticket text 1", last_updated="2026-03-01 08:00:00"),
                    self._web_chunk(chunk_id="web_chunk_1", text="web text 1", ingest_job_id="ing_web_001"),
                ],
            )

            script_path = Path(__file__).resolve().parents[1] / "scripts" / "14_rebuild_source_catalog.py"
            proc = subprocess.run(
                [sys.executable, str(script_path), "--config", str(config_path), "--json"],
                check=True,
                capture_output=True,
                text=True,
            )

            payload = json.loads(proc.stdout)
            self.assertEqual(payload["total_entries"], 2)
            self.assertTrue(payload["catalog_path"].endswith("data/reports/catalog/source_catalog.json"))
            self.assertTrue(Path(payload["catalog_path"]).exists())


if __name__ == "__main__":
    unittest.main()
