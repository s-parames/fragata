from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from RAG_v1 import metadata_func


ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, rel_path: str):
    module_path = ROOT / rel_path
    spec = importlib.util.spec_from_file_location(name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


merge_mod = load_module("merge_datasets_mod", "scripts/06_merge_datasets.py")
purge_mod = load_module("purge_department_dataset_mod", "scripts/11_purge_department_dataset.py")


def sample_row(text: str, *, ingest_label: str = "batchA", ingest_job_id: str = "ing_1") -> dict:
    return {
        "source": "https://example.com/doc",
        "chunk_id_original": "doc_chunk_001",
        "chunk_id": "doc_chunk_001",
        "conversation_id": "doc_123",
        "text": text,
        "department": "sistemas",
        "source_type": "pdf",
        "ingest_label": ingest_label,
        "ingest_job_id": ingest_job_id,
        "ingested_at": "2026-02-26 11:00:00",
    }


def sample_ticket_row(ticket_id: int, chunk_suffix: str, text: str) -> dict:
    conversation_id = f"conv_{ticket_id}"
    chunk_id = f"{conversation_id}_chunk_{chunk_suffix}"
    return {
        "source": f"https://rt.lan.cesga.es/Ticket/Display.html?id={ticket_id}",
        "chunk_id_original": chunk_id,
        "chunk_id": chunk_id,
        "conversation_id": conversation_id,
        "ticket_id": ticket_id,
        "text": text,
        "department": "sistemas",
        "source_type": "ticket",
    }


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as dst:
        for row in rows:
            dst.write(json.dumps(row, ensure_ascii=False))
            dst.write("\n")


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


class MergeDeltaTests(unittest.TestCase):
    def test_duplicate_reingest_has_zero_delta(self) -> None:
        base = [sample_row("same")]
        new = [sample_row("same")]
        merged, delta, summary = merge_mod.merge_with_delta(base, new)
        self.assertEqual(len(merged), 1)
        self.assertEqual(len(delta), 0)
        self.assertEqual(summary["delta_rows"], 0)
        self.assertEqual(summary["unchanged_count"], 1)

    def test_changed_content_produces_delta(self) -> None:
        base = [sample_row("old text")]
        new = [sample_row("new text")]
        merged, delta, summary = merge_mod.merge_with_delta(base, new)
        self.assertEqual(len(merged), 1)
        self.assertEqual(len(delta), 1)
        self.assertEqual(summary["updated_count"], 1)
        self.assertEqual(merged[0]["text"], "new text")

    def test_metadata_survives_merge(self) -> None:
        base = [sample_row("old text", ingest_label="a", ingest_job_id="ing_old")]
        new = [sample_row("new text", ingest_label="b", ingest_job_id="ing_new")]
        merged, _, _ = merge_mod.merge_with_delta(base, new)
        self.assertEqual(merged[0]["ingest_label"], "b")
        self.assertEqual(merged[0]["ingest_job_id"], "ing_new")
        self.assertEqual(merged[0]["source_type"], "pdf")

    def test_reingest_with_only_ingest_metadata_changes_is_no_delta(self) -> None:
        base = [sample_row("same text", ingest_label="old_batch", ingest_job_id="ing_old")]
        new_row = sample_row("same text", ingest_label="new_batch", ingest_job_id="ing_new")
        new_row["ingested_at"] = "2026-02-27 09:00:00"
        merged, delta, summary = merge_mod.merge_with_delta(base, [new_row])
        self.assertEqual(len(merged), 1)
        self.assertEqual(len(delta), 0)
        self.assertEqual(summary["delta_rows"], 0)
        self.assertEqual(summary["unchanged_count"], 1)
        self.assertEqual(merged[0]["ingest_job_id"], "ing_old")

    def test_repo_docs_metadata_survives_merge(self) -> None:
        base = [sample_row("old text")]
        new_row = sample_row("new text", ingest_label="repo_batch", ingest_job_id="ing_repo_001")
        new_row.update(
            {
                "source_type": "html",
                "original_url": "https://github.com/ACEsuit/mace/blob/main/README.md",
                "canonical_url": "https://github.com/ACEsuit/mace/blob/main/README.md",
                "acquisition_url": "https://raw.githubusercontent.com/ACEsuit/mace/main/README.md",
                "repo_docs_provider": "github",
                "repo_docs_kind": "readme",
                "repo_slug": "ACEsuit/mace",
                "repo_namespace": "ACEsuit",
                "repo_name": "mace",
            }
        )
        merged, _, _ = merge_mod.merge_with_delta(base, [new_row])
        self.assertEqual(merged[0]["source_type"], "html")
        self.assertEqual(merged[0]["repo_docs_provider"], "github")
        self.assertEqual(merged[0]["repo_slug"], "ACEsuit/mace")
        self.assertEqual(merged[0]["canonical_url"], "https://github.com/ACEsuit/mace/blob/main/README.md")

    def test_ticket_rechunk_replaces_previous_ticket_rows_atomically(self) -> None:
        base = [
            sample_ticket_row(100, "001", "old chunk 1"),
            sample_ticket_row(100, "002", "old chunk 2"),
            sample_row("unrelated web doc"),
        ]
        new = [
            sample_ticket_row(100, "001", "new chunk 1"),
            sample_ticket_row(100, "003", "new chunk 3"),
        ]

        merged, delta, summary = merge_mod.merge_with_delta(base, new)
        ticket_100_rows = [row for row in merged if int(row.get("ticket_id") or -1) == 100]
        ticket_100_chunk_ids = sorted(str(row.get("chunk_id")) for row in ticket_100_rows)

        self.assertEqual(ticket_100_chunk_ids, ["conv_100_chunk_001", "conv_100_chunk_003"])
        self.assertEqual(len(delta), 2)
        self.assertEqual(summary.get("affected_ticket_groups"), 1)
        self.assertEqual(summary.get("ticket_rows_removed"), 2)

    def test_ticket_chunks_are_contiguous_after_merge_sort(self) -> None:
        base = [
            sample_ticket_row(250, "001", "ticket 250 old"),
            sample_row("non-ticket row"),
        ]
        new = [
            sample_ticket_row(100, "001", "ticket 100 chunk 1"),
            sample_ticket_row(100, "002", "ticket 100 chunk 2"),
            sample_ticket_row(250, "001", "ticket 250 updated"),
        ]

        merged, _, _ = merge_mod.merge_with_delta(base, new)
        indices = [idx for idx, row in enumerate(merged) if int(row.get("ticket_id") or -1) == 100]
        self.assertGreaterEqual(len(indices), 2)
        self.assertEqual(indices, list(range(min(indices), max(indices) + 1)))


class MergeCliArtifactTests(unittest.TestCase):
    def test_cli_writes_delta_and_summary_when_rows_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base_path = root / "base.jsonl"
            new_path = root / "new.jsonl"
            out_path = root / "out.jsonl"
            delta_path = root / "delta.jsonl"
            summary_path = root / "summary.json"

            base_rows = [sample_row("old text")]
            updated_row = sample_row("new text")
            new_row = sample_row("fresh text")
            new_row["chunk_id_original"] = "doc_chunk_002"
            new_row["chunk_id"] = "doc_chunk_002"
            new_row["conversation_id"] = "doc_124"
            write_jsonl(base_path, base_rows)
            write_jsonl(new_path, [updated_row, new_row])

            argv = [
                "06_merge_datasets.py",
                "--base",
                str(base_path),
                "--new",
                str(new_path),
                "--out",
                str(out_path),
                "--out-delta",
                str(delta_path),
                "--summary-out",
                str(summary_path),
            ]
            with patch.object(sys, "argv", argv):
                merge_mod.main()

            merged_rows = read_jsonl(out_path)
            delta_rows = read_jsonl(delta_path)
            summary = json.loads(summary_path.read_text(encoding="utf-8"))

            self.assertEqual(len(merged_rows), 2)
            self.assertEqual(len(delta_rows), 2)
            self.assertEqual(summary["base_rows"], 1)
            self.assertEqual(summary["new_rows"], 2)
            self.assertEqual(summary["out_rows"], 2)
            self.assertEqual(summary["delta_rows"], 2)
            self.assertEqual(summary["new_count"], 1)
            self.assertEqual(summary["updated_count"], 1)
            self.assertEqual(summary["unchanged_count"], 0)
            self.assertEqual(summary["out_path"], str(out_path))
            self.assertEqual(summary["delta_path"], str(delta_path))

    def test_cli_writes_empty_delta_file_for_no_op_merge(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base_path = root / "base.jsonl"
            new_path = root / "new.jsonl"
            out_path = root / "out.jsonl"
            delta_path = root / "delta.jsonl"
            summary_path = root / "summary.json"

            same_row = sample_row("same text")
            write_jsonl(base_path, [same_row])
            write_jsonl(new_path, [same_row])

            argv = [
                "06_merge_datasets.py",
                "--base",
                str(base_path),
                "--new",
                str(new_path),
                "--out",
                str(out_path),
                "--out-delta",
                str(delta_path),
                "--summary-out",
                str(summary_path),
            ]
            with patch.object(sys, "argv", argv):
                merge_mod.main()

            merged_rows = read_jsonl(out_path)
            delta_rows = read_jsonl(delta_path)
            summary = json.loads(summary_path.read_text(encoding="utf-8"))

            self.assertEqual(len(merged_rows), 1)
            self.assertEqual(len(delta_rows), 0)
            self.assertEqual(delta_path.read_text(encoding="utf-8"), "")
            self.assertEqual(summary["delta_rows"], 0)
            self.assertEqual(summary["new_count"], 0)
            self.assertEqual(summary["updated_count"], 0)
            self.assertEqual(summary["unchanged_count"], 1)


class RetrieverMetadataMappingTests(unittest.TestCase):
    def test_metadata_mapping_exposes_ingest_fields(self) -> None:
        record = {
            "source": "https://example.com/doc",
            "conversation_id": "doc_123",
            "chunk_id": "doc_chunk_001",
            "ticket_id": None,
            "last_updated": "2026-02-26 11:00:00",
            "department": "sistemas",
            "source_type": "pdf",
            "ingest_label": "batchA",
            "ingest_job_id": "ing_abc",
            "ingested_at": "2026-02-26 11:00:00",
        }
        mapped = metadata_func(record, {})
        self.assertEqual(mapped["source_type"], "pdf")
        self.assertEqual(mapped["ingest_label"], "batchA")
        self.assertEqual(mapped["ingest_job_id"], "ing_abc")
        self.assertEqual(mapped["ingested_at"], "2026-02-26 11:00:00")

    def test_metadata_mapping_exposes_repo_docs_fields(self) -> None:
        record = {
            "source": "https://github.com/ACEsuit/mace/blob/main/README.md",
            "conversation_id": "doc_repo",
            "chunk_id": "doc_repo_chunk_001",
            "last_updated": "2026-03-04 10:00:00",
            "department": "sistemas",
            "source_type": "html",
            "page_title": "MACE",
            "ingest_label": "repo_batch",
            "ingest_job_id": "ing_repo_001",
            "ingested_at": "2026-03-04 10:00:00",
            "original_url": "https://github.com/ACEsuit/mace/blob/main/README.md?plain=1",
            "canonical_url": "https://github.com/ACEsuit/mace/blob/main/README.md",
            "acquisition_url": "https://raw.githubusercontent.com/ACEsuit/mace/main/README.md",
            "repo_docs_provider": "github",
            "repo_docs_kind": "readme",
            "repo_slug": "ACEsuit/mace",
            "repo_namespace": "ACEsuit",
            "repo_name": "mace",
        }
        mapped = metadata_func(record, {})
        self.assertEqual(mapped["source_type"], "html")
        self.assertEqual(mapped["canonical_url"], "https://github.com/ACEsuit/mace/blob/main/README.md")
        self.assertEqual(mapped["repo_docs_provider"], "github")
        self.assertEqual(mapped["repo_docs_kind"], "readme")
        self.assertEqual(mapped["repo_slug"], "ACEsuit/mace")


class PurgeDepartmentDatasetTests(unittest.TestCase):
    def test_purge_department_dataset_applies_and_preserves_non_target_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = root / "dataset.jsonl"
            backup = root / "backup" / "dataset_before_purge.jsonl"
            rows = [
                {"conversation_id": "1", "department": "sistemas", "text": "one"},
                {"conversation_id": "2", "department": "Data Science Team", "text": "two"},
                {"conversation_id": "3", "department": "aplicaciones", "text": "three"},
            ]
            dataset.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")

            summary = purge_mod.purge_department_dataset(
                dataset_path=dataset,
                target_department="data_science_team",
                dry_run=False,
                backup_out=backup,
            )

            self.assertEqual(summary["rows_before"], 3)
            self.assertEqual(summary["rows_removed"], 1)
            self.assertEqual(summary["rows_after"], 2)
            self.assertFalse(summary["dry_run"])
            self.assertTrue(summary["dataset_modified"])
            self.assertEqual(summary["output_dataset_path"], str(dataset))
            self.assertEqual(summary["backup_dataset_path"], str(backup))

            kept_rows = [json.loads(line) for line in dataset.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual([row["conversation_id"] for row in kept_rows], ["1", "3"])
            self.assertTrue(backup.exists())
            backup_rows = [json.loads(line) for line in backup.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(backup_rows), 3)

    def test_purge_department_dataset_dry_run_reports_without_modifying_dataset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = root / "dataset.jsonl"
            backup = root / "backup" / "dataset_before_purge.jsonl"
            rows = [
                {"conversation_id": "1", "department": "sistemas", "text": "one"},
                {"conversation_id": "2", "department": "aplicaciones", "text": "two"},
            ]
            original = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
            dataset.write_text(original, encoding="utf-8")

            summary = purge_mod.purge_department_dataset(
                dataset_path=dataset,
                target_department="sistemas",
                dry_run=True,
                backup_out=backup,
            )

            self.assertEqual(summary["rows_before"], 2)
            self.assertEqual(summary["rows_removed"], 1)
            self.assertEqual(summary["rows_after"], 1)
            self.assertTrue(summary["dry_run"])
            self.assertFalse(summary["dataset_modified"])
            self.assertIsNone(summary["backup_dataset_path"])
            self.assertEqual(dataset.read_text(encoding="utf-8"), original)
            self.assertFalse(backup.exists())


if __name__ == "__main__":
    unittest.main()
