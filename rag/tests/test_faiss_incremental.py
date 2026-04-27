from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from RAG_v1 import rebuild_index_cache
from ingest.faiss_incremental import (
    IncrementalAppendResult,
    append_delta_to_faiss,
    append_delta_with_fallback,
)
from ingest.index_backup_retention import IndexBackupPruneResult


ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, rel_path: str):
    module_path = ROOT / rel_path
    spec = importlib.util.spec_from_file_location(name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


append_script_mod = load_module("incremental_append_script_mod", "scripts/10_incremental_faiss_append.py")


class _FakeVectorStore:
    def __init__(self, count: int):
        self._count = count
        self.index = SimpleNamespace(ntotal=count)
        self.docstore = SimpleNamespace(_dict={f"id_{i}": object() for i in range(count)})

    def add_documents(self, docs):
        growth = len(docs)
        self._count += growth
        self.index.ntotal = self._count
        self.docstore._dict = {f"id_{i}": object() for i in range(self._count)}

    def save_local(self, path: str) -> None:
        target = Path(path)
        target.mkdir(parents=True, exist_ok=True)
        (target / "index.faiss").write_bytes(b"fake")
        (target / "index.pkl").write_bytes(b"fake")
        _FakeFAISS.COUNTS[str(target)] = self._count


class _FakeFAISS:
    COUNTS: dict[str, int] = {}

    @classmethod
    def load_local(cls, path: str, *_args, **_kwargs):
        return _FakeVectorStore(cls.COUNTS.get(path, 0))

    @classmethod
    def from_documents(cls, docs, *_args, **_kwargs):
        return _FakeVectorStore(len(docs))


class FaissIncrementalTests(unittest.TestCase):
    def test_append_success_small_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            faiss_dir = tmp_path / "faiss"
            faiss_dir.mkdir(parents=True, exist_ok=True)
            (faiss_dir / "index.faiss").write_bytes(b"base")
            (faiss_dir / "index.pkl").write_bytes(b"base")
            delta = tmp_path / "delta.jsonl"
            delta.write_text('{"text":"a"}\n{"text":"b"}\n', encoding="utf-8")

            _FakeFAISS.COUNTS[str(faiss_dir)] = 3
            cfg = SimpleNamespace(faiss_dir=str(faiss_dir), embeddings_model="fake", dataset_path="unused")

            with (
                patch("ingest.faiss_incremental.load_config", return_value=cfg),
                patch("ingest.faiss_incremental._build_embeddings", return_value=object()),
                patch("ingest.faiss_incremental._load_delta_documents", return_value=[object(), object()]),
                patch("ingest.faiss_incremental.FAISS", _FakeFAISS),
            ):
                result = append_delta_to_faiss(config_path="cfg.yaml", delta_path=str(delta))

            self.assertTrue(result.applied)
            self.assertEqual(result.reason, "ok")
            self.assertEqual(result.delta_docs_appended, 2)
            self.assertEqual(result.index_count_before, 3)
            self.assertEqual(result.index_count_after, 5)
            self.assertTrue((faiss_dir / "index.faiss").exists())
            self.assertTrue((faiss_dir / "index.pkl").exists())

    def test_noop_when_delta_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            faiss_dir = tmp_path / "faiss"
            faiss_dir.mkdir(parents=True, exist_ok=True)
            (faiss_dir / "index.faiss").write_bytes(b"base")
            (faiss_dir / "index.pkl").write_bytes(b"base")
            delta = tmp_path / "delta.jsonl"
            delta.write_text("", encoding="utf-8")

            cfg = SimpleNamespace(faiss_dir=str(faiss_dir), embeddings_model="fake", dataset_path="unused")
            with patch("ingest.faiss_incremental.load_config", return_value=cfg):
                result = append_delta_to_faiss(config_path="cfg.yaml", delta_path=str(delta))

            self.assertFalse(result.applied)
            self.assertEqual(result.reason, "delta_empty")
            self.assertEqual(result.delta_docs_appended, 0)

    def test_fallback_trigger_simulation(self) -> None:
        fallback_result = IncrementalAppendResult(
            applied=True,
            reason="fallback_full_rebuild",
            config_path="cfg.yaml",
            faiss_dir="/tmp/faiss",
            delta_path="/tmp/delta.jsonl",
            delta_input_rows=5,
            delta_docs_appended=0,
            index_count_before=10,
            index_count_after=20,
            docstore_count_before=10,
            docstore_count_after=20,
            fallback_used=True,
            append_error="RuntimeError: append failed",
            rebuilt_doc_count=20,
        )
        with (
            patch("ingest.faiss_incremental.append_delta_to_faiss", side_effect=RuntimeError("append failed")),
            patch("ingest.faiss_incremental.rebuild_full_faiss", return_value=fallback_result),
        ):
            result = append_delta_with_fallback(
                config_path="cfg.yaml",
                delta_path="/tmp/delta.jsonl",
                enable_fallback_rebuild=True,
            )

        self.assertTrue(result.applied)
        self.assertTrue(result.fallback_used)
        self.assertEqual(result.reason, "fallback_full_rebuild")
        self.assertIn("append failed", result.append_error or "")

    def test_append_failure_raises_when_fallback_disabled(self) -> None:
        with patch("ingest.faiss_incremental.append_delta_to_faiss", side_effect=RuntimeError("append failed")):
            with self.assertRaises(RuntimeError):
                append_delta_with_fallback(
                    config_path="cfg.yaml",
                    delta_path="/tmp/delta.jsonl",
                    enable_fallback_rebuild=False,
                )

    def test_manual_rebuild_uses_managed_rebuild_and_prune(self) -> None:
        rebuild_result = IncrementalAppendResult(
            applied=True,
            reason="fallback_full_rebuild",
            config_path="cfg.yaml",
            faiss_dir="data/index/faiss_v2",
            delta_path="manual_rebuild",
            delta_input_rows=0,
            delta_docs_appended=0,
            index_count_before=10,
            index_count_after=20,
            docstore_count_before=10,
            docstore_count_after=20,
            fallback_used=True,
            rebuilt_doc_count=20,
        )
        cfg = SimpleNamespace(
            faiss_dir="data/index/faiss_v2",
            index_backup_retention_enabled=True,
            index_backup_keep_last=1,
        )
        prune_result = IndexBackupPruneResult(
            active_dir="/tmp/project/data/index/faiss_v2",
            keep_last=1,
            retained=["/tmp/project/data/index/faiss_v2.backup.20260303_071457"],
            prunable=["/tmp/project/data/index/faiss_v2.backup.20260302_120531"],
            deleted=["/tmp/project/data/index/faiss_v2.backup.20260302_120531"],
            mode="apply",
        )
        with (
            patch("RAG_v1.load_config", return_value=cfg),
            patch("ingest.faiss_incremental.rebuild_full_faiss", return_value=rebuild_result) as rebuild_mock,
            patch("ingest.index_backup_retention.prune_index_backups", return_value=prune_result) as prune_mock,
        ):
            payload = rebuild_index_cache("/tmp/project/config/rag.yaml")

        rebuild_mock.assert_called_once_with(
            config_path="/tmp/project/config/rag.yaml",
            delta_path="manual_rebuild",
        )
        prune_mock.assert_called_once_with(
            Path("/tmp/project/data/index/faiss_v2"),
            keep_last=1,
            apply=True,
        )
        self.assertEqual(payload["rebuild"]["faiss_dir"], "data/index/faiss_v2")
        self.assertEqual(payload["backup_prune"]["keep_last"], 1)

    def test_manual_rebuild_skips_prune_when_retention_disabled(self) -> None:
        rebuild_result = IncrementalAppendResult(
            applied=True,
            reason="fallback_full_rebuild",
            config_path="cfg.yaml",
            faiss_dir="data/index/faiss_v2",
            delta_path="manual_rebuild",
            delta_input_rows=0,
            delta_docs_appended=0,
            index_count_before=10,
            index_count_after=20,
            docstore_count_before=10,
            docstore_count_after=20,
            fallback_used=True,
            rebuilt_doc_count=20,
        )
        cfg = SimpleNamespace(
            faiss_dir="data/index/faiss_v2",
            index_backup_retention_enabled=False,
            index_backup_keep_last=1,
        )
        with (
            patch("RAG_v1.load_config", return_value=cfg),
            patch("ingest.faiss_incremental.rebuild_full_faiss", return_value=rebuild_result),
            patch("ingest.index_backup_retention.prune_index_backups") as prune_mock,
        ):
            payload = rebuild_index_cache("/tmp/project/config/rag.yaml")

        prune_mock.assert_not_called()
        self.assertEqual(payload["backup_prune"]["status"], "skipped")
        self.assertEqual(payload["backup_prune"]["reason"], "retention_disabled")


class IncrementalAppendScriptTests(unittest.TestCase):
    def _result(self, *, applied: bool, reason: str, fallback_used: bool) -> IncrementalAppendResult:
        return IncrementalAppendResult(
            applied=applied,
            reason=reason,
            config_path="cfg.yaml",
            faiss_dir="/tmp/faiss_v2",
            delta_path="/tmp/delta.jsonl",
            delta_input_rows=3,
            delta_docs_appended=2 if applied else 0,
            index_count_before=10,
            index_count_after=12 if applied else 10,
            docstore_count_before=10,
            docstore_count_after=12 if applied else 10,
            fallback_used=fallback_used,
            append_error="RuntimeError: append failed" if fallback_used else None,
            rebuilt_doc_count=20 if fallback_used else 0,
        )

    def test_script_summary_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            summary = Path(tmp) / "append_summary.json"
            fake = self._result(applied=True, reason="ok", fallback_used=False)
            argv = [
                "10_incremental_faiss_append.py",
                "--config",
                "cfg.yaml",
                "--delta",
                "/tmp/delta.jsonl",
                "--summary-out",
                str(summary),
            ]
            with (
                patch.object(sys, "argv", argv),
                patch.object(append_script_mod, "append_delta_with_fallback", return_value=fake) as append_mock,
            ):
                append_script_mod.main()

            append_mock.assert_called_once_with(
                config_path="cfg.yaml",
                delta_path="/tmp/delta.jsonl",
                faiss_dir_override=None,
                enable_fallback_rebuild=True,
            )
            payload = json.loads(summary.read_text(encoding="utf-8"))
            self.assertTrue(payload["applied"])
            self.assertEqual(payload["reason"], "ok")
            self.assertFalse(payload["fallback_used"])

    def test_script_summary_noop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            summary = Path(tmp) / "append_summary.json"
            fake = self._result(applied=False, reason="delta_empty", fallback_used=False)
            argv = [
                "10_incremental_faiss_append.py",
                "--config",
                "cfg.yaml",
                "--delta",
                "/tmp/delta.jsonl",
                "--summary-out",
                str(summary),
            ]
            with (
                patch.object(sys, "argv", argv),
                patch.object(append_script_mod, "append_delta_with_fallback", return_value=fake),
            ):
                append_script_mod.main()

            payload = json.loads(summary.read_text(encoding="utf-8"))
            self.assertFalse(payload["applied"])
            self.assertEqual(payload["reason"], "delta_empty")
            self.assertFalse(payload["fallback_used"])

    def test_script_summary_fallback_and_disable_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            summary = Path(tmp) / "append_summary.json"
            fake = self._result(applied=True, reason="fallback_full_rebuild", fallback_used=True)
            argv = [
                "10_incremental_faiss_append.py",
                "--config",
                "cfg.yaml",
                "--delta",
                "/tmp/delta.jsonl",
                "--summary-out",
                str(summary),
                "--disable-fallback-rebuild",
            ]
            with (
                patch.object(sys, "argv", argv),
                patch.object(append_script_mod, "append_delta_with_fallback", return_value=fake) as append_mock,
            ):
                append_script_mod.main()

            append_mock.assert_called_once_with(
                config_path="cfg.yaml",
                delta_path="/tmp/delta.jsonl",
                faiss_dir_override=None,
                enable_fallback_rebuild=False,
            )
            payload = json.loads(summary.read_text(encoding="utf-8"))
            self.assertTrue(payload["applied"])
            self.assertEqual(payload["reason"], "fallback_full_rebuild")
            self.assertTrue(payload["fallback_used"])


if __name__ == "__main__":
    unittest.main()
