from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import torch
from langchain_community.document_loaders import JSONLoader
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS

from RAG_v1 import load_config, metadata_func, normalize_text


@dataclass
class IncrementalAppendResult:
    applied: bool
    reason: str
    config_path: str
    faiss_dir: str
    delta_path: str
    delta_input_rows: int
    delta_docs_appended: int
    index_count_before: int
    index_count_after: int
    docstore_count_before: int
    docstore_count_after: int
    fallback_used: bool = False
    append_error: Optional[str] = None
    fallback_error: Optional[str] = None
    rebuilt_doc_count: int = 0
    staging_dir: Optional[str] = None
    backup_dir: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _utc_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _count_jsonl_rows(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open("r", encoding="utf-8") as src:
        for line in src:
            if line.strip():
                count += 1
    return count


def _ensure_faiss_artifacts(directory: Path) -> None:
    idx = directory / "index.faiss"
    pkl = directory / "index.pkl"
    if not idx.exists() or not pkl.exists():
        raise FileNotFoundError(
            f"FAISS artifacts missing under {directory}. "
            f"Expected files: {idx.name}, {pkl.name}"
        )


def _docstore_size(vectorstore: FAISS) -> int:
    docstore = getattr(vectorstore, "docstore", None)
    if docstore is None:
        return -1
    mapping = getattr(docstore, "_dict", None)
    if isinstance(mapping, dict):
        return len(mapping)
    return -1


def _index_size(vectorstore: FAISS) -> int:
    index = getattr(vectorstore, "index", None)
    if index is None:
        return -1
    n_total = getattr(index, "ntotal", None)
    if n_total is None:
        return -1
    return int(n_total)


def _vector_counts(vectorstore: FAISS) -> Tuple[int, int]:
    return _index_size(vectorstore), _docstore_size(vectorstore)


def _load_delta_documents(delta_path: Path) -> List[Any]:
    loader = JSONLoader(
        file_path=str(delta_path),
        jq_schema=".",
        content_key="text",
        text_content=False,
        json_lines=True,
        metadata_func=metadata_func,
    )
    docs = loader.load()
    cleaned = []
    for doc in docs:
        doc.page_content = normalize_text(doc.page_content)
        if doc.page_content:
            cleaned.append(doc)
    return cleaned


def _load_dataset_documents(dataset_path: Path) -> List[Any]:
    loader = JSONLoader(
        file_path=str(dataset_path),
        jq_schema=".",
        content_key="text",
        text_content=False,
        json_lines=True,
        metadata_func=metadata_func,
    )
    docs = loader.load()
    cleaned = []
    for doc in docs:
        doc.page_content = normalize_text(doc.page_content)
        if doc.page_content:
            cleaned.append(doc)
    return cleaned


def _build_embeddings(model_name: str) -> HuggingFaceEmbeddings:
    device = "cuda" if torch.cuda.is_available() else "cpu"
    return HuggingFaceEmbeddings(
        model_name=model_name,
        model_kwargs={"device": device},
        encode_kwargs={"normalize_embeddings": True},
    )


def _atomic_promote_staging_dir(staging_dir: Path, active_dir: Path) -> Optional[Path]:
    if not staging_dir.exists():
        raise FileNotFoundError(f"Staging directory not found: {staging_dir}")
    _ensure_faiss_artifacts(staging_dir)

    parent = active_dir.parent
    backup_dir = parent / f"{active_dir.name}.backup.{_utc_tag()}"
    if backup_dir.exists():
        shutil.rmtree(backup_dir)

    active_existed = active_dir.exists()
    if active_existed:
        os.replace(str(active_dir), str(backup_dir))
    try:
        os.replace(str(staging_dir), str(active_dir))
    except Exception as exc:
        # Best effort rollback to keep active directory recoverable.
        if active_existed and backup_dir.exists() and not active_dir.exists():
            os.replace(str(backup_dir), str(active_dir))
        raise RuntimeError(f"Atomic promotion failed: {exc}") from exc
    return backup_dir if active_existed else None


def append_delta_to_faiss(
    *,
    config_path: str,
    delta_path: str,
    faiss_dir_override: Optional[str] = None,
) -> IncrementalAppendResult:
    cfg = load_config(config_path)
    faiss_dir = Path(faiss_dir_override or cfg.faiss_dir)
    faiss_dir.mkdir(parents=True, exist_ok=True)
    _ensure_faiss_artifacts(faiss_dir)

    delta_file = Path(delta_path)
    if not delta_file.exists():
        raise FileNotFoundError(f"Delta file not found: {delta_file}")

    delta_input_rows = _count_jsonl_rows(delta_file)
    if delta_input_rows == 0:
        return IncrementalAppendResult(
            applied=False,
            reason="delta_empty",
            config_path=config_path,
            faiss_dir=str(faiss_dir),
            delta_path=str(delta_file),
            delta_input_rows=0,
            delta_docs_appended=0,
            index_count_before=0,
            index_count_after=0,
            docstore_count_before=0,
            docstore_count_after=0,
        )

    docs = _load_delta_documents(delta_file)
    if not docs:
        return IncrementalAppendResult(
            applied=False,
            reason="delta_docs_empty_after_normalization",
            config_path=config_path,
            faiss_dir=str(faiss_dir),
            delta_path=str(delta_file),
            delta_input_rows=delta_input_rows,
            delta_docs_appended=0,
            index_count_before=0,
            index_count_after=0,
            docstore_count_before=0,
            docstore_count_after=0,
        )

    embeddings = _build_embeddings(cfg.embeddings_model)
    vectorstore = FAISS.load_local(
        str(faiss_dir),
        embeddings,
        allow_dangerous_deserialization=True,
    )

    idx_before, doc_before = _vector_counts(vectorstore)
    vectorstore.add_documents(docs)
    idx_after, doc_after = _vector_counts(vectorstore)

    expected_growth = len(docs)
    if idx_before >= 0 and idx_after - idx_before != expected_growth:
        raise RuntimeError(
            "Index growth mismatch after append. "
            f"before={idx_before} after={idx_after} appended={expected_growth}"
        )
    if doc_before >= 0 and doc_after - doc_before != expected_growth:
        raise RuntimeError(
            "Docstore growth mismatch after append. "
            f"before={doc_before} after={doc_after} appended={expected_growth}"
        )

    staging_dir = faiss_dir.parent / f"{faiss_dir.name}.staging.{_utc_tag()}"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    vectorstore.save_local(str(staging_dir))
    _ensure_faiss_artifacts(staging_dir)

    # Integrity check: staging index can be loaded and keeps expected counts.
    staging_store = FAISS.load_local(
        str(staging_dir),
        embeddings,
        allow_dangerous_deserialization=True,
    )
    idx_stage, doc_stage = _vector_counts(staging_store)
    if idx_after >= 0 and idx_stage != idx_after:
        raise RuntimeError(
            f"Staging index count mismatch: expected={idx_after} actual={idx_stage}"
        )
    if doc_after >= 0 and doc_stage != doc_after:
        raise RuntimeError(
            f"Staging docstore count mismatch: expected={doc_after} actual={doc_stage}"
        )

    backup_dir = _atomic_promote_staging_dir(staging_dir, faiss_dir)
    return IncrementalAppendResult(
        applied=True,
        reason="ok",
        config_path=config_path,
        faiss_dir=str(faiss_dir),
        delta_path=str(delta_file),
        delta_input_rows=delta_input_rows,
        delta_docs_appended=expected_growth,
        index_count_before=idx_before,
        index_count_after=idx_after,
        docstore_count_before=doc_before,
        docstore_count_after=doc_after,
        staging_dir=str(staging_dir),
        backup_dir=str(backup_dir) if backup_dir else None,
    )


def write_summary_json(path: str, payload: Dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f"{target.name}.", suffix=".tmp", dir=str(target.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as dst:
            json.dump(payload, dst, ensure_ascii=False, indent=2)
            dst.write("\n")
        os.replace(tmp_path, target)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def rebuild_full_faiss(
    *,
    config_path: str,
    delta_path: str,
    faiss_dir_override: Optional[str] = None,
    append_error: Optional[str] = None,
    dataset_path_override: Optional[str] = None,
) -> IncrementalAppendResult:
    cfg = load_config(config_path)
    faiss_dir = Path(faiss_dir_override or cfg.faiss_dir)
    faiss_dir.mkdir(parents=True, exist_ok=True)
    dataset_path = Path(dataset_path_override or cfg.dataset_path)
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset file not found for full rebuild: {dataset_path}")

    idx_before = 0
    doc_before = 0
    if (faiss_dir / "index.faiss").exists() and (faiss_dir / "index.pkl").exists():
        try:
            embeddings_before = _build_embeddings(cfg.embeddings_model)
            existing_store = FAISS.load_local(
                str(faiss_dir),
                embeddings_before,
                allow_dangerous_deserialization=True,
            )
            idx_before, doc_before = _vector_counts(existing_store)
        except Exception:
            idx_before = 0
            doc_before = 0

    docs = _load_dataset_documents(dataset_path)
    if not docs:
        raise RuntimeError(f"Dataset has no non-empty documents: {dataset_path}")

    embeddings = _build_embeddings(cfg.embeddings_model)
    rebuilt_store = FAISS.from_documents(docs, embeddings)
    idx_after, doc_after = _vector_counts(rebuilt_store)

    staging_dir = faiss_dir.parent / f"{faiss_dir.name}.rebuild.staging.{_utc_tag()}"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    rebuilt_store.save_local(str(staging_dir))
    _ensure_faiss_artifacts(staging_dir)

    staging_loaded = FAISS.load_local(
        str(staging_dir),
        embeddings,
        allow_dangerous_deserialization=True,
    )
    idx_stage, doc_stage = _vector_counts(staging_loaded)
    if idx_after >= 0 and idx_stage != idx_after:
        raise RuntimeError(
            f"Rebuild staging index count mismatch: expected={idx_after} actual={idx_stage}"
        )
    if doc_after >= 0 and doc_stage != doc_after:
        raise RuntimeError(
            f"Rebuild staging docstore count mismatch: expected={doc_after} actual={doc_stage}"
        )

    backup_dir = _atomic_promote_staging_dir(staging_dir, faiss_dir)
    return IncrementalAppendResult(
        applied=True,
        reason="fallback_full_rebuild",
        config_path=config_path,
        faiss_dir=str(faiss_dir),
        delta_path=delta_path,
        delta_input_rows=_count_jsonl_rows(Path(delta_path)) if Path(delta_path).exists() else 0,
        delta_docs_appended=0,
        index_count_before=idx_before,
        index_count_after=idx_after,
        docstore_count_before=doc_before,
        docstore_count_after=doc_after,
        fallback_used=True,
        append_error=append_error,
        rebuilt_doc_count=len(docs),
        staging_dir=str(staging_dir),
        backup_dir=str(backup_dir) if backup_dir else None,
    )


def append_delta_with_fallback(
    *,
    config_path: str,
    delta_path: str,
    faiss_dir_override: Optional[str] = None,
    enable_fallback_rebuild: bool = True,
) -> IncrementalAppendResult:
    try:
        return append_delta_to_faiss(
            config_path=config_path,
            delta_path=delta_path,
            faiss_dir_override=faiss_dir_override,
        )
    except Exception as exc:
        if not enable_fallback_rebuild:
            raise
        append_error = f"{exc.__class__.__name__}: {exc}"
        try:
            return rebuild_full_faiss(
                config_path=config_path,
                delta_path=delta_path,
                faiss_dir_override=faiss_dir_override,
                append_error=append_error,
            )
        except Exception as rebuild_exc:
            raise RuntimeError(
                "Append failed and fallback full rebuild also failed. "
                f"append_error={append_error}; "
                f"fallback_error={rebuild_exc.__class__.__name__}: {rebuild_exc}"
            ) from rebuild_exc
