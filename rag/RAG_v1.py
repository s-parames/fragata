#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml
from langchain_community.document_loaders import JSONLoader
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.retrievers import BM25Retriever
from langchain_community.vectorstores import FAISS
from sentence_transformers import CrossEncoder
import torch

try:
    from pydantic.v1.main import BaseModel as _PydanticV1BaseModel
except Exception:  # pragma: no cover - depends on installed pydantic variant
    _PydanticV1BaseModel = None

try:
    from scripts.common_department import normalize_department, normalize_search_department_filter
except ModuleNotFoundError:
    from common_department import normalize_department, normalize_search_department_filter


def _patch_pydantic_pickle_compat() -> None:
    """Allow loading pickles produced with newer pydantic state keys."""
    if _PydanticV1BaseModel is None:
        return
    if getattr(_PydanticV1BaseModel, "_rag_pickle_compat_patched", False):
        return

    original_setstate = _PydanticV1BaseModel.__setstate__

    def _compat_setstate(self, state):  # type: ignore[no-untyped-def]
        if isinstance(state, dict) and "__fields_set__" not in state:
            upgraded = dict(state)
            upgraded["__fields_set__"] = upgraded.get("__pydantic_fields_set__", set()) or set()
            if "__private_attribute_values__" not in upgraded:
                upgraded["__private_attribute_values__"] = upgraded.get("__pydantic_private__", {}) or {}
            elif upgraded.get("__private_attribute_values__") is None:
                upgraded["__private_attribute_values__"] = {}
            state = upgraded
        return original_setstate(self, state)

    _PydanticV1BaseModel.__setstate__ = _compat_setstate
    setattr(_PydanticV1BaseModel, "_rag_pickle_compat_patched", True)


_patch_pydantic_pickle_compat()


def normalize_text(text: str) -> str:
    text = (text or "").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_for_lexical(text: str) -> str:
    text = normalize_text(text).lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text


def bm25_preprocess(text: str) -> List[str]:
    text = normalize_for_lexical(text)
    return re.findall(r"[a-z0-9_./+\\-]+", text)


_DEPARTMENT_QUERY_TOKEN_RE = re.compile(r"[a-z0-9_-]+")
_DEPARTMENT_QUERY_BOOST = 5.0
_SHORT_QUERY_MAX_TOKENS = 2
_LOW_INFO_CHARS = 80
_LOW_INFO_TOKENS = 6
_SHORT_QUERY_LOW_INFO_PENALTY = 6.0
_SHORT_QUERY_TITLE_MATCH_BOOST = 1.5
_SHORT_QUERY_INTRO_CHUNK_BOOST = 1.0
_SHORT_QUERY_SUBSTANTIVE_BOOST = 0.5
_SHORT_QUERY_DYNAMIC_SEMANTIC_WEIGHT = 0.55
_SHORT_QUERY_DYNAMIC_LEXICAL_WEIGHT = 0.45
_SHORT_QUERY_MIN_SEMANTIC_K = 80
_SHORT_QUERY_MIN_LEXICAL_K = 120
_CASE_AWARE_RERANK_MAX_QUERIES = 6
_EXACT_MATCH_MIN_CANDIDATES = 3
_EXACT_MATCH_RESCUE_LIMIT = 8
_EXACT_MATCH_SINGLE_TOKEN_BONUS = 2.5
_LOW_CONFIDENCE_RERANK_THRESHOLD = 2.0
_LOW_CONFIDENCE_FUSED_SCALE = 200.0
_MAX_QUERY_VARIANTS = 6
_TYPO_CORRECTION_MIN_TOKEN_LEN = 4
_TYPO_CORRECTION_MAX_TOKEN_CHANGES = 1
_SPANISH_INTERROGATIVE_ACCENTS = {
    "que": "qué",
    "como": "cómo",
    "cual": "cuál",
    "cuanto": "cuánto",
    "cuantos": "cuántos",
    "cuanta": "cuánta",
    "cuantas": "cuántas",
    "donde": "dónde",
    "cuando": "cuándo",
    "quien": "quién",
    "quienes": "quiénes",
}
_QUERY_TRANSLATIONS_ES_EN = {
    "que": "what",
    "es": "is",
    "son": "are",
    "como": "how",
    "instalar": "install",
    "instalacion": "installation",
    "en": "in",
    "de": "of",
    "del": "of",
    "para": "for",
    "con": "with",
    "contenedor": "container",
    "contenedores": "containers",
    "autenticacion": "authentication",
    "errores": "errors",
    "error": "error",
    "falla": "failure",
    "fallas": "failures",
    "fallo": "failure",
    "fallos": "failures",
}
_DEFINITION_QUERY_PREFIXES = (("que", "es"), ("what", "is"))
_INSTALL_QUERY_PREFIXES = (("como", "instalar"), ("how", "to", "install"))
_DEFINITION_INTENT_TOKENS = {"que", "es", "what", "is"}
_INSTALL_INTENT_TOKENS = {"como", "instalar", "how", "to", "install", "installation"}
_CONTAINER_INTENT_TOKENS = {
    "contenedor",
    "contenedores",
    "container",
    "containers",
    "docker",
    "podman",
    "enroot",
    "pyxis",
    "singularity",
    "sarus",
}
_ERROR_INTENT_TOKENS = {
    "error",
    "errors",
    "errores",
    "falla",
    "fallas",
    "fallo",
    "fallos",
    "failed",
    "failure",
    "failures",
    "troubleshooting",
}
_QUERY_STOPWORDS = {
    "a",
    "al",
    "an",
    "and",
    "con",
    "de",
    "del",
    "el",
    "en",
    "for",
    "in",
    "la",
    "las",
    "los",
    "of",
    "or",
    "para",
    "the",
    "un",
    "una",
    "y",
}


@dataclass(frozen=True)
class QueryVariant:
    text: str
    semantic_weight: float = 1.0
    lexical_weight: float = 1.0


def metadata_func(record: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
    metadata["source"] = record.get("source") or record.get("link")
    metadata["conversation_id"] = record.get("conversation_id")
    metadata["chunk_id"] = record.get("chunk_id")
    metadata["ticket_id"] = record.get("ticket_id")
    metadata["last_updated"] = record.get("last_updated") or record.get("lastUpdated")
    metadata["department"] = normalize_department(record.get("department"), allow_unknown=True)
    metadata["source_type"] = record.get("source_type")
    metadata["original_url"] = record.get("original_url")
    metadata["canonical_url"] = record.get("canonical_url")
    metadata["acquisition_url"] = record.get("acquisition_url")
    metadata["repo_docs_provider"] = record.get("repo_docs_provider")
    metadata["repo_docs_kind"] = record.get("repo_docs_kind")
    metadata["repo_slug"] = record.get("repo_slug")
    metadata["repo_namespace"] = record.get("repo_namespace")
    metadata["repo_name"] = record.get("repo_name")
    metadata["page_title"] = record.get("page_title")
    metadata["page_number"] = record.get("page_number")
    metadata["page_total"] = record.get("page_total")
    metadata["page_label"] = record.get("page_label")
    metadata["chunk_in_page"] = record.get("chunk_in_page")
    metadata["char_len"] = record.get("char_len")
    metadata["ingest_label"] = record.get("ingest_label")
    metadata["ingest_job_id"] = record.get("ingest_job_id")
    metadata["ingested_at"] = record.get("ingested_at")
    metadata["conversation_id_original"] = record.get("conversation_id_original")
    metadata["chunk_id_original"] = record.get("chunk_id_original")
    return metadata


@dataclass
class RetrievalConfig:
    dataset_path: str
    embeddings_model: str
    reranker_model: str
    faiss_dir: str
    use_faiss_cache: bool
    semantic_k: int
    lexical_k: int
    semantic_rescue_k: int
    fusion_semantic_weight: float
    fusion_lexical_weight: float
    fusion_rrf_k: int
    rerank_top_n: int
    final_k: int
    index_backup_retention_enabled: bool
    index_backup_keep_last: int
    web_job_cleanup_enabled: bool
    web_job_cleanup_trigger: str


def load_config(config_path: str) -> RetrievalConfig:
    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    r = raw.get("retrieval", {})
    fcfg = raw.get("fusion", {})
    bcfg = raw.get("index_backups", {})
    wjcfg = raw.get("web_job_cleanup", {})
    web_job_cleanup_trigger = str(wjcfg.get("trigger", "after_merge") or "after_merge").strip().lower()
    if web_job_cleanup_trigger not in {"after_merge"}:
        raise ValueError(
            "web_job_cleanup.trigger must be one of: after_merge"
        )

    return RetrievalConfig(
        dataset_path=r.get("dataset_path", "data/dataset_final.jsonl"),
        embeddings_model=r.get(
            "embeddings_model", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
        ),
        reranker_model=r.get("reranker_model", "cross-encoder/ms-marco-MiniLM-L-6-v2"),
        faiss_dir=r.get("faiss_dir", "data/index/faiss"),
        use_faiss_cache=bool(r.get("use_faiss_cache", True)),
        semantic_k=int(r.get("semantic_k", 24)),
        lexical_k=int(r.get("lexical_k", 24)),
        semantic_rescue_k=int(r.get("semantic_rescue_k", 0)),
        fusion_semantic_weight=float(fcfg.get("semantic_weight", 0.6)),
        fusion_lexical_weight=float(fcfg.get("lexical_weight", 0.4)),
        fusion_rrf_k=int(fcfg.get("rrf_k", 60)),
        rerank_top_n=int(r.get("rerank_top_n", 30)),
        final_k=int(r.get("final_k", 8)),
        index_backup_retention_enabled=bool(bcfg.get("enabled", True)),
        index_backup_keep_last=max(0, int(bcfg.get("keep_last", 1))),
        web_job_cleanup_enabled=bool(wjcfg.get("enabled", True)),
        web_job_cleanup_trigger=web_job_cleanup_trigger,
    )


def _doc_key(doc: Any) -> str:
    md = doc.metadata or {}
    source = md.get("source") or "na"
    ticket_id = md.get("ticket_id") or "na"
    conversation_id = md.get("conversation_id") or "na"
    chunk_id = md.get("chunk_id") or "na"
    text_hash = hashlib.sha1(normalize_text(doc.page_content).encode("utf-8")).hexdigest()[:16]
    base = f"{source}|{ticket_id}|{conversation_id}|{chunk_id}|{text_hash}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _rrf_fuse(
    semantic_docs: List[Any],
    lexical_docs: List[Any],
    semantic_weight: float,
    lexical_weight: float,
    rrf_k: int,
) -> List[Tuple[Any, float]]:
    scores: Dict[str, float] = {}
    id_to_doc: Dict[str, Any] = {}

    for rank, doc in enumerate(semantic_docs, start=1):
        key = _doc_key(doc)
        id_to_doc[key] = doc
        scores[key] = scores.get(key, 0.0) + semantic_weight * (1.0 / (rrf_k + rank))

    for rank, doc in enumerate(lexical_docs, start=1):
        key = _doc_key(doc)
        id_to_doc[key] = doc
        scores[key] = scores.get(key, 0.0) + lexical_weight * (1.0 / (rrf_k + rank))

    ranked_ids = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return [(id_to_doc[k], s) for k, s in ranked_ids]


def _weighted_rrf_fuse(
    semantic_runs: List[Tuple[List[Any], float]],
    lexical_runs: List[Tuple[List[Any], float]],
    semantic_weight: float,
    lexical_weight: float,
    rrf_k: int,
) -> List[Tuple[Any, float]]:
    scores: Dict[str, float] = {}
    id_to_doc: Dict[str, Any] = {}

    for docs, run_weight in semantic_runs:
        effective_weight = semantic_weight * float(run_weight)
        if effective_weight <= 0:
            continue
        for rank, doc in enumerate(docs, start=1):
            key = _doc_key(doc)
            id_to_doc[key] = doc
            scores[key] = scores.get(key, 0.0) + effective_weight * (1.0 / (rrf_k + rank))

    for docs, run_weight in lexical_runs:
        effective_weight = lexical_weight * float(run_weight)
        if effective_weight <= 0:
            continue
        for rank, doc in enumerate(docs, start=1):
            key = _doc_key(doc)
            id_to_doc[key] = doc
            scores[key] = scores.get(key, 0.0) + effective_weight * (1.0 / (rrf_k + rank))

    ranked_ids = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return [(id_to_doc[k], s) for k, s in ranked_ids]


def _parse_filter_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _parse_last_updated_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    raw = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _parse_last_updated(value: Optional[str]) -> Optional[date]:
    dt = _parse_last_updated_datetime(value)
    if dt is None:
        return None
    return dt.date()


class HybridRAGRetriever:
    def __init__(self, cfg: RetrievalConfig):
        self.cfg = cfg
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.embeddings = HuggingFaceEmbeddings(
            model_name=cfg.embeddings_model,
            model_kwargs={"device": self.device},
            encode_kwargs={"normalize_embeddings": True},
        )
        if self.cfg.use_faiss_cache:
            # Avoid loading the full dataset JSONL when FAISS cache already contains
            # docstore documents. This prevents a second in-memory copy.
            self.vectorstore = self._build_or_load_faiss()
            self.documents = self._documents_from_vectorstore()
        else:
            self.documents = self._load_documents(cfg.dataset_path)
            self.vectorstore = self._build_or_load_faiss()
        self.semantic_retriever = self.vectorstore.as_retriever(search_kwargs={"k": cfg.semantic_k})
        self.lexical_retriever = self._build_bm25_retriever()
        self._typo_token_freq, self._typo_token_index = self._build_typo_token_index()
        self.reranker = CrossEncoder(cfg.reranker_model, device=self.device)
        print(
            f"[RAG] device={self.device} "
            f"(cuda_available={torch.cuda.is_available()})"
        )
        print(f"[RAG] documents_loaded={len(self.documents)}")

    def _load_documents(self, dataset_path: str) -> List[Any]:
        loader = JSONLoader(
            file_path=dataset_path,
            jq_schema=".",
            content_key="text",
            text_content=False,
            json_lines=True,
            metadata_func=metadata_func,
        )
        docs = loader.load()
        for d in docs:
            d.page_content = normalize_text(d.page_content)
        return docs

    def _build_or_load_faiss(self) -> FAISS:
        faiss_dir = Path(self.cfg.faiss_dir)
        idx_file = faiss_dir / "index.faiss"
        pkl_file = faiss_dir / "index.pkl"
        if self.cfg.use_faiss_cache:
            missing = [p.name for p in (idx_file, pkl_file) if not p.exists()]
            if missing:
                raise RuntimeError(
                    "FAISS cache is enabled but artifacts are missing under "
                    f"{faiss_dir}: {', '.join(missing)}. "
                    "Run incremental append ingestion first, or execute "
                    "`./RAG/bin/python RAG_v1.py --config config/rag.yaml --rebuild-index` "
                    "once to rebuild cache."
                )
            try:
                return FAISS.load_local(
                    str(faiss_dir), self.embeddings, allow_dangerous_deserialization=True
                )
            except Exception as exc:
                raise RuntimeError(
                    "Failed to load FAISS cache artifacts from "
                    f"{faiss_dir}. The index may be corrupt. "
                    "Run `./RAG/bin/python RAG_v1.py --config config/rag.yaml --rebuild-index` "
                    "to recover."
                ) from exc
        vectorstore = FAISS.from_documents(self.documents, self.embeddings)
        faiss_dir.mkdir(parents=True, exist_ok=True)
        vectorstore.save_local(str(faiss_dir))
        return vectorstore

    def _documents_from_vectorstore(self) -> List[Any]:
        docstore = getattr(self.vectorstore, "docstore", None)
        if docstore is None:
            return []
        store_dict = getattr(docstore, "_dict", None)
        if isinstance(store_dict, dict):
            return [doc for doc in store_dict.values() if doc is not None]
        return []

    def _build_bm25_retriever(self):
        try:
            retriever = BM25Retriever.from_documents(
                self.documents, preprocess_func=bm25_preprocess
            )
            retriever.k = self.cfg.lexical_k
            print("[RAG] lexical_retriever=BM25 enabled")
            return retriever
        except Exception as exc:
            print(
                "[RAG] lexical_retriever=disabled "
                f"(BM25 unavailable: {exc.__class__.__name__}: {exc})"
            )
            return None

    def _collect_typo_source_tokens(self) -> Dict[str, int]:
        token_freq: Dict[str, int] = {}
        lexical_retriever = getattr(self, "lexical_retriever", None)
        vectorizer = getattr(lexical_retriever, "vectorizer", None)
        idf = getattr(vectorizer, "idf", None) if vectorizer is not None else None
        if isinstance(idf, dict) and idf:
            for token in idf.keys():
                term = str(token or "").strip().lower()
                if len(term) < _TYPO_CORRECTION_MIN_TOKEN_LEN or term.isdigit():
                    continue
                token_freq[term] = token_freq.get(term, 0) + 1
            if token_freq:
                return token_freq

        for doc in self.documents:
            md = doc.metadata or {}
            text_parts = [
                normalize_text(doc.page_content),
                normalize_text(str(md.get("page_title") or "")),
                normalize_text(str(md.get("subject") or "")),
            ]
            for part in text_parts:
                if not part:
                    continue
                for token in bm25_preprocess(part):
                    term = str(token or "").strip().lower()
                    if len(term) < _TYPO_CORRECTION_MIN_TOKEN_LEN or term.isdigit():
                        continue
                    token_freq[term] = token_freq.get(term, 0) + 1
        return token_freq

    def _build_typo_token_index(self) -> Tuple[Dict[str, int], Dict[Tuple[str, int], List[str]]]:
        token_freq = self._collect_typo_source_tokens()
        index: Dict[Tuple[str, int], List[str]] = {}
        for token in token_freq.keys():
            n = len(token)
            if n < _TYPO_CORRECTION_MIN_TOKEN_LEN:
                continue
            prefix2 = token[:2]
            prefix1 = token[:1]
            index.setdefault((prefix2, n), []).append(token)
            index.setdefault((prefix1, n), []).append(token)
        return token_freq, index

    @staticmethod
    def _is_edit_distance_leq_one(left: str, right: str) -> bool:
        if left == right:
            return True
        len_left = len(left)
        len_right = len(right)
        if abs(len_left - len_right) > 1:
            return False

        if len_left == len_right:
            mismatches = sum(ch_left != ch_right for ch_left, ch_right in zip(left, right))
            return mismatches <= 1

        if len_left > len_right:
            left, right = right, left
            len_left, len_right = len_right, len_left

        i = 0
        j = 0
        skipped = False
        while i < len_left and j < len_right:
            if left[i] == right[j]:
                i += 1
                j += 1
                continue
            if skipped:
                return False
            skipped = True
            j += 1
        return True

    def _find_typo_correction(self, token: str) -> Optional[str]:
        normalized_token = str(token or "").strip().lower()
        if len(normalized_token) < _TYPO_CORRECTION_MIN_TOKEN_LEN or normalized_token.isdigit():
            return None

        token_freq = getattr(self, "_typo_token_freq", None)
        token_index = getattr(self, "_typo_token_index", None)
        if token_freq is None or token_index is None:
            token_freq, token_index = self._build_typo_token_index()
            self._typo_token_freq = token_freq
            self._typo_token_index = token_index
        if not token_index:
            return None

        candidate_pool: set[str] = set()
        token_len = len(normalized_token)
        prefix_keys = [normalized_token[:2], normalized_token[:1]]
        for prefix in prefix_keys:
            for length in (token_len - 1, token_len, token_len + 1):
                if length < _TYPO_CORRECTION_MIN_TOKEN_LEN:
                    continue
                candidate_pool.update(token_index.get((prefix, length), []))

        best_candidate: Optional[str] = None
        best_freq = -1
        for candidate in candidate_pool:
            if candidate == normalized_token:
                continue
            if not self._is_edit_distance_leq_one(normalized_token, candidate):
                continue
            freq = int(token_freq.get(candidate, 0))
            if freq > best_freq:
                best_candidate = candidate
                best_freq = freq
        return best_candidate

    def _get_semantic_docs(self, query: str, k: int) -> List[Any]:
        total_docs = len(self.documents)
        if total_docs == 0:
            return []
        return self.vectorstore.similarity_search(query, k=min(k, total_docs))

    def _get_lexical_docs(self, query: str, k: int) -> List[Any]:
        if not self.lexical_retriever:
            return []
        previous_k = self.lexical_retriever.k
        try:
            self.lexical_retriever.k = k
            return self.lexical_retriever.invoke(query)
        finally:
            self.lexical_retriever.k = previous_k

    def _date_in_range(
        self,
        doc: Any,
        date_from: Optional[date],
        date_to: Optional[date],
    ) -> bool:
        if date_from is None and date_to is None:
            return True
        md = doc.metadata or {}
        doc_date = _parse_last_updated(md.get("last_updated"))
        if doc_date is None:
            return False
        if date_from is not None and doc_date < date_from:
            return False
        if date_to is not None and doc_date > date_to:
            return False
        return True

    def _department_matches(self, doc: Any, department: str) -> bool:
        dept = normalize_search_department_filter(department)
        if dept is None:
            return False
        if dept == "all":
            return True
        md = doc.metadata or {}
        doc_dept = normalize_department(md.get("department"), allow_unknown=True)
        return doc_dept == dept

    def _query_department_hints(self, query: str) -> set[str]:
        normalized_query = normalize_for_lexical(query or "")
        if not normalized_query:
            return set()

        hints: set[str] = set()
        for token in _DEPARTMENT_QUERY_TOKEN_RE.findall(normalized_query):
            if len(token) < 3 or len(token) > 32:
                continue
            normalized = normalize_department(token, allow_unknown=True)
            if normalized:
                hints.add(normalized)
        return hints

    def _department_query_boost(self, doc: Any, query_department_hints: set[str]) -> float:
        if not query_department_hints:
            return 0.0
        md = doc.metadata or {}
        doc_dept = normalize_department(md.get("department"), allow_unknown=True)
        if doc_dept and doc_dept in query_department_hints:
            return _DEPARTMENT_QUERY_BOOST
        return 0.0

    def _is_non_ticket_source(self, doc: Any) -> bool:
        md = doc.metadata or {}
        source_type = str(md.get("source_type") or "").strip().lower()
        return source_type in {"html", "pdf"}

    def _query_tokens(self, query: str) -> List[str]:
        return bm25_preprocess(query or "")

    def _join_query_tokens(self, tokens: Iterable[str]) -> str:
        return " ".join(token for token in tokens if token).strip()

    def _canonical_query(self, query: str) -> str:
        return self._join_query_tokens(self._query_tokens(query))

    def _unique_docs(self, docs: Iterable[Any]) -> List[Any]:
        unique: List[Any] = []
        seen: set[str] = set()
        for doc in docs:
            key = _doc_key(doc)
            if key in seen:
                continue
            seen.add(key)
            unique.append(doc)
        return unique

    def _query_subject(self, tokens: List[str], intent_tokens: set[str]) -> str:
        subject_tokens = [
            token
            for token in tokens
            if token not in intent_tokens and token not in _QUERY_STOPWORDS
        ]
        return self._join_query_tokens(subject_tokens)

    def _english_query_variant(self, tokens: List[str]) -> str:
        translated = [str(_QUERY_TRANSLATIONS_ES_EN.get(token, token)) for token in tokens]
        return self._join_query_tokens(translated)

    def _accented_spanish_variant(self, tokens: List[str]) -> str:
        if not tokens:
            return ""
        rewritten = list(tokens)
        rewritten[0] = _SPANISH_INTERROGATIVE_ACCENTS.get(rewritten[0], rewritten[0])
        return self._join_query_tokens(rewritten)

    def _add_query_variant(
        self,
        variants: List[QueryVariant],
        seen: set[str],
        text: str,
        *,
        semantic_weight: float,
        lexical_weight: float,
    ) -> None:
        normalized = normalize_text(text)
        if not normalized:
            return
        dedupe_key = normalize_for_lexical(normalized)
        if dedupe_key in seen:
            return
        seen.add(dedupe_key)
        variants.append(
            QueryVariant(
                text=normalized,
                semantic_weight=semantic_weight,
                lexical_weight=lexical_weight,
            )
        )

    def _build_query_variants(self, query: str) -> List[QueryVariant]:
        normalized_query = normalize_text(query)
        tokens = self._query_tokens(normalized_query)
        variants: List[QueryVariant] = []
        seen: set[str] = set()

        self._add_query_variant(
            variants,
            seen,
            normalized_query,
            semantic_weight=1.0,
            lexical_weight=1.0,
        )

        canonical_query = self._canonical_query(normalized_query)
        if canonical_query and normalize_for_lexical(canonical_query) != normalize_for_lexical(normalized_query):
            self._add_query_variant(
                variants,
                seen,
                canonical_query,
                semantic_weight=0.85,
                lexical_weight=0.1,
            )

        if tokens:
            corrected_tokens: List[str] = []
            corrected_count = 0
            for token in tokens:
                corrected = self._find_typo_correction(token)
                if corrected and corrected != token:
                    corrected_tokens.append(corrected)
                    corrected_count += 1
                else:
                    corrected_tokens.append(token)
            if 0 < corrected_count <= _TYPO_CORRECTION_MAX_TOKEN_CHANGES:
                corrected_query = self._join_query_tokens(corrected_tokens)
                if corrected_query:
                    self._add_query_variant(
                        variants,
                        seen,
                        corrected_query,
                        semantic_weight=0.85,
                        lexical_weight=1.0,
                    )

        if any(tokens[: len(prefix)] == list(prefix) for prefix in _DEFINITION_QUERY_PREFIXES):
            subject = self._query_subject(tokens, _DEFINITION_INTENT_TOKENS)
            if subject:
                self._add_query_variant(
                    variants,
                    seen,
                    self._accented_spanish_variant(["que", "es", *subject.split()]),
                    semantic_weight=0.9,
                    lexical_weight=0.1,
                )
                self._add_query_variant(
                    variants,
                    seen,
                    f"what is {subject}",
                    semantic_weight=0.95,
                    lexical_weight=0.9,
                )
                self._add_query_variant(
                    variants,
                    seen,
                    f"{subject} overview",
                    semantic_weight=0.7,
                    lexical_weight=0.6,
                )

        if any(tokens[: len(prefix)] == list(prefix) for prefix in _INSTALL_QUERY_PREFIXES) or any(
            token in _INSTALL_INTENT_TOKENS for token in tokens
        ):
            subject = self._query_subject(tokens, _INSTALL_INTENT_TOKENS)
            if subject:
                self._add_query_variant(
                    variants,
                    seen,
                    f"how to install {subject}",
                    semantic_weight=0.9,
                    lexical_weight=0.85,
                )
                self._add_query_variant(
                    variants,
                    seen,
                    f"{subject} installation",
                    semantic_weight=0.75,
                    lexical_weight=0.75,
                )

        if any(token in _CONTAINER_INTENT_TOKENS for token in tokens):
            subject = self._query_subject(tokens, _CONTAINER_INTENT_TOKENS)
            if subject:
                self._add_query_variant(
                    variants,
                    seen,
                    f"{subject} containers",
                    semantic_weight=1.0,
                    lexical_weight=1.0,
                )
                self._add_query_variant(
                    variants,
                    seen,
                    f"{subject} container support",
                    semantic_weight=0.85,
                    lexical_weight=0.8,
                )
                self._add_query_variant(
                    variants,
                    seen,
                    f"{subject} docker podman enroot",
                    semantic_weight=0.7,
                    lexical_weight=0.8,
                )

        if any(token in _ERROR_INTENT_TOKENS for token in tokens):
            subject = self._query_subject(tokens, _ERROR_INTENT_TOKENS)
            if subject:
                self._add_query_variant(
                    variants,
                    seen,
                    f"{subject} errors",
                    semantic_weight=0.95,
                    lexical_weight=0.95,
                )
                self._add_query_variant(
                    variants,
                    seen,
                    f"{subject} troubleshooting",
                    semantic_weight=0.8,
                    lexical_weight=0.7,
                )
                self._add_query_variant(
                    variants,
                    seen,
                    f"{subject} failed",
                    semantic_weight=0.65,
                    lexical_weight=0.6,
                )

        english_query = self._english_query_variant(tokens)
        if english_query and normalize_for_lexical(english_query) != normalize_for_lexical(normalized_query):
            self._add_query_variant(
                variants,
                seen,
                english_query,
                semantic_weight=0.85,
                lexical_weight=0.75,
            )

        return variants[:_MAX_QUERY_VARIANTS]

    def _add_rerank_query_variant(self, variants: List[str], seen: set[str], text: str) -> None:
        normalized = normalize_text(text)
        if not normalized:
            return
        # Keep rerank prompts case-sensitive to avoid collapsing variants like
        # "molpro" and "MOLPRO".
        dedupe_key = normalized
        if dedupe_key in seen:
            return
        seen.add(dedupe_key)
        variants.append(normalized)

    def _add_rerank_case_variants(self, variants: List[str], seen: set[str], text: str) -> None:
        normalized = normalize_text(text)
        if not normalized:
            return
        self._add_rerank_query_variant(variants, seen, normalized)

        lower_variant = normalized.lower()
        if lower_variant != normalized:
            self._add_rerank_query_variant(variants, seen, lower_variant)

        upper_variant = normalized.upper()
        if upper_variant != normalized and upper_variant != lower_variant:
            self._add_rerank_query_variant(variants, seen, upper_variant)

    def _single_token_exact_match_target(self, query: str) -> Optional[str]:
        tokens = [
            token
            for token in self._query_tokens(query)
            if token and token not in _QUERY_STOPWORDS
        ]
        if len(tokens) != 1:
            return None
        token = tokens[0]
        if len(token) < 3 or token.isdigit():
            return None
        return token

    def _build_rerank_queries(self, query: str, retrieval_variants: List[QueryVariant]) -> List[str]:
        variants: List[str] = []
        seen: set[str] = set()

        # Keep original user input first while adding case-normalized variants.
        self._add_rerank_case_variants(variants, seen, query)

        for variant in retrieval_variants:
            self._add_rerank_case_variants(variants, seen, variant.text)
            if len(variants) >= _CASE_AWARE_RERANK_MAX_QUERIES:
                break

        if not variants:
            return [normalize_text(query)]
        return variants[:_CASE_AWARE_RERANK_MAX_QUERIES]

    def _is_short_query(self, query: str) -> bool:
        tokens = self._query_tokens(query)
        return 0 < len(tokens) <= _SHORT_QUERY_MAX_TOKENS

    def _doc_text_tokens(self, doc: Any) -> set[str]:
        md = doc.metadata or {}
        parts = [
            normalize_text(doc.page_content),
            normalize_text(str(md.get("page_title") or "")),
            normalize_text(str(md.get("source") or "")),
            normalize_text(str(md.get("subject") or "")),
        ]
        combined = "\n".join(part for part in parts if part)
        return set(self._query_tokens(combined))

    def _doc_contains_exact_token(self, doc: Any, token: Optional[str]) -> bool:
        if not token:
            return False
        tokens = self._doc_text_tokens(doc)
        return token in tokens

    def _rescue_exact_match_candidates(
        self,
        *,
        candidates: List[Tuple[Any, float]],
        fused_score_by_key: Dict[str, float],
        lexical_runs: List[Tuple[List[Any], float]],
        exact_token: Optional[str],
    ) -> List[Tuple[Any, float]]:
        if not exact_token:
            return candidates

        current_matches = sum(
            1 for doc, _ in candidates if self._doc_contains_exact_token(doc, exact_token)
        )
        if current_matches >= _EXACT_MATCH_MIN_CANDIDATES:
            return candidates

        out = list(candidates)
        seen = {_doc_key(doc) for doc, _ in out}
        rescued = 0
        for docs, _run_weight in lexical_runs:
            for doc in docs:
                if not self._doc_contains_exact_token(doc, exact_token):
                    continue
                key = _doc_key(doc)
                if key in seen:
                    continue
                out.append((doc, float(fused_score_by_key.get(key, 0.0))))
                seen.add(key)
                rescued += 1
                current_matches += 1
                if (
                    current_matches >= _EXACT_MATCH_MIN_CANDIDATES
                    or rescued >= _EXACT_MATCH_RESCUE_LIMIT
                ):
                    return out
        return out

    def _exact_match_score_bonus(self, doc: Any, exact_token: Optional[str]) -> float:
        if not exact_token:
            return 0.0
        if self._doc_contains_exact_token(doc, exact_token):
            return _EXACT_MATCH_SINGLE_TOKEN_BONUS
        return 0.0

    def _doc_char_len(self, doc: Any) -> int:
        md = doc.metadata or {}
        value = md.get("char_len")
        try:
            parsed = int(value)
            if parsed > 0:
                return parsed
        except (TypeError, ValueError):
            pass
        return len(normalize_text(doc.page_content))

    def _is_low_information_non_ticket_chunk(self, doc: Any) -> bool:
        if not self._is_non_ticket_source(doc):
            return False
        text = normalize_text(doc.page_content)
        if not text:
            return True
        if self._doc_char_len(doc) < _LOW_INFO_CHARS:
            return True
        if len(bm25_preprocess(text)) < _LOW_INFO_TOKENS:
            return True
        return False

    def _rerank_text(self, doc: Any) -> str:
        text = normalize_text(doc.page_content)
        md = doc.metadata or {}
        if not self._is_non_ticket_source(doc):
            return text
        title = normalize_text(str(md.get("page_title") or ""))
        if title and title.lower() not in text.lower():
            return normalize_text(f"{title}\n\n{text}" if text else title)
        return text

    def _short_query_boost(self, doc: Any, query_tokens: List[str]) -> float:
        if not query_tokens or not self._is_non_ticket_source(doc):
            return 0.0
        if self._is_low_information_non_ticket_chunk(doc):
            return -_SHORT_QUERY_LOW_INFO_PENALTY

        md = doc.metadata or {}
        boost = 0.0
        title = normalize_for_lexical(str(md.get("page_title") or ""))
        if title and all(token in title for token in query_tokens):
            boost += _SHORT_QUERY_TITLE_MATCH_BOOST

        try:
            page_number = int(md.get("page_number") or 0)
        except (TypeError, ValueError):
            page_number = 0
        try:
            chunk_in_page = int(md.get("chunk_in_page") or 0)
        except (TypeError, ValueError):
            chunk_in_page = 0

        if page_number in {1, 2} and chunk_in_page == 0:
            boost += _SHORT_QUERY_INTRO_CHUNK_BOOST

        if self._doc_char_len(doc) >= 160:
            boost += _SHORT_QUERY_SUBSTANTIVE_BOOST
        return boost

    def _ticket_group_key(self, doc: Any) -> str:
        md = doc.metadata or {}
        ticket_id = md.get("ticket_id")
        if ticket_id is not None and str(ticket_id).strip():
            return f"ticket:{ticket_id}"

        conversation_id = (md.get("conversation_id") or "").strip()
        if conversation_id:
            return f"conversation:{conversation_id}"

        source = (md.get("source") or "").strip()
        if source:
            return f"source:{source}"

        chunk_id = (md.get("chunk_id") or "").strip()
        fallback = f"{chunk_id}|{normalize_text(doc.page_content)[:80]}"
        return f"fallback:{hashlib.sha1(fallback.encode('utf-8')).hexdigest()}"

    def _build_rerank_candidates(
        self,
        *,
        fused: List[Tuple[Any, float]],
        semantic_docs: List[Any],
        k_final: int,
    ) -> List[Tuple[Any, float]]:
        candidate_limit = max(self.cfg.rerank_top_n, k_final * 4)
        candidates = list(fused[:candidate_limit])
        semantic_rescue_k = max(int(getattr(self.cfg, "semantic_rescue_k", 0) or 0), 0)
        if semantic_rescue_k == 0:
            return candidates

        seen = {_doc_key(doc) for doc, _ in candidates}
        fused_score_by_key = {_doc_key(doc): score for doc, score in fused}
        rescued = 0
        for doc in semantic_docs:
            key = _doc_key(doc)
            if key in seen:
                continue
            candidates.append((doc, float(fused_score_by_key.get(key, 0.0))))
            seen.add(key)
            rescued += 1
            if rescued >= semantic_rescue_k:
                break
        return candidates

    def retrieve_by_date(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        department: str = "all",
        k: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        start_date = _parse_filter_date(date_from) if date_from else None
        end_date = _parse_filter_date(date_to) if date_to else None

        department_filter = normalize_search_department_filter(department)
        if department_filter is None:
            return []

        filtered_docs = []
        for doc in self.documents:
            if self._is_non_ticket_source(doc):
                continue
            if not self._date_in_range(doc, start_date, end_date):
                continue
            if department_filter != "all" and not self._department_matches(doc, department_filter):
                continue
            filtered_docs.append(doc)

        # Keep one representative per ticket (latest updated).
        grouped: Dict[str, Tuple[Any, datetime]] = {}
        for doc in filtered_docs:
            md = doc.metadata or {}
            doc_dt = _parse_last_updated_datetime(md.get("last_updated")) or datetime.min
            key = self._ticket_group_key(doc)
            current = grouped.get(key)
            if current is None or doc_dt > current[1]:
                grouped[key] = (doc, doc_dt)

        tickets = [item[0] for item in grouped.values()]
        tickets.sort(
            key=lambda d: (
                _parse_last_updated_datetime((d.metadata or {}).get("last_updated")) or datetime.min,
                str((d.metadata or {}).get("ticket_id") or ""),
            ),
            reverse=True,
        )

        k_value = self.cfg.final_k if k is None else int(k)
        if k_value < 0:
            k_value = self.cfg.final_k
        if k_value == 0:
            selected = tickets
        else:
            selected = tickets[:k_value]

        return [
            {
                "doc": doc,
                "fused_score": 0.0,
                "rerank_score": 0.0,
            }
            for doc in selected
        ]

    def retrieve(
        self,
        query: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        department: str = "all",
        k: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        k_value = self.cfg.final_k if k is None else int(k)
        if k_value <= 0:
            k_value = self.cfg.final_k
        k_final = k_value
        start_date = _parse_filter_date(date_from) if date_from else None
        end_date = _parse_filter_date(date_to) if date_to else None
        has_date_filter = start_date is not None or end_date is not None

        department_filter = normalize_search_department_filter(department)
        if department_filter is None:
            return []

        query_is_short = self._is_short_query(query)
        base_pool = max(self.cfg.rerank_top_n, k_final * 4)
        expanded_pool = max(base_pool, k_final * 20, 100) if has_date_filter else base_pool
        semantic_k = max(self.cfg.semantic_k, expanded_pool)
        lexical_k = max(self.cfg.lexical_k, expanded_pool)
        fusion_semantic_weight = self.cfg.fusion_semantic_weight
        fusion_lexical_weight = self.cfg.fusion_lexical_weight
        if query_is_short:
            semantic_k = max(semantic_k, _SHORT_QUERY_MIN_SEMANTIC_K)
            lexical_k = max(lexical_k, _SHORT_QUERY_MIN_LEXICAL_K)
            fusion_semantic_weight = _SHORT_QUERY_DYNAMIC_SEMANTIC_WEIGHT
            fusion_lexical_weight = _SHORT_QUERY_DYNAMIC_LEXICAL_WEIGHT
        if department_filter != "all":
            semantic_k = max(semantic_k, 150)
            lexical_k = max(lexical_k, 150)

        query_variants = self._build_query_variants(query)
        semantic_runs: List[Tuple[List[Any], float]] = []
        lexical_runs: List[Tuple[List[Any], float]] = []
        semantic_docs_all: List[Any] = []

        for variant in query_variants:
            if variant.semantic_weight > 0:
                docs = self._get_semantic_docs(variant.text, semantic_k)
                semantic_runs.append((docs, variant.semantic_weight))
                semantic_docs_all.extend(docs)
            if variant.lexical_weight > 0:
                lexical_runs.append(
                    (self._get_lexical_docs(variant.text, lexical_k), variant.lexical_weight)
                )

        semantic_docs = self._unique_docs(semantic_docs_all)
        fused = _weighted_rrf_fuse(
            semantic_runs=semantic_runs,
            lexical_runs=lexical_runs,
            semantic_weight=fusion_semantic_weight,
            lexical_weight=fusion_lexical_weight,
            rrf_k=self.cfg.fusion_rrf_k,
        )

        if has_date_filter:
            fused = [
                (doc, score)
                for doc, score in fused
                if self._date_in_range(doc, start_date, end_date) and not self._is_non_ticket_source(doc)
            ]
            semantic_docs = [
                doc
                for doc in semantic_docs
                if self._date_in_range(doc, start_date, end_date) and not self._is_non_ticket_source(doc)
            ]

        if department_filter != "all":
            fused = [
                (doc, score)
                for doc, score in fused
                if self._department_matches(doc, department_filter)
            ]
            semantic_docs = [
                doc
                for doc in semantic_docs
                if self._department_matches(doc, department_filter)
            ]
            query_department_hints: set[str] = set()
        else:
            query_department_hints = self._query_department_hints(query)

        exact_token = self._single_token_exact_match_target(query)
        fused_score_by_key = {_doc_key(doc): score for doc, score in fused}
        candidates = self._build_rerank_candidates(
            fused=fused,
            semantic_docs=semantic_docs,
            k_final=k_final,
        )
        candidates = self._rescue_exact_match_candidates(
            candidates=candidates,
            fused_score_by_key=fused_score_by_key,
            lexical_runs=lexical_runs,
            exact_token=exact_token,
        )
        if not candidates:
            return []

        query_tokens = self._query_tokens(query) if query_is_short else []
        rerank_queries = self._build_rerank_queries(query, query_variants)
        pairs: List[Tuple[str, str]] = []
        pair_to_candidate: List[int] = []
        for idx, (doc, _) in enumerate(candidates):
            rerank_text = self._rerank_text(doc)
            for rerank_query in rerank_queries:
                pairs.append((rerank_query, rerank_text))
                pair_to_candidate.append(idx)
        raw_rerank_scores = self.reranker.predict(pairs)
        rerank_scores = [float("-inf")] * len(candidates)
        for candidate_idx, score in zip(pair_to_candidate, raw_rerank_scores):
            rerank_scores[candidate_idx] = max(rerank_scores[candidate_idx], float(score))

        reranked = []
        for (doc, fused_score), rr_score in zip(candidates, rerank_scores):
            boost = self._department_query_boost(doc, query_department_hints)
            boost += self._short_query_boost(doc, query_tokens)
            boost += self._exact_match_score_bonus(doc, exact_token)
            reranked.append(
                {
                    "doc": doc,
                    "fused_score": float(fused_score),
                    "rerank_score": float(rr_score) + boost,
                }
            )

        reranked.sort(
            key=lambda x: (x["rerank_score"], x["fused_score"]),
            reverse=True,
        )
        return reranked[:k_final]


def build_engine(config_path: str) -> HybridRAGRetriever:
    cfg = load_config(config_path)
    return HybridRAGRetriever(cfg)


def _print_results(query: str, results: List[Dict[str, Any]]) -> None:
    print(f"Query: {query}")
    print("")
    for idx, item in enumerate(results, start=1):
        doc = item["doc"]
        md = doc.metadata or {}
        snippet = normalize_text(doc.page_content)[:220]
        print(f"[{idx}] rerank={item['rerank_score']:.4f} fused={item['fused_score']:.6f}")
        print(
            "    "
            + f"conversation_id={md.get('conversation_id')} chunk_id={md.get('chunk_id')} "
            + f"ticket_id={md.get('ticket_id')} source={md.get('source')}"
        )
        print("    " + snippet)
        print("")


def rebuild_index_cache(config_path: str) -> Dict[str, Any]:
    from ingest.faiss_incremental import rebuild_full_faiss
    from ingest.index_backup_retention import prune_index_backups

    cfg = load_config(config_path)
    rebuild_result = rebuild_full_faiss(
        config_path=config_path,
        delta_path="manual_rebuild",
    )
    payload: Dict[str, Any] = {
        "rebuild": rebuild_result.to_dict(),
    }

    faiss_dir = Path(rebuild_result.faiss_dir)
    if not faiss_dir.is_absolute():
        faiss_dir = Path(config_path).resolve().parent.parent / faiss_dir

    if cfg.index_backup_retention_enabled:
        prune_result = prune_index_backups(
            faiss_dir,
            keep_last=cfg.index_backup_keep_last,
            apply=True,
        )
        payload["backup_prune"] = prune_result.to_dict()
    else:
        payload["backup_prune"] = {
            "status": "skipped",
            "reason": "retention_disabled",
            "active_dir": str(faiss_dir),
            "keep_last": cfg.index_backup_keep_last,
        }

    return payload


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/rag.yaml")
    ap.add_argument("--query", default=None)
    ap.add_argument("--rebuild-index", action="store_true")
    args = ap.parse_args()

    if args.rebuild_index:
        summary = rebuild_index_cache(args.config)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        if args.query is None:
            return

    engine = build_engine(args.config)
    query = args.query or "Ensembl Variant Effect Predictor (Ensembl VEP)"
    results = engine.retrieve(query)
    _print_results(query, results)


if __name__ == "__main__":
    main()
