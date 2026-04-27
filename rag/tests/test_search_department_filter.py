from __future__ import annotations

import unittest
from types import SimpleNamespace

from RAG_v1 import HybridRAGRetriever


class _Doc:
    def __init__(
        self,
        *,
        department: str,
        source: str,
        last_updated: str = "2026-02-26 10:00:00",
        source_type: str | None = None,
        page_content: str | None = None,
        page_title: str | None = None,
        page_number: int | None = None,
        chunk_in_page: int | None = None,
    ) -> None:
        resolved_page_content = page_content or f"document from {source}"
        self.page_content = resolved_page_content
        self.metadata = {
            "department": department,
            "source": source,
            "conversation_id": f"conv_{source}",
            "chunk_id": f"chunk_{source}",
            "ticket_id": None,
            "last_updated": last_updated,
            "source_type": source_type,
            "page_title": page_title,
            "page_number": page_number,
            "chunk_in_page": chunk_in_page,
            "char_len": len(resolved_page_content),
        }


class SearchDepartmentFilterTests(unittest.TestCase):
    def _build_retriever(self, docs):
        retriever = HybridRAGRetriever.__new__(HybridRAGRetriever)
        retriever.cfg = SimpleNamespace(
            final_k=8,
            semantic_k=24,
            lexical_k=24,
            semantic_rescue_k=0,
            rerank_top_n=30,
            fusion_semantic_weight=0.6,
            fusion_lexical_weight=0.4,
            fusion_rrf_k=60,
        )
        retriever.documents = docs
        retriever.reranker = SimpleNamespace(predict=lambda pairs: [1.0] * len(pairs))
        retriever._get_semantic_docs = lambda query, k: docs
        retriever._get_lexical_docs = lambda query, k: list(reversed(docs))
        return retriever

    def test_department_matches_keeps_legacy_aliases(self) -> None:
        retriever = self._build_retriever([])
        doc = _Doc(department="BIG DATA", source="legacy")
        self.assertTrue(retriever._department_matches(doc, "big data"))
        self.assertTrue(retriever._department_matches(doc, "bd"))
        self.assertTrue(retriever._department_matches(doc, "bigdata"))

    def test_department_matches_slurm_canonical(self) -> None:
        retriever = self._build_retriever([])
        doc = _Doc(department="SLURM", source="slurm")
        self.assertTrue(retriever._department_matches(doc, "slurm"))

    def test_retrieve_filters_by_custom_department(self) -> None:
        custom_doc = _Doc(department="Data Science Team", source="custom")
        canonical_doc = _Doc(department="sistemas", source="canonical")
        retriever = self._build_retriever([custom_doc, canonical_doc])

        rows = retriever.retrieve(query="scheduler", department="Data Science Team", k=8)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["doc"].metadata["source"], "custom")

    def test_retrieve_by_date_filters_by_custom_department(self) -> None:
        custom_doc = _Doc(
            department="Platform Ops Team",
            source="custom",
            last_updated="2026-02-26 09:00:00",
        )
        other_doc = _Doc(
            department="sistemas",
            source="canonical",
            last_updated="2026-02-26 11:00:00",
        )
        retriever = self._build_retriever([custom_doc, other_doc])

        rows = retriever.retrieve_by_date(
            date_from="2026-02-25",
            date_to="2026-02-27",
            department="Platform Ops Team",
            k=10,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["doc"].metadata["source"], "custom")

    def test_retrieve_by_date_excludes_web_and_pdf_sources(self) -> None:
        ticket_doc = _Doc(
            department="sistemas",
            source="ticket-source",
            last_updated="2026-02-26 10:00:00",
            source_type=None,
        )
        web_doc = _Doc(
            department="sistemas",
            source="https://example.com/guide",
            last_updated="2026-02-26 11:00:00",
            source_type="html",
        )
        pdf_doc = _Doc(
            department="sistemas",
            source="https://example.com/manual.pdf",
            last_updated="2026-02-26 12:00:00",
            source_type="pdf",
        )
        retriever = self._build_retriever([ticket_doc, web_doc, pdf_doc])

        rows = retriever.retrieve_by_date(
            date_from="2026-02-25",
            date_to="2026-02-27",
            department="all",
            k=10,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["doc"].metadata["source"], "ticket-source")

    def test_retrieve_with_date_filter_excludes_web_and_pdf_sources(self) -> None:
        ticket_doc = _Doc(
            department="sistemas",
            source="ticket-source",
            last_updated="2026-02-26 10:00:00",
            source_type=None,
        )
        web_doc = _Doc(
            department="sistemas",
            source="https://example.com/guide",
            last_updated="2026-02-26 11:00:00",
            source_type="html",
        )
        pdf_doc = _Doc(
            department="sistemas",
            source="https://example.com/manual.pdf",
            last_updated="2026-02-26 12:00:00",
            source_type="pdf",
        )
        retriever = self._build_retriever([ticket_doc, web_doc, pdf_doc])

        rows = retriever.retrieve(
            query="scheduler",
            date_from="2026-02-25",
            date_to="2026-02-27",
            department="all",
            k=10,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["doc"].metadata["source"], "ticket-source")

    def test_retrieve_query_department_hint_boosts_matching_docs(self) -> None:
        qiskit_doc = _Doc(
            department="qiskit",
            source="https://qiskit.github.io/documentation/main/index.html",
            source_type="html",
            page_title="Qiskit Documentation - Main Index",
            page_number=1,
            chunk_in_page=0,
            page_content=(
                "Qiskit documentation index for installation, tutorials, transpilation, "
                "runtime, simulators, and IBM Quantum workflows."
            ),
        )
        ticket_doc = _Doc(
            department="aplicaciones",
            source="https://rt.lan.cesga.es/Ticket/Display.html?id=1",
            source_type=None,
        )
        retriever = self._build_retriever([qiskit_doc, ticket_doc])
        # Base reranker prefers ticket_doc; query-department boost should promote qiskit_doc.
        retriever.reranker = SimpleNamespace(predict=lambda pairs: [0.1, 3.0])

        rows = retriever.retrieve(
            query="qiskit",
            department="all",
            k=2,
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["doc"].metadata["department"], "qiskit")
        self.assertGreater(rows[0]["rerank_score"], rows[1]["rerank_score"])

    def test_retrieve_semantic_rescue_keeps_cross_language_candidate_for_rerank(self) -> None:
        relevant_doc = _Doc(
            department="sistemas",
            source="guia-es",
            source_type="html",
            page_content="guia de instalacion del scheduler en espanol",
        )
        semantic_noise = [
            _Doc(department="sistemas", source=f"sem-{idx}", page_content=f"semantic noise {idx}")
            for idx in range(4)
        ]
        lexical_noise = [
            _Doc(department="sistemas", source=f"lex-{idx}", page_content=f"lexical noise {idx}")
            for idx in range(4)
        ]
        docs = semantic_noise + lexical_noise + [relevant_doc]
        retriever = self._build_retriever(docs)
        retriever.cfg.final_k = 1
        retriever.cfg.rerank_top_n = 4
        retriever.cfg.semantic_rescue_k = 1
        retriever._get_semantic_docs = lambda query, k: semantic_noise + [relevant_doc]
        retriever._get_lexical_docs = lambda query, k: lexical_noise
        retriever.reranker = SimpleNamespace(
            predict=lambda pairs: [10.0 if "instalacion" in doc_text else 0.1 for _, doc_text in pairs]
        )

        rows = retriever.retrieve(
            query="how to install the scheduler",
            department="all",
            k=1,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["doc"].metadata["source"], "guia-es")

    def test_short_query_filters_low_information_raw_site_chunks(self) -> None:
        noisy_doc = _Doc(
            department="slurm",
            source="https://slurm.schedmd.com/noise.html",
            source_type="html",
            page_content="26",
            page_title="Slurm Workload Manager - Noise",
            page_number=3,
            chunk_in_page=2,
        )
        relevant_doc = _Doc(
            department="slurm",
            source="https://slurm.schedmd.com/overview.html",
            source_type="html",
            page_content=(
                "Slurm is an open source, fault-tolerant, and highly scalable cluster management "
                "and job scheduling system for Linux clusters."
            ),
            page_title="Slurm Workload Manager - Overview",
            page_number=2,
            chunk_in_page=0,
        )
        retriever = self._build_retriever([noisy_doc, relevant_doc])
        retriever.reranker = SimpleNamespace(predict=lambda pairs: [1.0] * len(pairs))
        retriever._get_semantic_docs = lambda query, k: [noisy_doc, relevant_doc]
        retriever._get_lexical_docs = lambda query, k: [noisy_doc, relevant_doc]

        rows = retriever.retrieve(
            query="slurm",
            department="all",
            k=2,
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["doc"].metadata["source"], "https://slurm.schedmd.com/overview.html")

    def test_query_rewrite_recovers_accentless_definition_query(self) -> None:
        bad_pdf = _Doc(
            department="slurm",
            source="bad-pdf",
            source_type="pdf",
            page_content="Slurm and/or/vs Kubernetes Tim Wickberg CTO",
            page_title="Slurm and/or/vs Kubernetes",
            page_number=1,
            chunk_in_page=0,
        )
        relevant_doc = _Doc(
            department="slurm",
            source="https://slurm.schedmd.com/overview.html",
            source_type="html",
            page_content=(
                "Slurm is an open source, fault-tolerant, and highly scalable cluster management "
                "and job scheduling system for Linux clusters."
            ),
            page_title="Slurm Workload Manager - Overview",
            page_number=2,
            chunk_in_page=0,
        )
        retriever = self._build_retriever([bad_pdf, relevant_doc])
        retriever._get_semantic_docs = lambda query, k: (
            [relevant_doc, bad_pdf] if "what is slurm" in query.lower() or "qué es slurm" in query.lower() else [bad_pdf]
        )
        retriever._get_lexical_docs = lambda query, k: (
            [relevant_doc, bad_pdf] if "what is slurm" in query.lower() or "overview" in query.lower() else [bad_pdf]
        )
        retriever.reranker = SimpleNamespace(
            predict=lambda pairs: [
                10.0
                if "open source" in doc_text.lower() and ("what is slurm" in q.lower() or "qué es slurm" in q.lower())
                else -2.0
                for q, doc_text in pairs
            ]
        )

        rows = retriever.retrieve(
            query="que es slurm",
            department="slurm",
            k=2,
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["doc"].metadata["source"], "https://slurm.schedmd.com/overview.html")

    def test_query_rewrite_recovers_container_docs_across_languages(self) -> None:
        unrelated_doc = _Doc(
            department="slurm",
            source="https://slurm.schedmd.com/noise.html",
            source_type="html",
            page_content="Random conference slide title",
            page_title="Conference Deck",
            page_number=1,
            chunk_in_page=0,
        )
        container_doc = _Doc(
            department="slurm",
            source="https://slurm.schedmd.com/containers.html",
            source_type="html",
            page_content=(
                "Containers Guide for Slurm with Podman, Docker, ENROOT and OCI runtime examples."
            ),
            page_title="Slurm Workload Manager - Containers Guide",
            page_number=2,
            chunk_in_page=0,
        )
        retriever = self._build_retriever([unrelated_doc, container_doc])
        retriever._get_semantic_docs = lambda query, k: (
            [container_doc]
            if "containers" in query.lower() or "container support" in query.lower()
            else [unrelated_doc]
        )
        retriever._get_lexical_docs = lambda query, k: (
            [container_doc]
            if "containers" in query.lower() or "docker podman enroot" in query.lower()
            else [unrelated_doc]
        )
        retriever.reranker = SimpleNamespace(
            predict=lambda pairs: [
                9.0 if "containers guide" in doc_text.lower() else -1.0 for _, doc_text in pairs
            ]
        )

        rows = retriever.retrieve(
            query="contenedores en slurm",
            department="slurm",
            k=1,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["doc"].metadata["source"], "https://slurm.schedmd.com/containers.html")

    def test_query_rewrite_recovers_error_intent_for_slurmstepd(self) -> None:
        manpage_doc = _Doc(
            department="slurm",
            source="https://slurm.schedmd.com/slurmstepd.html",
            source_type="html",
            page_content="slurmstepd is a job step manager for Slurm.",
            page_title="Slurm Workload Manager - slurmstepd",
            page_number=1,
            chunk_in_page=0,
        )
        error_doc = _Doc(
            department="slurm",
            source="ticket-slurmstepd-error",
            source_type=None,
            page_content=(
                "slurmstepd: Unable to create TMPDIR [/scratch/36225]: Permission denied"
            ),
            page_title=None,
            page_number=None,
            chunk_in_page=None,
        )
        retriever = self._build_retriever([manpage_doc, error_doc])
        retriever._get_semantic_docs = lambda query, k: (
            [error_doc, manpage_doc]
            if "errors" in query.lower() or "troubleshooting" in query.lower()
            else [manpage_doc]
        )
        retriever._get_lexical_docs = lambda query, k: (
            [error_doc, manpage_doc]
            if "errors" in query.lower() or "failed" in query.lower()
            else [manpage_doc]
        )
        retriever.reranker = SimpleNamespace(
            predict=lambda pairs: [
                8.0 if "permission denied" in doc_text.lower() else 1.0 for _, doc_text in pairs
            ]
        )

        rows = retriever.retrieve(
            query="errores slurmstepd",
            department="slurm",
            k=2,
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["doc"].metadata["source"], "ticket-slurmstepd-error")

    def test_rerank_queries_include_case_variant_for_single_token_query(self) -> None:
        retriever = self._build_retriever([])
        retrieval_variants = retriever._build_query_variants("molpro")
        rerank_queries = retriever._build_rerank_queries("molpro", retrieval_variants)
        self.assertIn("molpro", rerank_queries)
        self.assertIn("MOLPRO", rerank_queries)

    def test_rerank_queries_include_case_variants_for_multi_token_query(self) -> None:
        retriever = self._build_retriever([])
        retrieval_variants = retriever._build_query_variants("Swap netCDF")
        rerank_queries = retriever._build_rerank_queries("Swap netCDF", retrieval_variants)
        self.assertIn("Swap netCDF", rerank_queries)
        self.assertIn("swap netcdf", rerank_queries)
        self.assertIn("SWAP NETCDF", rerank_queries)

    def test_query_variants_include_single_edit_typo_correction(self) -> None:
        swan_doc = _Doc(
            department="aplicaciones",
            source="ticket-swan",
            source_type=None,
            page_content="User: SWAN netCDF outputs and compile flags in FT3.",
        )
        retriever = self._build_retriever([swan_doc])
        variants = retriever._build_query_variants("swap netcdf")
        texts = {variant.text for variant in variants}
        self.assertIn("swan netcdf", texts)

    def test_single_token_exact_match_bonus_promotes_exact_ticket(self) -> None:
        relevant_doc = _Doc(
            department="aplicaciones",
            source="ticket-molpro",
            source_type=None,
            page_content="Subject: Instalación MOLPRO en FT3 y uso en cola.",
        )
        noise_a = _Doc(
            department="aplicaciones",
            source="ticket-noise-a",
            source_type=None,
            page_content="Subject: instalación MOLCAS",
        )
        noise_b = _Doc(
            department="sistemas",
            source="ticket-noise-b",
            source_type=None,
            page_content="Subject: proba",
        )
        retriever = self._build_retriever([noise_a, noise_b, relevant_doc])
        retriever._get_semantic_docs = lambda query, k: [noise_a, noise_b, relevant_doc]
        retriever._get_lexical_docs = lambda query, k: [noise_a, relevant_doc, noise_b]
        # Simulate a weak reranker where exact-token guardrails decide the ordering.
        retriever.reranker = SimpleNamespace(predict=lambda pairs: [-4.0] * len(pairs))

        rows = retriever.retrieve(
            query="molpro",
            department="all",
            k=3,
        )
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["doc"].metadata["source"], "ticket-molpro")


if __name__ == "__main__":
    unittest.main()
