from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "preprocess.yaml"
sys.path.insert(0, str(ROOT / "scripts"))


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


clean_anonymize = _load_module("clean_anonymize_module", ROOT / "scripts" / "02_clean_anonymize.py")
chunk_conversations = _load_module("chunk_conversations_module", ROOT / "scripts" / "03_chunk_conversations.py")
build_ready_dataset = _load_module("build_ready_dataset_module", ROOT / "scripts" / "05_build_ready_dataset.py")


class ChunkConversationCleanupIntegrationTests(unittest.TestCase):
    def _load_cfg(self) -> dict:
        with CONFIG_PATH.open("r", encoding="utf-8") as handle:
            cfg = yaml.safe_load(handle)
        cfg.setdefault("cleaning", {}).setdefault("ticket_phrase_cleanup", {})["enabled"] = True
        return cfg

    def _clean_message(self, text: str, role: str = "user") -> dict:
        cfg = self._load_cfg()
        runtime = clean_anonymize._build_clean_runtime(cfg)
        payload, _tags = clean_anonymize.clean_message(text, cfg, runtime, role=role)
        return payload

    def _build_chunks(self, row: dict, *, min_chars: int = 1, target_chars: int = 1, max_chars: int = 400) -> list[dict]:
        return chunk_conversations.build_chunks_for_conversation(
            row,
            min_chars=min_chars,
            target_chars=target_chars,
            max_chars=max_chars,
            overlap_turns=0,
            min_keep_chars=1,
        )

    def _finalize_chunk_rows(self, row: dict, chunks: list[dict]) -> list[dict]:
        finalized: list[dict] = []
        for chunk in chunks:
            chunk_row = dict(chunk)
            chunk_row["turn_start"] = 0
            chunk_row["turn_end"] = chunk_row["n_turns"] - 1
            chunk_row["pii_tags"] = row.get("stats", {}).get("pii_tags", {})
            finalized.append(chunk_row)
        return finalized

    def _build_ready_rows(self, rows: list[dict]) -> list[dict]:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_path = root / "chunked.jsonl"
            output_path = root / "ready.jsonl"
            input_path.write_text(
                "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
                encoding="utf-8",
            )
            argv = [
                "05_build_ready_dataset.py",
                "--input",
                str(input_path),
                "--out",
                str(output_path),
            ]
            with patch.object(sys, "argv", argv):
                build_ready_dataset.main()
            return [
                json.loads(line)
                for line in output_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]

    def test_chunker_skips_messages_removed_by_boilerplate_cleanup(self) -> None:
        row = {
            "conversation_id": "conv_removed",
            "source": "https://rt.lan.cesga.es/Ticket/Display.html?id=1",
            "last_updated": "2026-03-02 10:00:00",
            "department": "sistemas",
            "messages": [
                {
                    "role": "user",
                    **self._clean_message("This transaction appears to have no content"),
                }
            ],
        }

        chunks = self._build_chunks(row)

        self.assertEqual(chunks, [])

    def test_chunker_keeps_cleaned_retrieval_text_without_raw_fallback_reintroducing_header(self) -> None:
        cleaned_payload = self._clean_message(
            (
                'Greetings, This message has been automatically generated in response to the creation of a '
                'trouble ticket regarding: "Perdida conectividad HCIES", a summary of which appears below. '
                'There is no need to reply to this message right now. Your ticket has been assigned an ID of '
                '[example.com #1]. Please include the string: [example.com #1] in the subject line of all '
                'future correspondence about this issue. To do so, you may reply to this message. Thank you, '
                'Se ha perdido la conectividad con la unidad de investigacion del Hospital Xeral Cies'
            )
        )
        row = {
            "conversation_id": "conv_header",
            "source": "https://rt.lan.cesga.es/Ticket/Display.html?id=2",
            "last_updated": "2026-03-02 10:00:00",
            "department": "sistemas",
            "messages": [
                {
                    "role": "user",
                    **cleaned_payload,
                }
            ],
        }

        chunks = self._build_chunks(row)

        self.assertEqual(len(chunks), 1)
        self.assertFalse(chunks[0]["retrieval_fallback_used"])
        joined_text = "\n".join(chunk["text_retrieval"] for chunk in chunks)
        self.assertIn("Perdida conectividad HCIES", joined_text)
        self.assertIn(
            "Se ha perdido la conectividad con la unidad de investigacion del Hospital Xeral Cies",
            joined_text,
        )
        self.assertNotIn("This message has been automatically generated", joined_text)
        self.assertNotIn("There is no need to reply to this message right now", joined_text)
        self.assertNotIn("Please include the string:", joined_text)
        self.assertNotIn("Thank you,", joined_text)

    def test_chunker_keeps_subject_as_metadata_without_prepending_to_text(self) -> None:
        row = {
            "conversation_id": "conv_subject",
            "source": "https://rt.lan.cesga.es/Ticket/Display.html?id=3",
            "last_updated": "2026-03-02 10:00:00",
            "department": "sistemas",
            "subject": "Fallo en Open OnDemand",
            "status": "resolved",
            "messages": [
                {
                    "role": "user",
                    "content_clean": "No puedo abrir una shell interactiva.",
                    "content_retrieval": "No puedo abrir una shell interactiva.",
                    "content_raw": "No puedo abrir una shell interactiva.",
                    "noise_flags": [],
                    "noise_stats": {},
                    "retrieval_fallback_used": False,
                }
            ],
        }

        chunks = self._build_chunks(row)

        self.assertEqual(len(chunks), 1)
        self.assertEqual(
            chunks[0]["text_retrieval"],
            "User: No puedo abrir una shell interactiva.",
        )
        self.assertEqual(chunks[0]["subject"], "Fallo en Open OnDemand")
        self.assertEqual(chunks[0]["status"], "resolved")
        self.assertNotIn("Status:", chunks[0]["text_retrieval"])

    def test_chunker_deduplicates_subject_when_message_already_contains_same_subject_line(self) -> None:
        row = {
            "conversation_id": "conv_subject_dedup",
            "source": "https://rt.lan.cesga.es/Ticket/Display.html?id=4",
            "last_updated": "2026-03-02 10:00:00",
            "department": "sistemas",
            "subject": "Perdida conectividad HCIES",
            "messages": [
                {
                    "role": "user",
                    "content_clean": (
                        "Subject: Perdida conectividad HCIES\n"
                        "Se ha perdido la conectividad con la unidad de investigacion."
                    ),
                    "content_retrieval": (
                        "Subject: Perdida conectividad HCIES\n"
                        "Se ha perdido la conectividad con la unidad de investigacion."
                    ),
                    "content_raw": (
                        "Subject: Perdida conectividad HCIES\n"
                        "Se ha perdido la conectividad con la unidad de investigacion."
                    ),
                    "noise_flags": [],
                    "noise_stats": {},
                    "retrieval_fallback_used": False,
                }
            ],
        }

        chunks = self._build_chunks(row)

        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0]["text_retrieval"].count("Subject: Perdida conectividad HCIES"), 1)

    def test_chunker_skips_subject_only_rows_when_messages_are_empty(self) -> None:
        row = {
            "conversation_id": "conv_subject_only",
            "source": "https://rt.lan.cesga.es/Ticket/Display.html?id=5",
            "last_updated": "2026-03-02 10:00:00",
            "department": "sistemas",
            "subject": "Problema con licencia Matlab",
            "status": "resolved",
            "messages": [],
        }

        chunks = self._build_chunks(row, min_chars=20, target_chars=20, max_chars=400)

        self.assertEqual(chunks, [])

    def test_new_format_rows_survive_into_ready_dataset_with_status_metadata_and_clean_comments(self) -> None:
        row = {
            "conversation_id": "conv_withcomments",
            "source": "https://rt.lan.cesga.es/Ticket/Display.html?id=6",
            "last_updated": "2026-03-02 10:00:00",
            "department": "sistemas",
            "subject": "Fallo en Open OnDemand",
            "status": "resolved",
            "stats": {"pii_tags": {}},
            "messages": [
                {
                    "role": "user",
                    **self._clean_message("No puedo abrir una shell interactiva desde Open OnDemand."),
                },
                {
                    "role": "comment",
                    **self._clean_message(
                        (
                            "<p>He ajustado el parametro de memoria y ahora module load openmpi "
                            "sigue fallando con error 127.</p>"
                            "<p>Saludos,<br /> Carmen Cotelo Queijo <br /> CESGA <br /> "
                            "Tel.: +34 981 56 98 10</p>"
                        ),
                        role="comment",
                    ),
                },
            ],
        }

        chunks = self._build_chunks(row, min_chars=1, target_chars=9999, max_chars=9999)
        ready_rows = self._build_ready_rows(self._finalize_chunk_rows(row, chunks))

        self.assertEqual(len(ready_rows), 1)
        ready = ready_rows[0]
        self.assertEqual(ready["subject"], "Fallo en Open OnDemand")
        self.assertEqual(ready["status"], "resolved")
        self.assertEqual(ready["department"], "sistemas")
        self.assertIn("User: No puedo abrir una shell interactiva desde Open OnDemand.", ready["text"])
        self.assertIn("module load openmpi sigue fallando con error 127", ready["text"])
        self.assertIn("Tel.: +34 981 56 98 10", ready["text"])
        self.assertIn("Carmen Cotelo Queijo", ready["text"])
        self.assertNotIn("Status:", ready["text"])
        self.assertNotIn("text_retrieval", ready)
        self.assertNotIn("text_raw", ready)

    def test_noisy_comment_removed_before_chunking_and_not_reintroduced_in_ready_dataset(self) -> None:
        useful_message = {
            "role": "user",
            **self._clean_message("El job falla con segmentation fault al cargar el modulo."),
        }
        noisy_comment = self._clean_message(
            "<p>Ok! Cerramos este ticket, cualquier cosa nos decís.</p><p>Saludos,<br /> Carmen</p>",
            role="comment",
        )

        row = {
            "conversation_id": "conv_comment_noise",
            "source": "https://rt.lan.cesga.es/Ticket/Display.html?id=7",
            "last_updated": "2026-03-02 10:00:00",
            "department": "sistemas",
            "subject": "Crash en job MPI",
            "status": "resolved",
            "stats": {"pii_tags": {}},
            "messages": [useful_message],
        }
        if noisy_comment.get("content_retrieval") or noisy_comment.get("content_raw"):
            row["messages"].append({"role": "comment", **noisy_comment})

        self.assertEqual(len(row["messages"]), 1)

        chunks = self._build_chunks(row, min_chars=1, target_chars=9999, max_chars=9999)
        ready_rows = self._build_ready_rows(self._finalize_chunk_rows(row, chunks))

        self.assertEqual(len(ready_rows), 1)
        ready = ready_rows[0]
        self.assertIn("segmentation fault", ready["text"])
        self.assertNotIn("Cerramos este ticket", ready["text"])
        self.assertNotIn("cualquier cosa nos decís", ready["text"])
        self.assertNotIn("Saludos,", ready["text"])

    def test_chunker_drops_adjacent_duplicate_turns(self) -> None:
        row = {
            "conversation_id": "conv_dup_turns",
            "source": "https://rt.lan.cesga.es/Ticket/Display.html?id=70",
            "last_updated": "2026-03-02 10:00:00",
            "department": "sistemas",
            "messages": [
                {
                    "role": "user",
                    "content_clean": "Como compilar SWAN con netCDF en FT3?",
                    "content_retrieval": "Como compilar SWAN con netCDF en FT3?",
                    "content_raw": "Como compilar SWAN con netCDF en FT3?",
                },
                {
                    "role": "user",
                    "content_clean": "Como compilar SWAN con netCDF en FT3?",
                    "content_retrieval": "Como compilar SWAN con netCDF en FT3?",
                    "content_raw": "Como compilar SWAN con netCDF en FT3?",
                },
                {
                    "role": "assistant",
                    "content_clean": "Carga intel, impi y netcdf-fortran antes de compilar.",
                    "content_retrieval": "Carga intel, impi y netcdf-fortran antes de compilar.",
                    "content_raw": "Carga intel, impi y netcdf-fortran antes de compilar.",
                },
                {
                    "role": "assistant",
                    "content_clean": "Carga intel, impi y netcdf-fortran antes de compilar.",
                    "content_retrieval": "Carga intel, impi y netcdf-fortran antes de compilar.",
                    "content_raw": "Carga intel, impi y netcdf-fortran antes de compilar.",
                },
            ],
        }

        chunks = self._build_chunks(row, min_chars=1, target_chars=9999, max_chars=9999)

        self.assertEqual(len(chunks), 1)
        text = chunks[0]["text_retrieval"]
        self.assertEqual(text.count("User: Como compilar SWAN con netCDF en FT3?"), 1)
        self.assertEqual(
            text.count("Assistant: Carga intel, impi y netcdf-fortran antes de compilar."),
            1,
        )

    def test_chunker_avoids_triple_repeat_when_content_fields_match(self) -> None:
        repeated = {
            "role": "user",
            "content_clean": "SWAP netCDF falla al compilar con make mpi.",
            "content_retrieval": "SWAP netCDF falla al compilar con make mpi.",
            "content_raw": "SWAP netCDF falla al compilar con make mpi.",
        }
        row = {
            "conversation_id": "conv_tripled",
            "source": "https://rt.lan.cesga.es/Ticket/Display.html?id=71",
            "last_updated": "2026-03-02 10:00:00",
            "department": "sistemas",
            "messages": [repeated, repeated, repeated],
        }

        chunks = self._build_chunks(row, min_chars=1, target_chars=9999, max_chars=9999)

        self.assertEqual(len(chunks), 1)
        self.assertEqual(
            chunks[0]["text_retrieval"].count("User: SWAP netCDF falla al compilar con make mpi."),
            1,
        )

    def test_legacy_rows_still_build_without_subject_or_status(self) -> None:
        row = {
            "conversation_id": "conv_legacy",
            "source": "https://rt.lan.cesga.es/Ticket/Display.html?id=8",
            "last_updated": "2026-03-02 10:00:00",
            "department": "aplicaciones",
            "stats": {"pii_tags": {}},
            "messages": [
                {
                    "role": "user",
                    **self._clean_message("El modulo gaussian falla al compilar el ejemplo."),
                }
            ],
        }

        chunks = self._build_chunks(row, min_chars=1, target_chars=9999, max_chars=9999)
        ready_rows = self._build_ready_rows(self._finalize_chunk_rows(row, chunks))

        self.assertEqual(len(ready_rows), 1)
        ready = ready_rows[0]
        self.assertEqual(ready["department"], "aplicaciones")
        self.assertIsNone(ready["subject"])
        self.assertIsNone(ready["status"])
        self.assertEqual(ready["text"], "User: El modulo gaussian falla al compilar el ejemplo.")
        self.assertNotIn("text_retrieval", ready)
        self.assertNotIn("text_raw", ready)


if __name__ == "__main__":
    unittest.main()
