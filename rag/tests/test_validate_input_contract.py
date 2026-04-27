from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "00_validate_input_contract.py"
sys.path.insert(0, str(ROOT / "scripts"))

_SPEC = importlib.util.spec_from_file_location("validate_input_contract_module", MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
validate_input_contract = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(validate_input_contract)


class ValidateInputContractTests(unittest.TestCase):
    def test_empty_messages_are_ignored_when_row_has_other_useful_content(self) -> None:
        row = {
            "link": "https://rt.lan.cesga.es/Ticket/Display.html?id=1",
            "lastUpdated": "2026-03-02 10:00:00",
            "messages": [
                {"role": "user", "content": "Falla el job MPI"},
                {"role": "comment", "content": ""},
            ],
        }

        errors = validate_input_contract._validate_row(row, 1)

        self.assertEqual(errors, [])

    def test_subject_only_row_is_valid_even_with_empty_messages(self) -> None:
        row = {
            "link": "https://rt.lan.cesga.es/Ticket/Display.html?id=2",
            "lastUpdated": "2026-03-02 10:00:00",
            "subject": "Fallo en Open OnDemand",
            "messages": [],
        }

        errors = validate_input_contract._validate_row(row, 1)

        self.assertEqual(errors, [])

    def test_row_with_only_empty_messages_and_subject_is_valid(self) -> None:
        row = {
            "link": "https://rt.lan.cesga.es/Ticket/Display.html?id=3",
            "lastUpdated": "2026-03-02 10:00:00",
            "subject": "Problema con licencia Matlab",
            "messages": [
                {"role": "user", "content": "   "},
                {"role": "comment", "content": ""},
            ],
        }

        errors = validate_input_contract._validate_row(row, 1)

        self.assertEqual(errors, [])

    def test_row_with_only_empty_messages_and_no_subject_is_invalid(self) -> None:
        row = {
            "link": "https://rt.lan.cesga.es/Ticket/Display.html?id=4",
            "lastUpdated": "2026-03-02 10:00:00",
            "messages": [
                {"role": "user", "content": "   "},
                {"role": "comment", "content": ""},
            ],
        }

        errors = validate_input_contract._validate_row(row, 1)

        self.assertEqual(
            errors,
            [
                "line 1: row must contain at least one non-empty messages[].content or a non-empty 'subject'"
            ],
        )

    def test_non_empty_message_still_requires_role(self) -> None:
        row = {
            "link": "https://rt.lan.cesga.es/Ticket/Display.html?id=5",
            "lastUpdated": "2026-03-02 10:00:00",
            "messages": [{"role": "", "content": "Mensaje útil"}],
        }

        errors = validate_input_contract._validate_row(row, 1)

        self.assertEqual(errors, ["line 1: messages[0].role missing/empty"])


if __name__ == "__main__":
    unittest.main()
