from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "02_clean_anonymize.py"
CONFIG_PATH = ROOT / "config" / "preprocess.yaml"
sys.path.insert(0, str(ROOT / "scripts"))

_SPEC = importlib.util.spec_from_file_location("clean_anonymize_module", MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
clean_anonymize = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(clean_anonymize)


class CleanAnonymizeBoilerplateTests(unittest.TestCase):
    def _load_cfg(self) -> dict:
        with CONFIG_PATH.open("r", encoding="utf-8") as handle:
            cfg = yaml.safe_load(handle)
        cfg.setdefault("cleaning", {}).setdefault("ticket_phrase_cleanup", {})["enabled"] = True
        return cfg

    def _load_cfg_autoreply_only(self) -> dict:
        cfg = self._load_cfg()
        cfg.setdefault("cleaning", {})["mode"] = "autoreply_only"
        return cfg

    def test_pure_placeholder_message_is_fully_dropped(self) -> None:
        cfg = self._load_cfg()
        runtime = clean_anonymize._build_clean_runtime(cfg)

        payload, _tags = clean_anonymize.clean_message(
            "This transaction appears to have no content",
            cfg,
            runtime,
        )

        self.assertEqual(payload["content_retrieval"], "")
        self.assertEqual(payload["content_raw"], "")
        self.assertFalse(payload["retrieval_fallback_used"])
        self.assertIn("ticket_phrase_hard_drop", payload["noise_flags"])
        self.assertIn("ticket_phrase_removed_all", payload["noise_flags"])
        self.assertEqual(payload["noise_stats"]["phrase_cleanup_removed_all_content"], 1)

    def test_ticket_url_plus_placeholder_collapses_to_empty(self) -> None:
        cfg = self._load_cfg()
        runtime = clean_anonymize._build_clean_runtime(cfg)

        payload, _tags = clean_anonymize.clean_message(
            "<URL: http://rt.cesga.es/Ticket/Display.html?id=483 > This transaction appears to have no content",
            cfg,
            runtime,
        )

        self.assertEqual(payload["content_retrieval"], "")
        self.assertEqual(payload["content_raw"], "")
        self.assertFalse(payload["retrieval_fallback_used"])

    def test_hard_noise_is_removed_but_useful_content_remains(self) -> None:
        cfg = self._load_cfg()
        runtime = clean_anonymize._build_clean_runtime(cfg)

        payload, _tags = clean_anonymize.clean_message(
            "Useful diagnosis for the incident.\nOutput other\nThis transaction appears to have no content",
            cfg,
            runtime,
        )

        self.assertEqual(payload["content_retrieval"], "Useful diagnosis for the incident.")
        self.assertTrue(payload["content_raw"])
        self.assertFalse(payload["retrieval_fallback_used"])
        self.assertGreaterEqual(payload["noise_stats"]["phrase_cleanup_replacements"], 2)
        self.assertEqual(payload["noise_stats"]["phrase_cleanup_removed_all_content"], 0)

    def test_external_notification_boilerplate_is_removed(self) -> None:
        cfg = self._load_cfg()
        runtime = clean_anonymize._build_clean_runtime(cfg)

        payload, _tags = clean_anonymize.clean_message(
            (
                "This ticket has been created by ops@example.com, who has put you as Cc so you can follow it.\n"
                "We are sorry if you got notified about this twice.\n"
                "To access this ticket, click: https://tts.prace-ri.eu/Ticket/Display.html?id=5174"
            ),
            cfg,
            runtime,
        )

        self.assertEqual(payload["content_retrieval"], "")
        self.assertEqual(payload["content_raw"], "")
        self.assertFalse(payload["retrieval_fallback_used"])
        self.assertGreaterEqual(payload["noise_stats"]["phrase_cleanup_replacements"], 3)

    def test_automatic_ticket_header_is_removed_but_subject_and_body_remain(self) -> None:
        cfg = self._load_cfg()
        runtime = clean_anonymize._build_clean_runtime(cfg)

        payload, _tags = clean_anonymize.clean_message(
            (
                'Greetings, This message has been automatically generated in response to the creation of a '
                'trouble ticket regarding: "Perdida conectividad HCIES", a summary of which appears below. '
                'There is no need to reply to this message right now. Your ticket has been assigned an ID of '
                '[example.com #1]. Please include the string: [example.com #1] in the subject line of all '
                'future correspondence about this issue. To do so, you may reply to this message. Thank you, '
                'Se ha perdido la conectividad con la unidad de investigacion del Hospital Xeral Cies'
            ),
            cfg,
            runtime,
        )

        self.assertEqual(
            payload["content_retrieval"],
            "Subject: Perdida conectividad HCIES\n"
            "Se ha perdido la conectividad con la unidad de investigacion del Hospital Xeral Cies",
        )
        self.assertNotIn("This message has been automatically generated", payload["content_retrieval"])
        self.assertNotIn("There is no need to reply", payload["content_retrieval"])
        self.assertNotIn("Thank you,", payload["content_retrieval"])
        self.assertIn("ticket_phrase_header_context", payload["noise_flags"])
        self.assertIn("ticket_phrase_header", payload["noise_flags"])
        self.assertGreaterEqual(payload["noise_stats"]["phrase_cleanup_header_replacements"], 7)

    def test_normal_human_thank_you_is_preserved_outside_ticket_header_context(self) -> None:
        cfg = self._load_cfg()
        runtime = clean_anonymize._build_clean_runtime(cfg)

        payload, _tags = clean_anonymize.clean_message(
            "Thank you, I will check the logs tomorrow and send an update.",
            cfg,
            runtime,
        )

        self.assertEqual(
            payload["content_retrieval"],
            "Thank you, I will check the logs tomorrow and send an update.",
        )
        self.assertNotIn("ticket_phrase_header", payload["noise_flags"])

    def test_comment_html_signature_is_removed_but_technical_followup_remains(self) -> None:
        cfg = self._load_cfg()
        runtime = clean_anonymize._build_clean_runtime(cfg)

        payload, _tags = clean_anonymize.clean_message(
            (
                "<p>He cambiado el Makefile a +i8 y ahora module load openmpi sigue fallando con error 127.</p>"
                "<p>Saludos,<br /> Carmen Cotelo Queijo <br /> CESGA <br /> Tel.: +34 981 56 98 10</p>"
            ),
            cfg,
            runtime,
            role="comment",
        )

        self.assertIn("Makefile a +i8", payload["content_retrieval"])
        self.assertIn("error 127", payload["content_retrieval"])
        self.assertNotIn("<p>", payload["content_retrieval"])
        self.assertIn("Tel.: +34 981 56 98 10", payload["content_retrieval"])
        self.assertIn("Carmen Cotelo Queijo", payload["content_retrieval"])
        self.assertEqual(payload["noise_stats"]["comment_role_html_normalized"], 1)
        self.assertEqual(payload["noise_stats"]["comment_role_signature_lines_removed"], 0)
        self.assertIn("comment_role_html", payload["noise_flags"])
        self.assertNotIn("comment_role_signature", payload["noise_flags"])

    def test_email_phone_and_person_name_are_preserved(self) -> None:
        cfg = self._load_cfg()
        runtime = clean_anonymize._build_clean_runtime(cfg)

        payload, tags = clean_anonymize.clean_message(
            "Hola Kevin, escríbeme a nico@example.com o llámame al +34 600 123 123.",
            cfg,
            runtime,
            role="user",
        )

        self.assertIn("nico@example.com", payload["content_retrieval"])
        self.assertIn("+34 600 123 123", payload["content_retrieval"])
        self.assertIn("Kevin", payload["content_retrieval"])
        self.assertNotIn("[EMAIL]", payload["content_retrieval"])
        self.assertNotIn("[PHONE]", payload["content_retrieval"])
        self.assertNotIn("[PERSON]", payload["content_retrieval"])
        self.assertEqual(tags, {"EMAIL": 0, "PHONE": 0, "PERSON": 0})

    def test_ticketformater_reply_tail_is_trimmed_for_non_comment_roles(self) -> None:
        cfg = self._load_cfg()
        runtime = clean_anonymize._build_clean_runtime(cfg)

        payload, _tags = clean_anonymize.clean_message(
            (
                "Lo revisé y está correcto.\n\n"
                "El lun, 30 mar 2026 a las 8:08, Kevin escribió:\n"
                "> Hi Nicolas,\n"
                "> Thanks for your message."
            ),
            cfg,
            runtime,
            role="assistant",
        )

        self.assertEqual(payload["content_retrieval"], "Lo revisé y está correcto.")
        self.assertEqual(payload["noise_stats"]["ticketformater_quote_reply_trims"], 1)
        self.assertIn("ticketformater_reply_trim", payload["noise_flags"])

    def test_ticketformater_reply_tail_is_not_trimmed_for_comment_role(self) -> None:
        cfg = self._load_cfg()
        runtime = clean_anonymize._build_clean_runtime(cfg)

        payload, _tags = clean_anonymize.clean_message(
            (
                "Comentario interno.\n"
                "El lun, 30 mar 2026 a las 8:08, Kevin escribió:\n"
                "Mantener contexto para la nota interna."
            ),
            cfg,
            runtime,
            role="comment",
        )

        self.assertIn("El lun, 30 mar 2026 a las 8:08, Kevin escribió:", payload["content_retrieval"])
        self.assertEqual(payload["noise_stats"]["ticketformater_quote_reply_trims"], 0)
        self.assertNotIn("ticketformater_reply_trim", payload["noise_flags"])

    def test_comment_closure_only_message_is_dropped(self) -> None:
        cfg = self._load_cfg()
        runtime = clean_anonymize._build_clean_runtime(cfg)

        payload, _tags = clean_anonymize.clean_message(
            "<p>Ok! Cerramos este ticket, cualquier cosa nos decís.</p><p>Saludos,<br /> Carmen</p>",
            cfg,
            runtime,
            role="comment",
        )

        self.assertEqual(payload["content_retrieval"], "")
        self.assertEqual(payload["content_raw"], "")
        self.assertFalse(payload["retrieval_fallback_used"])
        self.assertIn("comment_role_closure", payload["noise_flags"])
        self.assertIn("comment_role_removed_all", payload["noise_flags"])
        self.assertEqual(payload["noise_stats"]["comment_role_removed_all_content"], 1)

    def test_comment_closure_phrase_is_removed_but_useful_followup_remains(self) -> None:
        cfg = self._load_cfg()
        runtime = clean_anonymize._build_clean_runtime(cfg)

        payload, _tags = clean_anonymize.clean_message(
            (
                "Hola Silvia, Cierro este ticket. Por cierto, vasp 5.2.2 esta disponible en SVG por si "
                "quieres probarlo. Un saludo, Carmen."
            ),
            cfg,
            runtime,
            role="comment",
        )

        self.assertIn("vasp 5.2.2 esta disponible en SVG", payload["content_retrieval"])
        self.assertNotIn("Cierro este ticket", payload["content_retrieval"])
        self.assertEqual(payload["noise_stats"]["comment_role_removed_all_content"], 0)
        self.assertIn("comment_role_closure", payload["noise_flags"])

    def test_message_passthrough_preserves_full_text_without_cleanup(self) -> None:
        cfg = self._load_cfg()
        cfg.setdefault("cleaning", {})["message_passthrough"] = True
        runtime = clean_anonymize._build_clean_runtime(cfg)

        text = (
            "Hola Silvia, Cierro este ticket. Por cierto, vasp 5.2.2 esta disponible en SVG.\n"
            "El lun, 30 mar 2026 a las 8:08, Kevin escribió:\n"
            "Mantener contexto completo para embedding."
        )
        payload, tags = clean_anonymize.clean_message(
            text,
            cfg,
            runtime,
            role="comment",
        )

        self.assertEqual(payload["content_retrieval"], text)
        self.assertEqual(payload["content_raw"], text)
        self.assertEqual(payload["noise_flags"], [])
        self.assertFalse(payload["retrieval_fallback_used"])
        self.assertEqual(payload["noise_stats"]["ticketformater_quote_reply_trims"], 0)
        self.assertEqual(payload["noise_stats"]["comment_role_closure_replacements"], 0)
        self.assertEqual(tags, {"EMAIL": 0, "PHONE": 0, "PERSON": 0})

    def test_autoreply_only_mode_removes_auto_header_but_keeps_non_auto_content(self) -> None:
        cfg = self._load_cfg_autoreply_only()
        runtime = clean_anonymize._build_clean_runtime(cfg)

        payload, _tags = clean_anonymize.clean_message(
            (
                'Greetings, This message has been automatically generated in response to the creation of a '
                'trouble ticket regarding: "Instalación Molpro", a summary of which appears below. '
                "There is no need to reply to this message right now. "
                "Your ticket has been assigned an ID of [example.com #1]. "
                "Please include the string: [example.com #1] in the subject line of all future correspondence "
                "about this issue. To do so, you may reply to this message. Thank you, "
                "Hola, sigo con error de licencia al ejecutar molpro."
            ),
            cfg,
            runtime,
            role="user",
        )

        self.assertIn("Subject: Instalación Molpro", payload["content_retrieval"])
        self.assertIn("Hola, sigo con error de licencia al ejecutar molpro.", payload["content_retrieval"])
        self.assertNotIn("This message has been automatically generated", payload["content_retrieval"])
        self.assertNotIn("There is no need to reply", payload["content_retrieval"])
        self.assertEqual(payload["noise_stats"]["comment_role_closure_replacements"], 0)
        self.assertEqual(payload["noise_stats"]["noise_candidates"], 0)

    def test_autoreply_only_mode_removes_split_forwarded_auto_header(self) -> None:
        cfg = self._load_cfg_autoreply_only()
        runtime = clean_anonymize._build_clean_runtime(cfg)

        payload, _tags = clean_anonymize.clean_message(
            (
                "RT [mailto:helpdesk_promocion@cesga.es] Enviado el: lunes, 16 de enero de 2012 19:55 "
                "Asunto: [cesga.es #21297] AutoReply: Web: solicitud servicio alojamiento web "
                "This message has been automatically generated\n"
                "in response to the creation of a trouble ticket regarding: "
                "\"Web: solicitud servicio alojamiento web\", a summary of which appears below. "
                "There is no need to reply to this message right now. "
                "Your ticket has been assigned an ID of [cesga.es #21297]. "
                "Please include the string: [cesga.es #21297] in the subject line of all future correspondence "
                "about this issue. To do so, you may reply to this message. "
                "Hola Juan, por favor revisa esta solicitud."
            ),
            cfg,
            runtime,
            role="comment",
        )

        lower = payload["content_retrieval"].lower()
        self.assertNotIn("this message has been automatically generated", lower)
        self.assertNotIn("there is no need to reply to this message right now", lower)
        self.assertIn("hola juan, por favor revisa esta solicitud.", lower)

    def test_autoreply_only_mode_does_not_reinject_header_from_subject_capture(self) -> None:
        cfg = self._load_cfg_autoreply_only()
        runtime = clean_anonymize._build_clean_runtime(cfg)

        payload, _tags = clean_anonymize.clean_message(
            (
                "Asunto: [cesga.es #21297] AutoReply: Web: solicitud servicio alojamiento web\n"
                "Date: Tue, 17 Jan 2012 11:11:50 +0100\n"
                "From: Fernando Bouzas <fbouzas@cesga.es>\n"
                "To: comunicaciones@cesga.es\n"
                "This message has been automatically generated in response to the creation of a trouble ticket regarding: "
                "\"Web: solicitud servicio alojamiento web\", a summary of which appears below. "
                "There is no need to reply to this message right now. Your ticket has been assigned an ID of [cesga.es #21297]. "
                "Please include the string: [cesga.es #21297] in the subject line of all future correspondence about this issue. "
                "To do so, you may reply to this message. Thank you, "
                "Texto útil final."
            ),
            cfg,
            runtime,
            role="user",
        )

        lower = payload["content_retrieval"].lower()
        self.assertNotIn("this message has been automatically generated", lower)
        self.assertNotIn("there is no need to reply to this message right now", lower)
        self.assertIn("texto útil final.", lower)

    def test_autoreply_only_mode_preserves_comment_closure_and_signature(self) -> None:
        cfg = self._load_cfg_autoreply_only()
        runtime = clean_anonymize._build_clean_runtime(cfg)

        text = (
            "<p>Ok! Cerramos este ticket, cualquier cosa nos decís.</p>"
            "<p>Saludos,<br /> Carmen Cotelo Queijo<br />CESGA</p>"
        )
        payload, _tags = clean_anonymize.clean_message(
            text,
            cfg,
            runtime,
            role="comment",
        )

        self.assertIn("Cerramos este ticket", payload["content_retrieval"])
        self.assertIn("Saludos,", payload["content_retrieval"])
        self.assertIn("Carmen Cotelo Queijo", payload["content_retrieval"])
        self.assertEqual(payload["noise_stats"]["comment_role_removed_all_content"], 0)
        self.assertEqual(payload["noise_stats"]["comment_role_closure_replacements"], 0)


if __name__ == "__main__":
    unittest.main()
