from __future__ import annotations

import subprocess
import unittest
from unittest.mock import patch

from ingest.hpc_executor import (
    HpcExecutor,
    HpcExecutorConfig,
    HpcResourceSpec,
    build_compute_request_command,
    build_remote_command,
    parse_allocation_id,
)


class HpcExecutorTests(unittest.TestCase):
    def test_resource_spec_normalizes_memory(self) -> None:
        spec = HpcResourceSpec(cores=8, memory="32g", gpu=False)
        self.assertEqual(spec.memory, "32G")
        self.assertEqual(spec.cores, 8)
        self.assertFalse(spec.gpu)

    def test_resource_spec_rejects_invalid_memory(self) -> None:
        with self.assertRaises(ValueError):
            HpcResourceSpec(cores=8, memory="0G", gpu=True)

    def test_build_compute_request_command(self) -> None:
        command = build_compute_request_command(HpcResourceSpec(cores=32, memory="32G", gpu=True))
        self.assertEqual(command, "compute -c 32 --mem 32G --gpu")

    def test_build_remote_command_default_template(self) -> None:
        bundle = build_remote_command(
            payload_command="bash scripts/main_daily_ingest.sh",
            spec=HpcResourceSpec(),
        )
        self.assertIn("compute -c 32 --mem 32G --gpu", bundle.request_command)
        self.assertIn(" -- bash -lc ", bundle.remote_command)

    def test_build_remote_command_requires_template_placeholders(self) -> None:
        with self.assertRaises(ValueError):
            build_remote_command(
                payload_command="echo ok",
                spec=HpcResourceSpec(),
                submit_template="compute only",
            )

    def test_parse_allocation_id(self) -> None:
        self.assertEqual(parse_allocation_id("allocation_id=abc123"), "abc123")
        self.assertEqual(parse_allocation_id("Submitted batch job 9898"), "9898")
        self.assertIsNone(parse_allocation_id("no allocation info"))

    def test_build_cancel_remote_command_requires_placeholder(self) -> None:
        executor = HpcExecutor(
            HpcExecutorConfig(
                remote_host="ft3.cesga.es",
                cancel_template="scancel",
            )
        )
        with self.assertRaises(ValueError):
            executor.build_cancel_remote_command("12345")

    def test_build_cancel_remote_command_quotes_allocation_id(self) -> None:
        executor = HpcExecutor(
            HpcExecutorConfig(
                remote_host="ft3.cesga.es",
                cancel_template="scancel {allocation_id}",
            )
        )
        command = executor.build_cancel_remote_command("12345;rm -rf /")
        self.assertEqual(command, "scancel '12345;rm -rf /'")

    def test_build_ssh_command_includes_target_and_workdir(self) -> None:
        executor = HpcExecutor(
            HpcExecutorConfig(
                remote_host="ft3.cesga.es",
                remote_user="tec_app2",
                ssh_key_path="/tmp/key",
                remote_workdir="/mnt/netapp1/Store_CESGA/home/cesga/tec_app2/rag",
            )
        )
        command = executor.build_ssh_command("echo ok")
        self.assertEqual(command[0], "ssh")
        self.assertIn("tec_app2@ft3.cesga.es", command)
        self.assertIn("bash", command)
        self.assertIn("-lc", command)
        self.assertTrue(command[-1].startswith("cd "))

    @patch("ingest.hpc_executor.subprocess.run")
    def test_run_returns_parsed_allocation_id(self, run_mock) -> None:
        run_mock.return_value = subprocess.CompletedProcess(
            args=["ssh"],
            returncode=0,
            stdout="allocation_id=777\n",
            stderr="",
        )
        executor = HpcExecutor(HpcExecutorConfig(remote_host="ft3.cesga.es"))
        result = executor.run(payload_command="echo hello")
        self.assertEqual(result.return_code, 0)
        self.assertEqual(result.allocation_id, "777")
        self.assertIn("compute -c 32 --mem 32G --gpu", result.request_command)
        self.assertTrue(run_mock.called)

    @patch("ingest.hpc_executor.subprocess.run")
    def test_cancel_uses_cancel_template(self, run_mock) -> None:
        run_mock.return_value = subprocess.CompletedProcess(
            args=["ssh"],
            returncode=0,
            stdout="cancelled\n",
            stderr="",
        )
        executor = HpcExecutor(
            HpcExecutorConfig(
                remote_host="ft3.cesga.es",
                cancel_template="scancel {allocation_id}",
            )
        )
        result = executor.cancel("12345")
        self.assertEqual(result.return_code, 0)
        self.assertEqual(result.allocation_id, "12345")
        self.assertIn("scancel", result.remote_command)
        self.assertTrue(run_mock.called)


if __name__ == "__main__":
    unittest.main()
