from __future__ import annotations

import os
import re
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional


_MEMORY_RE = re.compile(r"^[1-9][0-9]*[KMGTP]?$", re.IGNORECASE)
_ALLOCATION_PATTERNS = (
    re.compile(r"\ballocation(?:_id)?\s*[:=]\s*([A-Za-z0-9._-]+)", re.IGNORECASE),
    re.compile(r"\bjob(?:_id)?\s*[:=]\s*([A-Za-z0-9._-]+)", re.IGNORECASE),
    re.compile(r"Submitted batch job\s+([0-9]+)", re.IGNORECASE),
)


@dataclass(frozen=True)
class HpcResourceSpec:
    cores: int = 32
    memory: str = "32G"
    gpu: bool = True

    def __post_init__(self) -> None:
        if self.cores < 1:
            raise ValueError("cores must be >= 1")
        memory = str(self.memory or "").strip().upper()
        if not _MEMORY_RE.match(memory):
            raise ValueError("memory must match <int>[K|M|G|T|P], for example: 32G")
        object.__setattr__(self, "memory", memory)


@dataclass(frozen=True)
class HpcCommandBundle:
    request_command: str
    remote_command: str


@dataclass(frozen=True)
class HpcRunResult:
    return_code: int
    stdout: str
    stderr: str
    request_command: str
    remote_command: str
    allocation_id: Optional[str]


@dataclass(frozen=True)
class HpcExecutorConfig:
    remote_host: str
    remote_user: Optional[str] = None
    ssh_key_path: Optional[str] = None
    remote_workdir: Optional[str] = None
    ssh_binary: str = "ssh"
    submit_template: Optional[str] = None
    cancel_template: str = "scancel {allocation_id}"
    strict_host_key_checking: str = "accept-new"

    @classmethod
    def from_env(cls) -> Optional["HpcExecutorConfig"]:
        host = (os.getenv("RAG_HPC_REMOTE_HOST") or "").strip()
        if not host:
            return None
        return cls(
            remote_host=host,
            remote_user=(os.getenv("RAG_HPC_REMOTE_USER") or "").strip() or None,
            ssh_key_path=(os.getenv("RAG_HPC_SSH_KEY_PATH") or "").strip() or None,
            remote_workdir=(os.getenv("RAG_HPC_REMOTE_WORKDIR") or "").strip() or None,
            ssh_binary=(os.getenv("RAG_HPC_SSH_BINARY") or "ssh").strip() or "ssh",
            submit_template=(os.getenv("RAG_HPC_SUBMIT_TEMPLATE") or "").strip() or None,
            cancel_template=(os.getenv("RAG_HPC_CANCEL_TEMPLATE") or "scancel {allocation_id}").strip(),
            strict_host_key_checking=(os.getenv("RAG_HPC_STRICT_HOST_KEY_CHECKING") or "accept-new").strip(),
        )


def parse_allocation_id(text: str) -> Optional[str]:
    raw = str(text or "")
    for pattern in _ALLOCATION_PATTERNS:
        match = pattern.search(raw)
        if match:
            return match.group(1)
    return None


def build_compute_request_command(spec: HpcResourceSpec) -> str:
    parts = [
        "compute",
        "-c",
        str(spec.cores),
        "--mem",
        spec.memory,
    ]
    if spec.gpu:
        parts.append("--gpu")
    return " ".join(parts)


def build_remote_command(
    *,
    payload_command: str,
    spec: HpcResourceSpec,
    submit_template: Optional[str] = None,
) -> HpcCommandBundle:
    request = build_compute_request_command(spec)
    payload = shlex.quote(payload_command)
    if submit_template:
        if "{compute}" not in submit_template or "{payload}" not in submit_template:
            raise ValueError("submit_template must contain {compute} and {payload} placeholders")
        remote = submit_template.format(compute=request, payload=payload)
    else:
        remote = f"{request} -- bash -lc {payload}"
    return HpcCommandBundle(request_command=request, remote_command=remote)


class HpcExecutor:
    def __init__(self, config: HpcExecutorConfig):
        self.config = config

    def _target(self) -> str:
        user = (self.config.remote_user or "").strip()
        if user:
            return f"{user}@{self.config.remote_host}"
        return self.config.remote_host

    def _build_remote_shell_command(self, remote_command: str) -> str:
        workdir = (self.config.remote_workdir or "").strip()
        if not workdir:
            return remote_command
        return f"cd {shlex.quote(workdir)} && {remote_command}"

    def build_ssh_command(self, remote_command: str) -> list[str]:
        cmd = [
            self.config.ssh_binary,
            "-o",
            "BatchMode=yes",
            "-o",
            f"StrictHostKeyChecking={self.config.strict_host_key_checking}",
        ]
        if self.config.ssh_key_path:
            cmd.extend(["-i", self.config.ssh_key_path])
        cmd.extend(
            [
                self._target(),
                "bash",
                "-lc",
                self._build_remote_shell_command(remote_command),
            ]
        )
        return cmd

    def run(
        self,
        *,
        payload_command: str,
        spec: Optional[HpcResourceSpec] = None,
        timeout_sec: Optional[int] = None,
        submit_template: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
    ) -> HpcRunResult:
        effective_spec = spec or HpcResourceSpec()
        template = submit_template if submit_template is not None else self.config.submit_template
        bundle = build_remote_command(
            payload_command=payload_command,
            spec=effective_spec,
            submit_template=template,
        )
        proc = subprocess.run(
            self.build_ssh_command(bundle.remote_command),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            env=dict(env) if env is not None else None,
        )
        allocation = parse_allocation_id(f"{proc.stdout}\n{proc.stderr}")
        return HpcRunResult(
            return_code=int(proc.returncode),
            stdout=proc.stdout or "",
            stderr=proc.stderr or "",
            request_command=bundle.request_command,
            remote_command=bundle.remote_command,
            allocation_id=allocation,
        )

    def build_cancel_remote_command(self, allocation_id: str) -> str:
        clean_id = str(allocation_id or "").strip()
        if not clean_id:
            raise ValueError("allocation_id is required")
        template = self.config.cancel_template
        if "{allocation_id}" not in template:
            raise ValueError("cancel_template must contain {allocation_id}")
        return template.format(allocation_id=shlex.quote(clean_id))

    def cancel(self, allocation_id: str, timeout_sec: Optional[int] = 30) -> HpcRunResult:
        remote_command = self.build_cancel_remote_command(allocation_id)
        proc = subprocess.run(
            self.build_ssh_command(remote_command),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
        return HpcRunResult(
            return_code=int(proc.returncode),
            stdout=proc.stdout or "",
            stderr=proc.stderr or "",
            request_command="cancel",
            remote_command=remote_command,
            allocation_id=allocation_id,
        )


def resolve_hpc_ssh_key(path_value: Optional[str]) -> Optional[str]:
    value = str(path_value or "").strip()
    if not value:
        return None
    expanded = Path(value).expanduser()
    return str(expanded)
