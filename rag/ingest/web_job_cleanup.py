from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import shutil
from typing import List


@dataclass(frozen=True)
class WebJobCleanupTarget:
    base_root: str
    job_id: str
    raw_job_root: str
    delete_roots: List[str]
    file_count: int
    dir_count: int
    total_bytes: int

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class WebJobCleanupResult:
    base_root: str
    job_id: str
    raw_job_root: str
    delete_roots: List[str]
    deleted_roots: List[str]
    file_count: int
    dir_count: int
    total_bytes: int
    mode: str

    def to_dict(self) -> dict:
        return asdict(self)


def validate_web_job_id(job_id: str) -> str:
    clean = (job_id or "").strip()
    if not clean:
        raise ValueError("job_id must not be empty")
    candidate = Path(clean)
    if clean in {".", ".."} or candidate.name != clean or len(candidate.parts) != 1:
        raise ValueError("job_id must be a single direct child name under the web jobs root")
    return clean


def _resolve_cleanup_target(raw_job_root: Path, base_root: Path) -> tuple[Path, Path]:
    candidate = raw_job_root.resolve()
    base = base_root.resolve(strict=False)
    if candidate == base:
        raise ValueError("raw job root must be a direct child of the web jobs base root")
    try:
        rel = candidate.relative_to(base)
    except ValueError as exc:
        raise ValueError("raw job root must stay inside the web jobs base root") from exc
    if len(rel.parts) != 1:
        raise ValueError("raw job root must be a direct child of the web jobs base root")
    return candidate, base


def _summarize_tree(root: Path) -> tuple[int, int, int]:
    file_count = 0
    dir_count = 0
    total_bytes = 0

    for candidate in root.rglob("*"):
        if candidate.is_dir():
            dir_count += 1
            continue
        file_count += 1
        try:
            total_bytes += candidate.stat().st_size
        except OSError:
            continue

    return file_count, dir_count, total_bytes


def discover_web_job_cleanup_target(
    raw_job_root: Path,
    *,
    base_root: Path,
) -> WebJobCleanupTarget:
    if not raw_job_root.exists():
        raise FileNotFoundError(f"Web job raw root does not exist: {raw_job_root}")
    if raw_job_root.is_symlink():
        raise ValueError("Web job raw root must not be a symlink")
    if not raw_job_root.is_dir():
        raise ValueError("Web job raw root must be a directory")

    candidate, base = _resolve_cleanup_target(raw_job_root, base_root)
    file_count, dir_count, total_bytes = _summarize_tree(candidate)
    return WebJobCleanupTarget(
        base_root=str(base),
        job_id=candidate.name,
        raw_job_root=str(candidate),
        delete_roots=[str(candidate)],
        file_count=file_count,
        dir_count=dir_count,
        total_bytes=total_bytes,
    )


def discover_web_job_cleanup_targets(
    base_root: Path,
    *,
    job_id: str | None = None,
) -> List[WebJobCleanupTarget]:
    resolved_base = base_root.resolve(strict=False)

    if job_id is not None:
        safe_job_id = validate_web_job_id(job_id)
        return [
            discover_web_job_cleanup_target(
                resolved_base / safe_job_id,
                base_root=resolved_base,
            )
        ]

    if not base_root.exists():
        return []
    if not base_root.is_dir():
        raise ValueError(f"Web jobs base root must be a directory: {base_root}")

    targets: List[WebJobCleanupTarget] = []
    for candidate in sorted(base_root.iterdir(), key=lambda item: item.name):
        if candidate.is_symlink():
            raise ValueError(f"Unsafe symlink entry under web jobs root: {candidate}")
        if not candidate.is_dir():
            continue
        targets.append(discover_web_job_cleanup_target(candidate, base_root=resolved_base))
    return targets


def cleanup_web_job_artifacts(
    raw_job_root: Path,
    *,
    base_root: Path,
    apply: bool,
) -> WebJobCleanupResult:
    target = discover_web_job_cleanup_target(raw_job_root, base_root=base_root)
    deleted_roots: List[str] = []
    if apply:
        shutil.rmtree(raw_job_root)
        deleted_roots.append(target.raw_job_root)

    return WebJobCleanupResult(
        base_root=target.base_root,
        job_id=target.job_id,
        raw_job_root=target.raw_job_root,
        delete_roots=target.delete_roots,
        deleted_roots=deleted_roots,
        file_count=target.file_count,
        dir_count=target.dir_count,
        total_bytes=target.total_bytes,
        mode="apply" if apply else "dry-run",
    )
