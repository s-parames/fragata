from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
import re
import shutil
from typing import List, Tuple


_BACKUP_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"


@dataclass(frozen=True)
class IndexBackupDir:
    path: Path
    timestamp: datetime


@dataclass(frozen=True)
class IndexBackupPruneResult:
    active_dir: str
    keep_last: int
    retained: List[str]
    prunable: List[str]
    deleted: List[str]
    mode: str

    def to_dict(self) -> dict:
        return asdict(self)


def discover_index_backups(active_dir: Path) -> List[IndexBackupDir]:
    parent = active_dir.parent
    family_name = active_dir.name
    pattern = re.compile(rf"^{re.escape(family_name)}\.backup\.(\d{{8}}_\d{{6}})$")
    backups: List[IndexBackupDir] = []

    if not parent.exists():
        return backups

    for candidate in parent.iterdir():
        if not candidate.is_dir():
            continue
        match = pattern.fullmatch(candidate.name)
        if match is None:
            continue
        try:
            timestamp = datetime.strptime(match.group(1), _BACKUP_TIMESTAMP_FORMAT)
        except ValueError:
            continue
        backups.append(IndexBackupDir(path=candidate, timestamp=timestamp))

    backups.sort(key=lambda item: (item.timestamp, item.path.name), reverse=True)
    return backups


def compute_backup_retention(
    active_dir: Path,
    *,
    keep_last: int,
) -> Tuple[List[IndexBackupDir], List[IndexBackupDir]]:
    if keep_last < 0:
        raise ValueError("keep_last must be >= 0")

    backups = discover_index_backups(active_dir)
    retained = backups[:keep_last]
    prunable = backups[keep_last:]
    return retained, prunable


def prune_index_backups(
    active_dir: Path,
    *,
    keep_last: int,
    apply: bool,
) -> IndexBackupPruneResult:
    retained, prunable = compute_backup_retention(active_dir, keep_last=keep_last)
    deleted: List[str] = []
    if apply:
        for backup in prunable:
            shutil.rmtree(backup.path)
            deleted.append(str(backup.path))

    return IndexBackupPruneResult(
        active_dir=str(active_dir),
        keep_last=keep_last,
        retained=[str(item.path) for item in retained],
        prunable=[str(item.path) for item in prunable],
        deleted=deleted,
        mode="apply" if apply else "dry-run",
    )
