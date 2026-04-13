from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def resolve_project_path(raw_path: str, project_root: Path = PROJECT_ROOT) -> Path:
    """Resolve an absolute or project-relative path."""

    path = Path(raw_path)
    if path.is_absolute():
        return path
    return project_root / path
