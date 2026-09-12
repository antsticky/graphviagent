from __future__ import annotations

from pathlib import Path

SKIP_DIRS = {
    ".venv",
    "venv",
    ".graphviagent",
    ".git",
    "__pycache__",
    "node_modules",
    ".mypy_cache",
    ".pytest_cache",
    "dist",
    "build",
}


def discover_pipelines(root: Path) -> list[Path]:
    root = root.resolve()
    found: list[Path] = []
    for path in sorted(root.rglob("*_pipeline.py")):
        if any(part in SKIP_DIRS or part.endswith(".egg-info") for part in path.parts):
            continue
        found.append(path)
    return found
