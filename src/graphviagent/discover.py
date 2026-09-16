from __future__ import annotations

from pathlib import Path

from graphviagent.config import GVAConfig

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


def discover_pipelines(root: Path, config: GVAConfig | None = None) -> list[Path]:
    if config is not None and config.pipelines:
        return [spec.file.resolve() for spec in config.pipelines.values()]
    root = root.resolve()
    found: list[Path] = []
    for path in sorted(root.rglob("*_pipeline.py")):
        if any(part in SKIP_DIRS or part.endswith(".egg-info") for part in path.parts):
            continue
        found.append(path)
    return found
