from __future__ import annotations

import os
from pathlib import Path

from graphviagent.config import GVAConfig

SKIP_DIRS = {
    ".venv",
    "venv",
    "virtualenv",
    ".tox",
    ".nox",
    ".direnv",
    ".pixi",
    "__pypackages__",
    ".graphviagent",
    ".git",
    "__pycache__",
    "node_modules",
    ".mypy_cache",
    ".pytest_cache",
    "dist",
    "build",
}


def _fold_name(name: str) -> str:
    return name.casefold() if os.name == "nt" else name


def _is_skip_name(name: str) -> bool:
    folded = _fold_name(name)
    if any(folded == _fold_name(item) for item in SKIP_DIRS):
        return True
    suffix = folded if os.name == "nt" else name
    return suffix.endswith(".egg-info")


def is_python_env(path: Path) -> bool:
    if _is_skip_name(path.name):
        return True
    try:
        if (path / "pyvenv.cfg").is_file():
            return True
        if (path / "conda-meta").is_dir():
            return True
    except OSError:
        return False
    return False


def path_is_skipped(path: Path, root: Path | None = None) -> bool:
    try:
        resolved = path.resolve()
    except OSError:
        resolved = path
    if any(_is_skip_name(part) for part in resolved.parts):
        return True
    if root is None:
        return False
    try:
        base = root.resolve()
    except OSError:
        base = root
    try:
        rel = resolved.relative_to(base)
    except ValueError:
        return False
    acc = base
    for part in rel.parts:
        acc = acc / part
        if is_python_env(acc):
            return True
    return False


def discover_pipelines(root: Path, config: GVAConfig | None = None) -> list[Path]:
    if config is not None and config.pipelines:
        return [spec.file.resolve() for spec in config.pipelines.values()]
    root = root.resolve()
    found: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
        current = Path(dirpath)
        dirnames[:] = [name for name in dirnames if not is_python_env(current / name)]
        for name in filenames:
            if name.endswith("_pipeline.py"):
                found.append(current / name)
    return sorted(found)
