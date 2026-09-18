from __future__ import annotations

import os
import re
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


SKIP_FOLDED = {_fold_name(name) for name in SKIP_DIRS}

_ENV_LIKE = re.compile(
    r"""
    ^(?:
        \.?venv |
        \.?virtualenv |
        \.?env |
        python[\d.]* |
        pypy[\d.]* |
        miniconda\d* |
        anaconda\d* |
        miniforge\d* |
        mambaforge\d* |
        micromamba |
        conda |
        .*[-_](?:env|venv)
    )$
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _is_skip_name(name: str) -> bool:
    folded = _fold_name(name)
    if folded in SKIP_FOLDED:
        return True
    suffix = folded if os.name == "nt" else name
    return suffix.endswith(".egg-info")


def _looks_like_env_name(name: str) -> bool:
    return bool(_ENV_LIKE.match(name))


def _has_python_prefix(path: Path) -> bool:
    try:
        posix = (path / "bin" / "python").is_file() or (path / "bin" / "python3").is_file()
        windows = (path / "Scripts" / "python.exe").is_file()
        embed = (path / "python.exe").is_file()
        if not (posix or windows or embed):
            return False
        return (
            (path / "lib").is_dir()
            or (path / "lib64").is_dir()
            or (path / "Lib").is_dir()
        )
    except OSError:
        return False


def is_python_env(path: Path) -> bool:
    if _is_skip_name(path.name):
        return True
    if not _looks_like_env_name(path.name):
        return False
    try:
        if (path / "pyvenv.cfg").is_file():
            return True
        if (path / "conda-meta").is_dir():
            return True
    except OSError:
        return False
    return _has_python_prefix(path)


def abs_path(path: Path) -> Path:
    return Path(os.path.abspath(os.path.normpath(str(path))))


def _relative_to(path: Path, root: Path) -> Path | None:
    path_key = os.path.abspath(os.path.normpath(str(path)))
    root_key = os.path.abspath(os.path.normpath(str(root)))
    if os.name == "nt":
        path_key = os.path.normcase(path_key)
        root_key = os.path.normcase(root_key)
    if path_key == root_key:
        return Path()
    prefix = root_key.rstrip("\\/") + os.sep
    if not path_key.startswith(prefix):
        return None
    return Path(os.path.relpath(os.path.abspath(str(path)), os.path.abspath(str(root))))


def path_is_skipped(path: Path, root: Path | None = None) -> bool:
    path = abs_path(path)
    if root is None:
        return any(_is_skip_name(part) for part in path.parts)
    base = abs_path(root)
    rel = _relative_to(path, base)
    if rel is None:
        return False
    acc = base
    for part in rel.parts:
        if part in ("", os.curdir):
            continue
        acc = acc / part
        if _is_skip_name(part) or is_python_env(acc):
            return True
    return False


def discover_pipelines(root: Path, config: GVAConfig | None = None) -> list[Path]:
    if config is not None and config.pipelines:
        return [abs_path(spec.file) for spec in config.pipelines.values()]
    root = abs_path(root)
    found: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
        current = Path(dirpath)
        dirnames[:] = [name for name in dirnames if not is_python_env(current / name)]
        for name in filenames:
            if name.endswith("_pipeline.py"):
                found.append(current / name)
    return sorted(found)
