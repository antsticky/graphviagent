from __future__ import annotations

import os
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

CONFIG_NAME = "graphviagent.toml"


@dataclass
class PipelineSpec:
    id: str
    file: Path
    factory: str | None = None


@dataclass
class GVAConfig:
    root: Path
    path: Path | None = None
    pythonpath: list[Path] = field(default_factory=list)
    env_file: Path | None = None
    context: dict[str, Any] = field(default_factory=dict)
    pipelines: dict[str, PipelineSpec] = field(default_factory=dict)

    def factory_for(self, path: Path) -> str | None:
        path = path.resolve()
        for spec in self.pipelines.values():
            if spec.file.resolve() == path:
                return spec.factory
        return None

    def stem_for(self, path: Path) -> str:
        path = path.resolve()
        for spec in self.pipelines.values():
            if spec.file.resolve() == path:
                return spec.id
        return path.stem


def find_config_file(start: Path) -> Path | None:
    start = start.resolve()
    if start.is_file():
        start = start.parent
    for folder in [start, *start.parents]:
        candidate = folder / CONFIG_NAME
        if candidate.is_file():
            return candidate
    return None


def _resolve_against(base: Path, value: str | Path) -> Path:
    raw = Path(value)
    if raw.is_absolute():
        return raw.resolve()
    return (base / raw).resolve()


def load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


def apply_pythonpath(entries: list[Path]) -> None:
    for entry in reversed(entries):
        directory = str(entry.resolve())
        if directory not in sys.path:
            sys.path.insert(0, directory)


def activate_config(config: GVAConfig) -> None:
    apply_pythonpath(config.pythonpath)
    if config.env_file is not None:
        load_env_file(config.env_file)


def load_config(start: Path) -> GVAConfig:
    start = start.resolve()
    toml_path = find_config_file(start)
    if toml_path is None:
        root = start if start.is_dir() else start.parent
        return GVAConfig(root=root)

    with toml_path.open("rb") as handle:
        data = tomllib.load(handle)

    base = toml_path.parent
    root = _resolve_against(base, str(data.get("root") or "."))

    if "pythonpath" in data:
        raw_path = data.get("pythonpath") or []
        if isinstance(raw_path, str):
            raw_path = [raw_path]
    else:
        raw_path = ["."]
    pythonpath = [_resolve_against(root, str(item)) for item in raw_path]

    env_raw = data.get("env_file")
    env_file = _resolve_against(root, str(env_raw)) if env_raw else None

    context = data.get("context")
    if not isinstance(context, dict):
        context = {}

    pipelines: dict[str, PipelineSpec] = {}
    raw_pipelines = data.get("pipeline") or {}
    if isinstance(raw_pipelines, dict):
        for name, spec in raw_pipelines.items():
            if not isinstance(spec, dict):
                continue
            file_raw = spec.get("file")
            if not file_raw:
                continue
            factory = spec.get("factory")
            pipelines[str(name)] = PipelineSpec(
                id=str(name),
                file=_resolve_against(root, str(file_raw)),
                factory=str(factory) if factory else None,
            )

    return GVAConfig(
        root=root,
        path=toml_path,
        pythonpath=pythonpath,
        env_file=env_file,
        context=context,
        pipelines=pipelines,
    )
