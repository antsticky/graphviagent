from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from graphviagent.graph_hash import fingerprint_graph
from graphviagent.watch import file_sha256


FACTORY_NAMES = ("build_graph", "get_graph", "create_graph")


@dataclass
class LoadedPipeline:
    path: Path
    stem: str
    app: Any = None
    examples: list[dict] = field(default_factory=list)
    error: str | None = None
    has_checkpointer: bool = False
    module: Any = None
    graph: dict = field(default_factory=dict)
    graph_hash: str | None = None
    file_sha256: str | None = None


def _is_runnable(obj: Any) -> bool:
    return hasattr(obj, "stream") and hasattr(obj, "invoke")


def _compile_if_needed(obj: Any) -> tuple[Any, bool]:
    if _is_runnable(obj):
        checkpointer = getattr(obj, "checkpointer", None)
        return obj, checkpointer not in (None, False)
    compile_fn = getattr(obj, "compile", None)
    if callable(compile_fn):
        try:
            from langgraph.checkpoint.memory import MemorySaver

            app = compile_fn(checkpointer=MemorySaver())
            return app, True
        except TypeError:
            return compile_fn(), False
    raise TypeError("object is not a compiled graph or StateGraph")


def example_label(data: dict) -> str:
    parts: list[str] = []
    for key, value in data.items():
        text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
        parts.append(f"{key}={text}")
        if len(parts) >= 3:
            break
    label = "  ".join(parts) or "example"
    return label if len(label) <= 42 else label[:39] + "..."


def normalize_examples(raw: Any) -> list[dict]:
    if not isinstance(raw, list):
        return []
    out: list[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        if isinstance(item.get("label"), str) and isinstance(item.get("input"), dict):
            out.append({"label": item["label"], "input": item["input"]})
        else:
            out.append({"label": example_label(item), "input": item})
    return out


def load_module(path: Path) -> Any:
    path = path.resolve()
    digest = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    name = f"graphviagent_pipe_{digest}"
    cached = sys.modules.get(name)
    if cached is not None:
        return cached
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(name, None)
        raise
    return module


def load_pipeline(path: Path) -> LoadedPipeline:
    path = path.resolve()
    loaded = LoadedPipeline(path=path, stem=path.stem)
    try:
        module = load_module(path)
        loaded.module = module
        loaded.examples = normalize_examples(getattr(module, "EXAMPLES", None))

        candidate = None
        for attr in ("GRAPH", "app"):
            obj = getattr(module, attr, None)
            if obj is not None and (_is_runnable(obj) or hasattr(obj, "compile")):
                candidate = obj
                break
        if candidate is None:
            factory_name = getattr(module, "__graph__", None)
            names = [factory_name] if isinstance(factory_name, str) else []
            names.extend(FACTORY_NAMES)
            for name in names:
                fn = getattr(module, name, None)
                if callable(fn):
                    candidate = fn()
                    break
        if candidate is None:
            raise AttributeError(
                "need GRAPH, app, or build_graph()/get_graph()/create_graph()"
            )
        loaded.app, loaded.has_checkpointer = _compile_if_needed(candidate)
        try:
            loaded.file_sha256 = file_sha256(path)
        except OSError:
            loaded.file_sha256 = None
        if loaded.app is not None:
            loaded.graph, loaded.graph_hash = fingerprint_graph(
                loaded.app,
                loaded.file_sha256,
            )
    except Exception as exc:
        loaded.error = f"{type(exc).__name__}: {exc}"
    return loaded
