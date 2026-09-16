from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from graphviagent.config import GVAConfig
from graphviagent.graph_hash import fingerprint_graph
from graphviagent.watch import file_sha256


FACTORY_NAMES = ("build_graph", "get_graph", "create_graph")

_load_lock = threading.Lock()
_modules: dict[str, tuple[str, Any]] = {}


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
    context: dict = field(default_factory=dict)


def _is_runnable(obj: Any) -> bool:
    return hasattr(obj, "stream") and hasattr(obj, "invoke")


def _is_state_graph(obj: Any) -> bool:
    compile_fn = getattr(obj, "compile", None)
    return callable(compile_fn) and hasattr(obj, "add_node") and hasattr(obj, "add_edge")


def _with_memory_saver(compile_fn: Any) -> tuple[Any, bool]:
    try:
        from langgraph.checkpoint.memory import MemorySaver

        return compile_fn(checkpointer=MemorySaver()), True
    except TypeError:
        return compile_fn(), False


def _compile_if_needed(obj: Any) -> tuple[Any, bool]:
    if _is_runnable(obj):
        checkpointer = getattr(obj, "checkpointer", None)
        if checkpointer not in (None, False):
            return obj, True
        builder = getattr(obj, "builder", None)
        compile_fn = getattr(builder, "compile", None)
        if callable(compile_fn):
            try:
                return _with_memory_saver(compile_fn)
            except Exception:
                return obj, False
        return obj, False
    if _is_state_graph(obj):
        return _with_memory_saver(obj.compile)
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


def _ensure_on_path(directory: Path) -> None:
    directory = str(directory.resolve())
    if directory not in sys.path:
        sys.path.insert(0, directory)


def _package_context(path: Path) -> tuple[str, str | None, Path]:
    path = path.resolve()
    stem = path.stem if path.stem.isidentifier() else f"_{path.stem}"
    parts = [stem]
    pkg_dir = path.parent
    while (pkg_dir / "__init__.py").is_file() and pkg_dir.name.isidentifier():
        parts.append(pkg_dir.name)
        pkg_dir = pkg_dir.parent
    module_name = ".".join(reversed(parts))
    parent = module_name.rpartition(".")[0] or None
    return module_name, parent, pkg_dir


def load_module(path: Path, extra_roots: list[Path] | None = None) -> Any:
    path = path.resolve()
    digest = file_sha256(path)
    module_name, parent, pkg_root = _package_context(path)

    with _load_lock:
        cached = _modules.get(str(path))
        if cached is not None and cached[0] == digest:
            return cached[1]

        for extra in reversed(extra_roots or []):
            _ensure_on_path(extra)
        if parent is not None:
            _ensure_on_path(pkg_root)
        _ensure_on_path(path.parent)

        importlib.invalidate_caches()

        old = _modules.pop(str(path), None)
        if old is not None:
            sys.modules.pop(getattr(old[1], "__name__", ""), None)

        existing = sys.modules.get(module_name)
        if existing is not None:
            existing_file = Path(getattr(existing, "__file__", "") or "")
            try:
                same = existing_file.resolve() == path
            except OSError:
                same = False
            if same:
                sys.modules.pop(module_name, None)
            else:
                suffix = hashlib.sha256(str(path).encode()).hexdigest()[:12]
                leaf = path.stem if path.stem.isidentifier() else "pipeline"
                module_name = f"{parent}.{leaf}_{suffix}" if parent else f"{leaf}_{suffix}"

        if parent:
            try:
                importlib.import_module(parent)
            except ImportError:
                pass

        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot import {path}")
        module = importlib.util.module_from_spec(spec)
        if parent:
            module.__package__ = parent
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise
        _modules[str(path)] = (digest, module)
        return module


def _extract_graph(module: Any, factory: str | None = None) -> Any:
    names: list[str] = []
    if isinstance(factory, str) and factory:
        names.append(factory)
    marker = getattr(module, "__graph__", None)
    if isinstance(marker, str):
        names.append(marker)
    names.extend(FACTORY_NAMES)
    seen: set[str] = set()
    for name in names:
        if not name or name in seen:
            continue
        seen.add(name)
        fn = getattr(module, name, None)
        if callable(fn):
            return fn()

    if marker is not None and not isinstance(marker, str):
        if _is_runnable(marker) or _is_state_graph(marker):
            return marker

    graph = getattr(module, "GRAPH", None)
    if graph is not None and (_is_runnable(graph) or _is_state_graph(graph)):
        return graph

    app = getattr(module, "app", None)
    if app is not None and _is_runnable(app):
        return app

    raise AttributeError(
        "need build_graph()/get_graph()/create_graph(), GRAPH, or a compiled app"
    )


def load_pipeline(path: Path, config: GVAConfig | None = None) -> LoadedPipeline:
    path = path.resolve()
    stem = config.stem_for(path) if config is not None else path.stem
    loaded = LoadedPipeline(path=path, stem=stem)
    if config is not None:
        loaded.context = dict(config.context)
    try:
        extra_roots = list(config.pythonpath) if config is not None else None
        factory = config.factory_for(path) if config is not None else None
        module = load_module(path, extra_roots=extra_roots)
        loaded.module = module
        loaded.examples = normalize_examples(getattr(module, "EXAMPLES", None))
        candidate = _extract_graph(module, factory=factory)
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
        loaded.app = None
        loaded.error = f"{type(exc).__name__}: {exc}"
    return loaded
