from __future__ import annotations

import hashlib
import importlib.util
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


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


def load_module(path: Path) -> Any:
    path = path.resolve()
    key = hashlib.md5(str(path).encode(), usedforsecurity=False).hexdigest()
    name = f"graphviagent_pipe_{key}"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_pipeline(path: Path) -> LoadedPipeline:
    path = path.resolve()
    loaded = LoadedPipeline(path=path, stem=path.stem)
    try:
        module = load_module(path)
        loaded.module = module
        examples = getattr(module, "EXAMPLES", None)
        if isinstance(examples, list):
            loaded.examples = [item for item in examples if isinstance(item, dict)]

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
    except Exception as exc:
        loaded.error = f"{type(exc).__name__}: {exc}"
    return loaded
