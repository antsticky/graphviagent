from __future__ import annotations

import functools
import hashlib
import inspect
import json
from pathlib import Path
from typing import Any

from graphviagent.record import graph_edges

_SKIP_NODES = {"__start__", "__end__", "START", "END"}


def _graph_obj(app: Any) -> Any:
    getter = getattr(app, "get_graph", None)
    if not callable(getter):
        return None
    try:
        return getter()
    except Exception:
        return None


def graph_nodes(app: Any) -> list[str]:
    names: set[str] = set()
    graph = _graph_obj(app)
    nodes = getattr(graph, "nodes", None) if graph is not None else None
    if isinstance(nodes, dict):
        names.update(str(key) for key in nodes)
    elif nodes:
        for node in nodes:
            name = getattr(node, "id", None) or getattr(node, "name", None) or node
            names.add(str(name))
    raw = getattr(app, "nodes", None)
    if isinstance(raw, dict):
        names.update(str(key) for key in raw)
    return sorted(name for name in names if name not in _SKIP_NODES)


def graph_state_keys(app: Any) -> list[str]:
    keys: set[str] = set()
    channels = getattr(app, "channels", None)
    if isinstance(channels, dict):
        keys.update(str(key) for key in channels)
    builder = getattr(app, "builder", None)
    schema = getattr(builder, "schema", None) if builder is not None else None
    if schema is None:
        schema = getattr(app, "schema", None)
    if schema is not None:
        annotations = getattr(schema, "__annotations__", None)
        if isinstance(annotations, dict):
            keys.update(str(key) for key in annotations)
        fields = getattr(schema, "model_fields", None)
        if isinstance(fields, dict):
            keys.update(str(key) for key in fields)
    return sorted(key for key in keys if not str(key).startswith("__"))


def _tool_name(obj: Any) -> str | None:
    if obj is None:
        return None
    if isinstance(obj, str) and obj:
        return obj
    name = getattr(obj, "name", None) or getattr(obj, "__name__", None)
    if isinstance(name, str) and name:
        return name
    return None


def _collect_tools(obj: Any, found: set[str], seen: set[int] | None = None) -> None:
    if obj is None:
        return
    marker = id(obj)
    seen = seen if seen is not None else set()
    if marker in seen:
        return
    seen.add(marker)
    tools = getattr(obj, "tools", None)
    if tools:
        values = tools.values() if isinstance(tools, dict) else tools
        try:
            iterator = list(values)
        except TypeError:
            iterator = []
        for tool in iterator:
            name = _tool_name(tool)
            if name:
                found.add(name)
    kwargs = getattr(obj, "kwargs", None)
    if isinstance(kwargs, dict):
        extra = kwargs.get("tools")
        if extra:
            for tool in extra:
                name = _tool_name(tool)
                if name:
                    found.add(name)
    for attr in ("bound", "runnable", "func"):
        child = getattr(obj, attr, None)
        if child is not None:
            _collect_tools(child, found, seen)


def graph_tools(app: Any) -> list[str]:
    found: set[str] = set()
    nodes = getattr(app, "nodes", None)
    if isinstance(nodes, dict):
        for node in nodes.values():
            _collect_tools(node, found)
    return sorted(found)


_UNWRAP_ATTRS = ("func", "afunc", "fn", "bound", "runnable", "_func")
_LIBRARY_PATH_MARKERS = (
    "/langgraph/",
    "/langchain_core/",
    "/langchain/",
    "/graphviagent/",
    "/site-packages/",
    "/lib/python",
)


def _is_library_path(path: str) -> bool:
    if not path or path.startswith("<"):
        return True
    normalized = path.replace("\\", "/")
    return any(marker in normalized for marker in _LIBRARY_PATH_MARKERS)


def _walk_callables(obj: Any) -> Any:
    seen: set[int] = set()
    stack = [obj]
    while stack:
        current = stack.pop()
        if current is None:
            continue
        marker = id(current)
        if marker in seen:
            continue
        seen.add(marker)
        yield current
        if isinstance(current, functools.partial):
            stack.append(current.func)
            stack.extend(current.args)
            stack.extend(current.keywords.values() if current.keywords else ())
        for attr in _UNWRAP_ATTRS:
            stack.append(getattr(current, attr, None))


def _unwrap_callable(obj: Any) -> Any | None:
    functions: list[Any] = []
    others: list[Any] = []
    for current in _walk_callables(obj):
        try:
            inspect.getsourcefile(current)
        except (TypeError, OSError):
            continue
        if inspect.isfunction(current) or inspect.ismethod(current):
            functions.append(current)
        else:
            others.append(current)
    pool = functions or others
    if not pool:
        return None

    def score(item: Any) -> tuple[int, int]:
        try:
            file = inspect.getsourcefile(item) or ""
        except (TypeError, OSError):
            file = ""
        name = getattr(item, "__name__", "") or ""
        return (0 if _is_library_path(file) else 1, 0 if name == "<lambda>" else 1)

    return max(pool, key=score)


def _callable_source(obj: Any) -> str | None:
    target = _unwrap_callable(obj)
    if target is None:
        return None
    try:
        return inspect.getsource(target)
    except (TypeError, OSError):
        return None


def callable_location(obj: Any) -> dict[str, Any] | None:
    target = _unwrap_callable(obj)
    if target is None:
        return None
    try:
        raw_file = inspect.getsourcefile(target) or inspect.getfile(target)
    except (TypeError, OSError):
        raw_file = None
    if not raw_file or raw_file.startswith("<"):
        return None
    try:
        file = str(Path(raw_file).resolve())
    except OSError:
        file = raw_file
    line: int | None = None
    end_line: int | None = None
    try:
        lines, start = inspect.getsourcelines(target)
        line = int(start)
        end_line = int(start) + len(lines) - 1
    except (TypeError, OSError, ValueError):
        code = getattr(target, "__code__", None)
        first = getattr(code, "co_firstlineno", None)
        if isinstance(first, int):
            line = first
    name = getattr(target, "__qualname__", None) or getattr(target, "__name__", None)
    loc: dict[str, Any] = {"file": file}
    if line is not None:
        loc["line"] = line
    if end_line is not None:
        loc["end_line"] = end_line
    if isinstance(name, str) and name:
        loc["name"] = name
    return loc


def graph_impls(app: Any) -> dict[str, str]:
    impls: dict[str, str] = {}
    nodes = getattr(app, "nodes", None)
    if not isinstance(nodes, dict):
        return impls
    for name, node in nodes.items():
        if str(name) in _SKIP_NODES:
            continue
        source = _callable_source(node)
        if not source:
            continue
        impls[str(name)] = hashlib.sha256(source.encode("utf-8")).hexdigest()
    return impls


def graph_sources(app: Any) -> dict[str, dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    nodes = getattr(app, "nodes", None)
    if not isinstance(nodes, dict):
        return found
    for name, node in nodes.items():
        if str(name) in _SKIP_NODES:
            continue
        loc = callable_location(node)
        if loc:
            found[str(name)] = loc
    return found


def fingerprint_graph(app: Any, file_sha256: str | None = None) -> tuple[dict[str, Any], str]:
    spec = {
        "nodes": graph_nodes(app),
        "edges": [list(pair) for pair in sorted(set(graph_edges(app)))],
        "state": graph_state_keys(app),
        "tools": graph_tools(app),
        "impl": graph_impls(app),
        "file": file_sha256 or "",
    }
    raw = json.dumps(spec, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    spec["source"] = graph_sources(app)
    return spec, digest


def graph_diff(old: dict | None, new: dict | None) -> list[str]:
    old = old if isinstance(old, dict) else {}
    new = new if isinstance(new, dict) else {}
    lines: list[str] = []

    def _as_set(spec: dict, key: str) -> set[str]:
        return {str(item) for item in (spec.get(key) or []) if item is not None}

    for key, label in (("nodes", "node"), ("state", "state"), ("tools", "tool")):
        before = _as_set(old, key)
        after = _as_set(new, key)
        for item in sorted(after - before):
            lines.append(f"+ {label} {item}")
        for item in sorted(before - after):
            lines.append(f"- {label} {item}")

    def _edges(spec: dict) -> set[tuple[str, str]]:
        pairs: set[tuple[str, str]] = set()
        for item in spec.get("edges") or []:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                pairs.add((str(item[0]), str(item[1])))
        return pairs

    before_edges = _edges(old)
    after_edges = _edges(new)
    for source, target in sorted(after_edges - before_edges):
        lines.append(f"+ edge {source} → {target}")
    for source, target in sorted(before_edges - after_edges):
        lines.append(f"- edge {source} → {target}")

    old_impl = old.get("impl") if isinstance(old.get("impl"), dict) else {}
    new_impl = new.get("impl") if isinstance(new.get("impl"), dict) else {}
    for name in sorted(set(old_impl) | set(new_impl)):
        before = old_impl.get(name)
        after = new_impl.get(name)
        if before and after and before != after:
            lines.append(f"~ node {name}")
        elif after and not before:
            lines.append(f"~ node {name}")
        elif before and not after:
            lines.append(f"~ node {name}")
    if (old.get("file") or "") != (new.get("file") or "") and not any(
        line.startswith("~ node ") for line in lines
    ):
        lines.append("~ pipeline file")
    return lines


def attach_graph_meta(
    run: dict,
    graph: dict | None,
    graph_hash: str | None,
    file_sha256: str | None = None,
) -> dict:
    run = dict(run)
    run["graph"] = graph or {}
    run["graph_hash"] = graph_hash
    run["file_sha256"] = file_sha256
    return run
