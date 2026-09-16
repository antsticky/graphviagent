from __future__ import annotations

import copy
import json
import logging
import queue
import sys
import threading
import time
import traceback
import tracemalloc
from datetime import datetime, timezone
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

try:
    from langchain_core.callbacks import BaseCallbackHandler
except ImportError:  # pragma: no cover
    class BaseCallbackHandler:  # type: ignore[no-redef]
        pass

_SKIP_NODES = {"__start__", "START", "__end__", "END"}
MAX_RUN_THREADS = 3
_LOG_LIMIT = 500
_capture_tls = threading.local()
_capture_lock = threading.Lock()
_capture_depth = 0
_capture_stdout: Any = None
_capture_stderr: Any = None
_log_handler: logging.Handler | None = None
_log_handle_orig: Any = None


class RunCancelled(Exception):
    pass


def _elapsed_ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000, 2)


def _iso_from_wall(wall_start: float, offset_ms: float) -> str:
    return datetime.fromtimestamp(wall_start + offset_ms / 1000.0, timezone.utc).isoformat()


def _infer_started_ms(
    node: str,
    ended_ms: float,
    completed_end: dict[str, float],
    edges: list[tuple[str, str]],
) -> float:
    pred_ends = [
        completed_end[source]
        for source, target in edges
        if target == node and source not in _SKIP_NODES and source in completed_end
    ]
    if pred_ends:
        start = max(pred_ends)
    elif not completed_end:
        start = 0.0
    else:
        start = max(completed_end.values())
    if start > ended_ms:
        return 0.0
    return round(start, 2)


def _attach_timing(
    step: dict,
    *,
    started_ms: float,
    ended_ms: float,
    wall_start: float,
) -> dict:
    started_ms = round(max(0.0, started_ms), 2)
    ended_ms = round(max(started_ms, ended_ms), 2)
    step["started_ms"] = started_ms
    step["ended_ms"] = ended_ms
    step["elapsed_ms"] = round(ended_ms - started_ms, 2)
    step["started_at"] = _iso_from_wall(wall_start, started_ms)
    step["ended_at"] = _iso_from_wall(wall_start, ended_ms)
    return step


def _bytes_to_mb(value: float) -> float:
    return round(max(0.0, float(value)) / (1024 * 1024), 4)


class _MemoryTrace:
    def __init__(self) -> None:
        self.own = not tracemalloc.is_tracing()
        if self.own:
            tracemalloc.start()
        current, _peak = tracemalloc.get_traced_memory()
        self.prev = current

    def snapshot(self) -> tuple[float, float]:
        current, peak = tracemalloc.get_traced_memory()
        delta = max(0, current - self.prev)
        peak_from_baseline = max(0, peak - self.prev)
        self.prev = current
        return _bytes_to_mb(delta), _bytes_to_mb(max(delta, peak_from_baseline))

    def close(self) -> None:
        if self.own and tracemalloc.is_tracing():
            tracemalloc.stop()


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _tokens_from_mapping(obj: Any) -> tuple[int, int]:
    if not isinstance(obj, dict):
        return 0, 0
    prompt = obj.get("input_tokens") or obj.get("prompt_tokens") or obj.get("prompt") or 0
    completion = obj.get("output_tokens") or obj.get("completion_tokens") or obj.get("completion") or 0
    return _as_int(prompt), _as_int(completion)


def _usage_from_obj(obj: Any) -> tuple[int, int]:
    if obj is None:
        return 0, 0
    if not isinstance(obj, dict):
        usage_meta = getattr(obj, "usage_metadata", None)
        if isinstance(usage_meta, dict):
            prompt, completion = _tokens_from_mapping(usage_meta)
            if prompt or completion:
                return prompt, completion
        meta = getattr(obj, "response_metadata", None)
        if isinstance(meta, dict):
            prompt, completion = _tokens_from_mapping(
                meta.get("token_usage") or meta.get("usage") or {}
            )
            if prompt or completion:
                return prompt, completion
        llm_output = getattr(obj, "llm_output", None)
        if isinstance(llm_output, dict):
            return _tokens_from_mapping(
                llm_output.get("token_usage") or llm_output.get("usage") or {}
            )
        return 0, 0
    sources: list[dict] = []
    usage_meta = obj.get("usage_metadata")
    if isinstance(usage_meta, dict):
        sources.append(usage_meta)
    meta = obj.get("response_metadata")
    if isinstance(meta, dict):
        token_usage = meta.get("token_usage") or meta.get("usage")
        if isinstance(token_usage, dict):
            sources.append(token_usage)
    usage = obj.get("usage")
    if isinstance(usage, dict):
        sources.append(usage)
    llm_output = obj.get("llm_output")
    if isinstance(llm_output, dict):
        token_usage = llm_output.get("token_usage") or llm_output.get("usage")
        if isinstance(token_usage, dict):
            sources.append(token_usage)
    if not sources:
        return 0, 0
    return _tokens_from_mapping(sources[0])


def _usage_from_llm_response(response: Any) -> tuple[int, int]:
    llm_output = getattr(response, "llm_output", None)
    if isinstance(llm_output, dict):
        prompt, completion = _tokens_from_mapping(
            llm_output.get("token_usage") or llm_output.get("usage") or {}
        )
        if prompt or completion:
            return prompt, completion
    prompt = 0
    completion = 0
    generations = getattr(response, "generations", None) or []
    for row in generations:
        gens = row if isinstance(row, (list, tuple)) else [row]
        for gen in gens:
            extra_prompt, extra_completion = _usage_from_obj(getattr(gen, "message", None))
            prompt += extra_prompt
            completion += extra_completion
            info = getattr(gen, "generation_info", None)
            if isinstance(info, dict) and not extra_prompt and not extra_completion:
                extra_prompt, extra_completion = _tokens_from_mapping(
                    info.get("token_usage") or info.get("usage") or info
                )
                prompt += extra_prompt
                completion += extra_completion
    if prompt or completion:
        return prompt, completion
    return _usage_from_obj(response)


class _UsageHandler(BaseCallbackHandler):
    """Collect billed LLM tokens; attribute them to the probed node via TLS."""

    raise_error = False

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._by_node: dict[str, dict[str, int]] = {}

    def _add(self, prompt: int, completion: int) -> None:
        node = getattr(_capture_tls, "node", None) or "run"
        with self._lock:
            bucket = self._by_node.setdefault(
                node, {"prompt": 0, "completion": 0, "calls": 0}
            )
            bucket["prompt"] += prompt
            bucket["completion"] += completion
            bucket["calls"] += 1

    def on_llm_end(self, response: Any, **kwargs: Any) -> None:
        prompt, completion = _usage_from_llm_response(response)
        self._add(prompt, completion)

    def on_chat_model_end(self, response: Any, **kwargs: Any) -> None:
        prompt, completion = _usage_from_llm_response(response)
        self._add(prompt, completion)

    def take(self, node: str) -> dict[str, int]:
        with self._lock:
            return self._by_node.pop(
                node, {"prompt": 0, "completion": 0, "calls": 0}
            )


def _callback_config(
    handler: _UsageHandler | None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config: dict[str, Any] = dict(extra or {})
    if handler is not None:
        callbacks = list(config.get("callbacks") or [])
        if handler not in callbacks:
            callbacks.append(handler)
        config["callbacks"] = callbacks
    return config


@contextmanager
def _usage_scope(node: str | None, handler: _UsageHandler | None):
    previous_node = getattr(_capture_tls, "node", None)
    previous_usage = getattr(_capture_tls, "usage", None)
    if node is not None:
        _capture_tls.node = node
    if handler is not None:
        _capture_tls.usage = handler
    try:
        yield
    finally:
        _capture_tls.node = previous_node
        _capture_tls.usage = previous_usage


def _take_usage(node: str) -> dict[str, int]:
    handler = getattr(_capture_tls, "usage", None)
    take = getattr(handler, "take", None)
    if not callable(take):
        return {"prompt": 0, "completion": 0, "calls": 0}
    bucket = take(node)
    if not isinstance(bucket, dict):
        return {"prompt": 0, "completion": 0, "calls": 0}
    return bucket


def extract_tokens(update: Any, extra: dict[str, int] | None = None) -> dict[str, Any]:
    prompt = 0
    completion = 0
    tool = 0

    def add(obj: Any) -> None:
        nonlocal prompt, completion
        extra_prompt, extra_completion = _usage_from_obj(obj)
        prompt += extra_prompt
        completion += extra_completion

    if isinstance(update, dict):
        add(update)
        nested = update.get("additional_kwargs")
        if isinstance(nested, dict):
            add(nested)
        for message in extract_messages(update):
            add(message)
            kwargs = message.get("additional_kwargs")
            if isinstance(kwargs, dict):
                add(kwargs)
            if (message.get("role") or message.get("type")) == "tool":
                tool_prompt, tool_completion = _usage_from_obj(message)
                extra_prompt, extra_completion = (0, 0)
                if isinstance(kwargs, dict):
                    extra_prompt, extra_completion = _usage_from_obj(kwargs)
                tool += tool_prompt + tool_completion + extra_prompt + extra_completion
    scraped_prompt, scraped_completion = prompt, completion
    extra = extra or {}
    extra_prompt = _as_int(extra.get("prompt"))
    extra_completion = _as_int(extra.get("completion"))
    prompt += extra_prompt
    completion += extra_completion
    tokens: dict[str, Any] = {
        "prompt": prompt,
        "completion": completion,
        "total": prompt + completion,
        "tool": tool,
    }
    if scraped_prompt == 0 and scraped_completion == 0 and extra_prompt == 0 and extra_completion == 0:
        tokens["unavailable"] = True
    return tokens


def _tool_call_name(call: dict) -> str:
    function = call.get("function") if isinstance(call.get("function"), dict) else {}
    return str(call.get("name") or function.get("name") or call.get("tool") or "tool")


def _tool_call_id(call: dict) -> str | None:
    function = call.get("function") if isinstance(call.get("function"), dict) else {}
    value = call.get("id") or call.get("tool_call_id") or function.get("id")
    return str(value) if value else None


def _tool_call_args(call: dict) -> Any:
    function = call.get("function") if isinstance(call.get("function"), dict) else {}
    args = call.get("args")
    if args is None:
        args = function.get("arguments")
    if args is None:
        args = call.get("arguments")
    if isinstance(args, str):
        try:
            return json.loads(args)
        except Exception:
            return args
    return args


def _tool_latency(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _merge_tool(existing: dict, incoming: dict) -> dict:
    for key in ("name", "id", "args", "output", "latency_ms", "error"):
        value = incoming.get(key)
        if value in (None, "", []):
            continue
        if key == "name" and existing.get("name") and existing["name"] != "tool":
            continue
        existing[key] = value
    return existing


def extract_tool_metrics(update: Any, elapsed_ms: float | None) -> tuple[list[dict], float | None]:
    tools: list[dict] = []
    by_id: dict[str, dict] = {}
    requests = 0
    results = 0

    def upsert(item: dict) -> None:
        key = item.get("id")
        if key:
            current = by_id.get(str(key))
            if current is not None:
                _merge_tool(current, item)
                return
            by_id[str(key)] = item
        tools.append(item)

    def add_call(call: dict) -> None:
        nonlocal requests
        if not isinstance(call, dict):
            return
        requests += 1
        upsert(
            {
                "name": _tool_call_name(call),
                "id": _tool_call_id(call),
                "args": _tool_call_args(call),
                "output": None,
                "latency_ms": None,
                "error": None,
            }
        )

    def add_result(message: dict) -> None:
        nonlocal results
        results += 1
        error = message.get("error")
        if error is None and message.get("status") == "error":
            error = message.get("content")
        upsert(
            {
                "name": str(message.get("name") or message.get("tool") or "tool"),
                "id": str(message.get("tool_call_id") or message.get("id") or "") or None,
                "args": message.get("args"),
                "output": message.get("content"),
                "latency_ms": _tool_latency(message.get("latency_ms")),
                "error": None if error is None else str(error),
            }
        )

    if isinstance(update, dict):
        for call in update.get("tool_calls") or []:
            add_call(call)
        for message in extract_messages(update):
            for call in message.get("tool_calls") or []:
                add_call(call)
            role = message.get("role") or message.get("type")
            if role == "tool":
                add_result(message)

    known = [float(item["latency_ms"]) for item in tools if item.get("latency_ms") is not None]
    tool_only = results > 0 and requests == 0
    if tool_only and elapsed_ms is not None:
        if len(tools) == 1 and tools[0].get("latency_ms") is None:
            tools[0]["latency_ms"] = round(float(elapsed_ms), 2)
            known = [float(tools[0]["latency_ms"])]
        return tools, round(sum(known), 2) if known else round(float(elapsed_ms), 2)
    return tools, round(sum(known), 2) if known else None


def _attach_metrics(
    step: dict,
    update: Any,
    memory_mb: float,
    memory_peak_mb: float = 0.0,
    extra: dict[str, int] | None = None,
) -> dict:
    if extra is None:
        extra = _take_usage(str(step.get("node") or "run"))
    tokens = extract_tokens(update, extra)
    tools, tool_latency_ms = extract_tool_metrics(update, step.get("elapsed_ms"))
    step["memory_mb"] = round(float(memory_mb or 0), 4)
    step["memory_peak_mb"] = round(float(memory_peak_mb or 0), 4)
    step["tokens"] = tokens
    step["tools"] = jsonable(tools)
    step["tool_latency_ms"] = tool_latency_ms
    return step


def _probe_input(args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict | None:
    for candidate in (args[0] if args else None, kwargs.get("input"), kwargs.get("state")):
        if isinstance(candidate, dict):
            return jsonable(candidate)
    return None


def _with_recorded_payload(base: dict, recorded: dict) -> dict:
    incoming = copy.deepcopy(base) if isinstance(base, dict) else {}
    recorded_in = recorded.get("state_in") if isinstance(recorded.get("state_in"), dict) else {}
    send_like = "source" in recorded_in
    for key, value in recorded_in.items():
        if send_like or key not in incoming:
            incoming[key] = copy.deepcopy(value)
    update = recorded.get("update") if isinstance(recorded.get("update"), dict) else {}
    if "source" not in incoming:
        for item in update.get("partials") or []:
            if isinstance(item, dict) and item.get("source"):
                incoming["source"] = item["source"]
                break
    return incoming


def _invoke_target(node: Any) -> Any | None:
    for attr in ("proc", "bound", "runnable"):
        child = getattr(node, attr, None)
        if child is not None and callable(getattr(child, "invoke", None)):
            return child
    if callable(getattr(node, "invoke", None)):
        return node
    return None


class _CaptureStream:
    def __init__(self, original: Any, kind: str = "stdout") -> None:
        self.original = original
        self.kind = kind

    def write(self, data: Any) -> int:
        text = data if isinstance(data, str) else str(data)
        try:
            written = self.original.write(data)
        except Exception:
            written = len(text)
        if getattr(_capture_tls, "in_logging", 0):
            return written if isinstance(written, int) else len(text)
        hook = getattr(_capture_tls, "on_stdio", None)
        if hook is not None:
            for line in text.splitlines():
                if line.strip():
                    hook(line, self.kind)
        return written if isinstance(written, int) else len(text)

    def flush(self) -> None:
        flush = getattr(self.original, "flush", None)
        if callable(flush):
            flush()

    def isatty(self) -> bool:
        fn = getattr(self.original, "isatty", None)
        return bool(fn()) if callable(fn) else False

    def __getattr__(self, name: str) -> Any:
        return getattr(self.original, name)


def _install_stdio_capture() -> None:
    global _capture_depth, _capture_stdout, _capture_stderr, _log_handler
    with _capture_lock:
        if _capture_depth == 0:
            _capture_stdout = sys.stdout
            _capture_stderr = sys.stderr
            sys.stdout = _CaptureStream(_capture_stdout, "stdout")
            sys.stderr = _CaptureStream(_capture_stderr, "stderr")
            _patch_logging_handle()
            _log_handler = _RunLogHandler()
            logging.getLogger().addHandler(_log_handler)
        _capture_depth += 1


def _uninstall_stdio_capture() -> None:
    global _capture_depth, _log_handler, _log_handle_orig
    with _capture_lock:
        _capture_depth = max(0, _capture_depth - 1)
        if _capture_depth == 0 and _capture_stdout is not None:
            sys.stdout = _capture_stdout
            sys.stderr = _capture_stderr
            if _log_handler is not None:
                logging.getLogger().removeHandler(_log_handler)
                _log_handler = None
            if _log_handle_orig is not None:
                logging.Handler.handle = _log_handle_orig
                _log_handle_orig = None


class _RunLogHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.setFormatter(logging.Formatter("%(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        emit = getattr(_capture_tls, "emit", None)
        if emit is None:
            return
        try:
            text = record.getMessage()
            if record.exc_info and record.exc_info[0] is not None:
                text = text + "\n" + self.formatter.formatException(record.exc_info)
        except Exception:
            text = str(getattr(record, "msg", "") or "")
        clock0 = getattr(_capture_tls, "clock0", None)
        now = round((time.perf_counter() - clock0) * 1000, 2) if clock0 else 0
        node = getattr(_capture_tls, "node", None)
        emit(
            {
                "type": "log",
                "t": now,
                "src": node or record.name,
                "text": text,
                "level": (record.levelname or "INFO").lower(),
                "logger": record.name,
            }
        )


def _patch_logging_handle() -> None:
    global _log_handle_orig
    if _log_handle_orig is not None:
        return
    original = logging.Handler.handle
    _log_handle_orig = original

    def handle(self: logging.Handler, record: logging.LogRecord) -> Any:
        depth = getattr(_capture_tls, "in_logging", 0)
        _capture_tls.in_logging = depth + 1
        try:
            return original(self, record)
        finally:
            _capture_tls.in_logging = depth

    logging.Handler.handle = handle  # type: ignore[method-assign]


def _install_node_probes(
    app: Any,
    clock0: float,
    cancel: Any | None = None,
    emit: Any | None = None,
) -> tuple[list[dict], Any, threading.Lock]:
    probes: list[dict] = []
    lock = threading.Lock()
    restores: list[tuple[Any, Any]] = []
    nodes = getattr(app, "nodes", None)
    if not isinstance(nodes, dict):
        return probes, lambda: None, lock

    for name, node in nodes.items():
        if str(name) in _SKIP_NODES:
            continue
        target = _invoke_target(node)
        if target is None:
            continue
        original = target.invoke

        def make_probed(node_name: str, orig: Any) -> Any:
            def probed(*args: Any, **kwargs: Any) -> Any:
                if cancel is not None and getattr(cancel, "is_set", lambda: False)():
                    raise RunCancelled("cancelled")
                started_ms = round((time.perf_counter() - clock0) * 1000, 2)
                incoming = _probe_input(args, kwargs)
                if emit:
                    emit({"type": "node_start", "node": node_name, "started_ms": started_ms})
                    emit({"type": "log", "t": started_ms, "src": node_name, "text": "started", "level": "info"})
                previous_node = getattr(_capture_tls, "node", None)
                _capture_tls.node = node_name
                memory = _MemoryTrace()
                frames: list[dict[str, Any]] | None = None
                try:
                    return orig(*args, **kwargs)
                except Exception as exc:
                    frames = exception_frames(exc)
                    raise
                finally:
                    _capture_tls.node = previous_node
                    memory_mb, memory_peak_mb = memory.snapshot()
                    memory.close()
                    ended_ms = round((time.perf_counter() - clock0) * 1000, 2)
                    probe = {
                        "node": node_name,
                        "started_ms": started_ms,
                        "ended_ms": ended_ms,
                        "elapsed_ms": round(max(0.0, ended_ms - started_ms), 2),
                        "memory_mb": memory_mb,
                        "memory_peak_mb": memory_peak_mb,
                        "input": incoming,
                    }
                    if frames:
                        probe["error_frames"] = frames
                    with lock:
                        probes.append(probe)

            return probed

        target.invoke = make_probed(str(name), original)
        restores.append((target, original))

    def restore() -> None:
        for target, original in restores:
            target.invoke = original

    return probes, restore, lock


def _take_probe(
    probes: list[dict],
    node: str,
    lock: threading.Lock | None = None,
) -> dict | None:
    if lock is not None:
        lock.acquire()
    try:
        for index, probe in enumerate(probes):
            if probe.get("node") == node:
                return probes.pop(index)
        return None
    finally:
        if lock is not None:
            lock.release()


def _apply_unused(steps: list[dict], edges: list[tuple[str, str]]) -> None:
    for index, step in enumerate(steps):
        next_node = steps[index + 1]["node"] if index + 1 < len(steps) else None
        step["unused"] = unused_targets(step["node"], next_node, edges)


def _append_log(logs: list[dict], event: dict) -> dict:
    item = {
        "t": event.get("t"),
        "src": event.get("src") or "run",
        "text": event.get("text") or "",
        "level": str(event.get("level") or "info").lower(),
    }
    if event.get("logger"):
        item["logger"] = event["logger"]
    logs.append(item)
    if len(logs) > _LOG_LIMIT:
        del logs[:-_LOG_LIMIT]
    return {"type": "log", **item}


def _run_span(steps: list[dict], fallback_ms: float) -> float:
    ends = [float(step.get("ended_ms") or 0) for step in steps]
    if ends:
        return round(max(ends), 2)
    return fallback_ms


def _langchain_dump(obj: Any) -> dict | None:
    for attr in ("model_dump", "dict"):
        fn = getattr(obj, attr, None)
        if not callable(fn):
            continue
        try:
            data = fn()
        except TypeError:
            try:
                data = fn(mode="json")
            except Exception:
                continue
        except Exception:
            continue
        if isinstance(data, dict):
            return data
    kind = getattr(obj, "type", None)
    content = getattr(obj, "content", None)
    if kind is None or content is None:
        return None
    data = {"type": str(kind), "content": content}
    role = getattr(obj, "role", None)
    data["role"] = role or {"human": "user", "ai": "assistant", "tool": "tool"}.get(str(kind), str(kind))
    for name in ("name", "tool_calls", "tool_call_id", "id", "status", "additional_kwargs"):
        if hasattr(obj, name):
            value = getattr(obj, name)
            if value not in (None, [], {}):
                data[name] = value
    return data


def jsonable(obj: Any) -> Any:
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, dict):
        return {str(key): jsonable(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [jsonable(item) for item in obj]
    dumped = _langchain_dump(obj)
    if dumped is not None:
        return jsonable(dumped)
    return str(obj)


def merge_state(current: dict, update: dict) -> dict:
    merged = copy.deepcopy(current)
    for key, value in update.items():
        if isinstance(value, list) and isinstance(merged.get(key), list):
            merged[key] = list(merged[key]) + list(value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _format_error(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


_LIBRARY_FRAME_MARKERS = (
    "/langgraph/",
    "/langchain_core/",
    "/langchain/",
    "/graphviagent/",
    "/site-packages/",
    "/lib/python",
)


def _is_library_frame(path: str) -> bool:
    if not path or path.startswith("<"):
        return True
    normalized = path.replace("\\", "/")
    return any(marker in normalized for marker in _LIBRARY_FRAME_MARKERS)


def exception_frames(exc: BaseException, *, limit: int = 40) -> list[dict[str, Any]]:
    frames: list[dict[str, Any]] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        extracted = traceback.extract_tb(current.__traceback__) or []
        for item in extracted:
            raw = item.filename or ""
            try:
                file = str(Path(raw).resolve()) if raw and not raw.startswith("<") else raw
            except OSError:
                file = raw
            frame: dict[str, Any] = {
                "file": file,
                "line": item.lineno,
                "name": item.name,
                "text": item.line,
                "library": _is_library_frame(file),
            }
            frames.append(frame)
        current = current.__cause__ or current.__context__
    if len(frames) > limit:
        return frames[-limit:]
    return frames


def _error_update(exc: BaseException) -> dict[str, Any]:
    return {"error": _format_error(exc), "error_frames": exception_frames(exc)}


def _plain_update(update: Any) -> Any:
    if not isinstance(update, dict) or "error_frames" not in update:
        return update
    return {key: value for key, value in update.items() if key != "error_frames"}


def _source_map(app: Any) -> dict[str, dict[str, Any]]:
    try:
        from graphviagent.graph_hash import graph_sources
    except Exception:
        return {}
    try:
        found = graph_sources(app)
    except Exception:
        return {}
    return found if isinstance(found, dict) else {}


def _attach_source(
    step: dict,
    node: str,
    *,
    source_map: dict[str, dict[str, Any]] | None = None,
    probe: dict | None = None,
) -> None:
    loc = None
    if probe and isinstance(probe.get("source"), dict):
        loc = probe["source"]
    elif source_map:
        loc = source_map.get(node)
    if loc:
        step["source"] = jsonable(loc)


def _attach_error(
    step: dict,
    update: Any,
    *,
    probe: dict | None = None,
) -> None:
    if not (isinstance(update, dict) and update.get("error")):
        return
    step["error"] = str(update.get("error"))
    frames = None
    if probe and isinstance(probe.get("error_frames"), list):
        frames = probe["error_frames"]
    elif isinstance(update.get("error_frames"), list):
        frames = update["error_frames"]
    if frames:
        step["error_frames"] = jsonable(frames)


def _is_cancelled(exc: BaseException) -> bool:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, RunCancelled) or str(current) == "cancelled":
            return True
        current = current.__cause__ or current.__context__
    return False


def _is_interrupt(exc: BaseException) -> bool:
    current: BaseException | None = exc
    seen: set[int] = set()
    names = {"GraphInterrupt", "NodeInterrupt", "GraphBubbleUp"}
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if type(current).__name__ in names:
            return True
        current = current.__cause__ or current.__context__
    return False


def _branch_hints(update: dict) -> list[str]:
    hints: list[str] = []
    for key in ("path", "choice", "loop_choice"):
        value = update.get(key)
        if isinstance(value, str):
            hints.append(value)
    decisions = update.get("decisions") or []
    if decisions and isinstance(decisions[0], dict):
        choice = decisions[0].get("choice")
        if isinstance(choice, str):
            hints.append(choice)
    return hints


def _guess_failed_node(raw_events: list[tuple[str, dict, float]], edges: list[tuple[str, str]]) -> str | None:
    if not raw_events:
        starts = [target for source, target in edges if source in {"__start__", "START"}]
        return starts[0] if starts else None
    last, last_update, _ = raw_events[-1]
    nxt = [
        target
        for source, target in edges
        if source == last and target not in {"__end__", "END", "__start__", "START"}
    ]
    if not nxt:
        return last
    if len(nxt) == 1:
        return nxt[0]
    if isinstance(last_update, dict):
        for hint in _branch_hints(last_update):
            if hint in nxt:
                return hint
    return nxt[0]


def _is_message(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    role = item.get("role")
    kind = item.get("type")
    if role in {"system", "user", "assistant", "tool", "function", "human", "ai"}:
        return True
    if kind in {"system", "human", "ai", "tool", "function", "chat"}:
        return True
    if item.get("tool_calls") and "content" in item:
        return True
    return False


def _message_dict(item: Any) -> dict | None:
    if isinstance(item, dict):
        return item if _is_message(item) else None
    dumped = _langchain_dump(item)
    if not isinstance(dumped, dict):
        return None
    dumped = jsonable(dumped)
    return dumped if _is_message(dumped) else None


def extract_messages(value: Any) -> list[dict]:
    if isinstance(value, list):
        found = [item for raw in value if (item := _message_dict(raw))]
        return found if found and len(found) >= max(1, len(value) // 2) else []
    coerced = _message_dict(value)
    if coerced:
        return [coerced]
    if not isinstance(value, dict):
        return []
    for key in ("messages", "output", "result"):
        child = value.get(key)
        if child is value:
            continue
        found = extract_messages(child)
        if found:
            return found
    return []


def extract_reason(update: dict) -> str:
    if not isinstance(update, dict):
        return ""
    if isinstance(update.get("error"), str):
        return update["error"]
    if isinstance(update.get("reason"), str):
        return update["reason"]
    if update.get("choice") is not None and not update.get("decisions"):
        return str(update["choice"])
    decisions = update.get("decisions") or []
    if decisions and isinstance(decisions[0], dict):
        return str(decisions[0].get("reason") or decisions[0].get("choice") or "")
    return ""


def extract_decisions(update: dict) -> list[dict]:
    if not isinstance(update, dict):
        return []
    decisions = update.get("decisions")
    if isinstance(decisions, list):
        return [item for item in decisions if isinstance(item, dict)]
    if update.get("reason") or update.get("choice") is not None:
        return [
            {
                "step": update.get("step") or "",
                "choice": update.get("choice"),
                "reason": update.get("reason") or str(update.get("choice")),
            }
        ]
    return []


def graph_edges(app: Any) -> list[tuple[str, str]]:
    edges: list[tuple[str, str]] = []
    builder = getattr(app, "builder", None)
    if builder is not None:
        for pair in getattr(builder, "edges", ()) or ():
            if isinstance(pair, (tuple, list)) and len(pair) >= 2:
                edges.append((str(pair[0]), str(pair[1])))
        branches = getattr(builder, "branches", None) or {}
        send_sources: list[str] = []
        items = branches.items() if isinstance(branches, dict) else []
        for source, specs in items:
            values = specs.values() if isinstance(specs, dict) else specs or []
            for spec in values:
                ends = getattr(spec, "ends", None)
                if isinstance(ends, dict) and ends:
                    for target in ends.values():
                        if target is None:
                            continue
                        edges.append((str(source), str(target)))
                else:
                    send_sources.append(str(source))
        nodes = getattr(builder, "nodes", None)
        node_names = {str(name) for name in nodes} if isinstance(nodes, dict) else set()
        incoming = {target for _, target in edges}
        for source in send_sources:
            for name in node_names:
                if name == source or name in incoming:
                    continue
                edges.append((source, name))
                incoming.add(name)
        if edges:
            return sorted(set(edges))
    try:
        graph = app.get_graph()
    except Exception:
        return sorted(set(edges))
    for edge in getattr(graph, "edges", []) or []:
        source = getattr(edge, "source", None)
        target = getattr(edge, "target", None)
        if source is None and isinstance(edge, (tuple, list)) and len(edge) >= 2:
            source, target = edge[0], edge[1]
        if source is None or target is None:
            continue
        edges.append((str(source), str(target)))
    return sorted(set(edges))


def unused_targets(node: str, next_node: str | None, edges: list[tuple[str, str]]) -> list[str]:
    unused: list[str] = []
    for source, target in edges:
        if source != node:
            continue
        if target in {"__end__", "END", "__start__", "START"}:
            if next_node is None and target in {"__end__", "END"}:
                continue
        if next_node and target == next_node:
            continue
        if next_node is None and target in {"__end__", "END"}:
            continue
        unused.append(target)
    return unused


def invoke_node(
    app: Any,
    node_name: str,
    state: dict,
    config: dict[str, Any] | None = None,
) -> dict:
    node = app.nodes[node_name]

    def _call(target: Any) -> Any:
        if config is None:
            return target.invoke(state)
        try:
            return target.invoke(state, config)
        except TypeError:
            return target.invoke(state)

    if hasattr(node, "invoke"):
        result = _call(node)
        return result if isinstance(result, dict) else {"result": result}
    bound = getattr(node, "bound", None) or getattr(node, "runnable", None)
    if bound is not None and hasattr(bound, "invoke"):
        result = _call(bound)
        return result if isinstance(result, dict) else {"result": result}
    raise RuntimeError(f"cannot invoke node {node_name!r}")


def _app_has_checkpointer(app: Any) -> bool:
    return getattr(app, "checkpointer", None) not in (None, False)


def _stream_item(event: Any) -> tuple[str, Any]:
    if isinstance(event, tuple) and len(event) == 2 and isinstance(event[0], str):
        return str(event[0]), event[1]
    return "updates", event


def _checkpoint_id(snapshot: Any) -> str | None:
    config = getattr(snapshot, "config", None) or {}
    if isinstance(config, dict):
        return (config.get("configurable") or {}).get("checkpoint_id")
    return None


def _attach_checkpoint_ids(app: Any, config: dict[str, Any], steps: list[dict]) -> None:
    getter = getattr(app, "get_state_history", None)
    if not callable(getter) or not steps:
        return
    try:
        hist = list(getter(config))
    except Exception:
        return
    loop = [
        snap
        for snap in reversed(hist)
        if (getattr(snap, "metadata", None) or {}).get("source") == "loop"
    ]
    groups: dict[int, list[dict]] = {}
    for step in steps:
        groups.setdefault(int(step.get("superstep") or 0), []).append(step)
    for superstep, group in groups.items():
        after_idx = superstep + 1
        if after_idx >= len(loop):
            continue
        after = loop[after_idx]
        before = loop[superstep]
        cid = _checkpoint_id(after)
        parent = _checkpoint_id(before)
        for step in group:
            if cid:
                step["checkpoint_id"] = cid
            if parent:
                step["checkpoint_parent_id"] = parent


def _fork_thread(app: Any, snapshot: Any, new_thread_id: str) -> dict[str, Any]:
    saver = getattr(app, "checkpointer", None)
    if saver is None or snapshot is None:
        raise RuntimeError("checkpoint fork requires a checkpointer")
    get_tuple = getattr(saver, "get_tuple", None)
    put = getattr(saver, "put", None)
    if not callable(get_tuple) or not callable(put):
        raise RuntimeError("checkpointer cannot copy a thread")
    tup = get_tuple(snapshot.config)
    if tup is None:
        raise RuntimeError("checkpoint not found (process restart or missing thread)")
    ns = ((getattr(snapshot, "config", None) or {}).get("configurable") or {}).get(
        "checkpoint_ns"
    ) or ""
    dest = {"configurable": {"thread_id": new_thread_id, "checkpoint_ns": ns}}
    checkpoint = tup.checkpoint
    versions = checkpoint.get("channel_versions") if isinstance(checkpoint, dict) else {}
    put(dest, checkpoint, tup.metadata, versions or {})
    return {"configurable": {"thread_id": new_thread_id}}


def _snapshot_before_step(app: Any, run: dict, step: dict) -> Any:
    thread_id = run.get("thread_id")
    if not thread_id:
        raise RuntimeError("run has no thread_id")
    getter = getattr(app, "get_state_history", None)
    if not callable(getter):
        raise RuntimeError("graph has no get_state_history")
    hist = list(getter({"configurable": {"thread_id": thread_id}}))
    parent = step.get("checkpoint_parent_id")
    if parent:
        for snap in hist:
            if _checkpoint_id(snap) == parent:
                return snap
    node = str(step.get("node") or "")
    matches = [
        snap
        for snap in reversed(hist)
        if node in tuple(getattr(snap, "next", None) or ())
    ]
    visit = 0
    for prev in run.get("steps") or []:
        if prev.get("step_id") == step.get("step_id"):
            break
        if prev.get("node") == node:
            visit += 1
    if visit < len(matches):
        return matches[visit]
    if matches:
        return matches[-1]
    raise RuntimeError(f"no checkpoint before node {node}")


def _predecessor_node(app: Any, node: str) -> str:
    for source, target in graph_edges(app):
        if target == node:
            return "__start__" if source in _SKIP_NODES else source
    return "__start__"


def _seed_before_step(
    app: Any,
    node: str,
    state: dict,
    *,
    thread_id: str,
) -> dict[str, Any]:
    config: dict[str, Any] = {"configurable": {"thread_id": thread_id}}
    payload = state if isinstance(state, dict) else {"value": state}
    pred = _predecessor_node(app, node)
    last_error: BaseException | None = None
    for as_node in (pred, "__start__"):
        try:
            app.update_state(config, payload, as_node=as_node)
            snap = app.get_state(config)
            nxt = tuple(getattr(snap, "next", None) or ())
            if node in nxt:
                return config
        except Exception as exc:
            last_error = exc
    try:
        app.update_state(config, payload)
        return config
    except Exception as exc:
        last_error = exc
    raise RuntimeError(
        f"could not seed checkpoint before {node}"
        + (f": {last_error}" if last_error else "")
    )


def _can_native_replay(app: Any, run: dict) -> bool:
    return _app_has_checkpointer(app)


def _values_equal(left: Any, right: Any) -> bool:
    try:
        return json.dumps(jsonable(left), sort_keys=True, default=str) == json.dumps(
            jsonable(right), sort_keys=True, default=str
        )
    except TypeError:
        return False


def _editor_patch(
    step: dict, incoming: dict | None, state_patch: dict | None
) -> dict | None:
    patch = dict(state_patch or {})
    if isinstance(incoming, dict):
        recorded = step.get("state_in") if isinstance(step.get("state_in"), dict) else {}
        for key, value in incoming.items():
            if not _values_equal(value, recorded.get(key)):
                patch[key] = value
    return patch or None


def _build_step(
    *,
    node: str,
    update: dict,
    ended_ms: float,
    state: dict,
    visits: dict[str, int],
    probes: list[dict],
    probe_lock: threading.Lock,
    completed_end: dict[str, float],
    edges: list[tuple[str, str]],
    wall0: float,
    live_out: dict | None = None,
    superstep: int = 0,
    source_map: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict, dict]:
    visits[node] = visits.get(node, 0) + 1
    graph_in = state if isinstance(state, dict) else {}
    failed = isinstance(update, dict) and update.get("error")
    probe = _take_probe(probes, node, probe_lock)
    if probe:
        started_ms = float(probe["started_ms"])
        ended_ms = float(probe["ended_ms"])
        memory_mb = float(probe["memory_mb"])
        memory_peak_mb = float(probe.get("memory_peak_mb") or 0)
        invoke_input = probe.get("input")
    else:
        started_ms = _infer_started_ms(node, ended_ms, completed_end, edges)
        memory_mb = 0.0
        memory_peak_mb = 0.0
        invoke_input = None
    state_in = invoke_input if isinstance(invoke_input, dict) else graph_in
    if failed:
        state_out = copy.deepcopy(state_in)
        graph_out = copy.deepcopy(graph_in)
    elif isinstance(live_out, dict):
        state_out = live_out
        graph_out = live_out
    else:
        state_out = merge_state(state_in, update)
        graph_out = merge_state(graph_in, update)
    stored_update = _plain_update(update)
    step = {
        "step_id": f"{node}#{visits[node]}",
        "index": None,
        "node": node,
        "update": jsonable(stored_update),
        "state_in": jsonable(state_in),
        "state_out": jsonable(state_out),
        "reason": extract_reason(stored_update),
        "decisions": jsonable(extract_decisions(stored_update)),
        "unused": [],
        "superstep": superstep,
    }
    _attach_timing(step, started_ms=started_ms, ended_ms=ended_ms, wall_start=wall0)
    _attach_metrics(step, stored_update, memory_mb, memory_peak_mb)
    _attach_source(step, node, source_map=source_map, probe=probe)
    _attach_error(step, update, probe=probe)
    completed_end[node] = ended_ms
    return step, graph_out


def _build_run(
    *,
    run_id: str,
    user_input: dict,
    steps: list[dict],
    state: dict,
    has_checkpointer: bool,
    wall0: float,
    clock0: float,
    run_error: str | None,
    logs: list[dict],
    edges: list[tuple[str, str]],
) -> dict:
    for index, step in enumerate(steps):
        step["index"] = index
    _apply_unused(steps, edges)
    span = _run_span(steps, _elapsed_ms(clock0))
    return {
        "id": run_id,
        "input": jsonable(user_input),
        "steps": steps,
        "result": jsonable(state),
        "has_checkpointer": has_checkpointer,
        "thread_id": run_id,
        "started_at": _iso_from_wall(wall0, 0),
        "ended_at": _iso_from_wall(wall0, span),
        "elapsed_ms": span,
        "error": run_error,
        "logs": list(logs),
    }


def _execute_run(
    app: Any,
    user_input: dict | None,
    *,
    thread_id: str | None,
    has_checkpointer: bool,
    cancel: Any | None,
    max_concurrency: int,
    emit: Any,
    resume: bool = False,
    interrupt_after: list[str] | None = None,
    run_config: dict[str, Any] | None = None,
) -> None:
    run_id = thread_id or uuid4().hex
    usage = _UsageHandler()
    config: dict[str, Any] = _callback_config(
        usage, dict(run_config or {"max_concurrency": max_concurrency})
    )
    configurable = dict(config.get("configurable") or {})
    configurable["thread_id"] = run_id
    config["configurable"] = configurable
    has_checkpointer = has_checkpointer or _app_has_checkpointer(app)
    state: dict = copy.deepcopy(user_input) if isinstance(user_input, dict) else {}
    steps: list[dict] = []
    visits: dict[str, int] = {}
    edges = graph_edges(app)
    raw_events: list[tuple[str, dict, float]] = []
    logs: list[dict] = []
    logs_lock = threading.Lock()
    run_error: str | None = None
    clock0 = time.perf_counter()
    wall0 = time.time()

    def push(event: dict) -> None:
        if event.get("type") == "log":
            with logs_lock:
                emit(_append_log(logs, event))
            return
        emit(event)

    def on_stdio(line: str, stream: str) -> None:
        now = round((time.perf_counter() - clock0) * 1000, 2)
        node = getattr(_capture_tls, "node", None) or "run"
        push(
            {
                "type": "log",
                "t": now,
                "src": node,
                "text": line,
                "level": "print" if stream == "stdout" else "stderr",
            }
        )

    _capture_tls.emit = push
    _capture_tls.clock0 = clock0
    _capture_tls.on_stdio = on_stdio
    _capture_tls.node = None
    _capture_tls.usage = usage
    stored_input = user_input if isinstance(user_input, dict) else {}
    emit({"type": "start", "run_id": run_id, "input": jsonable(stored_input), "started_at": _iso_from_wall(wall0, 0)})
    push({"type": "log", "t": 0, "src": "run", "text": "run started", "level": "info"})
    probes, restore_probes, probe_lock = _install_node_probes(
        app, clock0, cancel=cancel, emit=push
    )
    completed_end: dict[str, float] = {}
    _install_stdio_capture()
    pending_updates: dict[str, Any] | None = None
    superstep = 0
    source_map = _source_map(app)

    def consume_updates(payload: dict, live_out: dict | None) -> None:
        nonlocal state
        ended_ms = _elapsed_ms(clock0)
        graph_in = state
        for node, update in payload.items():
            node = str(node)
            if node in _SKIP_NODES or node == "__interrupt__":
                continue
            if not isinstance(update, dict):
                update = {"value": update}
            raw_events.append((node, update, ended_ms))
            step, graph_out = _build_step(
                node=node,
                update=update,
                ended_ms=ended_ms,
                state=graph_in if live_out is not None else state,
                visits=visits,
                probes=probes,
                probe_lock=probe_lock,
                completed_end=completed_end,
                edges=edges,
                wall0=wall0,
                live_out=live_out,
                superstep=superstep,
                source_map=source_map,
            )
            step["index"] = len(steps)
            steps.append(step)
            if live_out is None:
                state = graph_out
            push({"type": "log", "t": step["ended_ms"], "src": node, "text": f"finished ({step['elapsed_ms']}ms)", "level": "info"})
            emit({"type": "step", "step": step})
        if live_out is not None:
            state = live_out
        emit({"type": "state", "state": jsonable(state)})

    def flush_pending() -> None:
        nonlocal pending_updates, state
        if not pending_updates:
            return
        live_end = None
        if has_checkpointer:
            try:
                vals = getattr(app.get_state(config), "values", None)
                if isinstance(vals, dict):
                    live_end = vals
            except Exception:
                pass
        consume_updates(pending_updates, live_end)
        pending_updates = None

    def sync_live_state() -> None:
        nonlocal state
        if not has_checkpointer:
            return
        try:
            vals = getattr(app.get_state(config), "values", None)
        except Exception:
            return
        if isinstance(vals, dict):
            state = vals
        _attach_checkpoint_ids(app, config, steps)

    try:
        stream_kwargs: dict[str, Any] = {"stream_mode": ["updates", "values"]}
        if interrupt_after:
            stream_kwargs["interrupt_after"] = interrupt_after
        incoming: Any = None if resume else (user_input or {})
        try:
            stream = app.stream(incoming, config, **stream_kwargs)
        except TypeError:
            stream_kwargs.pop("stream_mode", None)
            stream = app.stream(incoming, config, **stream_kwargs)
        for event in stream:
            if cancel is not None and getattr(cancel, "is_set", lambda: False)():
                raise RunCancelled("cancelled")
            mode, payload = _stream_item(event)
            if mode == "updates":
                if isinstance(payload, dict) and payload:
                    cleaned = {
                        key: value
                        for key, value in payload.items()
                        if str(key) not in _SKIP_NODES and str(key) != "__interrupt__"
                    }
                    if cleaned:
                        pending_updates = cleaned
                continue
            if mode == "values":
                live = payload if isinstance(payload, dict) else {"value": payload}
                if pending_updates:
                    consume_updates(pending_updates, live)
                    pending_updates = None
                    superstep += 1
                    state = live
                else:
                    state = live
                continue
            if isinstance(payload, dict) and payload:
                consume_updates(payload, None)
        flush_pending()
        sync_live_state()
    except Exception as exc:
        if _is_interrupt(exc):
            flush_pending()
            sync_live_state()
        elif _is_cancelled(exc):
            run_error = "cancelled"
        else:
            run_error = _format_error(exc)
            failed = _guess_failed_node(raw_events, edges)
            if failed:
                update = _error_update(exc)
                ended_ms = _elapsed_ms(clock0)
                raw_events.append((failed, update, ended_ms))
                step, state = _build_step(
                    node=failed,
                    update=update,
                    ended_ms=ended_ms,
                    state=state,
                    visits=visits,
                    probes=probes,
                    probe_lock=probe_lock,
                    completed_end=completed_end,
                    edges=edges,
                    wall0=wall0,
                    superstep=superstep,
                    source_map=source_map,
                )
                step["index"] = len(steps)
                steps.append(step)
                push({"type": "log", "t": ended_ms, "src": failed, "text": run_error, "level": "error"})
                emit({"type": "step", "step": step})
                emit({"type": "state", "state": jsonable(state)})
    finally:
        restore_probes()
        _uninstall_stdio_capture()
        _capture_tls.emit = None
        _capture_tls.on_stdio = None
        _capture_tls.node = None
        _capture_tls.clock0 = None
        _capture_tls.usage = None

    if run_error:
        push({"type": "log", "t": _elapsed_ms(clock0), "src": "run", "text": run_error, "level": "error"})
    else:
        push({"type": "log", "t": _elapsed_ms(clock0), "src": "run", "text": "run finished", "level": "info"})
    built = _build_run(
        run_id=run_id,
        user_input=stored_input,
        steps=steps,
        state=state,
        has_checkpointer=has_checkpointer,
        wall0=wall0,
        clock0=clock0,
        run_error=run_error,
        logs=logs,
        edges=edges,
    )
    emit({"type": "done", "run": built})


def iter_run_events(
    app: Any,
    user_input: dict | None,
    *,
    thread_id: str | None = None,
    has_checkpointer: bool = False,
    cancel: Any | None = None,
    max_concurrency: int = MAX_RUN_THREADS,
    resume: bool = False,
    interrupt_after: list[str] | None = None,
    run_config: dict[str, Any] | None = None,
) -> Iterator[dict]:
    pending: queue.Queue[dict | None] = queue.Queue()

    def emit(event: dict) -> None:
        pending.put(event)

    def worker() -> None:
        try:
            _execute_run(
                app,
                user_input,
                thread_id=thread_id,
                has_checkpointer=has_checkpointer,
                cancel=cancel,
                max_concurrency=max_concurrency,
                emit=emit,
                resume=resume,
                interrupt_after=interrupt_after,
                run_config=run_config,
            )
        except Exception as exc:
            pending.put({"type": "error", "error": _format_error(exc)})
        finally:
            pending.put(None)

    thread = threading.Thread(target=worker, name="graphviagent-run", daemon=True)
    thread.start()
    while True:
        item = pending.get()
        if item is None:
            break
        yield item
    thread.join(timeout=1)


def record_run(
    app: Any,
    user_input: dict | None,
    *,
    thread_id: str | None = None,
    has_checkpointer: bool = False,
    cancel: Any | None = None,
    max_concurrency: int = MAX_RUN_THREADS,
    resume: bool = False,
    interrupt_after: list[str] | None = None,
    run_config: dict[str, Any] | None = None,
) -> dict:
    run = None
    error = None
    for event in iter_run_events(
        app,
        user_input,
        thread_id=thread_id,
        has_checkpointer=has_checkpointer,
        cancel=cancel,
        max_concurrency=max_concurrency,
        resume=resume,
        interrupt_after=interrupt_after,
        run_config=run_config,
    ):
        if event.get("type") == "done":
            run = event.get("run")
        elif event.get("type") == "error":
            error = event.get("error")
    if run is not None:
        return run
    raise RuntimeError(error or "run produced no result")


def _incoming_state(
    step: dict, state_patch: dict | None = None, state_in: dict | None = None
) -> dict:
    if state_in is not None:
        if not isinstance(state_in, dict):
            return {"value": state_in}
        return copy.deepcopy(state_in)
    return merge_state(copy.deepcopy(step.get("state_in") or {}), state_patch or {})


def _finish_replay_run(
    recorded: dict,
    *,
    parent_id: str,
    mode: str,
    from_step: str,
    approximate: bool,
    reason: str | None = None,
) -> dict:
    recorded["parent_id"] = parent_id
    recorded["mode"] = mode
    recorded["from_step"] = from_step
    recorded["approximate"] = approximate
    if reason:
        recorded["approximate_reason"] = reason
    if approximate:
        recorded["has_checkpointer"] = False
    return recorded


def _replay_native(
    app: Any,
    run: dict,
    step: dict,
    *,
    continue_graph: bool,
    incoming: dict | None,
    state_patch: dict | None,
) -> dict:
    node = str(step.get("node") or "")
    incoming_state = _incoming_state(step, state_patch, incoming)
    patch = _editor_patch(step, incoming, state_patch)
    fork_id = uuid4().hex
    config: dict[str, Any] | None = None
    try:
        snapshot = _snapshot_before_step(app, run, step)
        config = _fork_thread(app, snapshot, fork_id)
        if patch:
            app.update_state(config, patch)
    except Exception:
        config = _seed_before_step(app, node, incoming_state, thread_id=fork_id)
    recorded = record_run(
        app,
        run.get("input") if isinstance(run.get("input"), dict) else incoming_state,
        thread_id=fork_id,
        has_checkpointer=True,
        resume=True,
        interrupt_after=None if continue_graph else [node],
        run_config=config,
    )
    return _finish_replay_run(
        recorded,
        parent_id=str(run.get("id") or ""),
        mode="replay_from" if continue_graph else "replay",
        from_step=str(step.get("step_id") or ""),
        approximate=False,
    )


def _replay_step_approximate(
    app: Any,
    run: dict,
    step: dict,
    state_in: dict,
) -> dict:
    clock0 = time.perf_counter()
    wall0 = time.time()
    memory = _MemoryTrace()
    usage = _UsageHandler()
    try:
        with _usage_scope(step["node"], usage):
            update = invoke_node(
                app, step["node"], state_in, config=_callback_config(usage)
            )
        if not isinstance(update, dict):
            update = {"value": update}
        run_error = None
    except Exception as exc:
        run_error = _format_error(exc)
        update = _error_update(exc)
    finally:
        memory_mb, memory_peak_mb = memory.snapshot()
        memory.close()
    extra = usage.take(step["node"])
    ended_ms = _elapsed_ms(clock0)
    stored_update = _plain_update(update)
    state_out = copy.deepcopy(state_in) if run_error else merge_state(state_in, stored_update)
    new_step = {
        "step_id": f"{step['node']}#replay",
        "index": 0,
        "node": step["node"],
        "update": jsonable(stored_update),
        "state_in": jsonable(state_in),
        "state_out": jsonable(state_out),
        "reason": extract_reason(stored_update),
        "decisions": jsonable(extract_decisions(stored_update)),
        "unused": [],
    }
    _attach_timing(new_step, started_ms=0, ended_ms=ended_ms, wall_start=wall0)
    _attach_metrics(new_step, stored_update, memory_mb, memory_peak_mb, extra=extra)
    _attach_source(new_step, step["node"], source_map=_source_map(app))
    _attach_error(new_step, update)
    return _finish_replay_run(
        {
            "id": uuid4().hex,
            "input": jsonable(state_in),
            "steps": [new_step],
            "result": jsonable(state_out),
            "has_checkpointer": False,
            "thread_id": None,
            "parent_id": run["id"],
            "mode": "replay",
            "from_step": step.get("step_id"),
            "started_at": new_step["started_at"],
            "ended_at": new_step["ended_at"],
            "elapsed_ms": new_step["elapsed_ms"],
            "error": run_error,
        },
        parent_id=str(run.get("id") or ""),
        mode="replay",
        from_step=str(step.get("step_id") or ""),
        approximate=True,
        reason="no checkpointer; reducers were not applied",
    )


def replay_step(
    app: Any,
    run: dict,
    step_id: str,
    state_patch: dict | None = None,
    state_in: dict | None = None,
) -> dict:
    step = _find_step(run, step_id)
    if _can_native_replay(app, run):
        try:
            return _replay_native(
                app,
                run,
                step,
                continue_graph=False,
                incoming=state_in,
                state_patch=state_patch,
            )
        except Exception as exc:
            approx = _replay_step_approximate(
                app,
                run,
                step,
                _incoming_state(step, state_patch, state_in),
            )
            approx["approximate_reason"] = _format_error(exc)
            return approx
    return _replay_step_approximate(
        app, run, step, _incoming_state(step, state_patch, state_in)
    )


def _resume_approximate(
    app: Any,
    run: dict,
    step: dict,
    state: dict,
) -> dict:
    remaining = (run.get("steps") or [])[int(step.get("index") or 0) :]
    steps: list[dict] = []
    visits: dict[str, int] = {}
    edges = graph_edges(app)
    raw: list[tuple[str, dict, dict, float, float, float, dict[str, int]]] = []
    current = state
    run_error: str | None = None
    clock0 = time.perf_counter()
    wall0 = time.time()
    usage = _UsageHandler()
    for recorded in remaining:
        incoming = _with_recorded_payload(current, recorded)
        memory = _MemoryTrace()
        try:
            with _usage_scope(recorded["node"], usage):
                update = invoke_node(
                    app,
                    recorded["node"],
                    incoming,
                    config=_callback_config(usage),
                )
            if not isinstance(update, dict):
                update = {"value": update}
        except Exception as exc:
            run_error = _format_error(exc)
            update = _error_update(exc)
            memory_mb, memory_peak_mb = memory.snapshot()
            extra = usage.take(recorded["node"])
            raw.append(
                (
                    recorded["node"],
                    incoming,
                    update,
                    _elapsed_ms(clock0),
                    memory_mb,
                    memory_peak_mb,
                    extra,
                )
            )
            memory.close()
            break
        memory_mb, memory_peak_mb = memory.snapshot()
        extra = usage.take(recorded["node"])
        raw.append(
            (
                recorded["node"],
                incoming,
                update,
                _elapsed_ms(clock0),
                memory_mb,
                memory_peak_mb,
                extra,
            )
        )
        memory.close()
        current = merge_state(current, update)

    current = copy.deepcopy(state)
    prev_end = 0.0
    source_map = _source_map(app)
    for index, (node, incoming, update, ended_ms, memory_mb, memory_peak_mb, extra) in enumerate(raw):
        visits[node] = visits.get(node, 0) + 1
        next_node = raw[index + 1][0] if index + 1 < len(raw) else None
        state_in = copy.deepcopy(incoming)
        failed = isinstance(update, dict) and update.get("error")
        stored_update = _plain_update(update)
        state_out = copy.deepcopy(state_in) if failed else merge_state(current, stored_update)
        started_ms = prev_end
        built = {
            "step_id": f"{node}#{visits[node]}",
            "index": index,
            "node": node,
            "update": jsonable(stored_update),
            "state_in": jsonable(state_in),
            "state_out": jsonable(state_out),
            "reason": extract_reason(stored_update),
            "decisions": jsonable(extract_decisions(stored_update)),
            "unused": unused_targets(node, next_node, edges),
        }
        _attach_timing(built, started_ms=started_ms, ended_ms=ended_ms, wall_start=wall0)
        _attach_metrics(built, stored_update, memory_mb, memory_peak_mb, extra=extra)
        _attach_source(built, node, source_map=source_map)
        _attach_error(built, update)
        steps.append(built)
        prev_end = ended_ms
        current = state_out

    return _finish_replay_run(
        {
            "id": uuid4().hex,
            "input": jsonable(state),
            "steps": steps,
            "result": jsonable(current),
            "has_checkpointer": False,
            "thread_id": None,
            "parent_id": run["id"],
            "mode": "replay_from",
            "from_step": step.get("step_id"),
            "started_at": _iso_from_wall(wall0, 0),
            "ended_at": _iso_from_wall(wall0, _run_span(steps, _elapsed_ms(clock0))),
            "elapsed_ms": _run_span(steps, _elapsed_ms(clock0)),
            "error": run_error,
        },
        parent_id=str(run.get("id") or ""),
        mode="replay_from",
        from_step=str(step.get("step_id") or ""),
        approximate=True,
        reason="no checkpointer; recorded path was replayed without routing",
    )


def resume_from_step(
    app: Any,
    run: dict,
    step_id: str,
    state_patch: dict | None = None,
    state_in: dict | None = None,
) -> dict:
    step = _find_step(run, step_id)
    if _can_native_replay(app, run):
        try:
            return _replay_native(
                app,
                run,
                step,
                continue_graph=True,
                incoming=state_in,
                state_patch=state_patch,
            )
        except Exception as exc:
            approx = _resume_approximate(
                app, run, step, _incoming_state(step, state_patch, state_in)
            )
            approx["approximate_reason"] = _format_error(exc)
            return approx
    return _resume_approximate(
        app, run, step, _incoming_state(step, state_patch, state_in)
    )


def _find_step(run: dict, step_id: str) -> dict:
    for step in run.get("steps") or []:
        if step.get("step_id") == step_id:
            return step
    raise KeyError(f"unknown step {step_id}")
