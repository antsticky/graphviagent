from __future__ import annotations

import copy
import json
import time
import tracemalloc
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

_SKIP_NODES = {"__start__", "START", "__end__", "END"}
MAX_RUN_THREADS = 3


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
    if not isinstance(obj, dict):
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
    if not sources:
        return 0, 0
    return _tokens_from_mapping(sources[0])


def extract_tokens(update: Any) -> dict[str, int]:
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
        extra = update.get("additional_kwargs")
        if isinstance(extra, dict):
            add(extra)
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
    return {
        "prompt": prompt,
        "completion": completion,
        "total": prompt + completion,
        "tool": tool,
    }


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
) -> dict:
    tokens = extract_tokens(update)
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


def _install_node_probes(
    app: Any,
    clock0: float,
    cancel: Any | None = None,
) -> tuple[list[dict], Any]:
    probes: list[dict] = []
    restores: list[tuple[Any, Any]] = []
    nodes = getattr(app, "nodes", None)
    if not isinstance(nodes, dict):
        return probes, lambda: None

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
                memory = _MemoryTrace()
                try:
                    return orig(*args, **kwargs)
                finally:
                    memory_mb, memory_peak_mb = memory.snapshot()
                    memory.close()
                    ended_ms = round((time.perf_counter() - clock0) * 1000, 2)
                    probes.append(
                        {
                            "node": node_name,
                            "started_ms": started_ms,
                            "ended_ms": ended_ms,
                            "elapsed_ms": round(max(0.0, ended_ms - started_ms), 2),
                            "memory_mb": memory_mb,
                            "memory_peak_mb": memory_peak_mb,
                            "input": incoming,
                        }
                    )

            return probed

        target.invoke = make_probed(str(name), original)
        restores.append((target, original))

    def restore() -> None:
        for target, original in restores:
            target.invoke = original

    return probes, restore


def _take_probe(probes: list[dict], node: str) -> dict | None:
    for index, probe in enumerate(probes):
        if probe.get("node") == node:
            return probes.pop(index)
    return None


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


def _is_cancelled(exc: BaseException) -> bool:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, RunCancelled) or str(current) == "cancelled":
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
    try:
        graph = app.get_graph()
    except Exception:
        return []
    edges: list[tuple[str, str]] = []
    for edge in getattr(graph, "edges", []) or []:
        source = getattr(edge, "source", None)
        target = getattr(edge, "target", None)
        if source is None and isinstance(edge, (tuple, list)) and len(edge) >= 2:
            source, target = edge[0], edge[1]
        if source is None or target is None:
            continue
        edges.append((str(source), str(target)))
    return edges


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


def invoke_node(app: Any, node_name: str, state: dict) -> dict:
    node = app.nodes[node_name]
    if hasattr(node, "invoke"):
        result = node.invoke(state)
        return result if isinstance(result, dict) else {"result": result}
    bound = getattr(node, "bound", None) or getattr(node, "runnable", None)
    if bound is not None and hasattr(bound, "invoke"):
        result = bound.invoke(state)
        return result if isinstance(result, dict) else {"result": result}
    raise RuntimeError(f"cannot invoke node {node_name!r}")


def record_run(
    app: Any,
    user_input: dict,
    *,
    thread_id: str | None = None,
    has_checkpointer: bool = False,
    cancel: Any | None = None,
    max_concurrency: int = MAX_RUN_THREADS,
) -> dict:
    run_id = thread_id or uuid4().hex
    config: dict[str, Any] = {"max_concurrency": max_concurrency}
    if has_checkpointer:
        config["configurable"] = {"thread_id": run_id}
    state = copy.deepcopy(user_input)
    steps: list[dict] = []
    visits: dict[str, int] = {}
    edges = graph_edges(app)

    raw_events: list[tuple[str, dict, float]] = []
    run_error: str | None = None
    clock0 = time.perf_counter()
    wall0 = time.time()
    probes, restore_probes = _install_node_probes(app, clock0, cancel=cancel)
    try:
        stream = app.stream(user_input, config)
        for event in stream:
            if cancel is not None and getattr(cancel, "is_set", lambda: False)():
                raise RunCancelled("cancelled")
            if not isinstance(event, dict) or not event:
                continue
            node, update = next(iter(event.items()))
            if not isinstance(update, dict):
                update = {"value": update}
            raw_events.append((str(node), update, _elapsed_ms(clock0)))
    except Exception as exc:
        if _is_cancelled(exc):
            run_error = "cancelled"
        else:
            run_error = _format_error(exc)
            failed = _guess_failed_node(raw_events, edges)
            if failed:
                raw_events.append((failed, {"error": run_error}, _elapsed_ms(clock0)))
    finally:
        restore_probes()

    completed_end: dict[str, float] = {}
    for index, (node, update, ended_ms) in enumerate(raw_events):
        visits[node] = visits.get(node, 0) + 1
        next_node = raw_events[index + 1][0] if index + 1 < len(raw_events) else None
        graph_in = copy.deepcopy(state)
        failed = isinstance(update, dict) and update.get("error")
        probe = _take_probe(probes, node)
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
        state_out = copy.deepcopy(state_in) if failed else merge_state(state_in, update)
        step = {
            "step_id": f"{node}#{visits[node]}",
            "index": index,
            "node": node,
            "update": jsonable(update),
            "state_in": jsonable(state_in),
            "state_out": jsonable(state_out),
            "reason": extract_reason(update),
            "decisions": jsonable(extract_decisions(update)),
            "unused": unused_targets(node, next_node, edges),
        }
        _attach_timing(step, started_ms=started_ms, ended_ms=ended_ms, wall_start=wall0)
        _attach_metrics(step, update, memory_mb, memory_peak_mb)
        if failed:
            step["error"] = str(update.get("error"))
        steps.append(step)
        completed_end[node] = ended_ms
        state = copy.deepcopy(graph_in) if failed else merge_state(graph_in, update)

    return {
        "id": run_id,
        "input": jsonable(user_input),
        "steps": steps,
        "result": jsonable(state),
        "has_checkpointer": has_checkpointer,
        "thread_id": run_id if has_checkpointer else None,
        "started_at": _iso_from_wall(wall0, 0),
        "ended_at": _iso_from_wall(wall0, _run_span(steps, _elapsed_ms(clock0))),
        "elapsed_ms": _run_span(steps, _elapsed_ms(clock0)),
        "error": run_error,
    }


def _incoming_state(
    step: dict, state_patch: dict | None = None, state_in: dict | None = None
) -> dict:
    if state_in is not None:
        if not isinstance(state_in, dict):
            return {"value": state_in}
        return copy.deepcopy(state_in)
    return merge_state(copy.deepcopy(step.get("state_in") or {}), state_patch or {})


def replay_step(
    app: Any,
    run: dict,
    step_id: str,
    state_patch: dict | None = None,
    state_in: dict | None = None,
) -> dict:
    step = _find_step(run, step_id)
    state_in = _with_recorded_payload(_incoming_state(step, state_patch, state_in), step)
    clock0 = time.perf_counter()
    wall0 = time.time()
    memory = _MemoryTrace()
    try:
        update = invoke_node(app, step["node"], state_in)
        if not isinstance(update, dict):
            update = {"value": update}
        run_error = None
    except Exception as exc:
        run_error = _format_error(exc)
        update = {"error": run_error}
    finally:
        memory_mb, memory_peak_mb = memory.snapshot()
        memory.close()
    ended_ms = _elapsed_ms(clock0)
    state_out = copy.deepcopy(state_in) if run_error else merge_state(state_in, update)
    new_step = {
        "step_id": f"{step['node']}#replay",
        "index": 0,
        "node": step["node"],
        "update": jsonable(update),
        "state_in": jsonable(state_in),
        "state_out": jsonable(state_out),
        "reason": extract_reason(update),
        "decisions": jsonable(extract_decisions(update)),
        "unused": [],
    }
    _attach_timing(new_step, started_ms=0, ended_ms=ended_ms, wall_start=wall0)
    _attach_metrics(new_step, update, memory_mb, memory_peak_mb)
    if run_error:
        new_step["error"] = run_error
    return {
        "id": uuid4().hex,
        "input": jsonable(state_in),
        "steps": [new_step],
        "result": jsonable(state_out),
        "has_checkpointer": False,
        "thread_id": None,
        "parent_id": run["id"],
        "mode": "replay",
        "from_step": step_id,
        "started_at": new_step["started_at"],
        "ended_at": new_step["ended_at"],
        "elapsed_ms": new_step["elapsed_ms"],
        "error": run_error,
    }


def resume_from_step(
    app: Any,
    run: dict,
    step_id: str,
    state_patch: dict | None = None,
    state_in: dict | None = None,
) -> dict:
    step = _find_step(run, step_id)
    start = step["index"]
    remaining = run["steps"][start:]
    state = _incoming_state(step, state_patch, state_in)
    steps: list[dict] = []
    visits: dict[str, int] = {}
    edges = graph_edges(app)

    if run.get("has_checkpointer") and run.get("thread_id"):
        try:
            return _resume_with_checkpointer(app, run, step, state)
        except Exception:
            pass

    raw: list[tuple[str, dict, dict, float, float, float]] = []
    current = state
    run_error: str | None = None
    clock0 = time.perf_counter()
    wall0 = time.time()
    for recorded in remaining:
        incoming = _with_recorded_payload(current, recorded)
        memory = _MemoryTrace()
        try:
            update = invoke_node(app, recorded["node"], incoming)
            if not isinstance(update, dict):
                update = {"value": update}
        except Exception as exc:
            run_error = _format_error(exc)
            update = {"error": run_error}
            memory_mb, memory_peak_mb = memory.snapshot()
            raw.append((recorded["node"], incoming, update, _elapsed_ms(clock0), memory_mb, memory_peak_mb))
            memory.close()
            break
        memory_mb, memory_peak_mb = memory.snapshot()
        raw.append((recorded["node"], incoming, update, _elapsed_ms(clock0), memory_mb, memory_peak_mb))
        memory.close()
        current = merge_state(current, update)

    current = copy.deepcopy(state)
    prev_end = 0.0
    for index, (node, incoming, update, ended_ms, memory_mb, memory_peak_mb) in enumerate(raw):
        visits[node] = visits.get(node, 0) + 1
        next_node = raw[index + 1][0] if index + 1 < len(raw) else None
        state_in = copy.deepcopy(incoming)
        failed = isinstance(update, dict) and update.get("error")
        state_out = copy.deepcopy(state_in) if failed else merge_state(current, update)
        started_ms = prev_end
        step = {
            "step_id": f"{node}#{visits[node]}",
            "index": index,
            "node": node,
            "update": jsonable(update),
            "state_in": jsonable(state_in),
            "state_out": jsonable(state_out),
            "reason": extract_reason(update),
            "decisions": jsonable(extract_decisions(update)),
            "unused": unused_targets(node, next_node, edges),
        }
        _attach_timing(step, started_ms=started_ms, ended_ms=ended_ms, wall_start=wall0)
        _attach_metrics(step, update, memory_mb, memory_peak_mb)
        if failed:
            step["error"] = str(update.get("error"))
        steps.append(step)
        prev_end = ended_ms
        current = state_out

    return {
        "id": uuid4().hex,
        "input": jsonable(state),
        "steps": steps,
        "result": jsonable(current),
        "has_checkpointer": False,
        "thread_id": None,
        "parent_id": run["id"],
        "mode": "replay_from",
        "from_step": step_id,
        "started_at": _iso_from_wall(wall0, 0),
        "ended_at": _iso_from_wall(wall0, _run_span(steps, _elapsed_ms(clock0))),
        "elapsed_ms": _run_span(steps, _elapsed_ms(clock0)),
        "error": run_error,
    }


def _resume_with_checkpointer(app: Any, run: dict, step: dict, state_in: dict) -> dict:
    config = {"configurable": {"thread_id": run["thread_id"]}}
    update = invoke_node(app, step["node"], state_in)
    if not isinstance(update, dict):
        update = {"value": update}
    app.update_state(config, update, as_node=step["node"])
    app.invoke(None, config)
    return record_run(
        app,
        run["input"],
        thread_id=uuid4().hex,
        has_checkpointer=True,
    )


def _find_step(run: dict, step_id: str) -> dict:
    for step in run.get("steps") or []:
        if step.get("step_id") == step_id:
            return step
    raise KeyError(f"unknown step {step_id}")
