from __future__ import annotations

import copy
import time
import tracemalloc
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

_SKIP_NODES = {"__start__", "START", "__end__", "END"}


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
        self.prev = tracemalloc.get_traced_memory()[0]

    def snapshot(self) -> float:
        current, _peak = tracemalloc.get_traced_memory()
        delta = max(0, current - self.prev)
        self.prev = current
        return _bytes_to_mb(delta)

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


def extract_tokens(update: Any) -> dict[str, int]:
    prompt = 0
    completion = 0

    def add(obj: Any) -> None:
        nonlocal prompt, completion
        if not isinstance(obj, dict):
            return
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
            return
        extra_prompt, extra_completion = _tokens_from_mapping(sources[0])
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
    return {"prompt": prompt, "completion": completion, "total": prompt + completion}


def _tool_call_name(call: dict) -> str:
    function = call.get("function") if isinstance(call.get("function"), dict) else {}
    return str(call.get("name") or function.get("name") or call.get("tool") or "tool")


def extract_tool_metrics(update: Any, elapsed_ms: float | None) -> tuple[list[dict], float | None]:
    tools: list[dict] = []
    requests = 0
    results = 0
    if isinstance(update, dict):
        for call in update.get("tool_calls") or []:
            if isinstance(call, dict):
                tools.append({"name": _tool_call_name(call), "latency_ms": None})
                requests += 1
        for message in extract_messages(update):
            for call in message.get("tool_calls") or []:
                if isinstance(call, dict):
                    tools.append({"name": _tool_call_name(call), "latency_ms": None})
                    requests += 1
            role = message.get("role") or message.get("type")
            if role == "tool":
                name = message.get("name") or message.get("tool") or "tool"
                tools.append({"name": str(name), "latency_ms": None})
                results += 1
    tool_only = results > 0 and requests == 0
    if tool_only and elapsed_ms is not None:
        if len(tools) == 1:
            tools[0]["latency_ms"] = elapsed_ms
        return tools, round(float(elapsed_ms), 2)
    known = [float(item["latency_ms"]) for item in tools if item.get("latency_ms") is not None]
    return tools, round(sum(known), 2) if known else None


def _attach_metrics(step: dict, update: Any, memory_mb: float) -> dict:
    tokens = extract_tokens(update)
    tools, tool_latency_ms = extract_tool_metrics(update, step.get("elapsed_ms"))
    step["memory_mb"] = round(float(memory_mb or 0), 4)
    step["tokens"] = tokens
    step["tools"] = jsonable(tools)
    step["tool_latency_ms"] = tool_latency_ms
    return step


def _invoke_target(node: Any) -> Any | None:
    for attr in ("proc", "bound", "runnable"):
        child = getattr(node, attr, None)
        if child is not None and callable(getattr(child, "invoke", None)):
            return child
    if callable(getattr(node, "invoke", None)):
        return node
    return None


def _install_node_probes(app: Any, clock0: float) -> tuple[list[dict], Any]:
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
                started_ms = round((time.perf_counter() - clock0) * 1000, 2)
                memory = _MemoryTrace()
                try:
                    return orig(*args, **kwargs)
                finally:
                    memory_mb = memory.snapshot()
                    memory.close()
                    ended_ms = round((time.perf_counter() - clock0) * 1000, 2)
                    probes.append(
                        {
                            "node": node_name,
                            "started_ms": started_ms,
                            "ended_ms": ended_ms,
                            "elapsed_ms": round(max(0.0, ended_ms - started_ms), 2),
                            "memory_mb": memory_mb,
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


def _steps_elapsed(steps: list[dict]) -> float:
    return round(sum(float(step.get("elapsed_ms") or 0) for step in steps), 2)


def jsonable(obj: Any) -> Any:
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, dict):
        return {str(key): jsonable(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [jsonable(item) for item in obj]
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


def extract_messages(value: Any) -> list[dict]:
    if isinstance(value, list):
        found = [item for item in value if _is_message(item)]
        return found if found and len(found) >= max(1, len(value) // 2) else []
    if not isinstance(value, dict):
        return []
    if _is_message(value):
        return [value]
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
) -> dict:
    run_id = thread_id or uuid4().hex
    config = {"configurable": {"thread_id": run_id}} if has_checkpointer else {}
    state = copy.deepcopy(user_input)
    steps: list[dict] = []
    visits: dict[str, int] = {}
    edges = graph_edges(app)

    raw_events: list[tuple[str, dict, float]] = []
    run_error: str | None = None
    clock0 = time.perf_counter()
    wall0 = time.time()
    probes, restore_probes = _install_node_probes(app, clock0)
    try:
        stream = app.stream(user_input, config) if config else app.stream(user_input)
        for event in stream:
            if not isinstance(event, dict) or not event:
                continue
            node, update = next(iter(event.items()))
            if not isinstance(update, dict):
                update = {"value": update}
            raw_events.append((str(node), update, _elapsed_ms(clock0)))
    except Exception as exc:
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
        state_in = copy.deepcopy(state)
        failed = isinstance(update, dict) and update.get("error")
        state_out = copy.deepcopy(state_in) if failed else merge_state(state, update)
        probe = _take_probe(probes, node)
        if probe:
            started_ms = float(probe["started_ms"])
            ended_ms = float(probe["ended_ms"])
            memory_mb = float(probe["memory_mb"])
        else:
            started_ms = _infer_started_ms(node, ended_ms, completed_end, edges)
            memory_mb = 0.0
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
        _attach_metrics(step, update, memory_mb)
        if failed:
            step["error"] = str(update.get("error"))
        steps.append(step)
        completed_end[node] = ended_ms
        state = state_out

    return {
        "id": run_id,
        "input": jsonable(user_input),
        "steps": steps,
        "result": jsonable(state),
        "has_checkpointer": has_checkpointer,
        "thread_id": run_id if has_checkpointer else None,
        "started_at": _iso_from_wall(wall0, 0),
        "ended_at": _iso_from_wall(wall0, _run_span(steps, _elapsed_ms(clock0))),
        "elapsed_ms": _steps_elapsed(steps),
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
    state_in = _incoming_state(step, state_patch, state_in)
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
        memory_mb = memory.snapshot()
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
    _attach_metrics(new_step, update, memory_mb)
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

    raw: list[tuple[str, dict, float, float]] = []
    current = state
    run_error: str | None = None
    clock0 = time.perf_counter()
    wall0 = time.time()
    for recorded in remaining:
        memory = _MemoryTrace()
        try:
            update = invoke_node(app, recorded["node"], current)
            if not isinstance(update, dict):
                update = {"value": update}
        except Exception as exc:
            run_error = _format_error(exc)
            update = {"error": run_error}
            raw.append((recorded["node"], update, _elapsed_ms(clock0), memory.snapshot()))
            memory.close()
            break
        raw.append((recorded["node"], update, _elapsed_ms(clock0), memory.snapshot()))
        memory.close()
        current = merge_state(current, update)

    current = copy.deepcopy(state)
    prev_end = 0.0
    for index, (node, update, ended_ms, memory_mb) in enumerate(raw):
        visits[node] = visits.get(node, 0) + 1
        next_node = raw[index + 1][0] if index + 1 < len(raw) else None
        state_in = copy.deepcopy(current)
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
        _attach_metrics(step, update, memory_mb)
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
        "elapsed_ms": _steps_elapsed(steps),
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
