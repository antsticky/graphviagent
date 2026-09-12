from __future__ import annotations

import copy
import time
from typing import Any
from uuid import uuid4


def _elapsed_ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000, 2)


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

    stream = app.stream(user_input, config) if config else app.stream(user_input)
    raw_events: list[tuple[str, dict, float]] = []
    run_error: str | None = None
    started = time.perf_counter()
    try:
        for event in stream:
            if not isinstance(event, dict) or not event:
                continue
            node, update = next(iter(event.items()))
            if not isinstance(update, dict):
                update = {"value": update}
            raw_events.append((str(node), update, _elapsed_ms(started)))
            started = time.perf_counter()
    except Exception as exc:
        run_error = _format_error(exc)
        failed = _guess_failed_node(raw_events, edges)
        if failed:
            raw_events.append((failed, {"error": run_error}, _elapsed_ms(started)))

    for index, (node, update, elapsed_ms) in enumerate(raw_events):
        visits[node] = visits.get(node, 0) + 1
        next_node = raw_events[index + 1][0] if index + 1 < len(raw_events) else None
        state_in = copy.deepcopy(state)
        failed = isinstance(update, dict) and update.get("error")
        state_out = copy.deepcopy(state_in) if failed else merge_state(state, update)
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
            "elapsed_ms": elapsed_ms,
        }
        if failed:
            step["error"] = str(update.get("error"))
        steps.append(step)
        state = state_out

    return {
        "id": run_id,
        "input": jsonable(user_input),
        "steps": steps,
        "result": jsonable(state),
        "has_checkpointer": has_checkpointer,
        "thread_id": run_id if has_checkpointer else None,
        "elapsed_ms": round(sum(step.get("elapsed_ms") or 0 for step in steps), 2),
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
    started = time.perf_counter()
    try:
        update = invoke_node(app, step["node"], state_in)
        if not isinstance(update, dict):
            update = {"value": update}
        run_error = None
    except Exception as exc:
        run_error = _format_error(exc)
        update = {"error": run_error}
    elapsed_ms = _elapsed_ms(started)
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
        "elapsed_ms": elapsed_ms,
    }
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
        "elapsed_ms": elapsed_ms,
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

    raw: list[tuple[str, dict, float]] = []
    current = state
    run_error: str | None = None
    for recorded in remaining:
        started = time.perf_counter()
        try:
            update = invoke_node(app, recorded["node"], current)
            if not isinstance(update, dict):
                update = {"value": update}
        except Exception as exc:
            run_error = _format_error(exc)
            update = {"error": run_error}
            raw.append((recorded["node"], update, _elapsed_ms(started)))
            break
        raw.append((recorded["node"], update, _elapsed_ms(started)))
        current = merge_state(current, update)

    current = copy.deepcopy(state)
    for index, (node, update, elapsed_ms) in enumerate(raw):
        visits[node] = visits.get(node, 0) + 1
        next_node = raw[index + 1][0] if index + 1 < len(raw) else None
        state_in = copy.deepcopy(current)
        failed = isinstance(update, dict) and update.get("error")
        state_out = copy.deepcopy(state_in) if failed else merge_state(current, update)
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
            "elapsed_ms": elapsed_ms,
        }
        if failed:
            step["error"] = str(update.get("error"))
        steps.append(step)
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
        "elapsed_ms": round(sum(item.get("elapsed_ms") or 0 for item in steps), 2),
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
