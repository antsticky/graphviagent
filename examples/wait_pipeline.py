"""Three timed waits so Cancel, Continue, and Step have something to catch."""

import time
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

EXAMPLES = [
    {"label": "1s waits", "input": {"label": "slow", "seconds": 1}},
    {"label": "3s waits", "input": {"label": "slower", "seconds": 3}},
]


class WaitState(TypedDict, total=False):
    label: str
    seconds: float
    stage: str
    notes: list


def _nap(state: WaitState) -> float:
    try:
        return max(0.0, float(state.get("seconds") or 1))
    except (TypeError, ValueError):
        return 1.0


def _note(state: WaitState, text: str) -> list:
    notes = list(state.get("notes") or [])
    notes.append(text)
    return notes


def wait_a(state: WaitState) -> dict:
    secs = _nap(state)
    print(f"wait_a sleeping {secs}s")
    time.sleep(secs)
    return {"stage": "a", "notes": _note(state, f"wait_a slept {secs}s")}


def wait_b(state: WaitState) -> dict:
    secs = _nap(state)
    print(f"wait_b sleeping {secs}s")
    time.sleep(secs)
    return {"stage": "b", "notes": _note(state, f"wait_b slept {secs}s")}


def wait_c(state: WaitState) -> dict:
    secs = _nap(state)
    print(f"wait_c sleeping {secs}s")
    time.sleep(secs)
    return {"stage": "c", "notes": _note(state, f"wait_c slept {secs}s")}


def build_graph():
    graph = StateGraph(WaitState)
    graph.add_node("wait_a", wait_a)
    graph.add_node("wait_b", wait_b)
    graph.add_node("wait_c", wait_c)
    graph.add_edge(START, "wait_a")
    graph.add_edge("wait_a", "wait_b")
    graph.add_edge("wait_b", "wait_c")
    graph.add_edge("wait_c", END)
    return graph.compile()


if __name__ == "__main__":
    app = build_graph()
    print(app.invoke({"label": "demo", "seconds": 2.1}))
    print("\nInspect with: graphviagent serve examples")
