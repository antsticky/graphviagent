"""Nested helpers so a failed run shows a real traceback you can click."""

from typing import TypedDict

from langgraph.graph import END, START, StateGraph

EXAMPLES = [
    {"label": "3 / 4", "input": {"hits": 3, "tries": 4}},
    {"label": "zero tries (raises)", "input": {"hits": 3, "tries": 0}},
]


class TraceState(TypedDict):
    hits: float
    tries: float
    ratio: float
    label: str


def _ratio(numer: float, denom: float) -> float:
    return numer / denom


def _score(state: TraceState) -> float:
    return _ratio(state["hits"], state["tries"])


def tally(state: TraceState) -> dict:
    return {"ratio": _score(state)}


def label(state: TraceState) -> dict:
    ratio = state["ratio"]
    grade = "ok" if ratio >= 0.5 else "low"
    return {
        "label": grade,
        "decisions": [
            {
                "step": "label",
                "choice": grade,
                "reason": f"{ratio:.2f} → {grade}",
            }
        ],
    }


def build_graph():
    graph = StateGraph(TraceState)
    graph.add_node("tally", tally)
    graph.add_node("label", label)
    graph.add_edge(START, "tally")
    graph.add_edge("tally", "label")
    graph.add_edge("label", END)
    return graph.compile()
