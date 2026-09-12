from typing import TypedDict

from langgraph.graph import END, START, StateGraph

EXAMPLES = [
    {"part": 3, "total": 10},
    {"part": 3, "total": 0},
    {"part": 25, "total": 40},
]


class RatioState(TypedDict):
    part: float
    total: float
    ratio: float
    percent: float
    label: str


def read_values(state: RatioState) -> dict:
    return {"part": float(state["part"]), "total": float(state["total"])}


def compute_ratio(state: RatioState) -> dict:
    return {"ratio": state["part"] / state["total"]}


def to_percent(state: RatioState) -> dict:
    percent = state["ratio"] * 100
    return {
        "percent": percent,
        "label": f"{state['part']} of {state['total']} = {percent:.1f}%",
        "decisions": [
            {
                "step": "to_percent",
                "choice": "scale",
                "reason": f"ratio {state['ratio']} × 100",
            }
        ],
    }


def build_graph():
    graph = StateGraph(RatioState)
    graph.add_node("read", read_values)
    graph.add_node("ratio", compute_ratio)
    graph.add_node("percent", to_percent)
    graph.add_edge(START, "read")
    graph.add_edge("read", "ratio")
    graph.add_edge("ratio", "percent")
    graph.add_edge("percent", END)
    return graph.compile()
