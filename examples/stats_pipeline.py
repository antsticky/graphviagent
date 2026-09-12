from typing import TypedDict

from langgraph.graph import END, START, StateGraph

EXAMPLES = [
    {"values": [2, 4, 6, 8]},
    {"values": []},
    {"values": [10]},
]


class StatsState(TypedDict):
    values: list[float]
    count: int
    total: float
    mean: float
    report: str


def collect(state: StatsState) -> dict:
    values = [float(item) for item in state.get("values") or []]
    return {"values": values, "count": len(values), "total": sum(values)}


def mean(state: StatsState) -> dict:
    return {"mean": state["total"] / state["count"]}


def report(state: StatsState) -> dict:
    return {
        "report": f"n={state['count']}  sum={state['total']}  mean={state['mean']}",
        "decisions": [
            {
                "step": "report",
                "choice": "ok",
                "reason": f"mean of {state['count']} values",
            }
        ],
    }


def build_graph():
    graph = StateGraph(StatsState)
    graph.add_node("collect", collect)
    graph.add_node("mean", mean)
    graph.add_node("report", report)
    graph.add_edge(START, "collect")
    graph.add_edge("collect", "mean")
    graph.add_edge("mean", "report")
    graph.add_edge("report", END)
    return graph.compile()
