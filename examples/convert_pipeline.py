from typing import Literal, TypedDict

from langgraph.graph import END, START, StateGraph

EXAMPLES = [
    {"label": "100°C to °F", "input": {"value": 100, "unit": "c_to_f"}},
    {"label": "32°F to °C", "input": {"value": 32, "unit": "f_to_c"}},
    {"label": "100 / 0", "input": {"value": 0, "unit": "per_unit"}},
    {"label": "unknown unit", "input": {"value": 21, "unit": "kelvin"}},
]


class ConvertState(TypedDict):
    value: float
    unit: str
    path: str
    result: float
    label: str


def parse(state: ConvertState) -> dict:
    return {"value": float(state["value"]), "unit": str(state.get("unit") or "")}


def choose_unit(state: ConvertState) -> dict:
    unit = state["unit"]
    known = {"c_to_f": "c_to_f", "f_to_c": "f_to_c", "per_unit": "per_unit"}
    if unit not in known:
        raise ValueError(f"unknown unit {unit!r}; use c_to_f, f_to_c, or per_unit")
    path = known[unit]
    return {
        "path": path,
        "decisions": [
            {
                "step": "choose_unit",
                "choice": path,
                "reason": f"unit={unit!r}",
            }
        ],
    }


def c_to_f(state: ConvertState) -> dict:
    return {"result": state["value"] * 9 / 5 + 32}


def f_to_c(state: ConvertState) -> dict:
    return {"result": (state["value"] - 32) * 5 / 9}


def per_unit(state: ConvertState) -> dict:
    return {"result": 100 / state["value"]}


def label(state: ConvertState) -> dict:
    names = {
        "c_to_f": f"{state['value']}°C = {state['result']}°F",
        "f_to_c": f"{state['value']}°F = {state['result']}°C",
        "per_unit": f"100 / {state['value']} = {state['result']}",
    }
    return {"label": names[state["path"]]}


def route_unit(state: ConvertState) -> Literal["c_to_f", "f_to_c", "per_unit"]:
    return state["path"]  # type: ignore[return-value]


def build_graph():
    graph = StateGraph(ConvertState)
    graph.add_node("parse", parse)
    graph.add_node("choose_unit", choose_unit)
    graph.add_node("c_to_f", c_to_f)
    graph.add_node("f_to_c", f_to_c)
    graph.add_node("per_unit", per_unit)
    graph.add_node("label", label)
    graph.add_edge(START, "parse")
    graph.add_edge("parse", "choose_unit")
    graph.add_conditional_edges(
        "choose_unit",
        route_unit,
        {"c_to_f": "c_to_f", "f_to_c": "f_to_c", "per_unit": "per_unit"},
    )
    graph.add_edge("c_to_f", "label")
    graph.add_edge("f_to_c", "label")
    graph.add_edge("per_unit", "label")
    graph.add_edge("label", END)
    return graph.compile()
