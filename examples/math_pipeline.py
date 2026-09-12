from typing import Literal, TypedDict

from langgraph.graph import END, START, StateGraph

EXAMPLES = [
    {"label": "12 / 3", "input": {"a": 12, "b": 3, "op": "div"}},
    {"label": "divide by zero", "input": {"a": 12, "b": 0, "op": "div"}},
    {"label": "7 + 5", "input": {"a": 7, "b": 5, "op": "add"}},
]


class MathState(TypedDict):
    a: float
    b: float
    op: str
    path: str
    result: float
    summary: str


def parse(state: MathState) -> dict:
    a = float(state["a"])
    b = float(state["b"])
    op = str(state.get("op") or "div").lower()
    return {"a": a, "b": b, "op": op}


def choose_op(state: MathState) -> dict:
    op = state["op"]
    path = "add" if op == "add" else "div"
    if path == "add":
        reason = f"op={op!r} → add {state['a']} + {state['b']}"
    else:
        reason = f"op={op!r} → divide {state['a']} / {state['b']}"
    return {
        "path": path,
        "decisions": [
            {
                "step": "choose_op",
                "choice": path,
                "reason": reason,
                "facts": {"a": state["a"], "b": state["b"], "op": op},
            }
        ],
    }


def add(state: MathState) -> dict:
    return {"result": state["a"] + state["b"]}


def divide(state: MathState) -> dict:
    return {"result": state["a"] / state["b"]}


def format_result(state: MathState) -> dict:
    symbol = "+" if state["path"] == "add" else "/"
    return {"summary": f"{state['a']} {symbol} {state['b']} = {state['result']}"}


def route_op(state: MathState) -> Literal["add", "div"]:
    return state["path"]  # type: ignore[return-value]


def build_graph():
    graph = StateGraph(MathState)
    graph.add_node("parse", parse)
    graph.add_node("choose_op", choose_op)
    graph.add_node("add", add)
    graph.add_node("div", divide)
    graph.add_node("format", format_result)
    graph.add_edge(START, "parse")
    graph.add_edge("parse", "choose_op")
    graph.add_conditional_edges("choose_op", route_op, {"add": "add", "div": "div"})
    graph.add_edge("add", "format")
    graph.add_edge("div", "format")
    graph.add_edge("format", END)
    return graph.compile()
