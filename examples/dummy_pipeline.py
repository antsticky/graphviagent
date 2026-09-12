from operator import add
from typing import Annotated, Literal, TypedDict

from langgraph.graph import END, START, StateGraph

EXAMPLES = [
    {"name": "Ada"},
    {"name": "Bo"},
    {"name": "Ada Lovelace"},
    {"name": "Grace"},
]


# Shared state. Each node returns only the keys it updates; LangGraph merges them in.
# `decisions` appends so every branch/loop choice stays visible in the final state.
class PipelineState(TypedDict):
    name: str
    greeting: str
    shout: str
    polished: str
    passes: int
    path: str
    loop_choice: str
    decisions: Annotated[list[dict], add]


def greet(state: PipelineState) -> dict:
    return {
        "greeting": f"Hello, {state['name']}!",
        "passes": 0,
        "shout": "",
        "polished": "",
    }


def choose_path(state: PipelineState) -> dict:
    """Visible decision node: short names shout, longer names skip."""
    name = state["name"]
    length = len(name)
    threshold = 3
    path = "shout" if length <= threshold else "skip"
    if path == "shout":
        reason = f"len({name!r}) = {length} <= {threshold} → shout"
    else:
        reason = f"len({name!r}) = {length} > {threshold} → skip"
    return {
        "path": path,
        "decisions": [
            {
                "step": "choose_path",
                "choice": path,
                "reason": reason,
                "facts": {"name": name, "name_length": length, "threshold": threshold},
            }
        ],
    }


def shout(state: PipelineState) -> dict:
    return {"shout": state["greeting"].upper()}


def skip_shout(state: PipelineState) -> dict:
    return {"shout": state["greeting"]}


def polish(state: PipelineState) -> dict:
    passes = state["passes"] + 1
    return {
        "passes": passes,
        "polished": f"{state['shout']} (pass {passes})",
    }


def check_loop(state: PipelineState) -> dict:
    """Visible decision node: keep polishing until 2 passes."""
    passes = state["passes"]
    required = 2
    if passes < required:
        choice = "polish"
        reason = f"passes = {passes} < {required} → polish again"
    else:
        choice = "done"
        reason = f"passes = {passes} >= {required} → stop"
    return {
        "loop_choice": choice,
        "decisions": [
            {
                "step": "check_loop",
                "choice": choice,
                "reason": reason,
                "facts": {"passes": passes, "required": required},
            }
        ],
    }


def route_after_choice(state: PipelineState) -> Literal["shout", "skip"]:
    return state["path"]  # type: ignore[return-value]


def route_after_loop(state: PipelineState) -> Literal["polish", "done"]:
    return state["loop_choice"]  # type: ignore[return-value]


def build_graph():
    graph = StateGraph(PipelineState)

    graph.add_node("greet", greet)
    graph.add_node("choose_path", choose_path)
    graph.add_node("shout", shout)
    graph.add_node("skip", skip_shout)
    graph.add_node("polish", polish)
    graph.add_node("check_loop", check_loop)

    graph.add_edge(START, "greet")
    graph.add_edge("greet", "choose_path")

    graph.add_conditional_edges(
        "choose_path",
        route_after_choice,
        {"shout": "shout", "skip": "skip"},
    )

    graph.add_edge("shout", "polish")
    graph.add_edge("skip", "polish")
    graph.add_edge("polish", "check_loop")

    graph.add_conditional_edges(
        "check_loop",
        route_after_loop,
        {"polish": "polish", "done": END},
    )

    return graph.compile()


if __name__ == "__main__":
    app = build_graph()
    for example in EXAMPLES[:2]:
        print(example, "->", app.invoke(example))
    print("\nInspect with: graphviagent serve examples")
