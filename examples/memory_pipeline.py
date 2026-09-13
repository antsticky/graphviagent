from operator import add
from typing import Annotated, Literal, TypedDict

from langgraph.graph import END, START, StateGraph

EXAMPLES = [
    {"name": "Ada"},
    {"name": "Bo"},
]


class MemoryState(TypedDict, total=False):
    name: str
    profile: str
    notes: str
    scratch: str
    answer: str
    passes: int
    loop_choice: str
    decisions: Annotated[list[dict], add]


def remember(state: MemoryState) -> dict:
    name = state["name"]
    return {
        "profile": f"{name} prefers short answers",
        "notes": f"first note about {name}",
        "scratch": f"tmp-{name}",
        "passes": 0,
    }


def recall(state: MemoryState) -> dict:
    notes = state.get("notes") or ""
    profile = state.get("profile") or ""
    return {
        "answer": f"using notes ({len(notes)} chars) and profile ({len(profile)} chars)",
        "passes": int(state.get("passes") or 0) + 1,
    }


def jot(state: MemoryState) -> dict:
    passes = state.get("passes") or 0
    return {"notes": f"{state.get('notes') or ''} | pass {passes}"}


def forget(state: MemoryState) -> dict:
    return {"scratch": None}


def check(state: MemoryState) -> dict:
    passes = int(state.get("passes") or 0)
    required = 2
    choice = "jot" if passes < required else "forget"
    reason = (
        f"passes = {passes} < {required} → jot again"
        if choice == "jot"
        else f"passes = {passes} >= {required} → forget scratch"
    )
    return {
        "loop_choice": choice,
        "decisions": [
            {
                "step": "check",
                "choice": choice,
                "reason": reason,
                "facts": {"passes": passes, "required": required},
            }
        ],
    }


def route_after_check(state: MemoryState) -> Literal["jot", "forget"]:
    return state["loop_choice"]  # type: ignore[return-value]


def build_graph():
    graph = StateGraph(MemoryState)
    graph.add_node("remember", remember)
    graph.add_node("recall", recall)
    graph.add_node("jot", jot)
    graph.add_node("check", check)
    graph.add_node("forget", forget)
    graph.add_edge(START, "remember")
    graph.add_edge("remember", "recall")
    graph.add_edge("recall", "check")
    graph.add_conditional_edges(
        "check",
        route_after_check,
        {"jot": "jot", "forget": "forget"},
    )
    graph.add_edge("jot", "recall")
    graph.add_edge("forget", END)
    return graph


if __name__ == "__main__":
    from langgraph.checkpoint.memory import MemorySaver

    app = build_graph().compile(checkpointer=MemorySaver())
    for example in EXAMPLES:
        print(example, "->", app.invoke(example, {"configurable": {"thread_id": example["name"]}}))
    print("\nInspect with: graphviagent serve examples")
