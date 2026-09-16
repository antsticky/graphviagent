"""Draft a note, pause for a human, then finalize."""

from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

EXAMPLES = [
    {"label": "happy draft", "input": {"topic": "weekend picnic"}},
]


class HitlState(TypedDict, total=False):
    topic: str
    draft: str
    decision: Any
    output: str


def draft(state: HitlState) -> dict:
    topic = state.get("topic") or "untitled"
    text = f"Let's plan a {topic}."
    decision = interrupt({"prompt": "Approve or edit this draft", "draft": text})
    return {"draft": text, "decision": decision}


def finalize(state: HitlState) -> dict:
    draft_text = state.get("draft") or ""
    decision = state.get("decision")
    if decision is True or decision == "true":
        output = draft_text
    elif isinstance(decision, str) and decision.strip():
        output = decision.strip()
    else:
        output = draft_text
    return {"output": output}


def build_graph():
    graph = StateGraph(HitlState)
    graph.add_node("draft", draft)
    graph.add_node("finalize", finalize)
    graph.add_edge(START, "draft")
    graph.add_edge("draft", "finalize")
    graph.add_edge("finalize", END)
    return graph.compile()
