"""Minimal second pipeline so discovery is not dummy-only."""

from typing import TypedDict

from langgraph.graph import END, START, StateGraph

EXAMPLES = [{"text": "hello"}, {"text": "GraphVIAgent"}]


class EchoState(TypedDict):
    text: str
    echoed: str
    length: int


def echo(state: EchoState) -> dict:
    text = state["text"]
    return {
        "echoed": text[::-1],
        "length": len(text),
        "decisions": [
            {
                "step": "echo",
                "choice": "reverse",
                "reason": f"reversed {len(text)} chars",
            }
        ],
    }


def build_graph():
    graph = StateGraph(EchoState)
    graph.add_node("echo", echo)
    graph.add_edge(START, "echo")
    graph.add_edge("echo", END)
    return graph.compile()
