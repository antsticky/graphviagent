import time
from typing import Annotated, TypedDict

from langchain_core.messages import AIMessage
from langchain_core.tools import tool
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

EXAMPLES = [
    {"topic": "paris"},
    {"label": "tool error", "input": {"topic": "maps", "fail": True}},
]


class ToolState(TypedDict, total=False):
    topic: str
    fail: bool
    messages: Annotated[list, add_messages]


@tool
def search(q: str) -> str:
    """Search for a topic."""
    time.sleep(0.03)
    return f"results for {q}"


@tool
def lookup(key: str) -> str:
    """Look up a stored value."""
    time.sleep(0.02)
    if key == "CRASH":
        raise RuntimeError("lookup exploded")
    return f"value[{key}]"


def planner(state: ToolState) -> dict:
    topic = state["topic"]
    lookup_key = "CRASH" if state.get("fail") else topic
    return {
        "messages": [
            AIMessage(
                content="Need search and lookup.",
                name="planner",
                tool_calls=[
                    {"id": "c1", "name": "search", "args": {"q": topic}},
                    {"id": "c2", "name": "lookup", "args": {"key": lookup_key}},
                ],
            )
        ]
    }


def build_graph():
    graph = StateGraph(ToolState)
    graph.add_node("planner", planner)
    graph.add_node("tools", ToolNode([search, lookup], handle_tool_errors=True))
    graph.add_edge(START, "planner")
    graph.add_edge("planner", "tools")
    graph.add_edge("tools", END)
    return graph.compile()


if __name__ == "__main__":
    app = build_graph()
    for example in EXAMPLES:
        payload = example["input"] if "input" in example else example
        print(payload, "->", app.invoke(payload))
    print("\nInspect with: graphviagent serve examples")
