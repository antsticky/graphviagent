from operator import add
from typing import Annotated, Literal, TypedDict

from langgraph.graph import END, START, StateGraph

EXAMPLES = [
    {"topic": "Ada Lovelace"},
    {"topic": "graph tracing"},
]


class AgentState(TypedDict, total=False):
    topic: str
    route: str
    messages: Annotated[list[dict], add]
    decisions: Annotated[list[dict], add]


def intake(state: AgentState) -> dict:
    topic = state["topic"]
    return {
        "messages": [
            {
                "role": "user",
                "type": "human",
                "name": "user",
                "content": f"Please cover {topic}.",
            }
        ]
    }


def supervisor(state: AgentState) -> dict:
    names = [item.get("name") for item in (state.get("messages") or [])]
    topic = state.get("topic") or "the topic"
    if "researcher" not in names:
        route = "researcher"
        content = f"Researcher, gather facts about {topic}."
    elif "writer" not in names:
        route = "writer"
        content = f"Writer, draft a short brief on {topic}."
    else:
        route = "done"
        content = f"Done. {topic} is ready."
    return {
        "route": route,
        "messages": [
            {
                "role": "assistant",
                "type": "ai",
                "name": "supervisor",
                "content": content,
            }
        ],
        "decisions": [
            {
                "step": "supervisor",
                "choice": route,
                "reason": content,
            }
        ],
    }


def researcher(state: AgentState) -> dict:
    topic = state.get("topic") or "the topic"
    return {
        "messages": [
            {
                "role": "assistant",
                "type": "ai",
                "name": "researcher",
                "content": f"Findings on {topic}: two sources, one open question.",
            }
        ]
    }


def writer(state: AgentState) -> dict:
    topic = state.get("topic") or "the topic"
    return {
        "messages": [
            {
                "role": "assistant",
                "type": "ai",
                "name": "writer",
                "content": f"Draft: {topic} in one paragraph.",
            }
        ]
    }


def route_supervisor(state: AgentState) -> Literal["researcher", "writer", "done"]:
    return state["route"]  # type: ignore[return-value]


def build_graph():
    graph = StateGraph(AgentState)
    graph.add_node("intake", intake)
    graph.add_node("supervisor", supervisor)
    graph.add_node("researcher", researcher)
    graph.add_node("writer", writer)
    graph.add_edge(START, "intake")
    graph.add_edge("intake", "supervisor")
    graph.add_conditional_edges(
        "supervisor",
        route_supervisor,
        {"researcher": "researcher", "writer": "writer", "done": END},
    )
    graph.add_edge("researcher", "supervisor")
    graph.add_edge("writer", "supervisor")
    return graph.compile()


if __name__ == "__main__":
    app = build_graph()
    for example in EXAMPLES:
        print(example, "->", app.invoke(example))
    print("\nInspect with: graphviagent serve examples")
