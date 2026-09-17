"""Read project `context` from graphviagent.toml via LangGraph runtime."""

from typing import TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.runtime import Runtime

EXAMPLES = [
    {"label": "greet", "input": {}},
]


class ContextSchema(TypedDict):
    user_name: str


class ContextState(TypedDict, total=False):
    greeting: str


def greet(state: ContextState, runtime: Runtime[ContextSchema]) -> dict:
    ctx = runtime.context
    if isinstance(ctx, dict):
        name = str(ctx.get("user_name") or "friend")
    else:
        name = str(getattr(ctx, "user_name", None) or "friend")
    return {
        "greeting": f"hello {name}",
        "decisions": [
            {
                "step": "greet",
                "choice": name,
                "reason": f"runtime context user_name={name!r}",
            }
        ],
    }


def build_graph():
    graph = StateGraph(ContextState, context_schema=ContextSchema)
    graph.add_node("greet", greet)
    graph.add_edge(START, "greet")
    graph.add_edge("greet", END)
    return graph.compile()


if __name__ == "__main__":
    app = build_graph()
    print(app.invoke({}, context={"user_name": "Ada"}))
    print("\nInspect with: graphviagent serve examples")
