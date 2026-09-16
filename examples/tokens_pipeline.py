"""Dummy billed tokens so Cost has numbers without an LLM."""

import random
from typing import Literal, TypedDict

from langgraph.graph import END, START, StateGraph

EXAMPLES = [
    {"label": "short topic", "input": {"topic": "rain"}},
    {"label": "longer topic", "input": {"topic": "Ada Lovelace"}},
    {"label": "empty topic", "input": {"topic": ""}},
]


class TokenState(TypedDict):
    topic: str
    path: str
    draft: str
    review: str
    usage: dict


def _bill(prompt_lo: int, prompt_hi: int, out_lo: int, out_hi: int) -> dict:
    prompt = random.randint(prompt_lo, prompt_hi)
    completion = random.randint(out_lo, out_hi)
    return {"prompt": prompt, "completion": completion}


def intake(state: TokenState) -> dict:
    topic = str(state.get("topic") or "").strip()
    if not topic:
        raise ValueError("topic is empty")
    return {
        "topic": topic,
        "usage": _bill(40, 120, 8, 40),
        "decisions": [
            {
                "step": "intake",
                "choice": "ok",
                "reason": f"accepted {topic!r}",
            }
        ],
    }


def choose_style(state: TokenState) -> dict:
    topic = state["topic"]
    path = "short_copy" if len(topic) < 8 else "long_copy"
    return {
        "path": path,
        "usage": _bill(80, 180, 12, 60),
        "decisions": [
            {
                "step": "choose_style",
                "choice": path,
                "reason": f"len({topic!r}) = {len(topic)} → {path}",
            }
        ],
    }


def short_copy(state: TokenState) -> dict:
    return {
        "draft": f"A short note on {state['topic']}.",
        "usage": _bill(120, 260, 40, 120),
    }


def long_copy(state: TokenState) -> dict:
    return {
        "draft": f"A longer brief about {state['topic']}, with dummy billed tokens.",
        "usage": _bill(400, 900, 180, 520),
    }


def polish(state: TokenState) -> dict:
    return {
        "review": f"ok: {state['draft']}",
        "usage": _bill(90, 220, 20, 90),
    }


def route_style(state: TokenState) -> Literal["short_copy", "long_copy"]:
    return state["path"]  # type: ignore[return-value]


def build_graph():
    graph = StateGraph(TokenState)
    graph.add_node("intake", intake)
    graph.add_node("choose_style", choose_style)
    graph.add_node("short_copy", short_copy)
    graph.add_node("long_copy", long_copy)
    graph.add_node("polish", polish)
    graph.add_edge(START, "intake")
    graph.add_edge("intake", "choose_style")
    graph.add_conditional_edges(
        "choose_style",
        route_style,
        {"short_copy": "short_copy", "long_copy": "long_copy"},
    )
    graph.add_edge("short_copy", "polish")
    graph.add_edge("long_copy", "polish")
    graph.add_edge("polish", END)
    return graph.compile()


if __name__ == "__main__":
    app = build_graph()
    for example in EXAMPLES[:2]:
        print(example, "->", app.invoke(example["input"]))
    print("\nInspect with: graphviagent serve examples")
