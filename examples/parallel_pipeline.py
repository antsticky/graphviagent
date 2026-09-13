import time
from operator import add
from typing import Annotated, Literal, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

EXAMPLES = [
    {"label": "2 sources (short)", "input": {"query": "Ada"}},
    {"label": "3 sources (long)", "input": {"query": "parallel graph tracing demo"}},
]

FETCH_MS = {"wiki": 800, "news": 1400, "archive": 1100}
REFINE_MS = 400


class ParallelState(TypedDict, total=False):
    query: str
    source: str
    sources: list[str]
    partials: Annotated[list[dict], add]
    merged: str
    passes: int
    route: str
    report: str
    decisions: Annotated[list[dict], add]


def intake(state: ParallelState) -> dict:
    query = str(state.get("query") or "").strip() or "Ada"
    print(f"intake {query}")
    return {"query": query, "passes": 0, "merged": "", "report": ""}


def choose_sources(state: ParallelState) -> dict:
    query = state["query"]
    sources = ["wiki", "news"] if len(query) < 12 else ["wiki", "news", "archive"]
    reason = f"len({query!r}) = {len(query)} → {', '.join(sources)}"
    print(reason)
    return {
        "sources": sources,
        "decisions": [
            {
                "step": "choose_sources",
                "choice": f"{len(sources)} sources",
                "reason": reason,
                "facts": {"query": query, "query_length": len(query), "sources": sources},
            }
        ],
    }


def dispatch(state: ParallelState) -> list[Send]:
    query = state["query"]
    return [Send("fetch", {"query": query, "source": source}) for source in state["sources"]]


def fetch(state: ParallelState) -> dict:
    source = state["source"]
    query = state["query"]
    delay = FETCH_MS.get(source, 800) / 1000
    print(f"fetch {source} for {query}")
    time.sleep(delay)
    return {
        "partials": [
            {
                "source": source,
                "snippet": f"{source} notes on {query}",
                "elapsed_ms": FETCH_MS.get(source, 800),
            }
        ]
    }


def merge(state: ParallelState) -> dict:
    bits = [
        f"{item.get('source')}: {item.get('snippet')}"
        for item in (state.get("partials") or [])
        if isinstance(item, dict)
    ]
    text = " | ".join(bits) or (state.get("merged") or "")
    passes = state.get("passes") or 0
    if passes:
        text = f"{text} (pass {passes})"
    print(f"merge {len(bits)} partials")
    return {"merged": text}


def check_quality(state: ParallelState) -> dict:
    passes = state.get("passes") or 0
    required = 2
    if passes < required:
        choice = "refine"
        reason = f"passes = {passes} < {required} → refine"
    else:
        choice = "done"
        reason = f"passes = {passes} >= {required} → finalize"
    print(reason)
    return {
        "route": choice,
        "decisions": [
            {
                "step": "check_quality",
                "choice": choice,
                "reason": reason,
                "facts": {"passes": passes, "required": required},
            }
        ],
    }


def refine(state: ParallelState) -> dict:
    time.sleep(REFINE_MS / 1000)
    passes = (state.get("passes") or 0) + 1
    print(f"refine pass {passes}")
    return {"passes": passes}


def finalize(state: ParallelState) -> dict:
    report = f"Report: {state.get('merged') or ''}"
    print("finalize")
    return {"report": report}


def route_quality(state: ParallelState) -> Literal["refine", "done"]:
    return state["route"]  # type: ignore[return-value]


def build_graph():
    graph = StateGraph(ParallelState)
    graph.add_node("intake", intake)
    graph.add_node("choose_sources", choose_sources)
    graph.add_node("fetch", fetch)
    graph.add_node("merge", merge)
    graph.add_node("check_quality", check_quality)
    graph.add_node("refine", refine)
    graph.add_node("finalize", finalize)

    graph.add_edge(START, "intake")
    graph.add_edge("intake", "choose_sources")
    graph.add_conditional_edges("choose_sources", dispatch)
    graph.add_edge("fetch", "merge")
    graph.add_edge("merge", "check_quality")
    graph.add_conditional_edges(
        "check_quality",
        route_quality,
        {"refine": "refine", "done": "finalize"},
    )
    graph.add_edge("refine", "merge")
    graph.add_edge("finalize", END)
    return graph.compile()


if __name__ == "__main__":
    app = build_graph()
    print(app.invoke({"query": "Ada"}))
