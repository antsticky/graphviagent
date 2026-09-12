from typing import TypedDict

from langgraph.graph import END, START, StateGraph

EXAMPLES = [
    {"label": "18 / 20", "input": {"score": 18, "maximum": 20}},
    {"label": "max is zero", "input": {"score": 18, "maximum": 0}},
    {"label": "7 / 10", "input": {"score": 7, "maximum": 10}},
]


class GradeState(TypedDict):
    score: float
    maximum: float
    percent: float
    letter: str


def normalize(state: GradeState) -> dict:
    return {"score": float(state["score"]), "maximum": float(state["maximum"])}


def score_percent(state: GradeState) -> dict:
    return {"percent": 100 * state["score"] / state["maximum"]}


def letter(state: GradeState) -> dict:
    percent = state["percent"]
    if percent >= 90:
        grade = "A"
    elif percent >= 80:
        grade = "B"
    elif percent >= 70:
        grade = "C"
    elif percent >= 60:
        grade = "D"
    else:
        grade = "F"
    return {
        "letter": grade,
        "decisions": [
            {
                "step": "letter",
                "choice": grade,
                "reason": f"{percent:.1f}% → {grade}",
            }
        ],
    }


def build_graph():
    graph = StateGraph(GradeState)
    graph.add_node("normalize", normalize)
    graph.add_node("percent", score_percent)
    graph.add_node("letter", letter)
    graph.add_edge(START, "normalize")
    graph.add_edge("normalize", "percent")
    graph.add_edge("percent", "letter")
    graph.add_edge("letter", END)
    return graph.compile()
