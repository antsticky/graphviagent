# GraphVIAgent

Local Graph-View-Agent for LangGraph. Scan `*_pipeline.py` files, record runs under `.graphviagent/`, and inspect them in a browser UI.

Pipeline files do not import GraphVIAgent. Nothing is sent to LangSmith. Only runs you start from the UI are stored.

## Requirements

- Python 3.10 or newer
- A folder of LangGraph files named `*_pipeline.py`

## Install

New folder:

```bash
mkdir my_graphs
cd my_graphs
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install graphviagent
```

From a clone of this repo:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## First pipeline

Installing the package does not copy example files into your project. Add one yourself. The file name must end with `_pipeline.py`.

Save this as `echo_pipeline.py`:

```python
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
        "reason": f"reversed {len(text)} chars",
    }


def build_graph():
    graph = StateGraph(EchoState)
    graph.add_node("echo", echo)
    graph.add_edge(START, "echo")
    graph.add_edge("echo", END)
    return graph.compile()
```

`EXAMPLES` is optional. It prefills the input box in the UI.

## List pipelines

From the folder that contains the file:

```bash
graphviagent .
```

You should see `echo_pipeline.py  ok  examples=2`. If you see `no *_pipeline.py`, you are in the wrong folder or the file name is wrong.

## Open the UI

```bash
graphviagent serve .
```

Then open [http://127.0.0.1:8765](http://127.0.0.1:8765). Leave the terminal running. Another port: `graphviagent serve . --port 9000`.

Restart `serve` after you add a new `*_pipeline.py` file.

### Trace

1. Select the pipeline in the sidebar.
2. Edit the JSON input, or keep an example.
3. Click **Run**.
4. Single-click a node to select it. Double-click to open **Node view** (input and the keys that node returned).

**Replay step** runs only the selected node again. **Replay from** runs that node and every recorded step after it. Both invoke the real node. Side effects will fire.

### Pipelines

Airflow-style grid: duration bars, then task × run (`✓` / `✕` / `○`). Click the name to open Trace. Click a bar, cell, or ▶ to open that run.

Failed nodes stay in history and show as red.

## Where runs are stored

Runs are written under the folder you served:

```text
my_graphs/.graphviagent/runs/echo_pipeline/<id>.json
```

Add `.graphviagent/` to `.gitignore`.

## Pipeline contract

A file is viewable if it is named `*_pipeline.py` and exposes one of:

- `build_graph()` / `get_graph()` / `create_graph()`
- compiled `GRAPH` or `app`
- `__graph__ = "factory_name"`

Optional: `EXAMPLES = [{"some": "input"}]`.

If a node update includes `decisions`, `reason`, or `choice`, the UI labels the branch. That is optional.

## Examples in this repo

After a clone:

```bash
graphviagent serve examples
```

- `examples/dummy_pipeline.py` — name-length branch and a polish loop
- `examples/echo_pipeline.py` — reverse a `text` field
- `examples/math_pipeline.py` — add or divide; `{a: 12, b: 0, op: "div"}` raises
- `examples/ratio_pipeline.py` — part / total; `total: 0` raises
- `examples/stats_pipeline.py` — mean of a list; empty `values` raises
- `examples/grade_pipeline.py` — score / max; `maximum: 0` raises
- `examples/convert_pipeline.py` — unit conversion; `per_unit` with `value: 0` or an unknown unit raises
