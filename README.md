# GraphVIAgent

[Docs](https://antsticky.github.io/graphviagent/) · [PyPI](https://pypi.org/project/graphviagent/) · [GitHub](https://github.com/antsticky/graphviagent)

Local Graph-View-Agent for LangGraph. Scan `*_pipeline.py` files, record runs under `.graphviagent/`, and inspect them in a browser UI.

Pipeline files do not import GraphVIAgent. Only runs you start from the UI or CLI are stored.

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

EXAMPLES = [
    {"label": "hello", "input": {"text": "hello"}},
    {"label": "GraphVIAgent", "input": {"text": "GraphVIAgent"}},
]


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

`EXAMPLES` is optional. The first item prefills the input box. Use `{"label": "hello", "input": {"text": "hello"}}` or a bare dict.

## List pipelines

From the folder that contains the file:

```bash
graphviagent .
graphviagent list .
```

You should see `echo_pipeline.py  ok  examples=2`. If you see `no *_pipeline.py`, you are in the wrong folder or the file name is wrong.

## Record a run from the CLI

```bash
graphviagent run echo_pipeline.py --input '{"text":"hi"}'
```

The run is stored under `.graphviagent/` in the current directory. Exit status is 1 if the graph raised.

## Open the UI

```bash
graphviagent serve .
graphviagent serve . --open
```

`--open` launches the browser. Or open [http://127.0.0.1:8765](http://127.0.0.1:8765) yourself. Leave the terminal running.

```bash
graphviagent serve . --port 9000
graphviagent serve . --host 127.0.0.1
```

New or edited `*_pipeline.py` files refresh the sidebar on their own. You do not need to restart `serve`. A file-change panel (bottom right) shows when a pipeline appears, is modified, or its SHA256 hash changes.

### Trace

1. Select the pipeline in the sidebar.
2. Edit the JSON input (the first `EXAMPLES` item is prefilled).
3. Click **Run**.
4. Single-click a node to select it. Double-click to open **Node view**.
5. Filter the run list by status (all / success / failed), node name, or input text.
6. **Export** downloads the open run as JSON. **Import** or drop a JSON file on the run list.
7. **Delete run** removes the open run. Hover a JSON box and use **Copy** to copy it.

**Replay step** runs only the selected node again. **Replay from** (and **Call node** in Node view) invoke the real node. Side effects will fire.

Each run stores a SHA256 of the graph (nodes, edges, state keys, tools). If that hash no longer matches the loaded pipeline, the run is marked **outdated** and replay asks before continuing.

If a node returns `messages` or tool calls, they render as a thread above the JSON tree.

### Pipelines

Airflow-style grid: duration bars, then task × run (`✓` / `✕` / `○`). Click the name to open Trace. Click a bar, cell, or ▶ to open that run. **Clear** deletes every run for that pipeline.

Failed nodes stay in history and show as red.

### Compare

Open **Compare**. Filter by pipeline (default **all**). When a pipeline is selected, Run A and Run B only list that pipeline’s runs. Diff input, path, per-node output, and timing. Hover a cell and use **Copy**.

## Where runs are stored

Runs are written under the folder you served or ran from:

```text
my_graphs/.graphviagent/runs/echo_pipeline/<id>.json
```

Add `.graphviagent/` to `.gitignore`.

## Pipeline contract

A file is viewable if it is named `*_pipeline.py` and exposes one of:

- `build_graph()` / `get_graph()` / `create_graph()`
- compiled `GRAPH` or `app`
- `__graph__ = "factory_name"`

Optional: `EXAMPLES = [{"label": "hello", "input": {"text": "hello"}}]` (or a bare dict). The first example prefills the Trace input.

If a node update includes `decisions`, `reason`, or `choice`, the UI labels the branch. That is optional.

## Examples in this repo

After a clone:

```bash
graphviagent serve examples
```

- `examples/dummy_pipeline.py` — name-length branch and a polish loop
- `examples/echo_pipeline.py` — reverse a `text` field
- `examples/math_pipeline.py` — add or divide; `{a: 12, b: 0, op: "div"}` raises
- `examples/ratio_pipeline.py` — part / total; `{total: 0}` raises
- `examples/stats_pipeline.py` — mean of a list; `{values: []}` raises
- `examples/grade_pipeline.py` — score / max; `{maximum: 0}` raises
- `examples/convert_pipeline.py` — `{value: 0, unit: "per_unit"}` or `{unit: "kelvin"}` raises
