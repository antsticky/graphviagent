# GraphVIAgent

[Docs](https://antsticky.github.io/graphviagent/) · [PyPI](https://pypi.org/project/graphviagent/) · [GitHub](https://github.com/antsticky/graphviagent)

Local Graph-View-Agent for LangGraph. Scan `*_pipeline.py` files, record runs under `.graphviagent/`, and inspect them in a browser UI.

Pipeline files do not import GraphVIAgent. Only runs you start from the UI or CLI are stored.

## Requirements

- Python 3.11 or newer
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

You should see `echo_pipeline.py  ok  examples=2`. If you see `no pipelines`, you are in the wrong folder or the file name is wrong.

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

The UI only accepts requests from its own origin (a page on another site cannot click Run for you). Bind stays on localhost unless you pass `--expose`:

```bash
graphviagent serve . --host 0.0.0.0 --expose
```

`--expose` prints a warning. Anyone who can open that URL can run your pipelines and read stored runs.

New or edited `*_pipeline.py` files (and toml-listed pipelines, including files outside the serve root) refresh the sidebar on their own. You do not need to restart `serve`. A file-change panel (bottom right) shows when a pipeline appears, is modified, or its SHA256 hash changes.

### Trace

1. Select the pipeline in the sidebar.
2. Edit the JSON input (the first `EXAMPLES` item is prefilled).
3. Click **Run**.
4. Single-click a node to select it. Double-click to open **Node view**. Node cards show `file:line`; click to open in the editor. Failed nodes include a traceback.
5. Click the gutter dot on a topology card to break before that node. Breakpoints persist per pipeline. **Continue** resumes with those breakpoints; **Step** runs the next node and pauses again.
6. Human-in-the-loop graphs (`interrupt(...)`) pause with **Resume**. Send JSON such as `true` to keep a draft, or `"edit this"` to replace it. Continue after a process restart fails — checkpoints are in-memory.
7. Filter the run list by status (all / success / paused / failed) or input text.
8. **Export** downloads the open run as JSON. **Import** or drop a JSON file on the run list.
9. **Delete run** removes the open run. Hover a JSON box and use **Copy** to copy it.

**Replay step** runs only the selected node again. **Replay from** continues the compiled graph from that checkpoint (reducers and routing included). GraphVIAgent attaches an in-process checkpointer when the pipeline did not. Approximate replay is only when that is impossible, or the original thread is gone after a restart. Side effects will fire.

Each run stores a SHA256 of the graph (nodes, edges, state keys, tools). If that hash no longer matches the loaded pipeline, the run is marked **outdated** and replay asks before continuing.

If a node returns `messages` or tool calls, they render as a thread above the JSON tree.

### Runs

Airflow-style grid: duration bars, then task × run (`✓` / `✕` / `○`). Click the name to open Trace. Click a bar, cell, or ▶ to open that run. **Clear** deletes every run for that pipeline.

Failed nodes stay in history and show as red.

### Compare

Open **Compare**. Filter by pipeline (default **all**). When a pipeline is selected, Run A and Run B only list that pipeline’s runs. Diff input, path, per-node output, and timing. If a node ran more than once (loop or branch), each visit is aligned separately. Hover a cell and use **Copy**.

## Where runs are stored

Runs are written under the folder you served or ran from:

```text
my_graphs/.graphviagent/runs/echo_pipeline/<id>.json
```

Each file includes `schema_version` (currently `1`) and `gva_version`. Older files with no version still load. A run from a newer GraphVIAgent is refused. Add `.graphviagent/` to `.gitignore`.

## Pipeline contract

A file is viewable if it is named `*_pipeline.py` (or listed in `graphviagent.toml`) and exposes, in this order:

- `build_graph()` / `get_graph()` / `create_graph()` / `__graph__`
- compiled or `StateGraph` `GRAPH`
- compiled `app` (`stream` and `invoke` only — a package named `app` is ignored)

Optional: `EXAMPLES = [{"label": "hello", "input": {"text": "hello"}}]` (or a bare dict). The first example prefills the Trace input.

If a node update includes `decisions`, `reason`, or `choice`, the UI labels the branch. That is optional.

Do not put `GRAPH` / `app` only under `if __name__ == "__main__":`. The loader imports the file; it does not run that block.

## Project config

Put `graphviagent.toml` at the project root so `from app.graph import build_graph` works even when you launch `graphviagent` from another directory (Desktop, a shortcut, `venv/Scripts`):

```toml
root = "."
pythonpath = ["."]
env_file = ".env"
context = {}

[pipeline.decision]
file = "decision_pipeline.py"
```

- `root` and `pythonpath` are resolved relative to the toml file, not the process cwd
- `pythonpath` is inserted on `sys.path` before the pipeline runs; the file's own directory is always added too
- `env_file` loads `KEY=VALUE` lines without overwriting variables already in the environment
- If any `[pipeline.*]` tables exist, those files are the pipelines (`file` may be any `.py`, including a path outside the serve root). Optional `factory = "build_graph"`
- If there are no `[pipeline.*]` tables, GraphVIAgent still globs `*_pipeline.py`

`graphviagent serve /path/to/proj` uses that project's toml even when cwd is elsewhere.

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
- `examples/traceback_pipeline.py` — nested helpers; `{hits: 3, tries: 0}` raises with a clickable traceback
- `examples/hitl_pipeline.py` — draft then `interrupt`; Resume from the UI with `true` or `"edit this"`
