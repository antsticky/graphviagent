# GraphVIAgent

[Docs](https://antsticky.github.io/graphviagent/) · [PyPI](https://pypi.org/project/graphviagent/) · [GitHub](https://github.com/antsticky/graphviagent)

Local Graph-View-Agent for LangGraph. Scan `*_pipeline.py` files, record runs under `.graphviagent/`, and inspect them in a browser UI.

Pipeline files do not import GraphVIAgent. Only runs you start from the UI or CLI are stored.

## What's new in 1.5.0-dev

- Header **Statistics** opens the Trace-selected pipeline (`#/statistics/<name>`)
- Statistics filters **status** (success / failed / all, default success), **nodes** (default all), and **last** (10 / 20 / 50 / 100 / all, default 10). Every metric follows that selection

See [CHANGELOG.md](CHANGELOG.md) for 1.4.4.

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

The run is stored under `.graphviagent/` in the current directory. Exit status is 1 if the graph raised or paused (`interrupt`).

## Open the UI

```bash
graphviagent serve .
graphviagent serve . --open
```

`--open` launches the browser. Or open [http://127.0.0.1:8765](http://127.0.0.1:8765) yourself. Leave the terminal running.

```bash
graphviagent serve . --port 9000
graphviagent serve . --host 127.0.0.1
graphviagent serve . --concurrent 5
```

`--concurrent` is how many graphs can run at once (default 3). Live Run, Continue/Step, `POST /api/run`, and Replay / Replay from each take a slot. Extra work waits in a server-side FIFO (SSE heartbeats until a slot opens). `--queue-limit N` caps that wait list (default 4× concurrent); overflow is HTTP 429 `{busy, active, queued, limit, queue_limit}`. A `run_id` that is already queued or running is HTTP 409. Two browser tabs share the same slots — the client’s in-flight list is not the cap. Resume / HITL continues jump ahead of brand-new runs so a paused graph is not stuck behind other live work, but they still occupy a slot when they execute. Closing the tab dequeues a wait without starting the graph, and cancels a live run. Restarting `serve` cancels leftover queued/running stubs from a crashed process. Each in-flight run gets its own LangGraph `thread_id` (and matching `user_id`), so a shared checkpointer cannot mix state. If a node `invoke` / `ainvoke` omits `thread_id`, probes still attach to the bound run, or to the only live run. A disconnect waits for that worker to finish after cancel; it does not free the slot while an LLM call is still running.

`--concurrent` does not cap parallel nodes inside one graph. Optional `--node-concurrency N` sets LangGraph `max_concurrency` for node fan-out; if omitted, a 4th parallel node is not queued behind that cap. `GVA_CONCURRENT`, `GVA_QUEUE_LIMIT`, and `GVA_NODE_CONCURRENCY` override toml; CLI flags override the environment.

The UI only accepts requests from its own origin (`http` or `https`; a TLS reverse proxy on the same host is fine). A page on another site cannot click Run for you. Bind stays on localhost unless you pass `--expose`:

```bash
graphviagent serve . --host 0.0.0.0 --expose
```

`--expose` prints a warning. Anyone who can open that URL can run your pipelines and read stored runs.

New or edited `*_pipeline.py` files (and toml-listed pipelines, including files outside the serve root) refresh the sidebar on their own. You do not need to restart `serve`. The UI poll reads the in-memory snapshot; Watchdog watches the workspace root and each pipeline's parent directory (not `.venv` or other skipped trees). The tree is re-walked when a watched folder sees an event, and at most every 45s if an event was missed — so a pipeline added in a **new** nested folder can take up to 45s to appear. A file-change panel (bottom right) shows when a pipeline appears, is modified, or its SHA256 hash changes. The first snapshot does not hash files. Hashing is skipped when the file size did not change, so a same-size edit is invisible there until you click **Run**, which always re-reads the file.

### Trace

1. Select the pipeline in the sidebar. Clicking it again keeps the open Trace, including a live run.
2. Edit the JSON input (the first `EXAMPLES` item is prefilled).
3. Click **Run**.
4. Single-click a node to select it. Double-click to open **Node view**. Node cards show `file:line`; click to open in the editor. Failed nodes include a traceback. Parallel `Send` visits show that visit’s `state_in` / `state_out`, not the merged superstep.
5. Click the gutter dot on a topology card to break before that node. Breakpoints persist per pipeline. **Continue** resumes with those breakpoints; **Step** runs the pending node(s) — all of them if several are next — and pauses again.
6. Human-in-the-loop graphs (`interrupt(...)`) pause with a resume-value box. **Continue** sends that JSON (`true` to keep a draft, or `"edit this"` to replace it). The box keeps what you type until you Continue or a new pause starts. Editing the pipeline or toml in the same `serve` process keeps the in-memory checkpointer, so Continue / Step still work. Replay asks before using an outdated graph; Continue / Step do not — they feed that checkpoint into the newly compiled graph (right for a whitespace save, wrong if you renamed a node or changed routing). Restarting `serve`, or deleting / unlisting that pipeline file, drops those checkpoints.
7. Filter the run list by status (all / success / paused / canceled / failed) or input text. The list loads 40 runs, then the next page when you scroll to the bottom.
8. **Export** downloads the open run as JSON. **Import** or drop a JSON file on the run list.
9. **Runs** and **Statistics** (between Import and Delete) jump to this pipeline. **Delete run** removes the open run. Hover a JSON box and use **Copy** to copy it. **Cancel** stops an in-flight run — including a Continue / Step still waiting for a slot, or a Replay — or abandons a paused one — stored as **canceled**, not failed. Closing the tab dequeues a wait without canceling a paused graph.

**Replay step** runs only the selected node again. **Replay from** continues the compiled graph from that checkpoint (reducers and routing included). The UI uses `POST /api/rerun/stream` (SSE, same events as `POST /api/run/stream`), so **Cancel** and closing the tab abort them. `POST /api/rerun` still returns one JSON run when the replay finishes — same split as `POST /api/run` vs `POST /api/run/stream`. GraphVIAgent attaches an in-process checkpointer when the pipeline did not. Approximate replay is only when that is impossible. A missing original thread is seeded; a failed fork is an error, not a silent approximate run. Side effects will fire.

Each run stores a SHA256 of the graph (nodes, edges, state keys, tools). If that hash no longer matches the loaded pipeline, the run is marked **outdated** and Replay asks before continuing. Continue / Step skip that prompt and reuse the interned checkpointer.

If a node returns `messages` or tool calls, they render as a thread above the JSON tree.

### Runs

Airflow-style grid: duration bars, then task × run (`✓` / `✕` / `○` / paused amber). Click the name to open Trace. Click a bar, cell, or ▶ to open that run. **Trace** and **Statistics** jump to this pipeline (`#/runs/<name>`). **Clear** deletes every run for that pipeline.

Failed nodes stay in history and show as red.

### Compare

Open **Compare**. Filter by pipeline (default **all**). When a pipeline is selected, Run A and Run B only list that pipeline’s runs. Diff input, path, per-node output, and timing. If a node ran more than once (loop or branch), each visit is aligned separately. Parallel `Send` visits compare that visit’s output, not the merged superstep. Hover a cell and use **Copy**.

### Statistics

Open **Statistics** (`#/statistics/<name>`). Choose a pipeline — there is no **all** view. **Trace** and **Runs** jump to that pipeline. Filter **status** (success, failed, or all; default success), **nodes** (default all), and **last** (10, 20, 50, 100, or all; default 10). Routing, Runtime, and Cost, including the summary strip and per-node numbers, use only that selection. **Routing**, **Runtime**, and **Cost** draw the same topology as Trace. Routing shows run count, average node visits per run, and a success / failed / canceled / paused split of the filtered runs. Runtime writes average duration on each node and a pipeline grand total / average per filtered run (violet average vs orange max on the same strip as Cost / Routing). Cost writes average in/out tokens on each node, a total badge, and a pipeline grand total / average per run. Hover a color strip for a legend of color dots and labels, one per line. Click a node on Routing to see branches grouped together, with similar `reason` text collapsed into a pattern. Click a node on Runtime for min / avg / max from the filtered visits.

## Where runs are stored

Runs are written under the folder you served or ran from:

```text
my_graphs/.graphviagent/runs/echo_pipeline/<id>.json
```

Each file is named with a 32-character hex run id (`GET` / `DELETE /api/runs/{id}` accept only that form, with optional dashes). The UI still emits that length on HTTP / older browsers where `crypto.randomUUID` is missing. Each file includes `schema_version` (currently `1`) and `gva_version`. Older files with no version still load. A run from a newer GraphVIAgent is refused. Add `.graphviagent/` to `.gitignore`.

## Pipeline contract

A file is viewable if it is named `*_pipeline.py` (or listed in `graphviagent.toml`) and exposes, in this order:

- `build_graph()` / `get_graph()` / `create_graph()` / `__graph__`
- compiled or `StateGraph` `GRAPH`
- compiled `app` (`stream`, `invoke`, and `ainvoke` — a package named `app` is ignored)

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
concurrent = 3
# node_concurrency = 8
# queue_limit = 12

[pipeline.decision]
file = "decision_pipeline.py"
```

- `root` and `pythonpath` are resolved relative to the toml file, not the process cwd
- `pythonpath` is inserted on `sys.path` before the pipeline runs; the file's own directory is always added too
- `env_file` loads `KEY=VALUE` lines without overwriting variables already in the environment
- `concurrent` is the max in-flight UI runs (default 3). `GVA_CONCURRENT` and `graphviagent serve --concurrent` override it
- `queue_limit` is how many runs may wait for a slot (default `4 × concurrent`). `GVA_QUEUE_LIMIT` and `--queue-limit` override it. A full queue returns 429 `{busy, …}`
- `node_concurrency` is LangGraph’s per-run node fan-out cap. Omit it (the default) so parallel `Send` nodes are not serialized. `GVA_NODE_CONCURRENCY` and `--node-concurrency` override it
- `context` is LangGraph runtime context (`runtime.context` / `get_runtime()`), not graph state. It is passed on every UI, CLI, HITL resume, and replay run. Empty `{}` is omitted
- If any `[pipeline.*]` tables exist, those files are the pipelines (`file` may be any `.py`, including a path outside the serve root). Optional `factory = "build_graph"`
- If there are no `[pipeline.*]` tables, GraphVIAgent still globs `*_pipeline.py`, skipping Python environments by directory name (`.venv` / `venv`, tox / pixi / direnv caches, `node_modules`, …). Folders whose names look like envs (`env`, `python3.12`, `miniconda3`, `analysis-env`) are probed for `pyvenv.cfg`, `conda-meta`, or `bin`/`Scripts` python plus `lib`/`Lib`. Other names (`src`, `.cursor`) are not probed.

`graphviagent serve /path/to/proj` uses that project's toml even when cwd is elsewhere.

## Examples in this repo

After a clone:

```bash
graphviagent serve examples
```

- `examples/dummy_pipeline.py` — name-length branch and a polish loop
- `examples/parallel_pipeline.py` — parallel `Send` fan-out (wiki / news / archive)
- `examples/tools_pipeline.py` — `ToolNode` tool calls (and a failing tool)
- `examples/agents_pipeline.py` — multi-speaker `messages` thread
- `examples/memory_pipeline.py` — in-process `MemorySaver` / long-term notes
- `examples/tokens_pipeline.py` — dummy random `usage` prompt/completion on each node (Cost view)
- `examples/echo_pipeline.py` — reverse a `text` field
- `examples/context_pipeline.py` — `runtime.context.user_name` from `examples/graphviagent.toml`
- `examples/math_pipeline.py` — add or divide; `{a: 12, b: 0, op: "div"}` raises
- `examples/ratio_pipeline.py` — part / total; `{total: 0}` raises
- `examples/stats_pipeline.py` — mean of a list; `{values: []}` raises
- `examples/grade_pipeline.py` — score / max; `{maximum: 0}` raises
- `examples/convert_pipeline.py` — `{value: 0, unit: "per_unit"}` or `{unit: "kelvin"}` raises
- `examples/traceback_pipeline.py` — nested helpers; `{hits: 3, tries: 0}` raises with a clickable traceback
- `examples/wait_pipeline.py` — three `time.sleep` nodes (`seconds`, default 1)
- `examples/hitl_pipeline.py` — draft then `interrupt`; Continue from the UI with `true` or `"edit this"`
