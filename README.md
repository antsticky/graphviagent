# GraphVIAgent

Local Graph-View-Agent for LangGraph. Scan `*_pipeline.py` files, record runs under `.graphviagent/`, and inspect them in a browser UI.

Pipeline files do not import GraphVIAgent. Only runs started from the UI or CLI are stored.

## Install

```bash
pip install graphviagent
```

From a clone:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## List pipelines

```bash
graphviagent .
```

## Open the UI

```bash
graphviagent serve .
```

Then open [http://127.0.0.1:8765](http://127.0.0.1:8765).

- **Trace** — run a pipeline, inspect the unrolled graph, replay a node
- **Pipelines** — Airflow-style grid of recent runs

Replay and Call node invoke the node again. Side effects will fire.

## Pipeline contract

A file is viewable if it is named `*_pipeline.py` and exposes one of:

- `build_graph()` / `get_graph()` / `create_graph()`
- compiled `GRAPH` or `app`
- `__graph__ = "factory_name"`

Optional: `EXAMPLES = [{"some": "input"}]` to prefill the form.

If a node update includes `decisions`, `reason`, or `choice`, the UI labels the branch. That is optional.

## Examples

This repo includes sample graphs:

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

Failed runs are saved. The failing node is marked on the graph and shows as a red cell on the Pipelines grid.

## Publish to PyPI

```bash
pip install -e ".[dev]"
python -m build
twine check dist/*
twine upload dist/*
```

Create the GitHub repo at [antsticky/graphviagent](https://github.com/antsticky/graphviagent) before the first upload.
