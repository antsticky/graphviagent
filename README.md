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

A push to `prod` builds the package and uploads it to PyPI. PyPI rejects the same version twice, so bump `version` in `pyproject.toml` before you merge.

### One-time PyPI setup

1. Create a [PyPI account](https://pypi.org/account/register/).
2. Open [Trusted publishers](https://pypi.org/manage/account/publishing/).
3. Add a **pending publisher** (the project does not exist on PyPI yet):
   - **PyPI project name:** `graphviagent`
   - **Owner:** `antsticky`
   - **Repository:** `graphviagent`
   - **Workflow:** `publish.yml`
   - **Environment name:** `pypi`
4. On GitHub: **Settings → Environments → New environment** named `pypi`.

Values must match the Action token exactly. Common miss: putting `.github/workflows/publish.yml` in **Workflow** — use only `publish.yml`. Leave nothing blank in **Environment name**.

If the job fails with `invalid-publisher`, the pending publisher is missing or does not match. Fix it, then **Re-run all jobs** on the failed Action. You do not need a new commit.

No API token. The workflow uses OpenID trusted publishing.

### Release

```bash
# on dev, bump version in pyproject.toml, then:
git checkout prod
git merge dev
git push origin prod
```

The [Publish](https://github.com/antsticky/graphviagent/actions) workflow runs `python -m build` and uploads only if that version is not already on PyPI.
