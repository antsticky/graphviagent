# Changelog

## 1.0.0-dev

First public alpha. MVP for inspecting and replaying local LangGraph pipelines without LangSmith.

Pipelines stay uninstrumented: GraphVIAgent only loads `*_pipeline.py` files and records runs you start from the UI or CLI. History lives under `.graphviagent/`.

### Added

- Discover `*_pipeline.py` files that expose `build_graph()` / `get_graph()` / `create_graph()`, compiled `GRAPH` / `app`, or `__graph__`
- Optional `EXAMPLES` (first item prefills Trace input) and branch labels from `decisions` / `reason` / `choice`
- `graphviagent` / `graphviagent list` to list pipelines
- `graphviagent run FILE --input '{...}'` to record a run from the CLI
- `graphviagent serve PATH [--host] [--port] [--open]` local UI
- Trace: run a graph, inspect node I/O, replay a step or from a step, filter history, export/import JSON
- Pipelines: Airflow-style grid of duration and task × run status, with per-pipeline clear
- Compare: Run A / Run B diffs for input, path, per-node output, and timing
- Failed runs stay in history (including divide-by-zero example graphs)
- File watch: new or edited `*_pipeline.py` files refresh the sidebar without restarting `serve`

### Known limits

- Alpha: APIs, UI, and on-disk run format may change
- Local only; nothing is sent to LangSmith

## 0.2.1

### Added

- `graphviagent serve . --open` opens the UI in a browser

## 0.2.0

Local debugger features for inspecting, comparing, and sharing LangGraph runs.

### Added

- File watch: new or edited `*_pipeline.py` files refresh the sidebar without restarting `serve`
- CLI: `graphviagent run FILE --input '{...}'` records a run without the browser
- Compare page: Run A / Run B diffs for input, path, per-node output, and timing
- Compare pipeline filter (default all)
- Run history search: all / success / failed, plus node name and input text
- Export and import a run as JSON (button or drop on the run list)
- Clear all runs for a pipeline from the Pipelines card
- Copy on hover for JSON boxes (input, state, node I/O, compare cells)
- Message / tool-call thread when a node returns `messages` or `tool_calls`
- Failed runs stay in history (including divide-by-zero examples)

### Changed

- README is a first-run manual (install, first pipeline, Trace / Pipelines / Compare)
- Input box resizes from the full bottom edge
