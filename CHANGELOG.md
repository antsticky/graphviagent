# Changelog

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
