# Changelog

## 1.2.1-dev

### Fixed
- Parallel `Send` nodes keep probe timing, print/log capture, token attribution, and cancellable `time.sleep` when LangGraph runs them on a worker thread; multiple visits of the same node in one superstep are recorded separately
- Routing color strip includes paused (amber) so it matches the grand total; caption and legend say **success**, not ok
- Runtime average / max / grand total use successful runs only (canceled, failed, and paused are listed as “other”)
- Runtime strip is the same single line as Cost / Routing, with average vs max (not max − avg)
- A canceled run no longer records the last visited node as `END`; Routing shows a **canceled** outcome instead
- Cancel mid-node writes that node as a timed step (`canceled: true`), so Runtime node times include the interrupted visit
- `_is_cancelled` matches `RunCancelled` only, not any exception whose message is `"cancelled"`
- Runs-board bar hover names canceled / failed / paused instead of a generic `run`
- Runs board duration bars (and waiting cells) show paused in amber

## 1.2.0

### Added
- Statistics page (`#/statistics`, `#/statistics/<pipeline>`): Routing, Runtime, and Cost on the same topology as Trace
  - Routing: run count, average node visits per run, success / failed / canceled split; click a node for grouped branch `reason`s
  - Runtime: wall-clock grand total and average per run; the strip is average vs longest run; click a node for min / avg / max
  - Cost: average in/out tokens per node, a pipeline grand total, and average per run (tokens, not dollars)
  - Hover a color strip for a legend of color dots and labels (one per line)
- `GET /api/stats` (optional `file=` filter). Trace, Runs, and Compare stay the same; run `status` may be `canceled`
- In-page jumps Trace ↔ Runs ↔ Statistics for the open pipeline (header menu colors stay the same)
- `graphviagent --version` / `-V`
- Runs deep-links `#/runs/<pipeline>` scroll to that pipeline card
- Origin / `Host` check on the local UI so other websites cannot trigger runs; `--host 0.0.0.0` requires `--expose` and prints a warning
- `schema_version` (currently `1`) and `gva_version` on saved runs; older files still load, newer schemas are refused
- `examples/tokens_pipeline.py` — dummy random token usage on each node so Cost has numbers without an LLM
- `examples/wait_pipeline.py` — three `time.sleep` nodes so Cancel / Continue / Step have time to catch the run

### Fixed
- `load_run` returns `None` for truncated or invalid JSON instead of raising (open/compare 404, not 500)
- Failed pipeline imports are not kept in the serve cache, so a fixed dependency reloads without a file-hash change
- Cancel after Resume matches both the run id and the LangGraph `thread_id`
- Delete, list, and import resolve toml pipeline ids (including files outside the serve root) instead of `Path.stem`
- File watch covers toml-listed pipelines whose files live outside the workspace
- Compare aligns every visit of a node (loops and branches) instead of only the first
- Cancel stops only the open run; concurrent runs of the same pipeline no longer share one cancel flag
- Cancel interrupts in-run `time.sleep` within ~50ms instead of waiting for the full nap

### Changed
- Rename the Pipelines page to **Runs** (`#/runs`; `#/pipelines` still opens it)
- Duration and memory labels scale (µs/ms/s, B/KB/MB/GB); Gantt hover Start/End use clock time
- Delete run and Clear use the same size as other toolbar buttons, in red
- Trace toolbar keeps Cancel / Continue / Step visible; they disable unless a run is in flight or paused. HITL `interrupt(...)` uses Continue with the resume-value panel
- Cancel writes `canceled: true` (status `canceled`) instead of `error: cancelled`, including the history filter. Older files with `error: cancelled` still count as canceled
- In-flight runs appear in the left Runs list (and a live mark on the pipeline)

## 1.1.3-dev

### Added
- `graphviagent.toml` for project `root`, `pythonpath`, `env_file`, `context`, and explicit `[pipeline.*]` entries
- LLM usage callbacks (`on_llm_end` / `on_chat_model_end`) so token counts follow billed `usage_metadata`, including replay and `with_structured_output`
- Source mapping: node `file:line` from `inspect`, clickable `vscode://file` links, and traceback frames on failed steps
- `examples/traceback_pipeline.py` — nested helpers; `{hits: 3, tries: 0}` raises so you can open the stack
- Live control: topology gutter breakpoints (`interrupt_before`), **Step** / **Continue**, and HITL **Resume** via `Command(resume=…)`
- `examples/hitl_pipeline.py` — draft node calls `interrupt`; Resume from the UI with `true` or `"edit this"`

### Changed
- Require Python 3.11+ (`tomllib`)
- Load `*_pipeline.py` with the file's directory (and toml `pythonpath`) on `sys.path`, independent of process cwd
- Graph extract order is `build_graph()` / `get_graph()` / `create_graph()` / `__graph__`, then `GRAPH`, then a compiled `app` (`stream` + `invoke` only)
- Failed or empty modules are not cached as a loaded pipeline
- Steps with no observed token usage set `tokens.unavailable` instead of a silent 0
- Replay uses LangGraph checkpoints (`get_state` / `update_state` / fork thread). `merge_state` is only the labeled approximate fallback
- Load attaches an in-process `MemorySaver` when a compiled graph has no checkpointer (`builder.compile(...)`), so Replay from can follow routing without changing `*_pipeline.py`
- Replay from seeds a checkpoint when the original thread is gone (restart / reload), instead of walking the recorded path

## 1.1.0-dev

### Added
- Live Run over SSE: topology pulses, Steps and Final state update, Log panel
- Topology graph (taken / skipped / loops / parallel)
- `print()` and `logging` capture with level colors
- Cancel and at most 3 concurrent runs
- GitHub Pages user manual (`docs/`)

### Changed
- Graph view is a topology map instead of an unrolled column

## 1.0.1-dev

### Added

- Node inspect shows a state diff of `state_in` vs `state_out` (changed, added, removed, unchanged)

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
