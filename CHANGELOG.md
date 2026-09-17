# Changelog

## 1.4.2-dev

### Changed
- Trace run list uses the same **success** label (green) as the status filter, instead of **run**
- First UI open shows a **scanning** overlay until the initial pipeline list returns (OneDrive / slow disks)
- `/api/scan` and `/api/changes` return the watcher's snapshot; they no longer re-walk the workspace on every UI poll. Watchdog still schedules refresh on file events

### Fixed
- Continue / Step keep per-node visit ids (`agent#2` after `agent#1`); Replay no longer hits the earlier loop visit
- A second start of the same `run_id` no longer deletes the in-flight stub of the request that won the slot
- `graphviagent run` records HITL / breakpoint pauses as **paused** (exit 1), not a successful truncated run
- **Step** runs every paused `next` node (parallel `Send`), not only the first; other branches are not left on `interrupt_before`
- Token maps keep a billed `0` (`input_tokens: 0` is not replaced by `prompt`)

## 1.4.1

### Changed
- Missing `thread_id` on node `invoke` / `ainvoke` uses the already-bound TLS capture or the only live run (1.4.0 left that uninferred)
- Graph workers are daemon threads. `serve` shutdown joins them for 2s so Ctrl+C does not wait out the current LLM / `sleep`; leftover `running` stubs are canceled on the next `serve`
- File watch still skips hashing when size is unchanged (cloud-sync `mtime` jitter). Same-size edits do not refresh the file-change panel or sidebar graph hash until you click **Run**, which always re-reads the file
- `POST /api/rerun` returns one JSON run when the replay finishes (same as `POST /api/run`). Live Replay in the UI uses `POST /api/rerun/stream` (`text/event-stream`, same events as `/api/run/stream`)
- Interned `MemorySaver`s stay keyed by pipeline path so Continue / Step survive a reload, and are dropped when that file is no longer discovered
- Pipeline contract documents `ainvoke` as well as `stream` / `invoke`. Replay asks before an outdated graph; Continue / Step do not

### Fixed
- Loopback `Host` checks accept `localhost`, `::1`, and `127.0.0.0/8` addresses — not hostnames that merely start with `127.` (`127.attacker.example`)
- File watch schedules pipeline directories added to `graphviagent.toml` after `serve` started, not only those present at `start()`
- Continue / Step after a pipeline file change, toml reload, or a failed import that was then fixed no longer lose in-memory checkpoints
- Cancel on a queued Continue / Step stores **canceled**; closing the tab still dequeues that wait and leaves the paused run alone
- Cancel after a node `invoke` finished no longer records that visit as `canceled: true`; only an in-flight node that raised `RunCancelled` is marked canceled
- Import of a queued/running stub no longer blocks Clear / Delete; in-flight flags are dropped (queued Continue stays paused, other stubs are stored as canceled)
- HITL resume-value box keeps what you type; it only refills when the pause (run / interrupt) changes, not on the 2s control poll
- Clicking the selected pipeline no longer clears Trace; a live SSE run stays on screen and opens when it finishes. A scan miss of that file also keeps the open run
- Replay step / Replay from can be canceled (or aborted by closing the tab) like a live Run
- Node probes still record timing, prints, tokens, and cancel-during-sleep when LangGraph omits `thread_id`. `ainvoke` is patched too
- Node `memory_mb` / peak no longer stop process-wide `tracemalloc` when another node is still running
- Parallel `Send` visits record that node’s own `state_out` (not the merged superstep)
- Replay no longer falls back to `merge_state` when a checkpoint fork fails — that error is stored as a failed replay. Approximate replay is only when the graph has no checkpointer
- Empty-`ends` `Send` branches no longer fan out to every node without an incoming edge
- Runtime node min / avg / max count only successful visits
- `GET` / `DELETE /api/runs/{id}` require a hex run id and look up the file by name
- Same-host `https` Origin / Referer (and `Host` on port 80 / 443) is allowed
- UI run ids stay 32 hex chars when `crypto.randomUUID` is missing
- Trace **Fit** scales the graph to the pane using both width and height
- Runs board cells use step `canceled` / `status: canceled`, so a mid-node cancel is a gray dash instead of a green check

## 1.4.0

### Added
- Run-slot limit is `--concurrent` / `concurrent` in `graphviagent.toml` / `GVA_CONCURRENT` (default 3). Every graph execution takes a slot: live Run, Continue/Step, `POST /api/run`, and Replay / Replay from. Overflow waits in a server-side FIFO (`--queue-limit` / `queue_limit` / `GVA_QUEUE_LIMIT`, default 4× concurrent). Resume / HITL jumps ahead of new runs so a paused run is not stuck behind other live work. LangGraph node fan-out is separate (`--node-concurrency` / `node_concurrency` / `GVA_NODE_CONCURRENCY`; omitted = unlimited). Slots and the queue live on the server (`GET /api/meta`); two browser tabs share them.

### Changed
- Concurrent runs in one process are keyed only by LangGraph `thread_id` (a missing id is not inferred from “the only capture”). `user_id` is set to that same id so a shared `MemorySaver` / store cannot mix long-term memory across runs
- Node probes stay installed for the compiled app (refcount), but each run has its own probe bucket, logs, and cancel via capture lookup
- Process-global stdout/stderr and `time.sleep` patches resolve the current run through TLS / `thread_id`, so queued and running workers do not leak logs or cancel
- `save_run` writes to a temp file and `os.replace`s it; list/load skip unreadable JSON and hidden temp files
- Graph workers are non-daemon and joined after cancel, so disconnecting cannot free a slot while an LLM call is still running
- Closing a tab dequeues a waiting run and still cancels a live one; a paused or finished run is left alone
- Leftover `queued` / `running` stubs from a crashed process are canceled on the next `serve` so they do not look runnable
- SSE queue waits send comment heartbeats (`: ping`) so proxies do not drop the connection

### Fixed
- Cancel shows **Canceling** on the button and pipeline until the run actually stops
- A 4th Run is queued instead of rejected; a full queue returns HTTP 429 `{busy, active, queued, limit, queue_limit}`, not a generic 400
- Closing the tab dequeues a queued wait without starting the graph
- A second start of the same `run_id` is refused (HTTP 409 `{active, run_id}`), not HTTP 400
- Trace graph edges no longer stretch into giant loops after opening Trace from a fresh start / another page (edges were drawn while the pane was `display: none`)
- Trace follows the run you opened: finishing another job does not replace the view; History / Runs clicks keep the live SSE
- Clear / Delete refuse while a pipeline or run is queued or running
- Pipeline glob no longer descends into Python environments (`.venv` / `venv`, `pyvenv.cfg`, conda prefixes, tox / pixi / direnv)
- File watch re-hashes pipeline files only when size changes, so cloud-sync `mtime` jitter does not re-read them

## 1.3.0

### Fixed
- Parallel `Send` nodes keep probe timing, print/log capture, token attribution, and cancellable `time.sleep` when LangGraph runs them on a worker thread; multiple visits of the same node in one superstep are recorded separately
- `graphviagent.toml` `context` is passed into `stream` / replay / CLI runs as LangGraph runtime context (legacy graphs still see it on `config["configurable"]`; empty `{}` is omitted)
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
