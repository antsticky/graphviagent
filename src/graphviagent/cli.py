from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from graphviagent.discover import discover_pipelines
from graphviagent.graph_hash import attach_graph_meta
from graphviagent.load import load_pipeline
from graphviagent.record import record_run
from graphviagent.server import serve
from graphviagent.store import save_run


def _list_pipelines(root: Path) -> int:
    found = discover_pipelines(root)
    if not found:
        print(f"no *_pipeline.py under {root}")
        return 0
    for path in found:
        loaded = load_pipeline(path)
        rel = path.relative_to(root)
        if loaded.error:
            print(f"{rel}  ERROR  {loaded.error}")
        else:
            print(f"{rel}  ok  examples={len(loaded.examples)}")
    return 0


def _resolve_pipeline(file: str, cwd: Path) -> Path:
    raw = Path(file)
    candidates = [raw, cwd / file]
    stem = raw.name
    if stem.endswith(".py"):
        stem = raw.stem
    if not stem.endswith("_pipeline"):
        candidates.append(cwd / f"{stem}_pipeline.py")
    if not file.endswith(".py"):
        candidates.append(cwd / f"{file}.py")
    for path in candidates:
        if path.is_file():
            return path.resolve()
    raise SystemExit(f"pipeline not found: {file}")


def _cmd_run(file: str, raw_input: str, workspace: Path) -> int:
    path = _resolve_pipeline(file, workspace)
    try:
        payload = json.loads(raw_input or "{}")
    except json.JSONDecodeError as exc:
        raise SystemExit(f"input must be JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit("input must be a JSON object")
    loaded = load_pipeline(path)
    if loaded.error:
        print(loaded.error, file=sys.stderr)
        return 1
    if loaded.app is None:
        print("pipeline has no graph", file=sys.stderr)
        return 1
    run = record_run(
        loaded.app,
        payload,
        has_checkpointer=loaded.has_checkpointer,
    )
    saved = save_run(
        workspace,
        loaded.stem,
        attach_graph_meta(run, loaded.graph, loaded.graph_hash, loaded.file_sha256),
    )
    status = "error" if saved.get("error") else "ok"
    print(f"{saved['id']}  {status}  {saved.get('elapsed_ms')}ms  {loaded.stem}")
    if saved.get("error"):
        print(saved["error"], file=sys.stderr)
    print(json.dumps(saved.get("result") or {}, indent=2))
    return 1 if saved.get("error") else 0


def main(argv: list[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(
        prog="graphviagent",
        description="GraphVIAgent — inspect and replay LangGraph *_pipeline.py files",
    )
    sub = parser.add_subparsers(dest="command")

    list_p = sub.add_parser("list", help="list pipelines")
    list_p.add_argument("path", nargs="?", default=".")

    serve_p = sub.add_parser("serve", help="open the local UI")
    serve_p.add_argument("path", nargs="?", default=".")
    serve_p.add_argument("--host", default="127.0.0.1")
    serve_p.add_argument("--port", type=int, default=8765)
    serve_p.add_argument("--open", action="store_true", help="open the UI in a browser")

    run_p = sub.add_parser("run", help="record a run without the browser")
    run_p.add_argument("file", help="pipeline file or stem")
    run_p.add_argument("--input", default="{}", help="JSON object")

    if not argv or argv[0] not in {"serve", "run", "list", "-h", "--help"}:
        args = parser.parse_args(["list", *argv])
    else:
        args = parser.parse_args(argv)

    if args.command == "serve":
        root = Path(args.path).resolve()
        if not root.exists():
            raise SystemExit(f"path not found: {root}")
        serve(root, host=args.host, port=args.port, open_browser=args.open)
        return
    if args.command == "run":
        raise SystemExit(_cmd_run(args.file, args.input, Path.cwd()))

    root = Path(args.path).resolve()
    if not root.exists():
        raise SystemExit(f"path not found: {root}")
    raise SystemExit(_list_pipelines(root))


if __name__ == "__main__":
    main()
