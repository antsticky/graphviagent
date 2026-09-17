from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from graphviagent import __version__
from graphviagent.config import GVAConfig, activate_config, load_config
from graphviagent.discover import discover_pipelines
from graphviagent.graph_hash import attach_graph_meta
from graphviagent.load import load_pipeline
from graphviagent.record import record_run
from graphviagent.server import serve
from graphviagent.store import run_status_of, save_run


def _workspace(path_arg: str) -> GVAConfig:
    start = Path(path_arg).resolve()
    if not start.exists():
        raise SystemExit(f"path not found: {start}")
    config = load_config(start)
    activate_config(config)
    return config


def _list_pipelines(config: GVAConfig) -> int:
    root = config.root
    found = discover_pipelines(root, config)
    if not found:
        print(f"no pipelines under {root}")
        return 0
    for path in found:
        loaded = load_pipeline(path, config)
        try:
            rel = path.resolve().relative_to(root)
            label = str(rel)
        except ValueError:
            label = loaded.stem
        if loaded.error:
            print(f"{label}  ERROR  {loaded.error}")
        else:
            print(f"{label}  ok  examples={len(loaded.examples)}")
    return 0


def _resolve_pipeline(file: str, config: GVAConfig) -> Path:
    if file in config.pipelines:
        return config.pipelines[file].file
    for spec in config.pipelines.values():
        if spec.file.name == file or spec.file.stem == file:
            return spec.file
    workspace = config.root
    raw = Path(file)
    candidates = [raw, workspace / file]
    stem = raw.name
    if stem.endswith(".py"):
        stem = raw.stem
    if not stem.endswith("_pipeline"):
        candidates.append(workspace / f"{stem}_pipeline.py")
    if not file.endswith(".py"):
        candidates.append(workspace / f"{file}.py")
    for path in candidates:
        if path.is_file():
            return path.resolve()
    raise SystemExit(f"pipeline not found: {file}")


def _cmd_run(file: str, raw_input: str, config: GVAConfig) -> int:
    path = _resolve_pipeline(file, config)
    try:
        payload = json.loads(raw_input or "{}")
    except json.JSONDecodeError as exc:
        raise SystemExit(f"input must be JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit("input must be a JSON object")
    loaded = load_pipeline(path, config)
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
        context=loaded.context,
    )
    saved = save_run(
        config.root,
        loaded.stem,
        attach_graph_meta(run, loaded.graph, loaded.graph_hash, loaded.file_sha256),
    )
    status = run_status_of(saved)
    print(f"{saved['id']}  {status}  {saved.get('elapsed_ms')}ms  {loaded.stem}")
    if status == "error" and saved.get("error"):
        print(saved["error"], file=sys.stderr)
    print(json.dumps(saved.get("result") or {}, indent=2))
    return 0 if status == "ok" else 1


def main(argv: list[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(
        prog="graphviagent",
        description="GraphVIAgent — inspect and replay LangGraph pipelines",
    )
    parser.add_argument("-V", "--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command")

    list_p = sub.add_parser("list", help="list pipelines")
    list_p.add_argument("path", nargs="?", default=".")

    serve_p = sub.add_parser("serve", help="open the local UI")
    serve_p.add_argument("path", nargs="?", default=".")
    serve_p.add_argument("--host", default="127.0.0.1")
    serve_p.add_argument("--port", type=int, default=8765)
    serve_p.add_argument("--open", action="store_true", help="open the UI in a browser")
    serve_p.add_argument(
        "--expose",
        action="store_true",
        help="allow --host beyond localhost (reachable on the network)",
    )

    run_p = sub.add_parser("run", help="record a run without the browser")
    run_p.add_argument("file", help="pipeline file, stem, or graphviagent.toml id")
    run_p.add_argument("--input", default="{}", help="JSON object")

    if not argv or argv[0] not in {"serve", "run", "list", "-h", "--help", "-V", "--version"}:
        args = parser.parse_args(["list", *argv])
    else:
        args = parser.parse_args(argv)

    if args.command == "serve":
        config = _workspace(args.path)
        try:
            serve(
                config.root,
                host=args.host,
                port=args.port,
                open_browser=args.open,
                config=config,
                expose=args.expose,
            )
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
        return
    if args.command == "run":
        start = Path.cwd()
        raw = Path(args.file)
        if raw.is_file():
            start = raw.resolve()
        config = load_config(start)
        activate_config(config)
        raise SystemExit(_cmd_run(args.file, args.input, config))

    config = _workspace(args.path)
    raise SystemExit(_list_pipelines(config))


if __name__ == "__main__":
    main()
