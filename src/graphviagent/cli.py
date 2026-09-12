from __future__ import annotations

import argparse
import sys
from pathlib import Path

from graphviagent.discover import discover_pipelines
from graphviagent.load import load_pipeline
from graphviagent.server import serve


def main(argv: list[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    serve_mode = False
    if argv and argv[0] == "serve":
        serve_mode = True
        argv = argv[1:]

    parser = argparse.ArgumentParser(
        prog="graphviagent",
        description="GraphVIAgent — inspect and replay LangGraph *_pipeline.py files",
    )
    parser.add_argument("path", nargs="?", default=".", help="folder to scan")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args(argv)
    root = Path(args.path).resolve()
    if not root.exists():
        raise SystemExit(f"path not found: {root}")

    if serve_mode:
        serve(root, host=args.host, port=args.port)
        return

    found = discover_pipelines(root)
    if not found:
        print(f"no *_pipeline.py under {root}")
        return
    for path in found:
        loaded = load_pipeline(path)
        rel = path.relative_to(root)
        if loaded.error:
            print(f"{rel}  ERROR  {loaded.error}")
        else:
            print(f"{rel}  ok  examples={len(loaded.examples)}")


if __name__ == "__main__":
    main()
