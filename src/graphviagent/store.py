from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


STORE_DIRNAME = ".graphviagent"


def store_root(workspace: Path) -> Path:
    return workspace.resolve() / STORE_DIRNAME / "runs"


def pipeline_dir(workspace: Path, stem: str) -> Path:
    path = store_root(workspace) / stem
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_run(workspace: Path, stem: str, run: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    run = {**run, "pipeline": stem, "created_at": run.get("created_at") or now}
    dest = pipeline_dir(workspace, stem) / f"{run['id']}.json"
    dest.write_text(json.dumps(run, indent=2), encoding="utf-8")
    return run


def list_runs(
    workspace: Path,
    stem: str,
    *,
    include_steps: bool = False,
    limit: int | None = None,
) -> list[dict]:
    folder = store_root(workspace) / stem
    if not folder.is_dir():
        return []
    runs: list[dict] = []
    for path in folder.glob("*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        item = {
            "id": data.get("id") or path.stem,
            "pipeline": data.get("pipeline") or stem,
            "created_at": data.get("created_at"),
            "input": data.get("input"),
            "mode": data.get("mode"),
            "from_step": data.get("from_step"),
            "parent_id": data.get("parent_id"),
            "step_count": len(data.get("steps") or []),
            "elapsed_ms": data.get("elapsed_ms"),
            "status": (
                "error"
                if data.get("error")
                else "paused"
                if data.get("paused")
                else "ok"
            ),
            "paused": bool(data.get("paused")),
            "next": data.get("next") or [],
            "graph_hash": data.get("graph_hash"),
            "file_sha256": data.get("file_sha256"),
        }
        if include_steps:
            item["steps"] = [
                {
                    "node": step.get("node"),
                    "step_id": step.get("step_id"),
                    "elapsed_ms": step.get("elapsed_ms"),
                    "status": "error" if step.get("error") else "ok",
                }
                for step in (data.get("steps") or [])
                if isinstance(step, dict)
            ]
        runs.append(item)
    runs.sort(key=lambda entry: entry.get("created_at") or "", reverse=True)
    if limit is not None:
        return runs[:limit]
    return runs


def load_run(workspace: Path, run_id: str) -> dict | None:
    root = store_root(workspace)
    if not root.is_dir():
        return None
    for path in root.glob(f"*/{run_id}.json"):
        return json.loads(path.read_text(encoding="utf-8"))
    return None


def delete_run(workspace: Path, run_id: str) -> bool:
    root = store_root(workspace)
    if not root.is_dir():
        return False
    for path in root.glob(f"*/{run_id}.json"):
        path.unlink()
        return True
    return False


def list_all_runs(
    workspace: Path,
    *,
    include_steps: bool = False,
    limit: int | None = None,
) -> list[dict]:
    root = store_root(workspace)
    if not root.is_dir():
        return []
    runs: list[dict] = []
    for folder in root.iterdir():
        if folder.is_dir():
            runs.extend(list_runs(workspace, folder.name, include_steps=include_steps))
    runs.sort(key=lambda entry: entry.get("created_at") or "", reverse=True)
    if limit is not None:
        return runs[:limit]
    return runs


def delete_runs(workspace: Path, stem: str) -> int:
    folder = store_root(workspace) / stem
    if not folder.is_dir():
        return 0
    count = 0
    for path in folder.glob("*.json"):
        path.unlink()
        count += 1
    return count


def import_run(workspace: Path, run: dict, *, fallback_stem: str, known_stems: list[str] | None = None) -> dict:
    if not isinstance(run, dict):
        raise ValueError("run must be a JSON object")
    if not run.get("id"):
        raise ValueError("run needs id")
    if not isinstance(run.get("steps"), list):
        raise ValueError("run needs steps")
    clean = {
        key: value
        for key, value in run.items()
        if key not in {"mermaid", "ascii"}
    }
    if load_run(workspace, str(clean["id"])):
        clean["id"] = uuid4().hex
    stem = str(clean.get("pipeline") or fallback_stem or "").strip()
    allowed = set(known_stems or [])
    if allowed and stem not in allowed:
        stem = fallback_stem
    if not stem:
        raise ValueError("run needs a pipeline name")
    clean["pipeline"] = stem
    return save_run(workspace, stem, clean)


def merge_resume_run(previous: dict, current: dict) -> dict:
    merged = dict(current)
    merged["id"] = previous.get("id") or current.get("id")
    merged["input"] = previous.get("input") if previous.get("input") is not None else current.get("input")
    merged["created_at"] = previous.get("created_at") or current.get("created_at")
    merged["pipeline"] = previous.get("pipeline") or current.get("pipeline")
    merged["parent_id"] = previous.get("parent_id") or current.get("parent_id")
    merged["mode"] = previous.get("mode") or current.get("mode")
    merged["from_step"] = previous.get("from_step") or current.get("from_step")
    merged["thread_id"] = (
        current.get("thread_id") or previous.get("thread_id") or merged.get("id")
    )
    prev_steps = [
        dict(step) for step in (previous.get("steps") or []) if isinstance(step, dict)
    ]
    new_steps = [dict(step) for step in (current.get("steps") or []) if isinstance(step, dict)]
    steps = prev_steps + new_steps
    for index, step in enumerate(steps):
        step["index"] = index
    merged["steps"] = steps
    merged["logs"] = list(previous.get("logs") or []) + list(current.get("logs") or [])
    try:
        merged["elapsed_ms"] = round(
            float(previous.get("elapsed_ms") or 0) + float(current.get("elapsed_ms") or 0),
            2,
        )
    except (TypeError, ValueError):
        merged["elapsed_ms"] = current.get("elapsed_ms")
    merged["started_at"] = previous.get("started_at") or current.get("started_at")
    merged["graph"] = current.get("graph") or previous.get("graph")
    merged["graph_hash"] = current.get("graph_hash") or previous.get("graph_hash")
    merged["file_sha256"] = current.get("file_sha256") or previous.get("file_sha256")
    return merged
