from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


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
            "status": "error" if data.get("error") else "ok",
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
