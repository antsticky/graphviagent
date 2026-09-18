from __future__ import annotations

import json
import os
import re
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


STORE_DIRNAME = ".graphviagent"
SCHEMA_VERSION = 1
_CANCELED_ERRORS = {"cancelled", "canceled"}
_RUN_ID_CHARS = frozenset("0123456789abcdefABCDEF")


def store_root(workspace: Path) -> Path:
    return workspace.resolve() / STORE_DIRNAME / "runs"


def pipeline_dir(workspace: Path, stem: str) -> Path:
    path = store_root(workspace) / stem
    path.mkdir(parents=True, exist_ok=True)
    return path


def normalize_run_id(value: object) -> str | None:
    if value is None or value == "":
        return None
    text = str(value).replace("-", "").strip()
    if not text or len(text) > 64 or any(ch not in _RUN_ID_CHARS for ch in text):
        raise ValueError("run_id must be a hex id")
    return text.lower()


def _run_json_paths(workspace: Path, run_id: object) -> list[Path]:
    try:
        rid = normalize_run_id(run_id)
    except ValueError:
        return []
    if not rid:
        return []
    root = store_root(workspace)
    if not root.is_dir():
        return []
    found: list[Path] = []
    for folder in root.iterdir():
        if not folder.is_dir():
            continue
        path = folder / f"{rid}.json"
        if path.is_file():
            found.append(path)
    return found


def _gva_version() -> str:
    try:
        from graphviagent import __version__

        return str(__version__)
    except Exception:
        return "unknown"


def run_schema_version(run: dict) -> int:
    raw = run.get("schema_version")
    if raw is None:
        return 0
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("run schema_version must be an integer") from exc


def run_is_canceled(data: dict | None) -> bool:
    if not isinstance(data, dict):
        return False
    if data.get("canceled"):
        return True
    err = str(data.get("error") or "").strip().lower()
    return err in _CANCELED_ERRORS


def run_is_busy(data: dict | None) -> bool:
    if not isinstance(data, dict):
        return False
    if data.get("queued") or data.get("running"):
        return True
    return run_status_of(data) in {"queued", "running"}


def run_status_of(data: dict | None) -> str:
    if run_is_canceled(data):
        return "canceled"
    if isinstance(data, dict) and data.get("error"):
        return "error"
    if isinstance(data, dict) and data.get("queued"):
        return "queued"
    if isinstance(data, dict) and data.get("running"):
        return "running"
    if isinstance(data, dict) and data.get("paused"):
        return "paused"
    return "ok"


def step_is_canceled(step: dict | None) -> bool:
    if not isinstance(step, dict):
        return False
    if step.get("canceled"):
        return True
    status = str(step.get("status") or "").strip().lower()
    if status in _CANCELED_ERRORS:
        return True
    err = str(step.get("error") or "").strip().lower()
    return err in _CANCELED_ERRORS


def step_status_of(step: dict | None) -> str:
    if step_is_canceled(step):
        return "canceled"
    if isinstance(step, dict) and step.get("error"):
        return "error"
    return "ok"


def migrate_run(run: dict) -> dict:
    if not isinstance(run, dict):
        raise ValueError("run must be a JSON object")
    version = run_schema_version(run)
    if version > SCHEMA_VERSION:
        raise ValueError(
            f"run schema_version {version} is newer than this GraphVIAgent "
            f"(supports {SCHEMA_VERSION})"
        )
    out = dict(run)
    out["schema_version"] = SCHEMA_VERSION
    if not out.get("gva_version"):
        out["gva_version"] = _gva_version()
    if run_is_canceled(out):
        out["canceled"] = True
        err = str(out.get("error") or "").strip().lower()
        if err in _CANCELED_ERRORS:
            out["error"] = None
    return out


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.stem}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def _read_run_file(path: Path, *, ignore_newer: bool = True) -> dict | None:
    if path.name.startswith("."):
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    try:
        return migrate_run(data)
    except ValueError:
        if ignore_newer:
            return None
        raise


def save_run(workspace: Path, stem: str, run: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    run = migrate_run({**run, "pipeline": stem, "created_at": run.get("created_at") or now})
    run["gva_version"] = _gva_version()
    dest = pipeline_dir(workspace, stem) / f"{run['id']}.json"
    _write_text_atomic(dest, json.dumps(run, indent=2))
    return run


def _append_run_log(run: dict, text: str, *, t: object = 0) -> None:
    logs = [dict(item) for item in (run.get("logs") or []) if isinstance(item, dict)]
    logs.append({"t": t, "src": "run", "text": text, "level": "info"})
    run["logs"] = logs


def _trace_run(run_id: object, message: str) -> None:
    print(f"[graphviagent] {run_id}  {message}", flush=True)


def save_queued_stub(
    workspace: Path,
    stem: str,
    *,
    run_id: str,
    user_input: dict | None,
    previous: dict | None = None,
    graph_hash: str | None = None,
    file_sha256: str | None = None,
    mode: str | None = None,
    parent_id: str | None = None,
    from_step: str | None = None,
) -> dict:
    if previous:
        run = dict(previous)
    else:
        run = {
            "id": run_id,
            "input": user_input or {},
            "steps": [],
            "result": {},
            "canceled": False,
            "paused": False,
            "error": None,
        }
    run["id"] = run_id
    run["queued"] = True
    run["running"] = False
    run["pipeline"] = stem
    if graph_hash:
        run["graph_hash"] = graph_hash
    if file_sha256:
        run["file_sha256"] = file_sha256
    if mode:
        run["mode"] = mode
    if parent_id:
        run["parent_id"] = parent_id
    if from_step:
        run["from_step"] = from_step
    _append_run_log(run, "queued")
    saved = save_run(workspace, stem, run)
    _trace_run(run_id, "queued")
    return saved


def mark_run_started(workspace: Path, stem: str, run_id: str, *, wait_ms: float) -> dict | None:
    run = load_run(workspace, run_id)
    if run is None:
        return None
    run["queued"] = False
    run["running"] = True
    run["wait_ms"] = wait_ms
    _append_run_log(run, f"running  wait_ms={wait_ms}", t=wait_ms)
    saved = save_run(workspace, stem, run)
    _trace_run(run_id, f"running  wait_ms={wait_ms}")
    return saved


def attach_run_timing(run: dict, *, wait_ms: float) -> dict:
    out = dict(run)
    out["queued"] = False
    out["running"] = False
    out["wait_ms"] = wait_ms
    out["run_ms"] = out.get("elapsed_ms") or 0
    status = (
        "canceled"
        if run_is_canceled(out)
        else "paused"
        if out.get("paused")
        else "error"
        if out.get("error")
        else "done"
    )
    _append_run_log(
        out,
        f"{status}  wait_ms={wait_ms}  run_ms={out['run_ms']}",
        t=out.get("elapsed_ms") or wait_ms,
    )
    _trace_run(out.get("id"), f"{status}  wait_ms={wait_ms}  run_ms={out['run_ms']}")
    return out


def cancel_queue_stub(
    workspace: Path,
    stem: str,
    run_id: str,
    *,
    wait_ms: float,
    resume: bool,
) -> dict | None:
    run = load_run(workspace, run_id)
    if run is None:
        return None
    run["queued"] = False
    run["running"] = False
    run["wait_ms"] = wait_ms
    run["run_ms"] = 0
    # resume=True: tab close / serve stop — put a queued Continue back to paused.
    # resume=False: Cancel button — store canceled even if this was a Continue/Step.
    if resume:
        run["paused"] = True
        run["canceled"] = False
        _append_run_log(run, f"left queue  wait_ms={wait_ms}", t=wait_ms)
        saved = save_run(workspace, stem, run)
        _trace_run(run_id, f"left queue  wait_ms={wait_ms}")
        return saved
    run["canceled"] = True
    run["paused"] = False
    run["error"] = None
    run["next"] = []
    run["interrupts"] = []
    _append_run_log(run, f"canceled  wait_ms={wait_ms}  run_ms=0", t=wait_ms)
    saved = save_run(workspace, stem, run)
    _trace_run(run_id, f"canceled  wait_ms={wait_ms}  run_ms=0")
    return saved


def abandon_busy_runs(workspace: Path) -> int:
    stale = [
        data
        for data in _iter_run_dicts(workspace)
        if data.get("queued") or data.get("running")
    ]
    count = 0
    for data in stale:
        stem = str(data.get("pipeline") or "")
        run_id = str(data.get("id") or "")
        if not stem or not run_id:
            continue
        try:
            wait_ms = float(data.get("wait_ms") or 0)
        except (TypeError, ValueError):
            wait_ms = 0.0
        running = bool(data.get("running"))
        resume = (not running) and bool(data.get("paused"))
        if cancel_queue_stub(
            workspace,
            stem,
            run_id,
            wait_ms=wait_ms,
            resume=resume,
        ):
            count += 1
    return count


def cancel_paused_run(workspace: Path, run_id: str) -> dict | None:
    run = load_run(workspace, run_id)
    if run is None or run_is_canceled(run) or run.get("error") or not run.get("paused"):
        return None
    stem = run.get("pipeline")
    if not stem:
        return None
    logs = [dict(item) for item in (run.get("logs") or []) if isinstance(item, dict)]
    logs.append({
        "t": run.get("elapsed_ms") or 0,
        "src": "run",
        "text": "run canceled",
        "level": "info",
    })
    run["canceled"] = True
    run["error"] = None
    run["paused"] = False
    run["next"] = []
    run["interrupts"] = []
    run["logs"] = logs
    return save_run(workspace, str(stem), run)


def list_runs(
    workspace: Path,
    stem: str,
    *,
    include_steps: bool = False,
    limit: int | None = None,
    offset: int = 0,
) -> list[dict]:
    folder = store_root(workspace) / stem
    if not folder.is_dir():
        return []
    runs: list[dict] = []
    for path in folder.glob("*.json"):
        data = _read_run_file(path)
        if data is None:
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
            "status": run_status_of(data),
            "paused": bool(data.get("paused")) and not run_is_canceled(data),
            "canceled": run_is_canceled(data),
            "queued": bool(data.get("queued")),
            "running": bool(data.get("running")),
            "approximate": bool(data.get("approximate")),
            "approximate_reason": data.get("approximate_reason"),
            "wait_ms": data.get("wait_ms"),
            "run_ms": data.get("run_ms"),
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
                    "status": step_status_of(step),
                    "canceled": step_is_canceled(step),
                }
                for step in (data.get("steps") or [])
                if isinstance(step, dict)
            ]
        runs.append(item)
    runs.sort(key=lambda entry: entry.get("created_at") or "", reverse=True)
    start = max(0, offset)
    if limit is None:
        return runs[start:]
    return runs[start : start + max(0, limit)]


def pipeline_has_busy_runs(workspace: Path, stem: str) -> bool:
    for item in list_runs(workspace, stem):
        if item.get("queued") or item.get("running") or item.get("status") in {"queued", "running"}:
            return True
    return False


def load_run(workspace: Path, run_id: str) -> dict | None:
    for path in _run_json_paths(workspace, run_id):
        data = _read_run_file(path, ignore_newer=False)
        if data is not None:
            return data
    return None


def delete_run(workspace: Path, run_id: str) -> bool:
    paths = _run_json_paths(workspace, run_id)
    if not paths:
        return False
    paths[0].unlink()
    return True


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


def _token_int(value: object) -> int:
    try:
        return max(0, int(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _topo_name(name: str) -> str:
    if name in {"__start__", "START"}:
        return "START"
    if name in {"__end__", "END"}:
        return "END"
    return name


_RE_QUOTED = re.compile(r'"[^"]*"|\'[^\']*\'')
_RE_HEX = re.compile(r"\b[0-9a-fA-F]{8,}\b")
_RE_NUM = re.compile(r"\b\d+(?:\.\d+)?\b")
_RE_NAME_RUN = re.compile(r"\b[A-Z][A-Za-z0-9''-]*(?:\s+[A-Z][A-Za-z0-9''-]*)+\b")
_RE_TOKEN = re.compile(r"[A-Za-z0-9''-]+|[^\sA-Za-z0-9]")


def _reason_skeleton(text: str) -> str:
    s = (text or "").strip()
    if not s:
        return ""
    s = _RE_QUOTED.sub("{q}", s)
    s = _RE_HEX.sub("{id}", s)
    s = _RE_NUM.sub("{n}", s)
    s = _RE_NAME_RUN.sub("{name}", s)
    return re.sub(r"\s+", " ", s)


def _reason_tokens(text: str) -> list[str]:
    return _RE_TOKEN.findall(text or "")


def _join_reason_tokens(tokens: list[str]) -> str:
    out: list[str] = []
    for tok in tokens:
        if not out:
            out.append(tok)
            continue
        prev = out[-1]
        if tok in ".,;:!?)%" or prev in "([{":
            out.append(tok)
        else:
            out.append(" " + tok)
    return "".join(out)


def _token_distance(left: list[str], right: list[str]) -> int:
    if len(left) != len(right):
        return 999
    return sum(a != b for a, b in zip(left, right))


def _align_reason_tokens(token_lists: list[list[str]]) -> str:
    slots = []
    for index in range(len(token_lists[0])):
        values = {toks[index] for toks in token_lists}
        slots.append(token_lists[0][index] if len(values) == 1 else "{var}")
    return _join_reason_tokens(slots)


def _group_reason_counts(reason_counts: dict[str, int]) -> list[dict]:
    if not reason_counts:
        return []
    skeleton_map: dict[str, dict[str, int]] = defaultdict(dict)
    for text, count in reason_counts.items():
        key = _reason_skeleton(text) or text
        examples = skeleton_map[key]
        examples[text] = examples.get(text, 0) + count

    clustered: list[tuple[str, dict[str, int]]] = []
    leftovers: list[tuple[str, int]] = []
    for key, examples in skeleton_map.items():
        only = next(iter(examples))
        if len(examples) > 1 or key != only:
            clustered.append((key, examples))
        else:
            leftovers.append((only, examples[only]))

    leftover_tok = [(text, count, _reason_tokens(text)) for text, count in leftovers]
    used = [False] * len(leftover_tok)
    for index, (text, count, toks) in enumerate(leftover_tok):
        if used[index]:
            continue
        members = [(text, count, toks)]
        used[index] = True
        max_diff = max(1, len(toks) // 3) if toks else 0
        for other in range(index + 1, len(leftover_tok)):
            if used[other]:
                continue
            other_text, other_count, other_toks = leftover_tok[other]
            if _token_distance(toks, other_toks) <= max_diff:
                used[other] = True
                members.append((other_text, other_count, other_toks))
        examples = {item[0]: item[1] for item in members}
        if len(members) == 1:
            clustered.append((text, examples))
        else:
            clustered.append((_align_reason_tokens([item[2] for item in members]), examples))

    groups = []
    for key, examples in clustered:
        items = sorted(examples.items(), key=lambda item: (-item[1], item[0]))
        groups.append(
            {
                "text": items[0][0] if len(items) == 1 else key,
                "count": sum(examples.values()),
                "unique": len(examples),
                "examples": [text for text, _ in items[:3]],
            }
        )
    groups.sort(key=lambda item: (-item["count"], item["text"]))
    total = sum(item["count"] for item in groups) or 1
    if len(groups) > 4 and groups[0]["count"] / total < 0.25:
        return [
            {
                "text": str(sum(item["unique"] for item in groups)) + " unique outputs",
                "count": sum(item["count"] for item in groups),
                "unique": sum(item["unique"] for item in groups),
                "examples": [ex for item in groups for ex in item["examples"][:1]][:3],
            }
        ]
    shown = groups[:5]
    rest = groups[5:]
    if rest:
        shown.append(
            {
                "text": "other unique outputs",
                "count": sum(item["count"] for item in rest),
                "unique": sum(item["unique"] for item in rest),
                "examples": [ex for item in rest for ex in item["examples"][:1]][:3],
            }
        )
    return shown


def _step_route_choices(step: dict) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    decisions = step.get("decisions")
    if isinstance(decisions, list):
        for item in decisions:
            if not isinstance(item, dict):
                continue
            choice = item.get("choice")
            if choice is None or choice == "":
                continue
            reason = item.get("reason")
            found.append((str(choice), str(reason if reason not in (None, "") else choice)))
        if found:
            return found
    update = step.get("update") if isinstance(step.get("update"), dict) else {}
    for source in (step, update):
        if not isinstance(source, dict):
            continue
        for key in ("choice", "loop_choice"):
            value = source.get(key)
            if value is None or value == "":
                continue
            reason = source.get("reason")
            if reason in (None, ""):
                reason = step.get("reason")
            return [(str(value), str(reason if reason not in (None, "") else value))]
    return found


def _iter_run_dicts(workspace: Path, stem: str | None = None):
    root = store_root(workspace)
    if not root.is_dir():
        return
    if stem:
        folders = [root / stem]
    else:
        folders = [path for path in root.iterdir() if path.is_dir()]
    for folder in folders:
        if not folder.is_dir():
            continue
        for path in folder.glob("*.json"):
            data = _read_run_file(path)
            if data is None:
                continue
            if not data.get("pipeline"):
                data["pipeline"] = folder.name
            yield data


def collect_stats(workspace: Path, stem: str | None = None) -> dict:
    pipe_acc: dict[str, dict] = {}
    node_acc: dict[tuple[str, str], dict] = {}
    decision_acc: dict[tuple[str, str, str], dict] = {}
    edge_acc: dict[tuple[str, str, str], int] = {}
    run_rows: list[dict] = []

    def bump_edge(pipe: str, src: str, dest: str) -> None:
        src_n = _topo_name(src)
        dest_n = _topo_name(dest)
        if not src_n or not dest_n or src_n == dest_n:
            return
        key = (pipe, src_n, dest_n)
        edge_acc[key] = edge_acc.get(key, 0) + 1

    def pipe_bucket(name: str) -> dict:
        return pipe_acc.setdefault(
            name,
            {
                "pipeline": name,
                "runs": 0,
                "prompt": 0,
                "completion": 0,
                "total": 0,
                "usage_runs": 0,
                "unavailable_runs": 0,
                "elapsed_ms": 0.0,
                "elapsed_n": 0,
                "node_elapsed_ms": 0.0,
            },
        )

    def node_bucket(pipe: str, node: str) -> dict:
        return node_acc.setdefault(
            (pipe, node),
            {
                "pipeline": pipe,
                "node": node,
                "prompt": 0,
                "completion": 0,
                "total": 0,
                "visits": 0,
                "unavailable": 0,
                "elapsed_ms": 0.0,
                "elapsed_n": 0,
                "elapsed_min": None,
                "elapsed_max": 0.0,
            },
        )

    def add_elapsed(bucket: dict, ms: float) -> None:
        if ms <= 0:
            return
        bucket["elapsed_ms"] = float(bucket.get("elapsed_ms") or 0) + ms
        bucket["elapsed_n"] = int(bucket.get("elapsed_n") or 0) + 1
        prev_min = bucket.get("elapsed_min")
        try:
            prev_f = float(prev_min) if prev_min is not None else None
        except (TypeError, ValueError):
            prev_f = None
        bucket["elapsed_min"] = ms if prev_f is None else min(prev_f, ms)
        try:
            prev_max = float(bucket.get("elapsed_max") or 0)
        except (TypeError, ValueError):
            prev_max = 0.0
        bucket["elapsed_max"] = max(prev_max, ms)

    for data in _iter_run_dicts(workspace, stem):
        if data.get("queued") or data.get("running"):
            continue
        pipe = str(data.get("pipeline") or "unknown")
        steps = [step for step in (data.get("steps") or []) if isinstance(step, dict)]
        prompt = 0
        completion = 0
        saw_unavailable = False
        saw_usage = False
        step_elapsed_sum = 0.0
        ok_step_elapsed_sum = 0.0
        run_ok = run_status_of(data) == "ok"
        try:
            run_elapsed = float(data.get("elapsed_ms") or 0)
        except (TypeError, ValueError):
            run_elapsed = 0.0
        for index, step in enumerate(steps):
            node = str(step.get("node") or "")
            try:
                step_ms = float(step.get("elapsed_ms") or 0)
            except (TypeError, ValueError):
                step_ms = 0.0
            if step_ms > 0:
                step_elapsed_sum += step_ms
            tokens = step.get("tokens")
            step_prompt = 0
            step_completion = 0
            step_unavailable = False
            if isinstance(tokens, dict):
                if tokens.get("unavailable"):
                    step_unavailable = True
                    saw_unavailable = True
                else:
                    step_prompt = _token_int(tokens.get("prompt"))
                    step_completion = _token_int(tokens.get("completion"))
                    if step_prompt or step_completion:
                        saw_usage = True
            prompt += step_prompt
            completion += step_completion
            step_ok = run_ok and not step.get("error") and not step.get("canceled")
            if node:
                bucket = node_bucket(pipe, node)
                bucket["prompt"] += step_prompt
                bucket["completion"] += step_completion
                bucket["total"] += step_prompt + step_completion
                bucket["visits"] += 1
                if step_unavailable:
                    bucket["unavailable"] += 1
                if step_ok:
                    add_elapsed(bucket, step_ms)
                    if step_ms > 0:
                        ok_step_elapsed_sum += step_ms

            choices = _step_route_choices(step)
            if not choices and not step.get("pending"):
                nxt = steps[index + 1].get("node") if index + 1 < len(steps) else None
                reason = step.get("error") or step.get("reason")
                last_step = index + 1 >= len(steps)
                if nxt:
                    nxt_name = str(nxt)
                    choices = [(nxt_name, str(reason if reason not in (None, "") else nxt_name))]
                elif step.get("canceled") or (run_is_canceled(data) and last_step):
                    choices = [("canceled", str(reason if reason not in (None, "") else "canceled"))]
                elif step.get("error"):
                    choices = [("failed", str(reason if reason not in (None, "") else "error"))]
                else:
                    unused = step.get("unused") or []
                    if unused or node:
                        choices = [("END", str(reason if reason not in (None, "") else "END"))]
            for choice, reason in choices:
                key = (pipe, node or "?", str(choice))
                entry = decision_acc.setdefault(key, {"count": 0, "reasons": {}})
                entry["count"] += 1
                if reason:
                    reasons = entry["reasons"]
                    reasons[reason] = reasons.get(reason, 0) + 1

        completed = [
            step
            for step in steps
            if step.get("node") and not step.get("pending")
        ]
        if completed:
            bump_edge(pipe, "START", str(completed[0].get("node") or ""))
            for index in range(len(completed) - 1):
                bump_edge(
                    pipe,
                    str(completed[index].get("node") or ""),
                    str(completed[index + 1].get("node") or ""),
                )
            last = completed[-1]
            last_name = str(last.get("node") or "")
            unused = {_topo_name(str(item)) for item in (last.get("unused") or [])}
            finished = (
                not data.get("error")
                and not run_is_canceled(data)
                and not data.get("paused")
                and not last.get("error")
            )
            if last_name and finished and "END" not in unused:
                bump_edge(pipe, last_name, "END")

        bucket = pipe_bucket(pipe)
        bucket["runs"] += 1
        bucket["prompt"] += prompt
        bucket["completion"] += completion
        bucket["total"] += prompt + completion
        if saw_usage:
            bucket["usage_runs"] += 1
        elif saw_unavailable:
            bucket["unavailable_runs"] += 1
        if run_elapsed <= 0 and step_elapsed_sum > 0:
            run_elapsed = step_elapsed_sum
        if run_ok:
            add_elapsed(bucket, run_elapsed)
            bucket["node_elapsed_ms"] = float(bucket.get("node_elapsed_ms") or 0) + ok_step_elapsed_sum

        run_rows.append(
            {
                "id": data.get("id") or "",
                "pipeline": pipe,
                "created_at": data.get("created_at"),
                "status": run_status_of(data),
                "prompt": prompt,
                "completion": completion,
                "total": prompt + completion,
                "unavailable": (not saw_usage) and saw_unavailable,
                "elapsed_ms": round(run_elapsed, 2) if run_elapsed else 0,
            }
        )

    run_rows.sort(key=lambda entry: entry.get("created_at") or "", reverse=True)
    decisions = []
    for (pipe, node, choice), entry in sorted(decision_acc.items()):
        reasons = sorted(
            ({"text": text, "count": count} for text, count in entry["reasons"].items()),
            key=lambda item: (-item["count"], item["text"]),
        )
        decisions.append(
            {
                "pipeline": pipe,
                "node": node,
                "choice": choice,
                "count": entry["count"],
                "unique_reasons": len(entry["reasons"]),
                "groups": _group_reason_counts(entry["reasons"]),
                "reasons": reasons[:8],
            }
        )
    return {
        "pipelines": sorted(pipe_acc.values(), key=lambda item: (-item["total"], item["pipeline"])),
        "nodes": sorted(
            node_acc.values(),
            key=lambda item: (item["pipeline"], -item["total"], item["node"]),
        ),
        "runs": run_rows,
        "decisions": decisions,
        "edges": [
            {"pipeline": pipe, "from": src, "to": dst, "count": count}
            for (pipe, src, dst), count in sorted(edge_acc.items())
        ],
    }


def delete_runs(workspace: Path, stem: str) -> int:
    folder = store_root(workspace) / stem
    if not folder.is_dir():
        return 0
    count = 0
    for path in folder.glob("*.json"):
        path.unlink()
        count += 1
    return count


def _settle_inflight_snapshot(run: dict) -> dict:
    """Imported JSON is not a live job; drop queued/running so Clear/Delete work."""
    if not (run.get("queued") or run.get("running")):
        return run
    restore_pause = bool(run.get("paused")) and not run.get("running")
    already_done = run_is_canceled(run) or bool(run.get("error"))
    run["queued"] = False
    run["running"] = False
    steps = run.get("steps")
    if isinstance(steps, list):
        cleaned: list[dict] = []
        for step in steps:
            if not isinstance(step, dict):
                continue
            item = dict(step)
            item.pop("pending", None)
            cleaned.append(item)
        run["steps"] = cleaned
    if restore_pause:
        run["paused"] = True
        run["canceled"] = False
        return run
    if already_done:
        return run
    run["canceled"] = True
    run["paused"] = False
    run["error"] = None
    run["next"] = []
    run["interrupts"] = []
    return run


def import_run(workspace: Path, run: dict, *, fallback_stem: str, known_stems: list[str] | None = None) -> dict:
    if not isinstance(run, dict):
        raise ValueError("run must be a JSON object")
    if not run.get("id"):
        raise ValueError("run needs id")
    if not isinstance(run.get("steps"), list):
        raise ValueError("run needs steps")
    clean = migrate_run(
        {
            key: value
            for key, value in run.items()
            if key not in {"mermaid", "ascii"}
        }
    )
    if load_run(workspace, str(clean["id"])):
        clean["id"] = uuid4().hex
    stem = str(clean.get("pipeline") or fallback_stem or "").strip()
    allowed = set(known_stems or [])
    if allowed and stem not in allowed:
        stem = fallback_stem
    if not stem:
        raise ValueError("run needs a pipeline name")
    clean["pipeline"] = stem
    _settle_inflight_snapshot(clean)
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
    visits: dict[str, int] = {}
    for index, step in enumerate(steps):
        step["index"] = index
        node = str(step.get("node") or "")
        if not node:
            continue
        visits[node] = visits.get(node, 0) + 1
        step["step_id"] = f"{node}#{visits[node]}"
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
