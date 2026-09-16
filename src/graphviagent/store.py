from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


STORE_DIRNAME = ".graphviagent"
SCHEMA_VERSION = 1


def store_root(workspace: Path) -> Path:
    return workspace.resolve() / STORE_DIRNAME / "runs"


def pipeline_dir(workspace: Path, stem: str) -> Path:
    path = store_root(workspace) / stem
    path.mkdir(parents=True, exist_ok=True)
    return path


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
    return out


def save_run(workspace: Path, stem: str, run: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    run = migrate_run({**run, "pipeline": stem, "created_at": run.get("created_at") or now})
    run["gva_version"] = _gva_version()
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
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict):
            continue
        try:
            data = migrate_run(data)
        except ValueError:
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
    if not root.is_dir() or not run_id:
        return None
    for path in root.glob(f"*/{run_id}.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(data, dict):
            return migrate_run(data)
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
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(data, dict):
                continue
            try:
                data = migrate_run(data)
            except ValueError:
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
            },
        )

    for data in _iter_run_dicts(workspace, stem):
        pipe = str(data.get("pipeline") or "unknown")
        steps = [step for step in (data.get("steps") or []) if isinstance(step, dict)]
        prompt = 0
        completion = 0
        saw_unavailable = False
        saw_usage = False
        for index, step in enumerate(steps):
            node = str(step.get("node") or "")
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
            if node:
                bucket = node_bucket(pipe, node)
                bucket["prompt"] += step_prompt
                bucket["completion"] += step_completion
                bucket["total"] += step_prompt + step_completion
                bucket["visits"] += 1
                if step_unavailable:
                    bucket["unavailable"] += 1

            choices = _step_route_choices(step)
            if not choices and not step.get("pending"):
                nxt = steps[index + 1].get("node") if index + 1 < len(steps) else None
                reason = step.get("error") or step.get("reason")
                if nxt:
                    nxt_name = str(nxt)
                    choices = [(nxt_name, str(reason if reason not in (None, "") else nxt_name))]
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
            finished = not data.get("error") and not data.get("paused") and not last.get("error")
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

        run_rows.append(
            {
                "id": data.get("id") or "",
                "pipeline": pipe,
                "created_at": data.get("created_at"),
                "status": (
                    "error"
                    if data.get("error")
                    else "paused"
                    if data.get("paused")
                    else "ok"
                ),
                "prompt": prompt,
                "completion": completion,
                "total": prompt + completion,
                "unavailable": (not saw_usage) and saw_unavailable,
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
