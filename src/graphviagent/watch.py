from __future__ import annotations

import hashlib
import threading
from pathlib import Path
from collections.abc import Callable
from typing import Any

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from graphviagent.discover import SKIP_DIRS, discover_pipelines

_DEBOUNCE_S = 0.2
_EVENT_CAP = 200


def _skipped(path: Path) -> bool:
    return any(part in SKIP_DIRS or part.endswith(".egg-info") for part in path.parts)


def _is_pipeline_name(name: str) -> bool:
    return name.endswith("_pipeline.py")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def snapshot_files(workspace: Path) -> dict[str, dict[str, Any]]:
    workspace = workspace.resolve()
    items: dict[str, dict[str, Any]] = {}
    for path in discover_pipelines(workspace):
        try:
            stat = path.stat()
            sha = file_sha256(path)
        except OSError:
            continue
        rel = path.resolve().relative_to(workspace).as_posix()
        items[rel] = {
            "id": rel,
            "stem": path.stem,
            "mtime": stat.st_mtime,
            "sha256": sha,
        }
    return items


class _DebouncedHandler(FileSystemEventHandler):
    def __init__(self, watcher: PipelineWatcher) -> None:
        self.watcher = watcher

    def on_any_event(self, event: FileSystemEvent) -> None:
        paths = [Path(event.src_path)]
        dest = getattr(event, "dest_path", None)
        if dest:
            paths.append(Path(dest))
        if event.is_directory:
            if any(not _skipped(path) for path in paths):
                self.watcher.schedule_refresh()
            return
        if any(not _skipped(path) and _is_pipeline_name(path.name) for path in paths):
            self.watcher.schedule_refresh()


class PipelineWatcher:
    def __init__(
        self,
        workspace: Path,
        on_change: Callable[[list[str]], None] | None = None,
    ) -> None:
        self.workspace = workspace.resolve()
        self._on_change = on_change
        self._lock = threading.Lock()
        self._seq = 0
        self._files: dict[str, dict[str, Any]] = {}
        self._events: list[dict[str, Any]] = []
        self._timer: threading.Timer | None = None
        self._observer: Observer | None = None

    def start(self) -> None:
        self.refresh(emit=False)
        handler = _DebouncedHandler(self)
        observer = Observer()
        observer.schedule(handler, str(self.workspace), recursive=True)
        observer.start()
        self._observer = observer

    def stop(self) -> None:
        with self._lock:
            timer = self._timer
            self._timer = None
        if timer is not None:
            timer.cancel()
        if self._observer is not None:
            self._observer.stop()
            self._observer.join(timeout=2)
            self._observer = None

    def files(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(item) for item in self._files.values()]

    def changes(self, since: int = 0) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(event) for event in self._events if event["seq"] > since]

    def schedule_refresh(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
            self._timer = threading.Timer(_DEBOUNCE_S, self.refresh)
            self._timer.daemon = True
            self._timer.start()

    def refresh(self, emit: bool = True) -> None:
        nxt = snapshot_files(self.workspace)
        changed: list[str] = []
        with self._lock:
            prev = self._files
            if emit:
                self._emit(prev, nxt)
            for file_id, item in nxt.items():
                before = prev.get(file_id)
                if before is None or before.get("sha256") != item.get("sha256"):
                    changed.append(file_id)
            for file_id in prev:
                if file_id not in nxt:
                    changed.append(file_id)
            self._files = nxt
            self._timer = None
        if changed and self._on_change is not None:
            self._on_change(changed)

    def _emit(self, prev: dict[str, dict[str, Any]], nxt: dict[str, dict[str, Any]]) -> None:
        for file_id, item in nxt.items():
            before = prev.get(file_id)
            if before is None:
                self._push("added", item, None)
                continue
            if before.get("mtime") != item.get("mtime"):
                self._push("modified", item, before)
            if before.get("sha256") != item.get("sha256"):
                self._push("hash_changed", item, before)
        for file_id, before in prev.items():
            if file_id not in nxt:
                self._push("removed", before, before)

    def _push(
        self,
        kind: str,
        item: dict[str, Any],
        before: dict[str, Any] | None,
    ) -> None:
        self._seq += 1
        event = {
            "seq": self._seq,
            "kind": kind,
            "id": item["id"],
            "stem": item["stem"],
            "mtime": item.get("mtime"),
            "sha256": item.get("sha256"),
            "prev_sha256": None if before is None else before.get("sha256"),
        }
        self._events.append(event)
        if len(self._events) > _EVENT_CAP:
            self._events = self._events[-_EVENT_CAP:]
