from __future__ import annotations

import hashlib
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from graphviagent.config import CONFIG_NAME, GVAConfig, activate_config, load_config
from graphviagent.discover import discover_pipelines, path_is_skipped

_DEBOUNCE_S = 0.2
_EVENT_CAP = 200


def _is_pipeline_name(name: str) -> bool:
    return name.endswith("_pipeline.py")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def snapshot_files(
    workspace: Path,
    config: GVAConfig | None = None,
    previous: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    workspace = workspace.resolve()
    prev = previous or {}
    items: dict[str, dict[str, Any]] = {}
    for path in discover_pipelines(workspace, config):
        try:
            resolved = path.resolve()
            stat = resolved.stat()
        except OSError:
            continue
        try:
            rel = resolved.relative_to(workspace).as_posix()
        except ValueError:
            rel = resolved.as_posix()
        size = stat.st_size
        before = prev.get(rel)
        if before is not None and before.get("size") == size and before.get("sha256"):
            sha = before["sha256"]
        else:
            try:
                sha = file_sha256(resolved)
            except OSError:
                continue
        stem = config.stem_for(resolved) if config is not None else resolved.stem
        items[rel] = {
            "id": rel,
            "stem": stem,
            "mtime": stat.st_mtime,
            "size": size,
            "sha256": sha,
        }
    return items


class _DebouncedHandler(FileSystemEventHandler):
    def __init__(self, watcher: PipelineWatcher) -> None:
        self.watcher = watcher

    def _skipped(self, path: Path) -> bool:
        return path_is_skipped(path, self.watcher.workspace)

    def on_any_event(self, event: FileSystemEvent) -> None:
        paths = [Path(event.src_path)]
        dest = getattr(event, "dest_path", None)
        if dest:
            paths.append(Path(dest))
        if event.is_directory:
            if any(not self._skipped(path) for path in paths):
                self.watcher.schedule_refresh()
            return
        if any(not self._skipped(path) and self.watcher.watches(path) for path in paths):
            self.watcher.schedule_refresh()


class PipelineWatcher:
    def __init__(
        self,
        workspace: Path,
        on_change: Callable[[list[str]], None] | None = None,
        config: GVAConfig | None = None,
        on_config: Callable[[GVAConfig], None] | None = None,
    ) -> None:
        self.workspace = workspace.resolve()
        self._on_change = on_change
        self._on_config = on_config
        self._config = config
        self._toml_mtime: float | None = None
        self._toml_size: int | None = None
        if config is not None and config.path is not None:
            try:
                stat = config.path.stat()
            except OSError:
                self._toml_mtime = None
                self._toml_size = None
            else:
                self._toml_mtime = stat.st_mtime
                self._toml_size = stat.st_size
        self._lock = threading.Lock()
        self._seq = 0
        self._files: dict[str, dict[str, Any]] = {}
        self._events: list[dict[str, Any]] = []
        self._timer: threading.Timer | None = None
        self._observer: Observer | None = None
        self._handler: _DebouncedHandler | None = None
        self._watches: dict[str, Any] = {}

    def watches(self, path: Path) -> bool:
        if path.name == CONFIG_NAME:
            return True
        try:
            resolved = path.resolve()
        except OSError:
            resolved = path
        config = self._config
        if config is not None and any(
            spec.file.resolve() == resolved for spec in config.pipelines.values()
        ):
            return True
        if _is_pipeline_name(path.name):
            return True
        try:
            key = resolved.relative_to(self.workspace).as_posix()
        except ValueError:
            key = resolved.as_posix()
        with self._lock:
            return key in self._files

    def _outside_workspace(self, folder: Path) -> bool:
        return folder != self.workspace and self.workspace not in folder.parents

    def _watch_folders(self) -> list[Path]:
        folders = [self.workspace]
        seen = {str(self.workspace)}

        def add(folder: Path) -> None:
            folder = folder.resolve()
            if not self._outside_workspace(folder):
                return
            key = str(folder)
            if key in seen:
                return
            seen.add(key)
            folders.append(folder)

        config = self._config
        if config is not None and config.path is not None:
            add(config.path.parent)
        for path in discover_pipelines(self.workspace, config):
            add(path.resolve().parent)
        return folders

    def _sync_watch_folders(self) -> None:
        observer = self._observer
        handler = self._handler
        if observer is None or handler is None:
            return
        wanted = {str(folder): folder for folder in self._watch_folders()}
        for key, watch in list(self._watches.items()):
            if key in wanted:
                continue
            try:
                observer.unschedule(watch)
            except KeyError:
                pass
            self._watches.pop(key, None)
        for key, folder in wanted.items():
            if key in self._watches:
                continue
            if not folder.is_dir():
                continue
            try:
                self._watches[key] = observer.schedule(
                    handler, str(folder), recursive=True
                )
            except OSError:
                continue

    def start(self) -> None:
        self.refresh(emit=False)
        self._handler = _DebouncedHandler(self)
        observer = Observer()
        self._observer = observer
        self._sync_watch_folders()
        observer.start()

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
        self._handler = None
        self._watches.clear()

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
        reloaded = self._reload_config()
        self._sync_watch_folders()
        with self._lock:
            prev = self._files
        nxt = snapshot_files(self.workspace, self._config, previous=prev)
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
        if reloaded:
            changed.append(CONFIG_NAME)
        if changed and self._on_change is not None:
            self._on_change(changed)

    def _reload_config(self) -> bool:
        toml_path = None
        if self._config is not None and self._config.path is not None:
            toml_path = self._config.path
        else:
            candidate = self.workspace / CONFIG_NAME
            if candidate.is_file():
                toml_path = candidate
        if toml_path is None:
            return False
        try:
            stat = toml_path.stat()
        except OSError:
            return False
        size = stat.st_size
        mtime = stat.st_mtime
        if self._toml_size is not None and size == self._toml_size:
            self._toml_mtime = mtime
            return False
        config = load_config(self.workspace)
        activate_config(config)
        self._config = config
        self._toml_mtime = mtime
        self._toml_size = size
        self.workspace = config.root
        if self._on_config is not None:
            self._on_config(config)
        return True

    def _emit(self, prev: dict[str, dict[str, Any]], nxt: dict[str, dict[str, Any]]) -> None:
        for file_id, item in nxt.items():
            before = prev.get(file_id)
            if before is None:
                self._push("added", item, None)
                continue
            if before.get("size") != item.get("size"):
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
            "size": item.get("size"),
            "sha256": item.get("sha256"),
            "prev_sha256": None if before is None else before.get("sha256"),
        }
        self._events.append(event)
        if len(self._events) > _EVENT_CAP:
            self._events = self._events[-_EVENT_CAP:]
