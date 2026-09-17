from __future__ import annotations

import queue
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


SENTINEL = object()
HEARTBEAT_S = 2.0


class DuplicateRun(Exception):
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        super().__init__(f"run {run_id} is already queued or running")


class QueueFull(Exception):
    def __init__(self, *, active: int, queued: int, limit: int, queue_limit: int) -> None:
        self.active = active
        self.queued = queued
        self.limit = limit
        self.queue_limit = queue_limit
        super().__init__(
            f"queue full ({queued}/{queue_limit} waiting, {active}/{limit} running)"
        )

    def payload(self) -> dict[str, int]:
        return {
            "active": self.active,
            "queued": self.queued,
            "limit": self.limit,
            "queue_limit": self.queue_limit,
        }


@dataclass
class RunJob:
    run_id: str
    file_id: str
    stem: str
    user_input: dict[str, Any]
    resume: bool
    enqueued_at: float
    thread_id: str
    cancel: threading.Event = field(default_factory=threading.Event)
    events: queue.Queue[Any] = field(default_factory=queue.Queue)
    aliases: tuple[str, ...] = ()
    interrupt_before: list[str] | None = None
    interrupt_after: list[str] | None = None
    resume_value: Any = None
    use_command: bool = False
    previous: dict[str, Any] | None = None
    wait_ms: float = 0.0
    kind: str = "run"
    step_id: str | None = None
    state_patch: dict[str, Any] | None = None
    replay_input: dict[str, Any] | None = None
    source_run_id: str | None = None

    @property
    def priority(self) -> int:
        return 0 if self.resume or self.kind == "resume" else 1

    def matches(self, run_id: str) -> bool:
        key = run_id.replace("-", "").strip().lower()
        if not key:
            return False
        names = {self.run_id, self.thread_id, *self.aliases}
        return any(key == name.replace("-", "").strip().lower() for name in names if name)

    def queued_event(
        self,
        position: int,
        *,
        limit: int,
        queued: int,
        queue_limit: int,
        active: int = 0,
    ) -> dict[str, Any]:
        return {
            "type": "queued",
            "position": position,
            "limit": limit,
            "queued": queued,
            "queue_limit": queue_limit,
            "active": active,
        }


class RunScheduler:
    def __init__(
        self,
        *,
        concurrent_fn: Callable[[], int],
        queue_limit_fn: Callable[[], int],
        execute_fn: Callable[[RunJob], None],
        persist_cancel: Callable[[RunJob], None] | None = None,
    ) -> None:
        self._concurrent_fn = concurrent_fn
        self._queue_limit_fn = queue_limit_fn
        self._execute_fn = execute_fn
        self._persist_cancel = persist_cancel
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._waiting: list[RunJob] = []
        self._active: dict[str, RunJob] = {}
        self._stopped = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stopped.clear()
        self._thread = threading.Thread(target=self._dispatch, name="gva-dispatch", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stopped.set()
        with self._cond:
            waiting = list(self._waiting)
            self._waiting.clear()
            active = list(self._active.values())
            self._cond.notify_all()
        for job in waiting:
            self._finish_cancel(job, started=False)
        for job in active:
            job.cancel.set()
        thread = self._thread
        self._thread = None
        if thread is not None:
            thread.join(timeout=2)

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "active": len(self._active),
                "queued": len(self._waiting),
                "limit": self._concurrent_fn(),
                "queue_limit": self._queue_limit_fn(),
            }

    def submit(self, job: RunJob) -> RunJob:
        with self._cond:
            if self._conflicts(job):
                raise DuplicateRun(job.run_id)
            queued = len(self._waiting)
            active = len(self._active)
            limit = self._concurrent_fn()
            queue_limit = self._queue_limit_fn()
            if active >= limit and queued >= queue_limit:
                raise QueueFull(
                    active=active, queued=queued, limit=limit, queue_limit=queue_limit
                )
            self._insert(job)
            self._broadcast_positions()
            self._cond.notify()
        return job

    def _conflicts(self, job: RunJob) -> bool:
        names = [name for name in (job.run_id, job.thread_id, *job.aliases) if name]
        held = (*self._waiting, *self._active.values())
        return any(existing.matches(name) for existing in held for name in names)

    def has_job(self, run_id: str) -> bool:
        with self._lock:
            if any(job.matches(run_id) for job in self._waiting):
                return True
            return any(job.matches(run_id) for job in self._active.values())

    def has_job_for(self, *, stem: str | None = None, file_id: str | None = None) -> bool:
        if not stem and not file_id:
            return False
        with self._lock:
            held = (*self._waiting, *self._active.values())
        return any(
            (stem and job.stem == stem) or (file_id and job.file_id == file_id)
            for job in held
        )

    def position_of(self, run_id: str) -> int | None:
        with self._lock:
            for index, job in enumerate(self._waiting):
                if job.matches(run_id):
                    return index + 1
        return None

    def cancel(self, run_id: str) -> bool:
        with self._cond:
            for index, job in enumerate(self._waiting):
                if job.matches(run_id):
                    self._waiting.pop(index)
                    self._broadcast_positions()
                    self._cond.notify_all()
                    waiting = True
                    break
            else:
                job = None
                waiting = False
                for active in self._active.values():
                    if active.matches(run_id):
                        job = active
                        break
        if job is None:
            return False
        if waiting:
            self._finish_cancel(job, started=False)
            return True
        job.cancel.set()
        return True

    def _insert(self, job: RunJob) -> None:
        key = (job.priority, job.enqueued_at)
        index = 0
        while index < len(self._waiting):
            other = self._waiting[index]
            if key < (other.priority, other.enqueued_at):
                break
            index += 1
        self._waiting.insert(index, job)

    def _broadcast_positions(self) -> None:
        limit = self._concurrent_fn()
        queue_limit = self._queue_limit_fn()
        queued = len(self._waiting)
        active = len(self._active)
        for index, job in enumerate(self._waiting):
            job.events.put(
                job.queued_event(
                    index + 1,
                    limit=limit,
                    queued=queued,
                    queue_limit=queue_limit,
                    active=active,
                )
            )

    def _dispatch(self) -> None:
        while not self._stopped.is_set():
            with self._cond:
                while not self._stopped.is_set() and (
                    not self._waiting or len(self._active) >= self._concurrent_fn()
                ):
                    self._cond.wait(timeout=0.5)
                if self._stopped.is_set():
                    return
                if not self._waiting or len(self._active) >= self._concurrent_fn():
                    continue
                job = self._waiting.pop(0)
                self._active[job.run_id] = job
                self._broadcast_positions()
            thread = threading.Thread(
                target=self._run, args=(job,), name=f"gva-run-{job.run_id[:8]}", daemon=True
            )
            thread.start()

    def _run(self, job: RunJob) -> None:
        job.wait_ms = round((time.monotonic() - job.enqueued_at) * 1000, 2)
        try:
            if job.cancel.is_set():
                self._finish_cancel(job, started=True, emit_sentinel=False)
                return
            self._execute_fn(job)
        except Exception as exc:
            job.events.put({"type": "error", "error": str(exc)})
        finally:
            job.events.put(SENTINEL)
            with self._cond:
                self._active.pop(job.run_id, None)
                self._cond.notify_all()

    def _finish_cancel(self, job: RunJob, *, started: bool, emit_sentinel: bool = True) -> None:
        job.cancel.set()
        wait_ms = round((time.monotonic() - job.enqueued_at) * 1000, 2)
        job.wait_ms = wait_ms
        if self._persist_cancel is not None:
            try:
                self._persist_cancel(job)
            except Exception:
                pass
        job.events.put(
            {
                "type": "canceled",
                "wait_ms": wait_ms,
                "started": started,
            }
        )
        if emit_sentinel:
            job.events.put(SENTINEL)
