from __future__ import annotations

import json
import queue
import threading
import time
import webbrowser
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from graphviagent import __version__
from graphviagent.config import (
    DEFAULT_CONCURRENT,
    GVAConfig,
    activate_config,
    apply_limit_overrides,
    effective_queue_limit,
    load_config,
)
from graphviagent.discover import discover_pipelines
from graphviagent.graph_hash import attach_graph_meta
from graphviagent.load import LoadedPipeline, load_pipeline
from graphviagent.record import iter_run_events, replay_step, resume_from_step
from graphviagent.render import ascii_tree, unrolled_mermaid
from graphviagent.scheduler import DuplicateRun, HEARTBEAT_S, QueueFull, RunJob, RunScheduler, SENTINEL
from graphviagent.store import (
    attach_run_timing,
    collect_stats,
    cancel_paused_run,
    cancel_queue_stub,
    delete_run,
    delete_runs,
    import_run,
    list_all_runs,
    list_runs,
    load_run,
    mark_run_started,
    merge_resume_run,
    pipeline_has_busy_runs,
    run_is_busy,
    save_queued_stub,
    save_run,
)
from graphviagent.watch import PipelineWatcher, file_sha256, snapshot_files

_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1", "0:0:0:0:0:0:0:1"}
_WILDCARD_BINDS = {"0.0.0.0", "::", "::0", "*", ""}
_MUTATING = {"POST", "PUT", "PATCH", "DELETE"}


def is_loopback_host(host: str) -> bool:
    name = (host or "").strip().lower().strip("[]")
    if name in _LOOPBACK_HOSTS or name.startswith("127."):
        return True
    return False


def is_public_bind(host: str) -> bool:
    name = (host or "").strip().lower().strip("[]")
    if name in _WILDCARD_BINDS:
        return True
    return not is_loopback_host(name)


def split_host_header(value: str) -> tuple[str, int | None]:
    text = (value or "").strip()
    if not text:
        return "", None
    if text.startswith("["):
        end = text.find("]")
        if end < 0:
            return text.lower(), None
        name = text[1:end].lower()
        rest = text[end + 1 :]
        if rest.startswith(":"):
            try:
                return name, int(rest[1:])
            except ValueError:
                return name, None
        return name, None
    if text.count(":") == 1:
        name, _, port_text = text.partition(":")
        try:
            return name.lower(), int(port_text)
        except ValueError:
            return text.lower(), None
    return text.lower(), None


def request_allowed(
    command: str,
    headers: object,
    *,
    bind_port: int,
    loopback_only: bool,
) -> bool:
    get = headers.get if hasattr(headers, "get") else lambda _key, _default=None: None
    host_header = str(get("Host") or "")
    host_name, host_port = split_host_header(host_header)
    if not host_name:
        return False
    if host_port is not None and host_port != bind_port:
        return False
    if loopback_only and not is_loopback_host(host_name):
        return False

    site = str(get("Sec-Fetch-Site") or "").lower()
    if site == "cross-site":
        return False

    origin = str(get("Origin") or "").strip()
    if origin:
        if origin.lower() == "null":
            return False
        return _origin_matches(origin, host_name=host_name, host_port=host_port, bind_port=bind_port)

    mutating = command.upper() in _MUTATING
    referer = str(get("Referer") or "").strip()
    if mutating and referer:
        return _origin_matches(referer, host_name=host_name, host_port=host_port, bind_port=bind_port)
    return True


def _origin_matches(
    url: str,
    *,
    host_name: str,
    host_port: int | None,
    bind_port: int,
) -> bool:
    parsed = urlparse(url)
    if parsed.scheme and parsed.scheme != "http":
        return False
    hostname = (parsed.hostname or "").lower()
    if not hostname or hostname != host_name:
        return False
    origin_port = parsed.port
    if origin_port is None and parsed.scheme == "http":
        origin_port = 80
    expected = host_port if host_port is not None else bind_port
    if origin_port is not None and origin_port != expected:
        return False
    return True


FAVICON_SVG = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">
  <defs>
    <linearGradient id="g" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#7c5cff"/>
      <stop offset="100%" stop-color="#4f46e5"/>
    </linearGradient>
  </defs>
  <rect width="32" height="32" rx="8" fill="url(#g)"/>
  <text x="16" y="21" text-anchor="middle" fill="#fff" font-family="Inter, ui-sans-serif, system-ui, sans-serif" font-size="10" font-weight="600">GVA</text>
</svg>"""

PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>GraphVIAgent</title>
  <link rel="icon" href="/favicon.svg" type="image/svg+xml"/>
  <link rel="preconnect" href="https://fonts.googleapis.com"/>
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet"/>
  <style>
    :root {
      --bg: #0e0e11;
      --raised: #16161a;
      --panel: #1c1c22;
      --ink: #ececee;
      --muted: #9b9ba3;
      --line: #2a2a32;
      --accent: #7c5cff;
      --accent-dim: rgba(124, 92, 255, 0.16);
      --danger: #f07178;
      --tok-in: #86efac;
      --tok-out: #047857;
    }
    * { box-sizing: border-box; }
    html, body { height: 100%; }
    body {
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, sans-serif;
      font-size: 13px;
      letter-spacing: -0.01em;
      background: var(--bg);
      color: var(--ink);
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      height: 48px;
      padding: 0 16px;
      background: var(--raised);
      border-bottom: 1px solid var(--line);
    }
    .brand { display: flex; align-items: center; gap: 14px; font-weight: 600; }
    .mark {
      width: 22px; height: 22px; border-radius: 6px;
      background: linear-gradient(135deg, #7c5cff, #4f46e5);
      color: #fff; font-size: 10px; font-weight: 600;
      display: grid; place-items: center;
    }
    .nav { display: flex; align-items: center; gap: 4px; margin-left: 8px; }
    .nav-link {
      height: 30px; padding: 0 10px; border: 0; border-radius: 7px;
      background: transparent; color: var(--muted); font: 500 12px Inter, sans-serif;
      cursor: pointer;
    }
    .nav-link:hover { color: var(--ink); background: #1f1f26; }
    .nav-link.active { color: var(--ink); background: var(--accent-dim); }
    .status-dot {
      width: 8px; height: 8px; border-radius: 99px; flex: 0 0 8px;
      background: #5a5a64;
    }
    .status-dot.ok { background: #22c55e; }
    .status-dot.error { background: #f07178; }
    .status-dot.canceled { background: #a8a29e; }
    .status-dot.paused { background: #fbbf24; }
    .status-dot.gray { background: #5a5a64; }
    .status-dot.live {
      background: var(--accent);
      animation: live-pulse 1.1s ease-in-out infinite;
    }
    .status-dot.canceling {
      background: #a8a29e;
      animation: live-pulse 1.1s ease-in-out infinite;
    }
    .status-dot.queued {
      background: #38bdf8;
      animation: live-pulse 1.1s ease-in-out infinite;
    }
    .page { height: calc(100vh - 48px); overflow: auto; padding: 24px 28px; }
    .page[hidden], .layout[hidden] { display: none; }
    .page-inner { width: 100%; max-width: none; }
    .page h1 { font-size: 22px; font-weight: 600; margin: 0 0 6px; }
    .lede { color: var(--muted); margin: 0 0 22px; }
    .af-card {
      background: #17171c; border: 1px solid var(--line);
      border-radius: 10px; padding: 16px 18px 18px; margin: 0 0 16px;
      overflow: auto; scroll-margin-top: 56px;
    }
    .af-card.active { border-color: #3d3d4a; }
    .af-head {
      display: flex; align-items: baseline; justify-content: space-between;
      gap: 16px; margin-bottom: 10px;
    }
    .af-title {
      border: 0; background: transparent; color: var(--ink);
      font: 600 15px Inter, sans-serif; padding: 0; cursor: pointer; text-align: left;
    }
    .af-title:hover { color: #c4b5fd; }
    .af-last {
      color: var(--muted); font-size: 12px;
      font-family: "IBM Plex Mono", ui-monospace, monospace;
    }
    .af-actions { display: flex; align-items: center; gap: 10px; }
    .af-grid { width: 100%; min-width: 0; }
    .af-bars, .af-plays, .af-row, .af-days, .af-times {
      display: grid; align-items: end; column-gap: 4px;
    }
    .af-bars { height: 58px; margin-bottom: 4px; }
    .af-days { margin-bottom: 1px; }
    .af-times { margin-bottom: 2px; }
    .af-day, .af-time {
      border: 0; background: transparent; padding: 0; cursor: pointer;
      font-family: Inter, ui-sans-serif, system-ui, sans-serif;
      text-align: center; line-height: 1.25; letter-spacing: 0;
      overflow: visible; white-space: nowrap;
    }
    .af-day { color: #7d7d86; font-size: 10px; font-weight: 500; }
    .af-time { color: #5c5c66; font-size: 10px; }
    .af-day:hover, .af-time:hover { color: #a1a1aa; }
    .af-bar {
      width: 100%; border: 0; border-radius: 2px 2px 0 0; padding: 0;
      cursor: pointer; min-height: 4px; align-self: end;
    }
    .af-bar.ok { background: #22c55e; }
    .af-bar.error { background: #ef4444; }
    .af-bar.canceled { background: #78716c; }
    .af-bar.paused { background: #fbbf24; }
    .af-bar.running {
      background: var(--accent);
      animation: live-pulse 1.1s ease-in-out infinite;
    }
    .af-bar.canceling {
      background: #a8a29e;
      animation: live-pulse 1.1s ease-in-out infinite;
    }
    .af-bar.queued {
      background: #38bdf8;
      animation: live-pulse 1.1s ease-in-out infinite;
    }
    .af-bar:hover { filter: brightness(1.15); }
    .af-bar.skel {
      height: 10px; min-height: 8px; background: #26262e; cursor: default;
    }
    .af-bar.skel:hover { filter: none; }
    .af-plays { margin-bottom: 8px; }
    .af-play {
      border: 0; background: transparent; color: #8b8b96; cursor: pointer;
      font-size: 9px; padding: 0; height: 16px;
    }
    .af-play:hover { color: var(--ink); }
    .af-task {
      color: var(--ink); font-size: 12px; padding: 3px 12px 3px 0;
      white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
      display: flex; align-items: center; gap: 8px;
    }
    .af-dot {
      width: 8px; height: 8px; border-radius: 99px; flex: 0 0 8px;
      background: #6366f1;
    }
    .af-dot.decision { background: #f59e0b; }
    .af-dot.loop { background: #22c55e; }
    .af-dot.error { background: var(--danger); }
    .af-task-name {
      min-width: 0; overflow: hidden; text-overflow: ellipsis;
    }
    .af-cell {
      width: 100%; height: 22px; border: 0; padding: 0; background: transparent;
      cursor: pointer; display: grid; place-items: center;
    }
    .af-mark {
      width: 16px; height: 16px; border-radius: 99px;
      display: grid; place-items: center; font-size: 10px; line-height: 1; color: #fff;
    }
    .af-mark.ok { background: #16a34a; }
    .af-mark.error { background: #dc2626; }
    .af-mark.canceled { background: #78716c; }
    .af-mark.paused { background: #fbbf24; color: #1c1917; }
    .af-mark.running {
      background: var(--accent);
      animation: live-pulse 1.1s ease-in-out infinite;
    }
    .af-mark.skip {
      background: transparent; color: #d97706; border: 1px solid #d97706;
    }
    .af-day.skel, .af-time.skel, .af-play.skel { cursor: default; color: transparent; }
    .af-cell.skel { cursor: default; }
    .af-mark.skel {
      width: 10px; height: 10px; background: #26262e; color: transparent;
    }
    .af-task.skel {
      height: 10px; margin: 6px 40px 6px 0; border-radius: 4px; background: #222228;
    }
    .af-empty { color: var(--muted); font-size: 12px; padding: 8px 0; }
    .layout { display: grid; grid-template-columns: 280px 1fr; height: calc(100vh - 48px); min-height: 0; }
    aside {
      background: var(--raised);
      border-right: 1px solid var(--line);
      padding: 12px;
      overflow: auto;
      min-height: 0;
    }
    main {
      padding: 16px 18px;
      overflow: auto;
      display: flex;
      flex-direction: column;
      min-height: 0;
    }
    h2 {
      font-size: 11px; font-weight: 500; text-transform: uppercase;
      letter-spacing: 0.06em; color: var(--muted); margin: 14px 0 8px;
    }
    .item, .run {
      width: 100%; text-align: left; cursor: pointer;
      border: 0; border-radius: 8px; background: transparent;
      color: var(--ink); padding: 8px 10px; margin: 0 0 2px;
    }
    .item { display: flex; align-items: center; gap: 8px; }
    .item .pill { margin-left: auto; }
    .item:hover, .run:hover { background: #1f1f26; }
    .item.active, .run.active { background: var(--accent-dim); color: #fff; }
    .item.error { color: var(--danger); }
    .run { display: block; }
    .run-top { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
    .pill {
      font-size: 10px; font-weight: 500; text-transform: lowercase;
      color: #c4b5fd; background: var(--accent-dim);
      border-radius: 999px; padding: 1px 7px;
      flex: 0 0 auto; width: auto; display: inline-block;
    }
    #history .pill.mode-run { color: #c4b5fd; background: rgba(124, 92, 255, 0.18); }
    #history .pill.mode-replay { color: #5eead4; background: rgba(45, 212, 191, 0.16); }
    #history .pill.mode-replay_from { color: #fbbf24; background: rgba(251, 191, 36, 0.16); }
    #history .pill.mode-approximate { color: #fdba74; background: rgba(251, 146, 60, 0.18); }
    #history .pill.mode-error { color: #fca5a5; background: rgba(240, 113, 120, 0.16); }
    #history .pill.mode-canceled { color: #d6d3d1; background: rgba(168, 162, 158, 0.2); }
    #history .pill.mode-outdated { color: #fbbf24; background: rgba(251, 191, 36, 0.16); }
    .run-pills { display: flex; gap: 4px; flex: 0 0 auto; align-items: center; }
    .meta {
      color: var(--muted); font-size: 11px; margin-top: 3px;
      white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
      font-family: "IBM Plex Mono", ui-monospace, monospace;
    }
    .toolbar { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin: 10px 0 8px; }
    textarea {
      width: 100%; min-height: 88px; resize: none;
      background: var(--panel); color: var(--ink);
      border: 1px solid var(--line); border-radius: 8px;
      font-family: "IBM Plex Mono", ui-monospace, monospace;
      font-size: 12px; padding: 10px 10px 16px; outline: none;
    }
    #input { display: block; height: 88px; }
    .resize-s {
      position: absolute; left: 0; right: 0; bottom: 0; height: 10px;
      cursor: ns-resize; z-index: 3;
    }
    .resize-s::after {
      content: ""; position: absolute; left: 50%; bottom: 4px;
      width: 32px; height: 3px; margin-left: -16px; border-radius: 99px;
      background: #3a3a44;
    }
    .copy-wrap:hover .resize-s::after { background: #5c5c66; }
    textarea:focus { border-color: var(--accent); }
    button.primary, button.ghost, button.danger {
      height: 30px; padding: 0 12px; border-radius: 7px;
      font: 500 12px Inter, sans-serif; cursor: pointer;
      box-sizing: border-box; line-height: 1;
      display: inline-flex; align-items: center; justify-content: center;
    }
    button[hidden] { display: none !important; }
    button.primary { background: var(--accent); color: #fff; border: 0; }
    button.ghost { background: transparent; color: var(--ink); border: 1px solid var(--line); }
    button.ghost:hover { background: #1f1f26; }
    button.ghost:disabled, button.primary:disabled, button.danger:disabled {
      opacity: 0.4; cursor: not-allowed;
    }
    button.ghost:disabled:hover { background: transparent; }
    button.cancel { border-color: #7f1d1d; color: #fca5a5; }
    button.cancel:hover { background: #2a1518; }
    button.cancel:disabled:hover { background: transparent; }
    button.cancel.canceling:disabled {
      opacity: 1; cursor: wait; color: #e7e5e4; border-color: #57534e;
    }
    button.ghost.danger, button.danger {
      border: 1px solid #7f1d1d; color: #fca5a5; background: transparent;
    }
    button.ghost.danger:hover, button.danger:hover { background: #2a1518; }
    button.ghost.danger:disabled:hover, button.danger:disabled:hover { background: transparent; }
    button.jump-trace {
      background: rgba(249, 115, 22, 0.16); border-color: #9a3412; color: #fdba74;
    }
    button.jump-trace:hover { background: rgba(249, 115, 22, 0.28); }
    button.jump-runs {
      background: rgba(6, 182, 212, 0.16); border-color: #155e75; color: #a5f3fc;
    }
    button.jump-runs:hover { background: rgba(6, 182, 212, 0.28); }
    button.jump-stats {
      background: rgba(192, 132, 252, 0.16); border-color: #6b21a8; color: #e9d5ff;
    }
    button.jump-stats:hover { background: rgba(192, 132, 252, 0.28); }
    .run-live {
      color: #c4b5fd; font-size: 12px; font-weight: 500;
      display: inline-flex; align-items: center; gap: 6px;
    }
    .run-live[hidden] { display: none; }
    .run-live::before {
      content: ""; width: 8px; height: 8px; border-radius: 99px;
      background: var(--accent); animation: live-pulse 1.1s ease-in-out infinite;
    }
    .run-live.canceling { color: #d6d3d1; }
    .run-live.canceling::before { background: #a8a29e; }
    @keyframes live-pulse {
      0%, 100% { opacity: 1; }
      50% { opacity: 0.35; }
    }
    .warn { color: #c4a574; font-size: 12px; margin: 0 0 14px; }
    .grid {
      display: grid; grid-template-columns: 1.15fr 1fr; gap: 12px;
      align-items: stretch;
      flex: 1 1 auto;
      min-height: 320px;
    }
    .pane {
      display: flex; flex-direction: column; min-height: 0;
      height: 100%;
      background: var(--panel); border: 1px solid var(--line);
      border-radius: 8px; padding: 12px;
    }
    #diagram {
      flex: 1;
      min-height: 0;
      overflow: hidden;
      position: relative;
      border-radius: 8px;
      cursor: grab;
      background:
        radial-gradient(circle at 1px 1px, #26262e 1px, transparent 0) 0 0 / 18px 18px;
    }
    #diagram:active { cursor: grabbing; }
    #diagram.empty { cursor: default; }
    #diagram.empty, .empty { color: var(--muted); }
    #diagram.empty { background: transparent; }
    .view-switch { display: flex; align-items: center; gap: 4px; margin-right: 8px; }
    .view-switch .ghost.active {
      background: var(--accent-dim); color: var(--ink); border-color: transparent;
    }
    .gflow.gantt {
      width: max-content;
      min-width: 520px;
      padding: 16px 20px 20px;
      transform-origin: top left;
    }
    .gantt-head, .gantt-row {
      display: grid;
      grid-template-columns: 104px 480px;
      gap: 10px;
      align-items: center;
    }
    .gantt-head {
      color: var(--muted); font-size: 10px; font-weight: 500;
      text-transform: uppercase; letter-spacing: 0.04em;
      margin-bottom: 8px;
    }
    .gantt-ticks { display: flex; justify-content: space-between; }
    .gantt-row { margin: 0 0 7px; }
    .gantt-label {
      font-size: 12px; color: var(--ink);
      overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    }
    .gantt-track {
      position: relative; height: 22px;
      background: #141418; border-radius: 6px;
    }
    .gantt-bar {
      position: absolute; top: 3px; height: 16px; min-width: 4px;
      border: 0; border-radius: 4px; cursor: pointer; padding: 0;
      background: #6366f1;
    }
    .gantt-bar.decision { background: #f59e0b; }
    .gantt-bar.loop { background: #22c55e; }
    .gantt-bar.error { background: var(--danger); }
    .gantt-bar:hover { filter: brightness(1.08); }
    .gantt-bar.selected { box-shadow: 0 0 0 2px #fff; }
    .gantt-tip {
      position: fixed; z-index: 40; min-width: 196px; max-width: 280px;
      padding: 8px 10px; background: #1c1c22; border: 1px solid #3a3a44;
      border-radius: 8px; box-shadow: 0 12px 28px rgba(0, 0, 0, 0.4);
      pointer-events: none; font-size: 12px;
    }
    .gantt-tip .tip-name { font-weight: 500; }
    .gantt-tip .tip-id { color: var(--muted); font-size: 11px; margin-top: 2px; }
    .gantt-tip .tip-row {
      display: flex; justify-content: space-between; gap: 14px;
      color: var(--muted); margin-top: 4px;
    }
    .gantt-tip .tip-row b { color: var(--ink); font-weight: 500; }
    .gantt-tip .tip-why { color: var(--muted); margin-top: 6px; line-height: 1.4; }
    .gantt-tip .tip-err { color: var(--danger); margin-top: 6px; }
    .gantt-lane { margin-bottom: 4px; }
    .gantt-lane:last-child { margin-bottom: 0; }
    .mem-table { width: 100%; border-collapse: collapse; font-size: 11px; }
    .mem-table th {
      text-align: left; color: var(--muted); font-weight: 600;
      text-transform: uppercase; letter-spacing: 0.04em; font-size: 10px;
      padding: 4px 6px 8px 0; border-bottom: 1px solid #2a2a32;
    }
    .mem-table td {
      padding: 6px 8px 6px 0; vertical-align: top; color: #d4d4d8;
      overflow-wrap: anywhere;
    }
    .mem-table tr.removed td { color: var(--danger); }
    .mem-table .mem-k { color: var(--ink); font-family: "IBM Plex Mono", ui-monospace, monospace; }
    .mem-table .mem-muted { color: var(--muted); }
    .agent-edge {
      color: var(--muted); font-size: 11px; text-align: center;
      max-width: 220px; margin: 0 auto; padding: 2px 0;
    }
    .g-card.agent { width: 220px; }
    .io-tools { grid-column: 1 / -1; }
    .io-tools[hidden] { display: none !important; }
    .io-trace { grid-column: 1 / -1; }
    .io-trace[hidden] { display: none !important; }
    .src-link {
      color: #c4b5fd; cursor: pointer; text-decoration: none;
      font-family: "IBM Plex Mono", ui-monospace, monospace;
      font-size: 11px;
    }
    .src-link:hover { color: #fff; text-decoration: underline; }
    .g-src { line-height: 1.3; }
    .step-src { color: var(--muted); font-size: 11px; margin-top: 1px; }
    .nv-meta .src-link { font-size: 12px; }
    .trace {
      margin: 0; padding: 8px 10px; border-radius: 8px;
      background: #121216; max-height: 280px; overflow: auto;
    }
    .trace.empty { color: var(--muted); }
    .trace-frame {
      display: grid; grid-template-columns: auto 1fr; gap: 6px 10px;
      padding: 6px 0; border-bottom: 1px solid #2a2a32;
      font: 12px/1.4 "IBM Plex Mono", ui-monospace, monospace;
    }
    .trace-frame:last-child { border-bottom: 0; }
    .trace-frame.library { opacity: 0.45; }
    .trace-loc { color: var(--muted); white-space: nowrap; }
    .trace-name { color: var(--ink); }
    .trace-text { grid-column: 1 / -1; color: #d4d4dc; white-space: pre-wrap; }
    .tool-bars { display: flex; gap: 6px; margin: 0 0 10px; }
    .tool-bar {
      flex: 1; min-width: 0; height: 10px; border-radius: 99px;
      background: #6366f1; opacity: 0.85;
    }
    .tool-bar.error { background: var(--danger); }
    .tool-row { margin: 0 0 10px; padding: 0 0 10px; border-bottom: 1px solid #2a2a32; }
    .tool-row:last-child { margin: 0; padding: 0; border: 0; }
    .tool-name { font-weight: 600; color: var(--ink); }
    .tool-meta { color: var(--muted); font-size: 10px; margin-top: 2px; }
    .tool-kv { margin-top: 6px; }
    .tool-kv .cmp-k { padding-top: 4px; }
    .tool-err {
      margin-top: 6px; color: var(--danger);
      white-space: pre-wrap; font-family: "IBM Plex Mono", ui-monospace, monospace;
    }
    .graph-zoom { display: flex; align-items: center; gap: 4px; }
    .graph-zoom .ghost {
      height: 24px; min-width: 24px; padding: 0 8px;
      font-size: 12px; line-height: 1;
    }
    #diagram .gzoom {
      position: absolute;
      inset: 0;
      display: flex;
      justify-content: center;
      align-items: flex-start;
      padding-top: 12px;
      pointer-events: none;
    }
    .gflow {
      display: flex; flex-direction: column; align-items: center;
      width: 228px;
      padding: 18px 0 22px;
      transform-origin: 114px 0;
      pointer-events: auto;
      will-change: transform;
    }
    .gflow.topo {
      width: max-content;
      min-width: 228px;
      padding: 28px 80px 44px;
      transform-origin: 50% 0;
      position: relative;
      gap: 52px;
    }
    .topo-svg {
      position: absolute; inset: 0; width: 100%; height: 100%;
      overflow: visible; pointer-events: none; z-index: 2;
    }
    .topo-edge { fill: none; stroke: #6b6b78; stroke-width: 2; }
    .topo-edge.taken { stroke: #a5b4fc; stroke-width: 2.2; }
    .topo-edge.parallel { stroke: #c4b5fd; stroke-width: 2.4; }
    .topo-edge.skipped { stroke: #fbbf24; stroke-width: 2; stroke-dasharray: 5 4; }
    .topo-edge.back { stroke: #4ade80; stroke-width: 2; stroke-dasharray: 6 4; }
    .topo-edge.idle { stroke: #6b6b78; }
    .topo-label-bg { fill: #121216; stroke: #3a3a44; stroke-width: 1; }
    .topo-label-bg.skipped { stroke: #92400e; }
    .topo-label-bg.parallel { stroke: #4c1d95; }
    .topo-label-bg.back { stroke: #166534; }
    .topo-label {
      fill: #ececee; font-size: 11px; font-weight: 600;
      font-family: Inter, ui-sans-serif, system-ui, sans-serif;
    }
    .topo-label.skipped { fill: #fbbf24; }
    .topo-label.parallel { fill: #ddd6fe; }
    .topo-label.back { fill: #86efac; }
    .topo-layer {
      display: flex; gap: 20px; justify-content: center; align-items: flex-start;
      position: relative; z-index: 1;
    }
    .topo-node { position: relative; padding-left: 16px; }
    .topo-node .bp-dot {
      position: absolute; left: 0; top: 14px; z-index: 2;
    }
    .g-badge {
      position: absolute; top: -7px; right: -7px;
      min-width: 18px; height: 18px; padding: 0 5px; border-radius: 99px;
      background: #4f46e5; color: #fff; font-size: 10px; font-weight: 600;
      display: grid; place-items: center; white-space: nowrap;
    }
    .g-card.idle { opacity: 0.55; }
    .g-card.taken { border-color: #3f3f68; }
    .g-card.running {
      opacity: 1;
      border-color: var(--accent);
      box-shadow: 0 0 0 3px var(--accent-dim), 0 8px 24px rgba(0, 0, 0, 0.18);
      animation: g-pulse 1s ease-in-out infinite;
    }
    @keyframes g-pulse {
      0%, 100% { box-shadow: 0 0 0 2px var(--accent-dim), 0 8px 24px rgba(0, 0, 0, 0.18); }
      50% { box-shadow: 0 0 0 6px var(--accent-dim), 0 8px 24px rgba(0, 0, 0, 0.18); }
    }
    .g-cap {
      height: 22px; padding: 0 10px; border-radius: 999px;
      border: 1px solid #2e2e36; background: #121216; color: #8b8b96;
      font-size: 10px; font-weight: 500; letter-spacing: 0.08em;
      text-transform: uppercase; display: inline-flex; align-items: center;
    }
    .g-line { width: 1px; height: 18px; background: #32323c; }
    .g-row { position: relative; width: 228px; display: flex; justify-content: flex-start; }
    .g-card {
      width: 228px; display: flex; align-items: flex-start; gap: 10px;
      padding: 10px 12px; border-radius: 10px; cursor: pointer;
      background: linear-gradient(180deg, #1a1a20, #16161b);
      border: 1px solid #2e2e36; color: inherit; text-align: left;
      font: inherit; appearance: none; -webkit-appearance: none;
      box-shadow: 0 8px 24px rgba(0, 0, 0, 0.18);
    }
    .g-card:hover { border-color: #45454f; }
    .g-card.selected {
      border-color: var(--accent);
      box-shadow: 0 0 0 3px var(--accent-dim), 0 8px 24px rgba(0, 0, 0, 0.18);
    }
    .g-dot {
      width: 8px; height: 8px; border-radius: 99px; flex: 0 0 8px;
      margin-top: 5px; background: #6366f1;
    }
    .g-card.decision .g-dot { background: #f59e0b; }
    .g-card.loop .g-dot { background: #22c55e; }
    .g-card.error { border-color: #7f1d1d; }
    .g-card.error .g-dot { background: var(--danger); }
    .g-card.error .g-sub { color: var(--danger); }
    .g-card.next {
      border-color: #fbbf24;
      box-shadow: 0 0 0 2px rgba(251, 191, 36, 0.22);
    }
    .bp-dot {
      width: 10px; height: 10px; border-radius: 99px; flex: 0 0 10px;
      margin-top: 4px; border: 1.5px solid #5a5a64; background: transparent;
      cursor: pointer; box-sizing: border-box;
    }
    .bp-dot:hover { border-color: #f43f5e; }
    .bp-dot.on { border-color: #f43f5e; background: #f43f5e; }
    .pause-banner {
      margin: 8px 0 0; padding: 8px 10px; border-radius: 8px;
      background: rgba(251, 191, 36, 0.12); color: #fbbf24;
      font-size: 12px; font-weight: 500;
    }
    .pause-banner[hidden], .hitl-panel[hidden] { display: none !important; }
    .hitl-panel {
      margin: 8px 0 0; padding: 10px 12px; border-radius: 8px;
      border: 1px solid var(--line); background: var(--panel);
    }
    .hitl-panel label { display: block; color: var(--muted); font-size: 11px; margin-bottom: 6px; }
    .hitl-hint {
      font: 11px/1.4 "IBM Plex Mono", ui-monospace, monospace;
      color: var(--muted); white-space: pre-wrap; margin: 0 0 8px; max-height: 72px; overflow: auto;
    }
    #hitlValue {
      width: 100%; min-height: 72px; resize: vertical;
      background: #121216; color: var(--ink); border: 1px solid var(--line);
      border-radius: 7px; padding: 8px; font: 12px/1.4 "IBM Plex Mono", ui-monospace, monospace;
    }
    #history .pill.mode-paused { color: #fbbf24; background: rgba(251, 191, 36, 0.16); }
    #history .pill.mode-running, .item .pill.mode-running {
      color: #c4b5fd; background: rgba(124, 92, 255, 0.22);
    }
    #history .pill.mode-canceling, .item .pill.mode-canceling {
      color: #e7e5e4; background: rgba(168, 162, 158, 0.28);
    }
    #history .pill.mode-queued, .item .pill.mode-queued {
      color: #7dd3fc; background: rgba(56, 189, 248, 0.2);
    }
    .g-body { min-width: 0; display: flex; flex-direction: column; gap: 2px; }
    .g-name { font-size: 13px; font-weight: 500; }
    .g-sub {
      color: var(--muted); font-size: 11px; line-height: 1.35;
      white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 180px;
    }
    .g-skip {
      position: absolute; left: calc(100% + 2px); top: 50%;
      transform: translateY(-50%); display: flex; align-items: center;
    }
    .g-skips { display: flex; flex-direction: column; gap: 8px; }
    .g-skip-arm { width: 22px; height: 1px; border-top: 1px dashed #3a3a44; }
    .g-card.skipped {
      width: 168px; opacity: 0.55; cursor: default;
      box-shadow: none; background: #141418;
    }
    .g-card.skipped:hover { border-color: #2e2e36; }
    .waterfall {
      display: flex; flex-direction: column; gap: 4px;
      flex: 1; min-height: 0; overflow: auto;
    }
    .step {
      display: grid; grid-template-columns: 14px 1fr auto auto; gap: 8px;
      padding: 8px 8px 8px 0; border-radius: 8px; cursor: pointer;
    }
    .step:hover { background: #22222a; }
    .step.selected { background: var(--accent-dim); }
    .step.error .step-why { color: var(--danger); }
    .step.pending { opacity: 0.7; }
    .step.pending .step-why { color: var(--accent); }
    .run-log {
      max-height: 220px; overflow: auto; margin: 0; padding: 8px 10px;
      border-radius: 8px; background: #121216; color: #c8c8d0;
      font: 12px/1.45 ui-monospace, SFMono-Regular, Menlo, monospace;
    }
    .run-log:empty::before { content: "No log lines yet."; color: var(--muted); }
    .log-line {
      display: grid; grid-template-columns: 56px minmax(64px, auto) 52px 1fr;
      gap: 8px; align-items: start;
    }
    .log-t { color: var(--muted); }
    .log-src { color: #d4d4dc; }
    .log-lvl { font-weight: 700; letter-spacing: 0.04em; }
    .log-msg { white-space: pre-wrap; word-break: break-word; }
    .log-line.log-debug, .log-line.log-debug .log-lvl { color: #8b8b96; }
    .log-line.log-info, .log-line.log-info .log-lvl { color: #93c5fd; }
    .log-line.log-print, .log-line.log-print .log-lvl { color: #c8c8d0; }
    .log-line.log-warning, .log-line.log-warn, .log-line.log-warning .log-lvl, .log-line.log-warn .log-lvl { color: #fbbf24; }
    .log-line.log-error, .log-line.log-stderr, .log-line.log-error .log-lvl, .log-line.log-stderr .log-lvl { color: #f07178; }
    .log-line.log-critical, .log-line.log-fatal, .log-line.log-critical .log-lvl { color: #fb7185; font-weight: 700; }
    .bar { width: 3px; border-radius: 99px; background: #6366f1; margin: 2px 0 2px 6px; }
    .bar.decision { background: #f59e0b; }
    .bar.loop { background: #22c55e; }
    .bar.error { background: var(--danger); }
    .step-id { color: var(--muted); font-size: 11px; }
    .step-name { font-weight: 500; }
    .step-why { color: var(--muted); margin-top: 2px; }
    .step-ms, .run-ms, #stepsMs {
      color: var(--muted); font-variant-numeric: tabular-nums;
      font-size: 11px; white-space: nowrap; text-transform: none;
    }
    .spark {
      display: flex; align-items: flex-end; gap: 2px;
      width: 42px; height: 22px; align-self: center;
    }
    .spark i {
      flex: 1; min-width: 4px; border-radius: 1px 1px 0 0;
      background: #6366f1; cursor: help;
    }
    .spark i.mb { background: #06b6d4; }
    .spark i.peak { background: #c084fc; }
    .spark i.prompt { background: var(--tok-in); }
    .spark i.completion { background: var(--tok-out); }
    .spark i.tokens { background: var(--tok-in); }
    .spark i.tool { background: #f59e0b; }
    .spark i.zero { opacity: 0.22; }
    .gantt-tip .tip-spark { width: 96px; height: 28px; margin-top: 8px; }
    .spark-wrap { display: flex; flex-direction: column; align-items: flex-end; gap: 3px; }
    .spark-labels {
      display: flex; gap: 2px; width: 96px;
      color: var(--muted); font-size: 8px; letter-spacing: 0;
    }
    .spark-labels span { flex: 1; text-align: center; }
    .steps-meta { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; justify-content: flex-end; }
    .spark-legend {
      display: flex; align-items: center; gap: 7px;
      color: var(--muted); font-size: 10px; font-weight: 400;
      text-transform: none; letter-spacing: 0;
    }
    .spark-legend span {
      display: inline-flex; align-items: center; gap: 3px;
      cursor: help;
    }
    .spark-legend i {
      width: 6px; height: 8px; border-radius: 1px;
      background: #6366f1;
    }
    .spark-legend i.mb { background: #06b6d4; }
    .spark-legend i.peak { background: #c084fc; }
    .spark-legend i.prompt { background: var(--tok-in); }
    .spark-legend i.completion { background: var(--tok-out); }
    .spark-legend i.tokens { background: var(--tok-in); }
    .spark-legend i.tool { background: #f59e0b; }
    .pane-title { display: flex; align-items: baseline; justify-content: space-between; gap: 8px; }
    pre, .json {
      background: var(--bg); border: 1px solid var(--line); border-radius: 8px;
      color: #d4d4d8; padding: 10px; overflow: auto;
      font-family: "IBM Plex Mono", ui-monospace, monospace; font-size: 11px;
    }
    pre { white-space: pre-wrap; }
    .json { line-height: 1.55; position: relative; }
    .json.empty { color: var(--muted); }
    .copy-wrap { position: relative; }
    .copy-btn {
      position: absolute; top: 6px; right: 6px; z-index: 2;
      height: 22px; padding: 0 8px; border-radius: 6px;
      border: 1px solid var(--line); background: #1c1c22; color: var(--muted);
      font: 500 10px Inter, sans-serif; cursor: pointer;
      opacity: 0; pointer-events: none;
    }
    .json:hover .copy-btn, .cmp-a:hover .copy-btn, .cmp-b:hover .copy-btn,
    .copy-wrap:hover .copy-btn { opacity: 1; pointer-events: auto; }
    #stateDiff.json:hover .copy-btn { opacity: 0; pointer-events: none; }
    #stateDiff .cmp-a:hover .copy-btn, #stateDiff .cmp-b:hover .copy-btn {
      opacity: 1; pointer-events: auto;
    }
    .copy-btn:hover { color: var(--ink); border-color: #45454f; }
    .j-node { margin: 0; color: inherit; }
    .j-node > summary {
      cursor: pointer; list-style: none;
      display: flex; align-items: baseline; gap: 6px;
    }
    .j-node > summary::-webkit-details-marker { display: none; }
    .j-node > summary::before {
      content: "▶"; color: var(--muted); font-size: 8px;
      width: 10px; flex: 0 0 10px;
    }
    .j-node[open] > summary::before { content: "▼"; }
    .j-kids { margin-left: 8px; border-left: 1px solid var(--line); padding-left: 10px; }
    .j-leaf { display: flex; gap: 8px; align-items: baseline; padding: 1px 0 1px 16px; }
    .j-key { color: #c4b5fd; }
    .j-key::after { content: ":"; color: var(--muted); }
    .j-hint { color: var(--muted); }
    .j-str { color: #86efac; }
    .j-num { color: #93c5fd; }
    .j-bool { color: #fbbf24; }
    .j-null, .j-empty { color: var(--muted); }
    .nv-overlay {
      position: fixed; inset: 0; z-index: 30;
      background: rgba(8, 8, 10, 0.72);
      display: flex; align-items: center; justify-content: center;
      padding: 28px;
    }
    .nv-overlay[hidden] { display: none; }
    .nv {
      width: min(1080px, 100%);
      max-height: calc(100vh - 56px);
      overflow: auto;
      background: var(--raised);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 18px 20px 20px;
      box-shadow: 0 24px 80px rgba(0, 0, 0, 0.45);
    }
    .nv-head {
      display: flex; justify-content: space-between; align-items: flex-start;
      gap: 12px; margin-bottom: 16px;
    }
    .nv-kicker {
      font-size: 11px; font-weight: 500; text-transform: uppercase;
      letter-spacing: 0.06em; color: var(--muted);
    }
    .nv-head h3 { margin: 4px 0 0; font-size: 18px; font-weight: 600; }
    .nv-meta { color: var(--muted); font-size: 12px; margin-top: 4px; }
    .io { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .io .pane { min-height: 0; }
    .io-diff { grid-column: 1 / -1; }
    .io-diff[hidden] { display: none !important; }
    .io h2 .diff-legend,
    .io h2 .diff-legend span {
      display: flex; flex-wrap: wrap; gap: 10px;
      color: var(--muted); font-size: 10px; font-weight: 400;
      text-transform: none; letter-spacing: 0; margin-left: 0;
    }
    .io h2 .diff-legend span { display: inline-flex; align-items: center; gap: 4px; }
    .diff-legend i { width: 7px; height: 7px; border-radius: 99px; }
    .diff-legend i.changed { background: #fbbf24; }
    .diff-legend i.added { background: #86efac; }
    .diff-legend i.removed { background: var(--danger); }
    .diff-legend i.same { background: #5c5c66; }
    .cmp-row.same .cmp-a, .cmp-row.same .cmp-b { color: var(--muted); }
    .io h2 span { color: var(--ink); text-transform: none; letter-spacing: 0; font-size: 12px; margin-left: 6px; }
    .io .json { max-height: 360px; }
    #nodeInput {
      display: none; min-height: 220px; max-height: 360px;
      background: var(--bg); margin: 0;
    }
    #nodeInput.visible { display: block; }
    #nodeActions { display: none; margin: 8px 0 0; }
    #nodeActions.visible { display: flex; }
    .tree-details { margin-top: 10px; color: var(--muted); }
    .tree-details > summary { cursor: pointer; font-size: 12px; }
    .tree-details.section { margin: 12px 0 0; }
    .tree-details.section > summary {
      font-size: 11px; font-weight: 500; text-transform: uppercase;
      letter-spacing: 0.06em; color: var(--muted); margin: 0;
    }
    .tree-details.section[open] > summary { margin-bottom: 8px; }
    .chips { display: none; }
    .chip {
      height: 26px; padding: 0 10px; border-radius: 999px; cursor: pointer;
      border: 1px solid var(--line); background: #1a1a20; color: var(--ink);
      font: 500 11px Inter, sans-serif;
    }
    .chip:hover, .chip.active { border-color: var(--accent); color: #c4b5fd; }
    .hist-tools { display: flex; gap: 6px; margin: 0 0 8px; }
    .hist-tools input, .hist-tools select {
      width: 100%; height: 28px; border-radius: 7px; border: 1px solid var(--line);
      background: var(--panel); color: var(--ink); font: 12px Inter, sans-serif; padding: 0 8px;
    }
    .hist-tools select { width: 88px; flex: 0 0 88px; }
    .history-box { min-height: 80px; border-radius: 8px; }
    .history-box.drop { outline: 1px dashed var(--accent); background: var(--accent-dim); }
    .thread { display: flex; flex-direction: column; gap: 8px; margin-bottom: 10px; }
    .bubble {
      border: 1px solid var(--line); border-radius: 8px; padding: 8px 10px; background: #141418;
    }
    .bubble-role {
      font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em;
      color: #c4b5fd; margin-bottom: 4px;
    }
    .bubble-body { white-space: pre-wrap; color: #d4d4d8; font-size: 12px; }
    .bubble-tool {
      margin-top: 6px; color: var(--muted); font-family: "IBM Plex Mono", ui-monospace, monospace;
      font-size: 11px;
    }
    .cmp-filter {
      display: flex; align-items: center; gap: 10px; margin: 0 0 14px; max-width: 360px;
    }
    .cmp-filter label {
      font-size: 11px; font-weight: 600; letter-spacing: 0.06em;
      text-transform: uppercase; color: var(--muted); white-space: nowrap;
    }
    .cmp-filter select {
      flex: 1; min-width: 0; height: 34px; border-radius: 8px; border: 1px solid var(--line);
      background: var(--panel); color: var(--ink); font: 12px Inter, sans-serif; padding: 0 8px;
    }
    .st-toolbar {
      display: flex; align-items: flex-end; justify-content: space-between;
      gap: 16px; flex-wrap: wrap; margin: 0 0 20px;
    }
    .st-toolbar .st-nav {
      display: flex; align-items: center; gap: 8px;
    }
    .st-toolbar .st-nav .view-switch { margin-right: 0; }
    .st-kpis {
      display: grid; grid-template-columns: 1.6fr 1fr 1fr 1fr;
      gap: 10px; margin: 0 0 16px;
    }
    .st-kpi {
      background: #17171c; border: 1px solid var(--line); border-radius: 10px;
      padding: 14px 16px;
    }
    .st-kpi.hero { padding: 16px 18px 18px; }
    .st-kpi b, .st-hero-value {
      display: block; font-size: 22px; font-weight: 600; letter-spacing: -0.04em;
      font-family: Inter, ui-sans-serif, system-ui, sans-serif;
    }
    .st-kpi span, .st-kicker {
      display: block; color: var(--muted); font-size: 11px;
      text-transform: uppercase; letter-spacing: 0.06em; font-weight: 600;
    }
    .st-kpi span { margin-top: 6px; }
    .st-kicker { margin-bottom: 6px; }
    .st-hero-split {
      height: 8px; border-radius: 99px; overflow: hidden; display: flex;
      background: #26262e; margin: 12px 0 8px;
    }
    .st-hero-split i { display: block; height: 100%; flex: 0 0 auto; min-width: 0; pointer-events: none; }
    .st-hero-split i.in, .st-fill i.in { background: var(--tok-in); }
    .st-hero-split i.out, .st-fill i.out { background: var(--tok-out); }
    .st-hero-legend {
      display: flex; gap: 14px; color: var(--muted); font-size: 12px;
    }
    .st-hero-legend em {
      font-style: normal; font-family: "IBM Plex Mono", ui-monospace, monospace;
      color: var(--ink); font-weight: 500;
    }
    .st-swatch {
      display: inline-block; width: 8px; height: 8px; border-radius: 99px;
      margin-right: 6px; vertical-align: middle;
    }
    .st-swatch.in { background: var(--tok-in); }
    .st-swatch.out { background: var(--tok-out); }
    .st-swatch.ms { background: #818cf8; }
    .st-swatch.rest { background: #f59e0b; }
    .st-grid {
      display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
      gap: 14px; margin: 0 0 14px;
    }
    .st-card {
      background: #17171c; border: 1px solid var(--line); border-radius: 10px;
      padding: 16px 18px 18px; margin: 0 0 14px; min-width: 0;
    }
    .st-grid .st-card { margin: 0; }
    .st-card-head {
      display: flex; align-items: flex-start; justify-content: space-between;
      gap: 16px; margin: 0 0 14px;
    }
    .st-card h2 {
      margin: 0; font-size: 15px; font-weight: 600; letter-spacing: -0.02em;
    }
    .st-head-stat { text-align: right; flex: 0 0 auto; }
    .st-head-stat b {
      display: block; font-size: 20px; font-weight: 600; letter-spacing: -0.03em;
      color: #c4b5fd;
    }
    .st-head-stat span {
      color: var(--muted); font-size: 11px; text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .st-split {
      height: 18px; border-radius: 7px; overflow: hidden; display: flex;
      background: #26262e; margin: 0 0 12px; gap: 2px;
    }
    .st-split button {
      border: 0; padding: 0; height: 100%; cursor: pointer; min-width: 6px;
      background: var(--accent);
    }
    .st-split button.s0 { background: #7c5cff; }
    .st-split button.s1 { background: #a78bfa; }
    .st-split button.s2 { background: #6366f1; }
    .st-split button.s3 { background: #818cf8; }
    .st-split button.s4 { background: #c4b5fd; }
    .st-split button.dim { opacity: 0.28; }
    .st-chips { display: flex; flex-wrap: wrap; gap: 8px; margin: 0 0 12px; }
    .st-chip {
      border: 1px solid var(--line); background: #121216; color: var(--ink);
      border-radius: 8px; padding: 6px 10px; cursor: pointer;
      font: 500 12px Inter, sans-serif; display: flex; align-items: center; gap: 8px;
    }
    .st-chip:hover, .st-chip.active { border-color: #4a4a58; background: #1c1c24; }
    .st-chip i {
      width: 8px; height: 8px; border-radius: 99px; flex: 0 0 8px;
    }
    .st-chip i.s0 { background: #7c5cff; }
    .st-chip i.s1 { background: #a78bfa; }
    .st-chip i.s2 { background: #6366f1; }
    .st-chip i.s3 { background: #818cf8; }
    .st-chip i.s4 { background: #c4b5fd; }
    .st-chip b {
      font-family: "IBM Plex Mono", ui-monospace, monospace;
      font-weight: 500; font-size: 11px; color: var(--muted);
    }
    .st-why { display: grid; gap: 12px; }
    .st-why-group { display: grid; gap: 4px; }
    .st-why-row {
      display: grid; grid-template-columns: minmax(0, 1fr) 40px;
      gap: 8px; align-items: center;
    }
    .st-why-row.choice { grid-template-columns: minmax(0, 1fr) 42px 40px; }
    .st-why-row.choice .st-why-text {
      color: var(--ink); font-weight: 600; font-size: 13px;
    }
    .st-why-text {
      min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
      font-size: 12px; color: var(--muted);
    }
    .st-why-pct {
      font-family: "IBM Plex Mono", ui-monospace, monospace;
      font-size: 11px; color: var(--muted); text-align: right;
    }
    .st-why-n {
      font-family: "IBM Plex Mono", ui-monospace, monospace;
      font-size: 11px; color: var(--ink); text-align: right;
    }
    .st-why-bar {
      grid-column: 1 / -1; height: 3px; background: #26262e; border-radius: 99px;
      overflow: hidden;
    }
    .st-why-bar i { display: block; height: 100%; background: var(--accent); opacity: 0.7; }
    .st-why-sub {
      display: grid; gap: 4px;
      margin: 2px 0 0 6px;
      padding: 4px 0 0 10px;
      border-left: 1px solid var(--line);
    }
    .st-why-eg {
      font-size: 11px; color: #6b6b78;
      overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    }
    .st-rank { display: grid; gap: 10px; }
    .st-rank-row { display: grid; gap: 6px; min-width: 0; }
    .st-rank-top {
      display: flex; align-items: baseline; justify-content: space-between; gap: 10px;
    }
    .st-label {
      min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
      font-size: 12px; color: var(--ink); font-weight: 500;
    }
    .st-meta {
      font-family: "IBM Plex Mono", ui-monospace, monospace;
      font-size: 11px; color: var(--muted); white-space: nowrap;
    }
    .st-track {
      height: 8px; background: #26262e; border-radius: 99px; overflow: hidden;
      display: flex; min-width: 0;
    }
    .st-fill { height: 100%; display: flex; min-width: 2px; }
    .st-fill i { display: block; height: 100%; }
    .st-table-wrap { overflow: auto; }
    .st-table { width: 100%; border-collapse: collapse; font-size: 12px; }
    .st-table th {
      text-align: left; font-size: 11px; font-weight: 600; letter-spacing: 0.05em;
      text-transform: uppercase; color: var(--muted); padding: 0 10px 10px;
      border-bottom: 1px solid var(--line);
    }
    .st-table td { padding: 10px; border-bottom: 1px solid #222228; vertical-align: middle; }
    .st-table tr.run { cursor: pointer; }
    .st-table tr.run:hover td { background: #1c1c22; }
    .st-table .num {
      font-family: "IBM Plex Mono", ui-monospace, monospace; font-size: 11px; text-align: right;
    }
    .st-mini { width: 120px; }
    .st-empty {
      padding: 48px 24px; text-align: center; color: var(--muted);
      background: #17171c; border: 1px dashed var(--line); border-radius: 10px;
    }
    .st-empty strong { display: block; color: var(--ink); font-size: 15px; margin-bottom: 6px; }
    .st-map { margin: 0 0 18px; }
    .st-map-title {
      margin: 0 0 8px; font-size: 13px; font-weight: 600; color: var(--muted);
      letter-spacing: 0.04em; text-transform: uppercase;
    }
    .st-sum, .st-cost-sum {
      display: grid;
      grid-template-columns: max-content max-content minmax(0, 1fr);
      align-items: end;
      gap: 8px 28px;
      margin: 0 0 10px;
      padding: 12px 14px;
      min-height: 64px;
      box-sizing: border-box;
      background: #17171c; border: 1px solid var(--line); border-radius: 10px;
    }
    .st-sum .fig b, .st-cost-sum .fig b {
      display: block; font-size: 20px; font-weight: 600; letter-spacing: -0.04em;
      line-height: 1.1;
    }
    .st-sum .fig span, .st-cost-sum .fig span {
      display: block; margin-top: 2px; color: var(--muted);
      font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; font-weight: 600;
    }
    .st-sum .split, .st-cost-sum .split { min-width: 0; position: relative; }
    .st-sum .split .st-hero-split, .st-cost-sum .split .st-hero-split {
      margin: 0 0 8px; width: 100%;
      cursor: help;
    }
    .st-hero-split i.ok, .st-split-tip i.ok { background: #4ade80; }
    .st-hero-split i.fail, .st-split-tip i.fail { background: var(--danger); }
    .st-hero-split i.canceled, .st-split-tip i.canceled { background: #a8a29e; }
    .st-hero-split i.paused, .st-split-tip i.paused { background: #fbbf24; }
    .st-hero-split i.avg, .st-hero-split i.ms, .st-split-tip i.avg { background: #818cf8; }
    .st-hero-split i.max, .st-hero-split i.rest, .st-split-tip i.max { background: #f59e0b; }
    .st-split-tip i.in { background: var(--tok-in); }
    .st-split-tip i.out { background: var(--tok-out); }
    .st-split-tip {
      display: none;
      position: absolute;
      left: 0;
      top: calc(100% + 6px);
      z-index: 30;
      padding: 8px 10px;
      background: #1c1c22;
      border: 1px solid #3a3a44;
      border-radius: 8px;
      box-shadow: 0 12px 28px rgba(0, 0, 0, 0.4);
      pointer-events: none;
      gap: 6px;
    }
    .st-hero-split:hover + .st-split-tip { display: grid; }
    .st-split-tip-row {
      display: flex; align-items: center; gap: 8px;
      color: var(--ink); font-size: 12px; white-space: nowrap; line-height: 1.2;
    }
    .st-split-tip i {
      width: 8px; height: 8px; border-radius: 99px; flex: 0 0 8px;
    }
    .st-sum .meta, .st-cost-sum .meta {
      font-size: 12px; color: var(--muted);
      overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    }
    .st-graph {
      position: relative;
      min-height: 360px;
      background: #121216;
      border: 1px solid var(--line);
      border-radius: 10px;
      overflow: auto;
      scrollbar-gutter: stable;
    }
    .st-graph .gzoom {
      display: flex;
      justify-content: center;
      padding: 8px 12px 28px;
    }
    .st-why-panel {
      margin-top: 12px; padding: 12px 14px;
      background: #17171c; border: 1px solid var(--line); border-radius: 10px;
    }
    .st-why-panel h3 {
      margin: 0 0 8px; font-size: 13px; font-weight: 600;
    }
    @media (max-width: 900px) {
      .st-kpis, .st-grid { grid-template-columns: 1fr; }
    }
    .cmp-pickers {
      display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
      gap: 16px; margin-bottom: 22px; max-width: 100%;
    }
    .cmp-pick {
      min-width: 0; overflow: hidden;
      background: #17171c; border: 1px solid var(--line); border-radius: 10px;
      padding: 12px 14px 14px;
    }
    .cmp-pick.run-a { border-color: #3d3566; }
    .cmp-pick.run-b { border-color: #1e3a32; }
    .cmp-pick label {
      display: block; font-size: 11px; font-weight: 600; letter-spacing: 0.06em;
      text-transform: uppercase; color: var(--muted); margin-bottom: 8px;
    }
    .cmp-pickers select {
      display: block; width: 100%; max-width: 100%; min-width: 0; box-sizing: border-box;
      height: 34px; border-radius: 8px; border: 1px solid var(--line);
      background: var(--panel); color: var(--ink); font: 12px Inter, sans-serif;
      padding: 0 28px 0 8px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    }
    .cmp-section {
      background: #17171c; border: 1px solid var(--line); border-radius: 10px;
      padding: 12px 14px 10px; margin: 0 0 14px; overflow: hidden; max-width: 100%;
    }
    .cmp-section h2 { margin-top: 0; }
    .cmp-head, .cmp-row {
      display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
      gap: 16px; width: 100%; align-items: start;
    }
    .cmp-head {
      padding: 0 0 8px; margin-bottom: 6px; border-bottom: 1px solid #2a2a32;
      font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em;
      color: var(--muted);
    }
    .cmp-row { padding: 4px 0; }
    .cmp-side {
      display: grid; grid-template-columns: minmax(108px, 0.38fr) minmax(0, 1fr);
      gap: 8px; min-width: 0; align-items: start;
    }
    .cmp-row.changed .cmp-a, .cmp-row.changed .cmp-b { color: #fbbf24; }
    .cmp-row.only-a .cmp-a { color: var(--danger); }
    .cmp-row.only-b .cmp-b { color: #86efac; }
    .cmp-k { color: var(--muted); font-size: 11px; padding-top: 8px; min-width: 0; overflow: hidden; text-overflow: ellipsis; }
    .cmp-a, .cmp-b {
      position: relative;
      font-family: "IBM Plex Mono", ui-monospace, monospace; font-size: 11px;
      white-space: pre-wrap; overflow-wrap: anywhere; word-break: break-word;
      border-radius: 8px; padding: 8px 10px; min-height: 34px; min-width: 0;
      width: 100%; max-width: 100%; box-sizing: border-box;
      border: 1px solid #2a2a32;
    }
    .cmp-a { background: #16141f; }
    .cmp-b { background: #121916; }
    .cmp-a:empty::before, .cmp-b:empty::before { content: "—"; color: #5c5c66; }
    #fileChanges {
      position: fixed; right: 16px; bottom: 16px; z-index: 40;
      width: min(360px, calc(100vw - 32px));
      background: var(--raised); border: 1px solid var(--line); border-radius: 10px;
      box-shadow: 0 12px 40px rgba(0, 0, 0, 0.4);
    }
    #fileChanges[hidden] { display: none; }
    .fc-head {
      display: flex; align-items: center; justify-content: space-between;
      padding: 8px 10px; border-bottom: 1px solid var(--line);
      font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em;
      color: var(--muted);
    }
    .fc-head button {
      border: 0; background: transparent; color: var(--muted);
      font: 500 11px Inter, sans-serif; cursor: pointer;
    }
    .fc-head button:hover { color: var(--ink); }
    #fcList { max-height: 240px; overflow: auto; padding: 6px; }
    .fc-item {
      display: grid; gap: 2px; padding: 8px 8px; margin-bottom: 4px;
      border-radius: 8px; background: var(--panel); color: var(--ink);
    }
    .fc-item:last-child { margin-bottom: 0; }
    .fc-row { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
    .fc-kind {
      font-size: 10px; font-weight: 500; text-transform: lowercase;
      border-radius: 999px; padding: 1px 7px; flex: 0 0 auto;
    }
    .fc-item.added .fc-kind { color: #86efac; background: rgba(34, 197, 94, 0.16); }
    .fc-item.modified .fc-kind { color: #93c5fd; background: rgba(59, 130, 246, 0.16); }
    .fc-item.hash_changed .fc-kind { color: #fbbf24; background: rgba(251, 191, 36, 0.16); }
    .fc-item.removed .fc-kind { color: #fca5a5; background: rgba(240, 113, 120, 0.16); }
    .fc-name { font-size: 12px; font-weight: 500; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .fc-hash {
      color: var(--muted); font-size: 11px;
      font-family: "IBM Plex Mono", ui-monospace, monospace;
    }
    .fc-item button {
      border: 0; background: transparent; color: var(--muted); cursor: pointer;
      font-size: 14px; line-height: 1; padding: 0 2px;
    }
    .fc-item button:hover { color: var(--ink); }
    .warn {
      width: min(480px, 100%);
      background: var(--raised);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 18px 20px 16px;
      box-shadow: 0 24px 80px rgba(0, 0, 0, 0.45);
    }
    .warn h3 { margin: 0 0 8px; font-size: 16px; }
    .warn p { margin: 0 0 10px; color: var(--muted); font-size: 13px; }
    .warn-hash {
      font-family: "IBM Plex Mono", ui-monospace, monospace;
      font-size: 12px; color: var(--ink);
      background: var(--panel); border-radius: 8px; padding: 8px 10px;
      margin: 0 0 10px;
    }
    .warn-hash div { display: flex; justify-content: space-between; gap: 12px; padding: 2px 0; }
    .warn-hash span { color: var(--muted); }
    .warn ul { margin: 0 0 14px; padding-left: 18px; color: var(--ink); }
    .warn li { margin: 3px 0; font-size: 12px; }
    .warn-actions { display: flex; justify-content: flex-end; gap: 8px; }
    @media (max-width: 900px) { .layout, .grid, .io, .cmp-pickers, .cmp-grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <span class="mark">GVA</span> GraphVIAgent
      <nav class="nav">
        <button class="nav-link active" id="navTrace" type="button">Trace</button>
        <button class="nav-link" id="navPipelines" type="button">Runs</button>
        <button class="nav-link" id="navCompare" type="button">Compare</button>
        <button class="nav-link" id="navStats" type="button">Statistics</button>
      </nav>
    </div>
  </header>
  <section id="statsView" class="page" hidden>
    <div class="page-inner">
      <div class="st-toolbar">
        <div class="cmp-filter">
          <label for="statsPipe">Pipeline</label>
          <select id="statsPipe">
            <option value="">Select a pipeline</option>
          </select>
        </div>
        <span class="st-nav">
          <span class="view-switch">
            <button type="button" class="ghost jump-trace" id="gotoTraceFromStats">Trace</button>
            <button type="button" class="ghost jump-runs" id="gotoRunsFromStats">Runs</button>
          </span>
          <span class="view-switch">
            <button type="button" class="ghost active" id="statsTabRouting">Routing</button>
            <button type="button" class="ghost" id="statsTabCost">Cost</button>
            <button type="button" class="ghost" id="statsTabRuntime">Runtime</button>
          </span>
        </span>
      </div>
      <div id="statsBoard"></div>
    </div>
  </section>
  <section id="compareView" class="page" hidden>
    <div class="page-inner">
      <div class="cmp-filter">
        <label for="cmpPipe">Pipeline</label>
        <select id="cmpPipe">
          <option value="all">all</option>
        </select>
      </div>
      <div class="cmp-pickers">
        <div class="cmp-pick run-a">
          <label for="cmpA">Run A</label>
          <select id="cmpA"></select>
        </div>
        <div class="cmp-pick run-b">
          <label for="cmpB">Run B</label>
          <select id="cmpB"></select>
        </div>
      </div>
      <div id="cmpBoard"></div>
    </div>
  </section>
  <section id="pipelinesView" class="page" hidden>
    <div class="page-inner">
      <div id="pipeBoard"></div>
    </div>
  </section>
  <div id="traceView" class="layout">
    <aside>
      <h2>Pipelines</h2>
      <div id="pipelines"></div>
      <h2>Runs</h2>
      <div class="hist-tools">
        <input id="runFilter" type="search" placeholder="search input fields…"/>
        <select id="runStatus">
          <option value="all">all</option>
          <option value="running">running</option>
          <option value="queued">queued</option>
          <option value="ok">success</option>
          <option value="paused">paused</option>
          <option value="canceled">canceled</option>
          <option value="error">failed</option>
        </select>
      </div>
      <div id="history" class="empty history-box">Select a pipeline</div>
      <input id="importFile" type="file" accept="application/json,.json" hidden/>
    </aside>
    <main>
      <h2>Input</h2>
      <div class="copy-wrap">
        <textarea id="input">{}</textarea>
        <button type="button" class="copy-btn">Copy</button>
        <div class="resize-s" id="inputResize" aria-hidden="true"></div>
      </div>
      <div id="exampleChips" class="chips"></div>
      <div class="toolbar">
        <button class="primary" id="runBtn">Run</button>
        <span id="runLive" class="run-live" hidden>Running</span>
        <button class="ghost cancel" id="cancelBtn" disabled>Cancel</button>
        <button class="ghost" id="continueBtn" disabled>Continue</button>
        <button class="ghost" id="stepBtn" disabled>Step</button>
        <button class="ghost" id="replayBtn" disabled>Replay step</button>
        <button class="ghost" id="replayFromBtn" disabled>Replay from</button>
        <button class="ghost" id="exportBtn" disabled>Export</button>
        <button class="ghost" id="importBtn">Import</button>
        <button class="ghost jump-runs" id="gotoRunsFromTrace">Runs</button>
        <button class="ghost jump-stats" id="gotoStatsFromTrace">Statistics</button>
        <button class="ghost danger" id="deleteBtn">Delete run</button>
      </div>
      <div id="pauseBanner" class="pause-banner" hidden></div>
      <div id="hitlPanel" class="hitl-panel" hidden>
        <label for="hitlValue">Resume value</label>
        <div id="hitlHint" class="hitl-hint"></div>
        <textarea id="hitlValue" spellcheck="false">true</textarea>
      </div>
      <div class="grid">
        <div class="pane">
          <h2 class="pane-title">Graph
            <span style="display:flex;align-items:center">
              <span class="view-switch">
                <button type="button" class="ghost active" id="viewGraph">Graph</button>
                <button type="button" class="ghost" id="viewGantt">Gantt</button>
                <button type="button" class="ghost" id="viewAgents">Agents</button>
              </span>
              <span class="graph-zoom">
                <button type="button" class="ghost" id="zoomOut" title="Zoom out">−</button>
                <button type="button" class="ghost" id="zoomFit" title="Fit to window">Fit</button>
                <button type="button" class="ghost" id="zoomIn" title="Zoom in">+</button>
              </span>
            </span>
          </h2>
          <div id="diagram" class="empty">Select a run to inspect the unrolled path. Double-click a node to open its view.</div>
        </div>
        <div class="pane">
          <h2 class="pane-title">Steps
            <span class="steps-meta">
              <span class="spark-legend">
                <span data-metric="ms"><i class="ms"></i> time</span>
                <span data-metric="mb"><i class="mb"></i> mem</span>
                <span data-metric="peak"><i class="peak"></i> peak</span>
                <span data-metric="prompt"><i class="prompt"></i> in</span>
                <span data-metric="completion"><i class="completion"></i> out</span>
                <span data-metric="tool"><i class="tool"></i> tool</span>
              </span>
              <span id="stepsMs"></span>
            </span>
          </h2>
          <div id="steps" class="waterfall"></div>
          <details class="tree-details section">
            <summary>Final state</summary>
            <div id="state" class="json"></div>
          </details>
          <details class="tree-details section" id="logDetails">
            <summary>Log</summary>
            <div id="runLog" class="run-log"></div>
          </details>
          <details class="tree-details section">
            <summary>Memory</summary>
            <div id="memory" class="json empty">Select a run to see which state keys were written and read.</div>
          </details>
          <details class="tree-details section">
            <summary>Text tree</summary>
            <pre id="ascii"></pre>
          </details>
        </div>
      </div>
    </main>
  </div>
  <div id="nodeView" class="nv-overlay" hidden>
    <div class="nv">
      <div class="nv-head">
        <div>
          <div class="nv-kicker" id="nvKicker">Node view</div>
          <h3 id="nvTitle">Node</h3>
          <div class="nv-meta" id="nvMeta"></div>
        </div>
        <button class="ghost" id="nvClose">Close</button>
      </div>
      <div class="io">
        <div class="pane">
          <h2>Node input <span id="ioName"></span></h2>
          <div id="stepIn" class="json empty">Double-click a node to inspect it.</div>
          <div class="copy-wrap">
            <textarea id="nodeInput" spellcheck="false"></textarea>
            <button type="button" class="copy-btn">Copy</button>
          </div>
          <div class="toolbar" id="nodeActions">
            <button class="primary" id="callBtn">Call node</button>
            <button class="ghost" id="replayFromHereBtn">Replay from here</button>
          </div>
        </div>
        <div class="pane">
          <h2>Node output</h2>
          <div id="stepOut" class="json empty">The keys this node returned.</div>
        </div>
        <div class="pane io-diff" hidden>
          <h2 class="pane-title">State diff
            <span class="diff-legend">
              <span><i class="changed"></i> changed</span>
              <span><i class="added"></i> added</span>
              <span><i class="removed"></i> removed</span>
              <span><i class="same"></i> unchanged</span>
            </span>
          </h2>
          <div id="stateDiff" class="json empty">Select a node to see what changed in state.</div>
        </div>
        <div class="pane io-tools" hidden>
          <h2>Tools</h2>
          <div id="stepTools" class="json empty">Select a node to see tool calls.</div>
        </div>
        <div class="pane io-trace" hidden>
          <h2>Traceback</h2>
          <div id="stepTrace" class="trace empty">No traceback for this node.</div>
        </div>
      </div>
    </div>
  </div>
  <div id="fileChanges" hidden>
    <div class="fc-head">
      <span>File changes</span>
      <button type="button" id="fcClear">Clear</button>
    </div>
    <div id="fcList"></div>
  </div>
  <div id="outdatedModal" class="nv-overlay" hidden>
    <div class="warn" role="dialog" aria-labelledby="outdatedTitle">
      <h3 id="outdatedTitle">Pipeline changed</h3>
      <p>This run is outdated. Replay will use the current graph.</p>
      <div class="warn-hash" id="outdatedHashes"></div>
      <ul id="outdatedDiffs"></ul>
      <div class="warn-actions">
        <button type="button" class="ghost" id="outdatedCancel">Cancel</button>
        <button type="button" class="primary" id="outdatedGo">Replay anyway</button>
      </div>
    </div>
  </div>
  <script>
    let pipelines = [];
    let fileId = null;
    let currentRun = null;
    let selectedStep = null;
    let nodeViewMode = "all";
    let currentView = "trace";
    let historyRuns = [];
    let scanKey = "";
    let scanFiles = [];
    let changeSeq = 0;
    let fileChangeEvents = [];
    let compareRuns = [];
    let statsData = { pipelines: [], runs: [], nodes: [], decisions: [], edges: [] };
    let topoMarkerSeq = 0;
    let statsTab = "routing";
    let statsChoice = null;
    let statsPipePreferred = "";
    let runsFocusStem = "";
    let graphZoom = 1;
    let graphPanX = 0;
    let graphPanY = 0;
    let graphViewMode = "graph";
    let inflightRuns = [];
    let pendingCancel = null;
    let runLimit = 3;
    let queueLimit = 12;
    let slotActive = 0;
    let slotQueued = 0;
    let breakpoints = {};

    const $ = (id) => document.getElementById(id);

    function bpStorageKey() {
      return fileId ? "gva.bp:" + fileId : "";
    }

    function loadBreakpoints() {
      breakpoints = {};
      const key = bpStorageKey();
      if (!key) return;
      try {
        const raw = JSON.parse(localStorage.getItem(key) || "[]");
        if (Array.isArray(raw)) {
          raw.forEach((name) => {
            if (name) breakpoints[String(name)] = true;
          });
        }
      } catch (err) {}
    }

    function saveBreakpoints() {
      const key = bpStorageKey();
      if (!key) return;
      localStorage.setItem(key, JSON.stringify(interruptBeforeList()));
    }

    function interruptBeforeList() {
      return Object.keys(breakpoints).filter((name) => breakpoints[name]);
    }

    function paintBreakpointDots() {
      document.querySelectorAll(".bp-dot[data-node]").forEach((el) => {
        const name = el.dataset.node;
        const on = Boolean(breakpoints[name]);
        el.classList.toggle("on", on);
        el.title = on ? "Clear breakpoint before " + name : "Break before " + name;
      });
    }

    function toggleBreakpoint(name) {
      if (!name) return;
      if (breakpoints[name]) delete breakpoints[name];
      else breakpoints[name] = true;
      saveBreakpoints();
      paintBreakpointDots();
    }

    function breakpointDot(name) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "bp-dot" + (breakpoints[name] ? " on" : "");
      btn.dataset.node = name;
      btn.title = breakpoints[name] ? "Clear breakpoint before " + name : "Break before " + name;
      btn.onclick = (event) => {
        event.preventDefault();
        event.stopPropagation();
        toggleBreakpoint(name);
      };
      return btn;
    }

    function hitlPayloads(run) {
      const items = run && run.interrupts;
      return Array.isArray(items) ? items : [];
    }

    function runIsCanceled(run) {
      if (!run) return false;
      if (run.canceled || run.status === "canceled") return true;
      const err = String(run.error || "").trim().toLowerCase();
      return err === "cancelled" || err === "canceled";
    }

    function runIsPaused(run) {
      return Boolean(
        run &&
        run.paused &&
        !run.error &&
        !runIsCanceled(run) &&
        !runIsQueued(run) &&
        !runIsLive(run)
      );
    }

    function runIsQueued(run) {
      return Boolean(run && (run.queued || run.status === "queued"));
    }

    function runIsLive(run) {
      return Boolean(run && (run.live || run.status === "running" || run.status === "canceling"));
    }

    function runIsBusy(run) {
      return runIsQueued(run) || runIsLive(run);
    }

    function formatResumeDefault(run) {
      const hits = hitlPayloads(run);
      if (!hits.length) return "true";
      try {
        return JSON.stringify(hits.length === 1 ? hits[0] : hits, null, 2);
      } catch (err) {
        return "true";
      }
    }

    function pauseBannerText(run) {
      const nxt = (run && run.next) || [];
      if (hitlPayloads(run).length) {
        return nxt.length
          ? "Waiting for a resume value — next: " + nxt.join(", ")
          : "Waiting for a resume value";
      }
      return nxt.length ? "Paused before " + nxt.join(", ") : "Paused";
    }

    function currentLiveJob() {
      return inflightRuns.find((job) => currentRun && job.id === currentRun.id) || null;
    }

    function currentIsCanceling() {
      const job = currentLiveJob();
      if (job && job.canceling) return true;
      if (currentRun && currentRun.status === "canceling") return true;
      return Boolean(pendingCancel && currentRun && pendingCancel.runId === currentRun.id);
    }

    function pipelineIsCanceling(p) {
      if (!p) return false;
      const jobs = inflightRuns.filter((job) => job.fileId === p.id);
      if (jobs.length) return jobs.every((job) => job.canceling);
      return Boolean(pendingCancel && pendingCancel.fileId === p.id);
    }

    function syncCancelButton() {
      const btn = $("cancelBtn");
      if (!btn) return;
      const canceling = currentIsCanceling();
      btn.textContent = canceling ? "Canceling" : "Cancel";
      btn.classList.toggle("canceling", canceling);
      btn.disabled = canceling || !(currentRun && currentRun.id && (
        currentLiveJob() || runIsBusy(currentRun) || runIsPaused(currentRun)
      ));
    }

    function syncPauseControls() {
      const banner = $("pauseBanner");
      const panel = $("hitlPanel");
      const paused = runIsPaused(currentRun);
      const hits = paused ? hitlPayloads(currentRun) : [];
      const nxt = (currentRun && currentRun.next) || [];
      $("continueBtn").disabled = !paused;
      $("stepBtn").disabled = !paused || !nxt.length || hits.length > 0;
      syncCancelButton();
      if (!paused) {
        if (banner) banner.hidden = true;
        if (panel) panel.hidden = true;
        return;
      }
      if (banner) {
        banner.hidden = false;
        banner.textContent = pauseBannerText(currentRun);
      }
      if (panel) {
        panel.hidden = !hits.length;
        if (hits.length) {
          $("hitlHint").textContent = JSON.stringify(hits, null, 2);
          $("hitlValue").value = formatResumeDefault(currentRun);
        }
      }
    }

    function markNextNodes(run) {
      document.querySelectorAll(".g-card.next").forEach((el) => {
        el.classList.remove("next");
        if (!el.classList.contains("taken")) el.classList.remove("running");
      });
      if (!runIsPaused(run)) return;
      (run.next || []).forEach((name) => {
        document.querySelectorAll(nodeCardSel(name)).forEach((card) => {
          card.classList.add("next", "running");
          card.classList.remove("idle");
        });
      });
    }

    function escapeHtml(value) {
      return String(value ?? "").replace(/[&<>"']/g, (ch) => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
      }[ch]));
    }

    function sourceMap(run) {
      const spec = graphSpec(run || currentRun);
      return (spec && spec.source) || {};
    }

    function nodeSource(stepOrName, run) {
      if (stepOrName && typeof stepOrName === "object") {
        if (stepOrName.source && stepOrName.source.file) return stepOrName.source;
        const mapped = sourceMap(run)[stepOrName.node];
        return mapped || null;
      }
      return sourceMap(run)[stepOrName] || null;
    }

    function sourceLabel(loc) {
      if (!loc || !loc.file) return "";
      const parts = String(loc.file).replace(/\\/g, "/").split("/");
      const base = parts[parts.length - 1] || loc.file;
      return loc.line ? base + ":" + loc.line : base;
    }

    function editorHref(loc) {
      if (!loc || !loc.file) return "";
      const path = String(loc.file).replace(/\\/g, "/");
      const line = loc.line || 1;
      const prefixed = path.charAt(1) === ":" ? "/" + path : path;
      return "vscode://file" + prefixed + ":" + line;
    }

    function sourceChipHtml(loc) {
      if (!loc || !loc.file) return "";
      const href = editorHref(loc);
      const label = sourceLabel(loc);
      const title = loc.file + (loc.line ? ":" + loc.line : "");
      return '<a class="src-link" href="' + escapeHtml(href) + '" data-src="1" title="' +
        escapeHtml(title) + '">' + escapeHtml(label) + "</a>";
    }

    const EN_MONTHS = ["Jan.", "Feb.", "Mar.", "Apr.", "May", "June", "July", "Aug.", "Sept.", "Oct.", "Nov.", "Dec."];

    function pad2(value) {
      return String(value).padStart(2, "0");
    }

    function formatClock(date) {
      return pad2(date.getHours()) + ":" + pad2(date.getMinutes());
    }

    function formatDayEn(date) {
      return EN_MONTHS[date.getMonth()] + " " + date.getDate();
    }

    function formatTime(iso) {
      if (!iso) return "";
      const d = new Date(iso);
      if (Number.isNaN(d.getTime())) return iso.slice(0, 16);
      return formatClock(d) + "  " + formatDayEn(d);
    }

    function formatScaled(value, units) {
      if (!Number.isFinite(value)) return "";
      const zeroUnit = units.find((unit) => unit.zero) || units[units.length - 1];
      if (value === 0) return "0" + zeroUnit.suffix;
      for (let i = 0; i < units.length; i++) {
        const unit = units[i];
        const last = i === units.length - 1;
        const scaled = value / unit.size;
        const shown = unit.digits === 0
          ? Math.round(scaled)
          : Number(scaled.toFixed(unit.digits));
        if (shown === 0 && !last) continue;
        if (!last && scaled < 1 && !unit.fraction) continue;
        if (unit.digits === 0) return shown + unit.suffix;
        return shown.toFixed(unit.digits).replace(/\.?0+$/, "") + unit.suffix;
      }
      return "0" + zeroUnit.suffix;
    }

    function formatElapsed(ms) {
      const value = Number(ms);
      if (!Number.isFinite(value)) return "";
      return formatScaled(value, [
        { size: 1000, suffix: "s", digits: 2 },
        { size: 1, suffix: "ms", digits: 1, fraction: true, zero: true },
        { size: 0.001, suffix: "µs", digits: 0 },
      ]);
    }

    function formatMemory(mb) {
      if (mb == null || !Number.isFinite(Number(mb))) return "—";
      const bytes = Number(mb) * 1024 * 1024;
      return formatScaled(bytes, [
        { size: 1024 * 1024 * 1024, suffix: " GB", digits: 2 },
        { size: 1024 * 1024, suffix: " MB", digits: 2, fraction: true },
        { size: 1024, suffix: " KB", digits: 1, fraction: true },
        { size: 1, suffix: " B", digits: 0, zero: true },
      ]);
    }

    function stepMetrics(step) {
      if (!step) {
        return {
          ms: null, mb: null, peak: null, tokens: null,
          prompt: null, completion: null, toolTokens: null, tool: null,
          tokenUnavailable: false,
        };
      }
      const tokens = step.tokens;
      const unavailable = !!(tokens && tokens.unavailable);
      const hasUsage = !!(tokens && !unavailable);
      return {
        ms: step.elapsed_ms != null ? Number(step.elapsed_ms) : null,
        mb: step.memory_mb != null ? Number(step.memory_mb) : null,
        peak: step.memory_peak_mb != null ? Number(step.memory_peak_mb) : null,
        tokens: hasUsage ? Number(tokens.total || 0) : null,
        prompt: hasUsage ? Number(tokens.prompt || 0) : null,
        completion: hasUsage ? Number(tokens.completion || 0) : null,
        toolTokens: hasUsage && tokens.tool != null ? Number(tokens.tool) : null,
        tool: step.tool_latency_ms != null ? Number(step.tool_latency_ms) : null,
        tokenUnavailable: unavailable,
      };
    }

    function runMetricMax(run) {
      const maxes = { ms: 0, mb: 0, peak: 0, prompt: 0, completion: 0, tool: 0 };
      ((run && run.steps) || []).forEach((step) => {
        const metrics = stepMetrics(step);
        maxes.ms = Math.max(maxes.ms, metrics.ms || 0);
        maxes.mb = Math.max(maxes.mb, metrics.mb || 0);
        maxes.peak = Math.max(maxes.peak, metrics.peak || 0);
        maxes.prompt = Math.max(maxes.prompt, metrics.prompt || 0);
        maxes.completion = Math.max(maxes.completion, metrics.completion || 0);
        maxes.tool = Math.max(maxes.tool, metrics.tool || 0);
      });
      return maxes;
    }

    function formatTokens(metrics) {
      if (metrics && metrics.tokenUnavailable) return "—";
      if (!metrics || metrics.tokens == null) return "—";
      return (metrics.prompt || 0) + " in + " + (metrics.completion || 0) + " out";
    }

    function sparkValueLabel(key, metrics) {
      if (key === "ms") return formatElapsed(metrics.ms) || "—";
      if (key === "mb") return formatMemory(metrics.mb);
      if (key === "peak") return formatMemory(metrics.peak);
      if (key === "prompt") return metrics.prompt == null ? "—" : metrics.prompt + " tok";
      if (key === "completion") return metrics.completion == null ? "—" : metrics.completion + " tok";
      return metrics.tool != null ? formatElapsed(metrics.tool) : "—";
    }

    const SPARK_METRICS = [
      {
        key: "ms",
        cls: "ms",
        label: "time",
        title: "Time",
        help: "Wall-clock duration of this node visit.",
      },
      {
        key: "mb",
        cls: "mb",
        label: "mem",
        title: "Mem",
        help: "Extra Python memory still held when the node finished, versus when it started. Net leftover, not the whole process.",
      },
      {
        key: "peak",
        cls: "peak",
        label: "peak",
        title: "Peak",
        help: "Highest memory this visit reached while it ran. Higher than mem when the node allocated, then freed before finishing.",
      },
      {
        key: "prompt",
        cls: "prompt",
        label: "in",
        title: "In — prompt tokens",
        help: "Tokens sent to the LLM as input. Zero if this node did not call a model.",
      },
      {
        key: "completion",
        cls: "completion",
        label: "out",
        title: "Out — completion tokens",
        help: "Tokens the LLM generated as output. Priced separately from in. Zero if this node did not call a model.",
      },
      {
        key: "tool",
        cls: "tool",
        label: "tool",
        title: "Tool",
        help: "Time spent inside tool calls on this visit. This is latency, not a token count.",
      },
    ];

    function sparkMetric(key) {
      return SPARK_METRICS.find((item) => item.key === key);
    }

    function metricTipHtml(metric, valueLabel) {
      if (!metric) return "";
      return '<div class="tip-name">' + escapeHtml(metric.title) + "</div>" +
        (valueLabel ? '<div class="tip-id">' + escapeHtml(valueLabel) + "</div>" : "") +
        '<div class="tip-why">' + escapeHtml(metric.help) + "</div>";
    }

    function bindMetricTip(el, html) {
      el.addEventListener("mouseenter", (mouse) => placeGanttTip(mouse, html));
      el.addEventListener("mousemove", (mouse) => placeGanttTip(mouse, html));
      el.addEventListener("mouseleave", hideGanttTip);
    }

    function bindSparkHovers(root) {
      if (!root || !root.querySelectorAll) return;
      root.querySelectorAll(".spark:not(.tip-spark) i[data-metric]").forEach((el) => {
        const metric = sparkMetric(el.dataset.metric);
        if (!metric) return;
        bindMetricTip(el, metricTipHtml(metric, el.getAttribute("data-value")));
      });
    }

    function sparkBars(metrics, maxes, extraClass) {
      const memScale = Math.max((maxes && maxes.mb) || 0, (maxes && maxes.peak) || 0);
      const bars = SPARK_METRICS.map((metric) => {
        const value = metrics[metric.key];
        const max = (metric.key === "mb" || metric.key === "peak") ? memScale : ((maxes && maxes[metric.key]) || 0);
        const missing = value == null || !Number.isFinite(value);
        const ratio = !missing && max > 0 ? value / max : 0;
        const height = missing || value === 0 ? 2 : Math.max(3, Math.round(ratio * 22));
        return '<i class="' + metric.cls + (missing || value === 0 ? " zero" : "") +
          '" data-metric="' + metric.key +
          '" data-value="' + escapeHtml(sparkValueLabel(metric.key, metrics)) +
          '" style="height:' + height + 'px"></i>';
      });
      const labeled = extraClass ? '<div class="spark-labels">' +
        SPARK_METRICS.map((item) => "<span>" + item.label + "</span>").join("") + "</div>" : "";
      return '<div class="spark-wrap">' +
        '<div class="spark' + (extraClass ? " " + extraClass : "") + '">' + bars.join("") + "</div>" +
        labeled + "</div>";
    }

    function truncateJson(value) {
      const text = JSON.stringify(value ?? {});
      return text.length > 72 ? text.slice(0, 69) + "..." : text;
    }

    function stepKind(name) {
      const n = (name || "").toLowerCase();
      if (n.includes("choose") || n.includes("check") || n.includes("route") || n.includes("decide")) return "decision";
      if (n.includes("polish") || n.includes("loop")) return "loop";
      return "";
    }

    async function api(path, opts) {
      const res = await fetch(path, { cache: "no-store", ...(opts || {}) });
      const data = await res.json().catch(() => ({}));
      if (res.status === 429) {
        throw new Error(
          data.error ||
          ("queue full (" + (data.queued || 0) + "/" + (data.queue_limit || queueLimit) +
            " waiting, " + (data.active || 0) + "/" + (data.limit || runLimit) + " running)")
        );
      }
      if (!res.ok) throw new Error(data.error || res.statusText);
      return data;
    }

    function pipelineStatus(p) {
      if (pipelineIsCanceling(p)) return "canceling";
      const jobs = inflightRuns.filter((job) => p && job.fileId === p.id);
      if (jobs.some((job) => !job.queued)) return "live";
      if (jobs.length) return "queued";
      if (p && p.last_run && (p.last_run.status === "running" || p.last_run.status === "canceling")) return "live";
      if (p && p.last_run && p.last_run.status === "queued") return "queued";
      if (p && p.error) return "error";
      if (p && p.last_run && p.last_run.status === "error") return "error";
      if (p && p.last_run && p.last_run.status === "canceled") return "canceled";
      if (p && p.last_run && p.last_run.status === "paused") return "paused";
      if (p && p.last_run) return "ok";
      return "gray";
    }

    function runDotClass(run) {
      if (run && (run.status === "canceling" || run.canceling)) return "canceling";
      if (run && (run.status === "queued" || run.queued)) return "queued";
      if (run && (run.live || run.status === "running")) return "running";
      if (runIsCanceled(run) || (run && run.status === "canceled")) return "canceled";
      if (run && (run.status === "paused" || run.paused)) return "paused";
      return run && run.status === "error" ? "error" : "ok";
    }

    function runDateParts(run) {
      const date = run.created_at ? new Date(run.created_at) : null;
      if (!date || Number.isNaN(date.getTime())) return { day: "", time: "", key: "" };
      return {
        day: formatDayEn(date),
        time: formatClock(date),
        key: date.getFullYear() + "-" + date.getMonth() + "-" + date.getDate(),
      };
    }

    function runHoverText(run) {
      if (run && (run.status === "canceling" || run.canceling)) return "canceling";
      if (run && (run.status === "queued" || run.queued)) {
        return run.queue_position ? "queued #" + run.queue_position : "queued";
      }
      if (run && (run.live || run.status === "running")) return "running";
      const when = run.created_at ? new Date(run.created_at) : null;
      const date = when && !Number.isNaN(when.getTime())
        ? formatClock(when) + "  " + formatDayEn(when)
        : (run.created_at || "");
      const kind = runIsCanceled(run)
        ? "canceled"
        : run.status === "error"
          ? "failed"
          : run.status === "paused"
            ? "paused"
            : (run.mode || "run");
      return [date, kind, formatElapsed(run.elapsed_ms)].filter(Boolean).join("  ·  ");
    }

    function taskOrder(runsNewestFirst) {
      const names = [];
      const seen = new Set();
      runsNewestFirst.forEach((run) => {
        (run.steps || []).forEach((step) => {
          if (step.node && !seen.has(step.node)) {
            seen.add(step.node);
            names.push(step.node);
          }
        });
      });
      return names;
    }

    function cellStatus(run, node) {
      const hits = (run.steps || []).filter((step) => step.node === node);
      if (!hits.length) {
        const waiting = (run.next || []).some((name) => name === node);
        if (waiting && (run.status === "paused" || run.paused) && !runIsCanceled(run)) {
          return "paused";
        }
        return "skip";
      }
      if (hits.some((step) => step.status === "error" || step.error)) return "error";
      if (hits.some((step) => step.canceled || step.status === "canceled")) return "canceled";
      if (hits.some((step) => step.pending || step.status === "running")) return "running";
      return "ok";
    }

    function cellLabel(status) {
      if (status === "error") return "✕";
      if (status === "canceled") return "–";
      if (status === "paused") return "❚";
      if (status === "skip") return "○";
      if (status === "running") return "●";
      return "✓";
    }

    function gridTemplate(count) {
      return "200px repeat(" + Math.max(count, 1) + ", minmax(54px, 1fr))";
    }

    function gridSlotCount() {
      const view = $("pipelinesView");
      const width = (view && view.clientWidth) || document.body.clientWidth || 960;
      const avail = width - 28 * 2 - 18 * 2 - 200;
      const fit = Math.floor((avail + 4) / 58);
      return Math.max(1, fit);
    }

    function skelNode(tag, className) {
      const el = document.createElement(tag);
      el.className = className;
      if (tag === "button") el.type = "button";
      el.tabIndex = -1;
      el.setAttribute("aria-hidden", "true");
      return el;
    }

    function eachSlot(placeholders, columns, fn) {
      for (let i = 0; i < placeholders; i += 1) fn(null);
      columns.forEach((run) => fn(run));
    }

    function liveRunsForPipeline(p) {
      return inflightRuns
        .filter((job) => job.fileId === p.id)
        .map((job) => {
          const run = job.run || {};
          return {
            id: job.id,
            input: run.input != null ? run.input : job.input,
            created_at: run.started_at || job.started_at,
            elapsed_ms: run.elapsed_ms,
            status: job.canceling ? "canceling" : job.queued ? "queued" : "running",
            queue_position: job.position,
            queued: Boolean(job.queued),
            mode: "run",
            live: true,
            steps: run.steps || [],
          };
        });
    }

    function boardRunsForPipeline(p) {
      const byId = {};
      (p.recent || []).forEach((row) => {
        if (row && row.id) byId[row.id] = row;
      });
      inflightRuns.filter((job) => job.fileId === p.id).forEach((job) => {
        byId[job.id] = liveHistoryRow(job);
      });
      return Object.keys(byId).map((id) => byId[id]).sort((a, b) => {
        const left = a.created_at || "";
        const right = b.created_at || "";
        if (left === right) return 0;
        return left < right ? 1 : -1;
      });
    }

    let pipeBoardLiveTimer = 0;
    function schedulePipeBoardRefresh() {
      if (currentView !== "runs") return;
      if (pipeBoardLiveTimer) return;
      pipeBoardLiveTimer = requestAnimationFrame(() => {
        pipeBoardLiveTimer = 0;
        renderPipeBoard();
      });
    }

    function renderPipeBoard() {
      const page = $("pipelinesView");
      const keepTop = page && currentView === "runs" ? page.scrollTop : null;
      const board = $("pipeBoard");
      board.innerHTML = "";
      if (!pipelines.length) {
        board.innerHTML = '<p class="empty">No pipelines</p>';
        return;
      }
      const slots = gridSlotCount();
      pipelines.forEach((p) => {
        const card = document.createElement("div");
        card.className = "af-card" + (p.id === fileId || p.stem === runsFocusStem ? " active" : "");
        card.id = "pipe-" + p.stem;
        const newestFirst = boardRunsForPipeline(p);
        const columns = newestFirst.slice(0, slots).slice().reverse();
        const placeholders = Math.max(0, slots - columns.length);
        const last = newestFirst[0];
        const head = document.createElement("div");
        head.className = "af-head";
        const title = document.createElement("button");
        title.type = "button";
        title.className = "af-title";
        title.textContent = p.stem + (p.error ? " (error)" : "");
        title.onclick = () => openPipeline(p.id);
        const lastEl = document.createElement("div");
        lastEl.className = "af-last";
        lastEl.textContent = last ? runHoverText(last) : "No runs";
        const actions = document.createElement("div");
        actions.className = "af-actions";
        const toTrace = document.createElement("button");
        toTrace.type = "button";
        toTrace.className = "ghost jump-trace";
        toTrace.textContent = "Trace";
        toTrace.onclick = () => openPipeline(p.id).catch((e) => alert(e.message));
        const toStats = document.createElement("button");
        toStats.type = "button";
        toStats.className = "ghost jump-stats";
        toStats.textContent = "Statistics";
        toStats.onclick = () => jumpToStatistics(p.stem);
        const clear = document.createElement("button");
        clear.type = "button";
        clear.className = "ghost danger";
        clear.textContent = "Clear";
        clear.disabled = !newestFirst.length || newestFirst.some((run) => runIsBusy(run));
        clear.title = clear.disabled && newestFirst.length ? "Cancel queued or running jobs first" : "";
        clear.onclick = () => clearPipelineRuns(p).catch((e) => alert(e.message));
        actions.appendChild(lastEl);
        actions.appendChild(toTrace);
        actions.appendChild(toStats);
        actions.appendChild(clear);
        head.appendChild(title);
        head.appendChild(actions);
        card.appendChild(head);
        const grid = document.createElement("div");
        grid.className = "af-grid";
        const template = gridTemplate(slots);
        const maxMs = Math.max(...columns.map((run) => Number(run.elapsed_ms) || 0), 1);
        const bars = document.createElement("div");
        bars.className = "af-bars";
        bars.style.gridTemplateColumns = template;
        bars.appendChild(document.createElement("div"));
        eachSlot(placeholders, columns, (run) => {
          if (!run) {
            bars.appendChild(skelNode("div", "af-bar skel"));
            return;
          }
          const bar = document.createElement("button");
          bar.type = "button";
          bar.className = "af-bar " + runDotClass(run);
          const ms = Number(run.elapsed_ms) || 0;
          const liveH = run.live || run.status === "running" || run.status === "canceling" || run.status === "queued" ? 22 : 6;
          bar.style.height = Math.max(liveH, Math.round((ms / maxMs) * 52)) + "px";
          bar.title = runHoverText(run);
          bar.onclick = () => openPipelineRun(p.id, run.id);
          bars.appendChild(bar);
        });
        const days = document.createElement("div");
        days.className = "af-days";
        days.style.gridTemplateColumns = template;
        days.appendChild(document.createElement("div"));
        const times = document.createElement("div");
        times.className = "af-times";
        times.style.gridTemplateColumns = template;
        times.appendChild(document.createElement("div"));
        let previousDay = "";
        eachSlot(placeholders, columns, (run) => {
          if (!run) {
            days.appendChild(skelNode("div", "af-day skel"));
            times.appendChild(skelNode("div", "af-time skel"));
            return;
          }
          const parts = runDateParts(run);
          const day = document.createElement("button");
          day.type = "button";
          day.className = "af-day";
          day.textContent = parts.key && parts.key !== previousDay ? parts.day : "";
          day.title = runHoverText(run);
          day.onclick = () => openPipelineRun(p.id, run.id);
          days.appendChild(day);
          const time = document.createElement("button");
          time.type = "button";
          time.className = "af-time";
          time.textContent = parts.time;
          time.title = runHoverText(run);
          time.onclick = () => openPipelineRun(p.id, run.id);
          times.appendChild(time);
          previousDay = parts.key || previousDay;
        });
        const plays = document.createElement("div");
        plays.className = "af-plays";
        plays.style.gridTemplateColumns = template;
        plays.appendChild(document.createElement("div"));
        eachSlot(placeholders, columns, (run) => {
          if (!run) {
            plays.appendChild(skelNode("div", "af-play skel"));
            return;
          }
          const play = document.createElement("button");
          play.type = "button";
          play.className = "af-play";
          play.textContent = "▶";
          play.title = runHoverText(run);
          play.onclick = () => openPipelineRun(p.id, run.id);
          plays.appendChild(play);
        });
        grid.appendChild(bars);
        grid.appendChild(days);
        grid.appendChild(times);
        grid.appendChild(plays);
        const tasks = taskOrder(columns.slice().reverse());
        const rows = tasks.length ? tasks : [];
        rows.forEach((node) => {
          const row = document.createElement("div");
          row.className = "af-row";
          row.style.gridTemplateColumns = template;
          const label = document.createElement("div");
          label.className = "af-task";
          const kind = stepKind(node);
          const dot = document.createElement("span");
          dot.className = "af-dot" + (kind ? " " + kind : "");
          const name = document.createElement("span");
          name.className = "af-task-name";
          name.textContent = node;
          label.appendChild(dot);
          label.appendChild(name);
          row.appendChild(label);
          eachSlot(placeholders, columns, (run) => {
            if (!run) {
              const cell = skelNode("div", "af-cell skel");
              cell.appendChild(skelNode("span", "af-mark skel"));
              row.appendChild(cell);
              return;
            }
            const status = cellStatus(run, node);
            const cell = document.createElement("button");
            cell.type = "button";
            cell.className = "af-cell";
            cell.title = node + "  ·  " + runHoverText(run);
            cell.onclick = () => openPipelineRun(p.id, run.id);
            const mark = document.createElement("span");
            mark.className = "af-mark " + status;
            mark.textContent = cellLabel(status);
            cell.appendChild(mark);
            row.appendChild(cell);
          });
          grid.appendChild(row);
        });
        card.appendChild(grid);
        board.appendChild(card);
      });
      if (keepTop != null) {
        page.scrollTop = keepTop;
        requestAnimationFrame(() => { page.scrollTop = keepTop; });
      }
    }

    function hashParts() {
      const raw = String(location.hash || "").replace(/^#\/?/, "");
      const segs = raw.split("/").filter(Boolean);
      let rest = "";
      try {
        rest = segs.slice(1).map(decodeURIComponent).join("/");
      } catch (err) {
        rest = segs.slice(1).join("/");
      }
      return { view: segs[0] || "", rest: rest };
    }

    function viewFromHash() {
      const parts = hashParts();
      const view = parts.view;
      if (view === "runs" || view === "pipelines") {
        if (parts.rest) runsFocusStem = parts.rest;
        return "runs";
      }
      if (view === "compare") return "compare";
      if (view === "statistics" || view === "stats") {
        if (parts.rest) statsPipePreferred = parts.rest;
        return "statistics";
      }
      return "trace";
    }

    function showView(name) {
      if (name === "pipelines") name = "runs";
      if (name === "stats") name = "statistics";
      currentView = name === "runs" || name === "compare" || name === "statistics" ? name : "trace";
      $("traceView").hidden = currentView !== "trace";
      $("pipelinesView").hidden = currentView !== "runs";
      $("compareView").hidden = currentView !== "compare";
      $("statsView").hidden = currentView !== "statistics";
      $("navTrace").classList.toggle("active", currentView === "trace");
      $("navPipelines").classList.toggle("active", currentView === "runs");
      $("navCompare").classList.toggle("active", currentView === "compare");
      $("navStats").classList.toggle("active", currentView === "statistics");
      if (currentView === "runs") renderPipeBoard();
      if (currentView === "compare") loadCompare().catch((e) => alert(e.message));
      if (currentView === "statistics") loadStats().catch((e) => alert(e.message));
      let hash = "#/" + currentView;
      if (currentView === "runs" && runsFocusStem) hash += "/" + encodeURIComponent(runsFocusStem);
      if (location.hash !== hash) location.hash = hash;
      if (currentView === "runs" && runsFocusStem) {
        requestAnimationFrame(() => {
          const el = document.getElementById("pipe-" + runsFocusStem);
          if (el && el.scrollIntoView) el.scrollIntoView({ block: "start", behavior: "smooth" });
        });
      }
    }

    async function openPipeline(pipelineId) {
      await selectPipeline(pipelineId);
      showView("trace");
    }

    async function openPipelineRun(pipelineId, runId) {
      if (pipelineId !== fileId) await selectPipeline(pipelineId);
      if (followRun(runId)) {
        showView("trace");
        return;
      }
      await openRun(runId);
      showView("trace");
    }

    async function loadMeta() {
      try {
        const data = await api("/api/meta");
        const n = Number(data.concurrent);
        if (Number.isFinite(n) && n >= 1) runLimit = Math.floor(n);
        const q = Number(data.queue_limit);
        if (Number.isFinite(q) && q >= 0) queueLimit = Math.floor(q);
        else queueLimit = runLimit * 4;
        if (Number.isFinite(Number(data.active))) slotActive = Math.max(0, Math.floor(Number(data.active)));
        if (Number.isFinite(Number(data.queued))) slotQueued = Math.max(0, Math.floor(Number(data.queued)));
        if (Number.isFinite(Number(data.limit)) && Number(data.limit) >= 1) runLimit = Math.floor(Number(data.limit));
      } catch (err) {}
    }

    async function loadPipelines() {
      pipelines = (await api("/api/pipelines")).pipelines;
      paintPipelines();
      if (currentView === "runs") renderPipeBoard();
      if (fileId) renderExampleChips();
      if (!fileId && pipelines.length) selectPipeline(pipelines[0].id);
    }

    function paintPipelines() {
      const box = $("pipelines");
      box.innerHTML = "";
      pipelines.forEach((p) => {
        const canceling = pipelineIsCanceling(p);
        const jobs = inflightRuns.filter((job) => job.fileId === p.id);
        const queued = jobs.length > 0 && jobs.every((job) => job.queued) && !canceling;
        const live = jobs.some((job) => !job.queued) && !canceling;
        const btn = document.createElement("button");
        btn.className = "item" + (p.error ? " error" : "") + (p.id === fileId ? " active" : "");
        const lastQueued = p.last_run && p.last_run.status === "queued";
        const lastLive = p.last_run && (p.last_run.status === "running" || p.last_run.status === "canceling");
        btn.innerHTML =
          '<span class="status-dot ' + pipelineStatus(p) + '"></span> ' +
          escapeHtml(p.stem + (p.error ? " (error)" : "")) +
          (canceling ? ' <span class="pill mode-canceling">canceling</span>' : live || lastLive ? ' <span class="pill mode-running">running</span>' : queued || lastQueued ? ' <span class="pill mode-queued">queued</span>' : "");
        btn.title = p.error || p.id;
        btn.onclick = () => selectPipeline(p.id);
        box.appendChild(btn);
      });
    }

    function pipeline() {
      return pipelines.find((p) => p.id === fileId);
    }

    function exampleInput(item) {
      if (!item) return {};
      if (item.input && typeof item.input === "object" && !Array.isArray(item.input)) return item.input;
      return item;
    }

    function renderExampleChips() {
      const box = $("exampleChips");
      box.innerHTML = "";
      const items = (pipeline() && pipeline().examples) || [];
      items.forEach((item) => {
        const chip = document.createElement("button");
        chip.type = "button";
        chip.className = "chip";
        chip.textContent = item.label || "example";
        chip.onclick = () => {
          $("input").value = JSON.stringify(exampleInput(item), null, 2);
          box.querySelectorAll(".chip").forEach((el) => el.classList.remove("active"));
          chip.classList.add("active");
        };
        box.appendChild(chip);
      });
    }

    async function selectPipeline(id) {
      fileId = id;
      currentRun = null;
      selectedStep = null;
      loadBreakpoints();
      const p = pipeline();
      const example = (p && p.examples && p.examples[0]) || null;
      $("input").value = JSON.stringify(exampleInput(example), null, 2);
      renderExampleChips();
      await loadPipelines();
      renderExampleChips();
      await loadHistory();
      renderRun(null);
    }

    function flattenInput(value, prefix, out) {
      const acc = out || [];
      if (value === null || value === undefined) {
        if (prefix) acc.push(prefix, prefix + ":null", prefix + "=null");
        return acc;
      }
      if (typeof value === "object") {
        if (Array.isArray(value)) {
          if (prefix) acc.push(prefix);
          value.forEach((item, index) => {
            flattenInput(item, prefix ? prefix + "[" + index + "]" : String(index), acc);
            if (prefix) flattenInput(item, prefix, acc);
          });
          return acc;
        }
        Object.keys(value).forEach((key) => {
          const path = prefix ? prefix + "." + key : key;
          acc.push(key, path);
          flattenInput(value[key], path, acc);
        });
        return acc;
      }
      const text = String(value);
      acc.push(text);
      if (prefix) acc.push(prefix + ":" + text, prefix + "=" + text, prefix + " " + text);
      return acc;
    }

    function inputSearchText(input) {
      const data = input && typeof input === "object" ? input : {};
      return (
        flattenInput(data).join(" ") + " " +
        JSON.stringify(data) + " " +
        JSON.stringify(data, null, 2)
      ).toLowerCase();
    }

    function runMatchesFilter(run) {
      const status = $("runStatus").value;
      if (status === "running") {
        if (!(runIsLive(run) || run.status === "running" || run.status === "canceling")) return false;
      } else if (status === "queued") {
        if (!runIsQueued(run)) return false;
      } else if (status === "paused") {
        if (!runIsPaused(run) && run.status !== "paused") return false;
      } else if (status !== "all" && run.status !== status) {
        return false;
      }
      const q = ($("runFilter").value || "").trim().toLowerCase();
      if (!q) return true;
      if (q === "has error" || q === "error" || q === "failed") return run.status === "error";
      if (q === "canceled" || q === "cancelled" || q === "cancel") return run.status === "canceled";
      if (q === "queued" || q === "queue") return runIsQueued(run);
      if (q === "ok" || q === "success") return run.status === "ok";
      if (q === "paused") return runIsPaused(run) || run.status === "paused";
      if (q === "running" || q === "live") return runIsLive(run) || run.status === "running" || run.status === "canceling";
      return inputSearchText(run.input).includes(q);
    }

    function liveHistoryRow(job) {
      const run = job.run || {};
      return {
        id: job.id,
        input: run.input != null ? run.input : job.input,
        created_at: run.started_at || job.started_at,
        elapsed_ms: run.elapsed_ms,
        status: job.canceling ? "canceling" : job.queued ? "queued" : "running",
        queue_position: job.position,
        queued: Boolean(job.queued),
        running: !job.queued,
        mode: run.mode || "run",
        live: !job.queued,
        steps: run.steps || [],
      };
    }

    function historyDisplayRows() {
      const byId = {};
      (historyRuns || []).forEach((row) => {
        if (row && row.id) byId[row.id] = row;
      });
      inflightRuns.filter((job) => job.fileId === fileId).forEach((job) => {
        byId[job.id] = liveHistoryRow(job);
      });
      return Object.keys(byId).map((id) => byId[id]).sort((a, b) => {
        const left = a.created_at || "";
        const right = b.created_at || "";
        if (left === right) return 0;
        return left < right ? 1 : -1;
      });
    }

    function renderHistory() {
      const box = $("history");
      if (!fileId) { box.textContent = "Select a pipeline"; return; }
      const listed = historyDisplayRows();
      const rows = listed.filter(runMatchesFilter);
      if (!listed.length) { box.innerHTML = '<p class="empty">No runs — drop a JSON file to import</p>'; return; }
      if (!rows.length) { box.innerHTML = '<p class="empty">No runs match</p>'; return; }
      box.innerHTML = "";
      rows.forEach((r) => {
        const btn = document.createElement("button");
        btn.className = "run" + (currentRun && currentRun.id === r.id ? " active" : "");
        const mode = r.mode || "run";
        const pill = runIsCanceled(r) ? "canceled" : r.status === "error" ? "error" : r.status === "paused" ? "paused" : r.status === "canceling" ? "canceling" : r.status === "queued" ? (r.queue_position ? "queued #" + r.queue_position : "queued") : r.status === "running" ? "running" : mode;
        const known = { run: 1, replay: 1, replay_from: 1, error: 1, canceled: 1, canceling: 1, queued: 1, approximate: 1, paused: 1, running: 1 };
        const pillClass = known[pill] ? pill : (String(pill).indexOf("queued") === 0 ? "queued" : "run");
        const outdated = isOutdated(r)
          ? '<span class="pill mode-outdated">outdated</span>'
          : "";
        const approx = r.approximate
          ? '<span class="pill mode-approximate">approximate</span>'
          : "";
        btn.innerHTML =
          '<div class="run-top"><span>' + escapeHtml(formatTime(r.created_at)) +
          '</span><span class="run-pills"><span class="pill mode-' + pillClass + '">' +
          escapeHtml(pill) + "</span>" + approx + outdated + "</span></div>" +
          '<div class="meta">' + escapeHtml(truncateJson(r.input)) +
          (r.elapsed_ms != null ? "  ·  " + escapeHtml(formatElapsed(r.elapsed_ms)) : "") +
          "</div>";
        btn.onclick = () => openHistoryRun(r);
        box.appendChild(btn);
      });
    }

    function followRun(id) {
      const job = inflightRuns.find((item) => item.id === id);
      if (!job || !job.run) return false;
      currentRun = job.run;
      selectedStep = null;
      renderRun(currentRun);
      renderHistory();
      syncRunControls();
      return true;
    }

    function openHistoryRun(row) {
      if (followRun(row.id)) return;
      openRun(row.id);
    }

    async function loadHistory() {
      if (!fileId) { historyRuns = []; renderHistory(); return; }
      const { runs } = await api("/api/runs?file=" + encodeURIComponent(fileId));
      historyRuns = runs;
      renderHistory();
    }

    async function openRun(id) {
      if (followRun(id)) return;
      currentRun = await api("/api/runs/" + id);
      selectedStep = null;
      renderRun(currentRun);
      await loadHistory();
    }

    function jsonHint(value) {
      if (Array.isArray(value)) return "[" + value.length + "]";
      if (value && typeof value === "object") return "{" + Object.keys(value).length + "}";
      return "";
    }

    function jsonValueClass(value) {
      if (value === null) return "j-null";
      const type = typeof value;
      if (type === "string") return "j-str";
      if (type === "number") return "j-num";
      if (type === "boolean") return "j-bool";
      return "j-null";
    }

    function renderJsonTree(value, key, depth) {
      const isArr = Array.isArray(value);
      const isObj = Boolean(value) && typeof value === "object";
      if (!isArr && !isObj) {
        const row = document.createElement("div");
        row.className = "j-leaf";
        if (key !== undefined) {
          const k = document.createElement("span");
          k.className = "j-key";
          k.textContent = key;
          row.appendChild(k);
        }
        const v = document.createElement("span");
        v.className = jsonValueClass(value);
        v.textContent = value === null ? "null" : JSON.stringify(value);
        row.appendChild(v);
        return row;
      }
      const entries = isArr
        ? value.map((item, index) => [String(index), item])
        : Object.entries(value);
      if (!entries.length) {
        const row = document.createElement("div");
        row.className = "j-leaf";
        if (key !== undefined) {
          const k = document.createElement("span");
          k.className = "j-key";
          k.textContent = key;
          row.appendChild(k);
        }
        const v = document.createElement("span");
        v.className = "j-empty";
        v.textContent = isArr ? "[]" : "{}";
        row.appendChild(v);
        return row;
      }
      const node = document.createElement("details");
      node.className = "j-node";
      node.open = depth === 0;
      const summary = document.createElement("summary");
      if (key !== undefined) {
        const k = document.createElement("span");
        k.className = "j-key";
        k.textContent = key;
        summary.appendChild(k);
      }
      const hint = document.createElement("span");
      hint.className = "j-hint";
      hint.textContent = jsonHint(value);
      summary.appendChild(hint);
      node.appendChild(summary);
      const kids = document.createElement("div");
      kids.className = "j-kids";
      entries.forEach(([childKey, child]) => {
        kids.appendChild(renderJsonTree(child, childKey, depth + 1));
      });
      node.appendChild(kids);
      return node;
    }

    function isMessage(item) {
      if (!item || typeof item !== "object") return false;
      const roles = ["system", "user", "assistant", "tool", "function", "human", "ai"];
      const kinds = ["system", "human", "ai", "tool", "function", "chat"];
      if (roles.includes(item.role) || kinds.includes(item.type)) return true;
      return Boolean(item.tool_calls && "content" in item);
    }

    function extractMessages(value) {
      if (Array.isArray(value)) {
        const found = value.filter(isMessage);
        return found.length && found.length >= Math.max(1, Math.floor(value.length / 2)) ? found : [];
      }
      if (!value || typeof value !== "object") return [];
      if (isMessage(value)) return [value];
      for (const key of ["messages", "output", "result"]) {
        if (value[key] !== undefined) {
          const found = extractMessages(value[key]);
          if (found.length) return found;
        }
      }
      return [];
    }

    function messageText(item) {
      const content = item.content;
      if (typeof content === "string") return content;
      if (Array.isArray(content)) {
        return content.map((part) => {
          if (typeof part === "string") return part;
          return (part && part.text) || JSON.stringify(part);
        }).join("\n");
      }
      return content == null ? "" : JSON.stringify(content);
    }

    function renderThread(messages) {
      const wrap = document.createElement("div");
      wrap.className = "thread";
      messages.forEach((item) => {
        const bubble = document.createElement("div");
        bubble.className = "bubble";
        const tools = item.tool_calls || [];
        bubble.innerHTML =
          '<div class="bubble-role">' + escapeHtml(item.role || item.type || "message") + "</div>" +
          (messageText(item) ? '<div class="bubble-body">' + escapeHtml(messageText(item)) + "</div>" : "") +
          tools.map((call) => {
            const name = call.name || (call.function && call.function.name) || "tool";
            const args = call.args || (call.function && call.function.arguments) || call.arguments || {};
            return '<div class="bubble-tool">' + escapeHtml(name) + "  " +
              escapeHtml(typeof args === "string" ? args : JSON.stringify(args)) + "</div>";
          }).join("");
        wrap.appendChild(bubble);
      });
      return wrap;
    }

    function setJson(el, value, emptyText, opts) {
      el.innerHTML = "";
      if (emptyText) {
        el.classList.add("empty");
        el.textContent = emptyText;
        return;
      }
      el.classList.remove("empty");
      const messages = opts && opts.thread === false ? [] : extractMessages(value);
      if (messages.length) el.appendChild(renderThread(messages));
      const tree = renderJsonTree(value ?? {}, undefined, 0);
      if (messages.length) {
        const details = document.createElement("details");
        details.className = "tree-details";
        const summary = document.createElement("summary");
        summary.textContent = "JSON";
        details.appendChild(summary);
        details.appendChild(tree);
        el.appendChild(details);
      } else {
        el.appendChild(tree);
      }
      el._copyPayload = JSON.stringify(value ?? {}, null, 2);
      const copy = document.createElement("button");
      copy.type = "button";
      copy.className = "copy-btn";
      copy.textContent = "Copy";
      el.appendChild(copy);
    }

    function stateDiffRows(before, after) {
      const left = before && typeof before === "object" && !Array.isArray(before) ? before : {};
      const right = after && typeof after === "object" && !Array.isArray(after) ? after : {};
      const keys = Array.from(new Set([...Object.keys(left), ...Object.keys(right)])).sort();
      const rank = { changed: 0, "only-b": 1, "only-a": 2, same: 3 };
      return keys.map((key) => {
        const hasA = Object.prototype.hasOwnProperty.call(left, key);
        const hasB = Object.prototype.hasOwnProperty.call(right, key);
        let kind = "same";
        if (hasA && hasB) kind = jsonEqual(left[key], right[key]) ? "same" : "changed";
        else if (hasA) kind = "only-a";
        else kind = "only-b";
        return {
          key,
          a: hasA ? left[key] : undefined,
          b: hasB ? right[key] : undefined,
          kind,
        };
      }).sort((a, b) => (rank[a.kind] - rank[b.kind]) || a.key.localeCompare(b.key));
    }

    function renderStateDiff(step) {
      const board = $("stateDiff");
      const pane = board && board.closest(".io-diff");
      const inspect = nodeViewMode === "all";
      if (pane) pane.hidden = !inspect;
      if (!inspect) return;
      if (!step) {
        board.className = "json empty";
        board.textContent = "Select a node to see what changed in state.";
        return;
      }
      const rows = stateDiffRows(step.state_in, step.state_out);
      board.className = "json";
      board.innerHTML =
        '<div class="cmp-head"><div>Before</div><div>After</div></div>' +
        (rows.length ? diffRowsHtml(rows) : '<p class="empty">No state keys</p>');
    }

    function stateObject(value) {
      return value && typeof value === "object" && !Array.isArray(value) ? value : {};
    }

    function keyPresent(state, key) {
      if (!Object.prototype.hasOwnProperty.call(state, key)) return false;
      return state[key] != null;
    }

    function memoryModel(run) {
      const rows = {};
      ((run && run.steps) || []).forEach((step) => {
        if (!step || step.pending) return;
        const left = stateObject(step.state_in);
        const right = stateObject(step.state_out);
        const keys = new Set([...Object.keys(left), ...Object.keys(right)]);
        keys.forEach((key) => {
          const row = rows[key] || {
            key,
            first: null,
            last: null,
            writer: "",
            readers: [],
            kind: "same",
          };
          if (keyPresent(left, key) && step.node && row.readers.indexOf(step.node) < 0) {
            row.readers.push(step.node);
          }
          const had = keyPresent(left, key);
          const has = keyPresent(right, key);
          if (had && !has) {
            row.kind = "removed";
            row.last = step.step_id;
            row.writer = step.node || row.writer;
          } else if (has && (!had || !jsonEqual(left[key], right[key]))) {
            row.kind = row.kind === "removed" ? "removed" : "written";
            if (!row.first) row.first = step.step_id;
            row.last = step.step_id;
            row.writer = step.node || row.writer;
          }
          rows[key] = row;
        });
      });
      return Object.keys(rows).sort().map((key) => rows[key]);
    }

    function renderMemory(run) {
      const board = $("memory");
      if (!board) return;
      if (!run) {
        board.className = "json empty";
        board.textContent = "Select a run to see which state keys were written and read.";
        return;
      }
      const rows = memoryModel(run);
      if (!rows.length) {
        board.className = "json empty";
        board.textContent = "No state keys in this run.";
        return;
      }
      board.className = "json";
      board.innerHTML =
        "<table class=\"mem-table\"><thead><tr>" +
        "<th>Key</th><th>Last writer</th><th>First</th><th>Last</th><th>Readers</th>" +
        "</tr></thead><tbody>" +
        rows.map((row) =>
          '<tr class="' + (row.kind === "removed" ? "removed" : "") + '">' +
          '<td class="mem-k">' + escapeHtml(row.key) + "</td>" +
          "<td>" + escapeHtml(row.writer || "—") + "</td>" +
          '<td class="mem-muted">' + escapeHtml(row.first || "—") + "</td>" +
          '<td class="mem-muted">' + escapeHtml(row.last || "—") + "</td>" +
          "<td>" + escapeHtml(row.readers.length ? row.readers.join(", ") : "no later reader") + "</td>" +
          "</tr>"
        ).join("") +
        "</tbody></table>";
    }

    function stepMessages(state) {
      const obj = stateObject(state);
      const fromList = extractMessages(obj.messages);
      return fromList.length ? fromList : extractMessages(obj);
    }

    function newMessages(step) {
      const before = stepMessages(step.state_in);
      const after = stepMessages(step.state_out);
      if (after.length > before.length) return after.slice(before.length);
      return extractMessages(step.update);
    }

    function messageSpeaker(message, step) {
      return message.name || message.role || step.node || "agent";
    }

    function messageExcerpt(message) {
      const text = messageText(message);
      if (text) return text;
      const tools = message.tool_calls || [];
      if (tools.length) {
        return tools.map((call) => call.name || "tool").join(", ");
      }
      return "";
    }

    function agentsModel(run) {
      const events = [];
      ((run && run.steps) || []).forEach((step) => {
        newMessages(step).forEach((message) => {
          events.push({
            step,
            message,
            speaker: messageSpeaker(message, step),
            excerpt: messageExcerpt(message),
          });
        });
      });
      const edges = [];
      for (let i = 1; i < events.length; i += 1) {
        edges.push({
          from: events[i - 1].speaker,
          to: events[i].speaker,
          excerpt: events[i].excerpt,
          step: events[i].step,
        });
      }
      return { events, edges };
    }

    function renderAgents(run) {
      const target = $("diagram");
      if (!run) {
        target.className = "empty";
        target.innerHTML = "Select a run to see who spoke to whom.";
        applyGraphZoom();
        return;
      }
      const model = agentsModel(run);
      if (!model.events.length) {
        target.className = "empty";
        target.innerHTML = "No agent messages in this run.";
        applyGraphZoom();
        return;
      }
      const flow = document.createElement("div");
      flow.className = "gflow";
      flow.appendChild(graphCap("Agents"));
      model.events.forEach((event, index) => {
        if (index) {
          const edge = document.createElement("div");
          edge.className = "agent-edge";
          const label = model.edges[index - 1];
          edge.textContent = (label && label.excerpt) ? label.excerpt : "→";
          flow.appendChild(graphLine());
          flow.appendChild(edge);
        } else {
          flow.appendChild(graphLine());
        }
        const row = document.createElement("div");
        row.className = "g-row";
        const card = graphCard({
          node: event.speaker,
          reason: event.excerpt,
          error: event.step.error,
          step_id: event.step.step_id,
        }, false);
        card.classList.add("agent");
        bindStepActions(card, event.step);
        row.appendChild(card);
        flow.appendChild(row);
      });
      target.className = "";
      target.innerHTML = "";
      mountGraph(target, flow);
      highlightSelection();
    }

    function parseToolsFromUpdate(update, elapsedMs) {
      const byId = {};
      const tools = [];
      const merge = (item) => {
        const key = item.id;
        if (key && byId[key]) {
          Object.keys(item).forEach((name) => {
            if (item[name] != null && item[name] !== "") byId[key][name] = item[name];
          });
          return;
        }
        if (key) byId[key] = item;
        tools.push(item);
      };
      const addCall = (call) => {
        if (!call || typeof call !== "object") return;
        const fn = call.function && typeof call.function === "object" ? call.function : {};
        let args = call.args != null ? call.args : (fn.arguments != null ? fn.arguments : call.arguments);
        if (typeof args === "string") {
          try { args = JSON.parse(args); } catch (err) { /* keep string */ }
        }
        merge({
          name: call.name || fn.name || call.tool || "tool",
          id: call.id || call.tool_call_id || fn.id || null,
          args: args,
          output: null,
          latency_ms: null,
          error: null,
        });
      };
      if (!update || typeof update !== "object") return tools;
      (update.tool_calls || []).forEach(addCall);
      extractMessages(update).forEach((message) => {
        (message.tool_calls || []).forEach(addCall);
        const role = message.role || message.type;
        if (role === "tool") {
          merge({
            name: message.name || message.tool || "tool",
            id: message.tool_call_id || message.id || null,
            args: message.args,
            output: message.content,
            latency_ms: message.latency_ms != null ? Number(message.latency_ms) : null,
            error: message.error || null,
          });
        }
      });
      if (tools.length === 1 && tools[0].latency_ms == null && elapsedMs != null) {
        tools[0].latency_ms = Number(elapsedMs);
      }
      return tools;
    }

    function stepTools(step) {
      const listed = (step && step.tools) || [];
      const rich = listed.some((item) => item && (item.args != null || item.output != null || item.id || item.error));
      const base = (rich ? listed : parseToolsFromUpdate(step && step.update, step && step.elapsed_ms))
        .map((item) => Object.assign({}, item));
      const prior = parseToolsFromUpdate({ messages: ((step && step.state_in) || {}).messages });
      const byId = {};
      prior.forEach((item) => { if (item.id) byId[item.id] = item; });
      base.forEach((item) => {
        const extra = item.id && byId[item.id];
        if (!extra) return;
        if (item.args == null) item.args = extra.args;
        if (!item.name || item.name === "tool") item.name = extra.name;
      });
      return base;
    }

    function renderStepTools(step) {
      const board = $("stepTools");
      const pane = board && board.closest(".io-tools");
      const inspect = nodeViewMode === "all";
      if (pane) pane.hidden = !inspect;
      if (!inspect || !board) return;
      if (!step) {
        board.className = "json empty";
        board.textContent = "Select a node to see tool calls.";
        return;
      }
      const tools = stepTools(step);
      if (!tools.length) {
        board.className = "json empty";
        board.textContent = "This node did not call a tool.";
        return;
      }
      const maxMs = Math.max(1, ...tools.map((item) => Number(item.latency_ms) || 0));
      const bars = tools.length > 1
        ? '<div class="tool-bars">' + tools.map((item) => {
          const width = Math.max(8, Math.round(((Number(item.latency_ms) || 0) / maxMs) * 100));
          return '<span class="tool-bar' + (item.error ? " error" : "") + '" style="flex:' +
            width + '" title="' + escapeHtml((item.name || "tool") + " " + (formatElapsed(item.latency_ms) || "—")) +
            '"></span>';
        }).join("") + "</div>"
        : "";
      board.className = "json";
      board.innerHTML = bars + tools.map((item) => {
        const args = item.args == null ? "" : (typeof item.args === "string" ? item.args : JSON.stringify(item.args));
        const output = item.output == null ? "" : (typeof item.output === "string" ? item.output : JSON.stringify(item.output));
        return '<div class="tool-row">' +
          '<div class="tool-name">' + escapeHtml(item.name || "tool") + "</div>" +
          '<div class="tool-meta">' +
          escapeHtml(item.id || "") +
          (item.latency_ms != null ? "  ·  " + escapeHtml(formatElapsed(item.latency_ms)) : "") +
          "</div>" +
          (args ? '<div class="tool-kv"><div class="cmp-k">Input</div><div class="cmp-a">' + escapeHtml(args) + "</div></div>" : "") +
          (output ? '<div class="tool-kv"><div class="cmp-k">Output</div><div class="cmp-b">' + escapeHtml(output) + "</div></div>" : "") +
          (item.error ? '<div class="tool-err">' + escapeHtml(item.error) + "</div>" : "") +
          "</div>";
      }).join("");
    }

    function renderTraceback(step) {
      const board = $("stepTrace");
      const pane = board && board.closest(".io-trace");
      if (!board) return;
      const frames = (step && step.error_frames) || [];
      const inspect = nodeViewMode === "all";
      board.dataset.hasFrames = frames.length ? "1" : "0";
      if (pane) pane.hidden = !inspect || !frames.length;
      if (!inspect) return;
      if (!frames.length) {
        board.className = "trace empty";
        board.textContent = step && step.error
          ? "No Python frames were captured for this error."
          : "No traceback for this node.";
        return;
      }
      board.className = "trace";
      board.innerHTML = frames.map((frame) => {
        const loc = { file: frame.file, line: frame.line };
        const chip = sourceChipHtml(loc);
        return '<div class="trace-frame' + (frame.library ? " library" : "") + '">' +
          '<span class="trace-loc">' + (chip || escapeHtml(sourceLabel(loc) || "—")) + "</span>" +
          '<span class="trace-name">' + escapeHtml(frame.name || "") + "</span>" +
          (frame.text ? '<div class="trace-text">' + escapeHtml(frame.text) + "</div>" : "") +
          "</div>";
      }).join("");
    }

    function showStepIO(step) {
      const empty = $("stepIn");
      const editor = $("nodeInput");
      const actions = $("nodeActions");
      if (!step) {
        $("nvTitle").textContent = "Node";
        $("nvMeta").textContent = "";
        $("ioName").textContent = "";
        empty.style.display = "";
        setJson(empty, null, "Double-click a node to inspect it.");
        editor.classList.remove("visible");
        actions.classList.remove("visible");
        setJson($("stepOut"), null, "The keys this node returned.");
        renderStateDiff(null);
        renderStepTools(null);
        renderTraceback(null);
        return;
      }
      $("nvTitle").textContent = step.node || "Node";
      const metrics = stepMetrics(step);
      const loc = nodeSource(step);
      $("nvMeta").innerHTML = [
        escapeHtml(step.step_id || ""),
        sourceChipHtml(loc),
        escapeHtml(formatElapsed(step.elapsed_ms) || "—"),
        escapeHtml(formatMemory(metrics.mb) + " mem"),
        escapeHtml(formatMemory(metrics.peak) + " peak"),
        escapeHtml(formatTokens(metrics)),
        metrics.toolTokens ? escapeHtml(metrics.toolTokens + " tool tok") : "",
        metrics.tool != null ? escapeHtml(formatElapsed(metrics.tool) + " tool") : escapeHtml("— tool"),
        step.error ? escapeHtml("error") : "",
      ].filter(Boolean).join("  ·  ");
      $("ioName").textContent = formatElapsed(step.elapsed_ms);
      empty.style.display = "none";
      editor.classList.add("visible");
      actions.classList.add("visible");
      editor.value = JSON.stringify(step.state_in ?? {}, null, 2);
      setJson($("stepOut"), step.update);
      renderStateDiff(step);
      renderStepTools(step);
      renderTraceback(step);
    }

    function isNodeViewOpen() {
      return !$("nodeView").hidden;
    }

    function setNodeViewMode(mode) {
      nodeViewMode = mode || "all";
      const call = $("callBtn");
      const from = $("replayFromHereBtn");
      const diff = $("stateDiff") && $("stateDiff").closest(".io-diff");
      const tools = $("stepTools") && $("stepTools").closest(".io-tools");
      const trace = $("stepTrace") && $("stepTrace").closest(".io-trace");
      call.hidden = nodeViewMode === "replay_from";
      from.hidden = nodeViewMode === "replay";
      if (diff) diff.hidden = nodeViewMode !== "all";
      if (tools) tools.hidden = nodeViewMode !== "all";
      if (trace) trace.hidden = nodeViewMode !== "all" || !($("stepTrace") && $("stepTrace").dataset.hasFrames === "1");
      call.textContent = nodeViewMode === "replay" ? "Replay step" : "Call node";
      call.className = "primary";
      from.className = nodeViewMode === "replay_from" ? "primary" : "ghost";
      $("nvKicker").textContent = ({
        all: "Node view",
        replay: "Replay step",
        replay_from: "Replay from",
      })[nodeViewMode];
    }

    function openNodeView(step, mode) {
      setNodeViewMode(mode || "all");
      selectStep(step);
      $("nodeView").hidden = false;
      $("nodeInput").focus();
    }

    function closeNodeView() {
      $("nodeView").hidden = true;
      nodeViewMode = "all";
    }

    function readNodeInput() {
      try {
        const value = JSON.parse($("nodeInput").value || "{}");
        if (value === null || typeof value !== "object" || Array.isArray(value)) {
          throw new Error("Node input must be a JSON object");
        }
        return value;
      } catch (err) {
        throw new Error(err.message.includes("JSON object") ? err.message : "Node input must be JSON");
      }
    }

    function highlightSelection() {
      document.querySelectorAll(".step, .g-card[data-step-id], .gantt-bar[data-step-id]").forEach((el) => {
        el.classList.toggle("selected", el.dataset.stepId === selectedStep);
      });
    }

    function syncStepButtons() {
      const disabled = !selectedStep;
      $("replayBtn").disabled = disabled;
      $("replayFromBtn").disabled = disabled;
      $("exportBtn").disabled = !currentRun;
      const del = $("deleteBtn");
      if (del) del.disabled = !currentRun || runIsBusy(currentRun);
    }

    function selectedStepRecord() {
      if (!currentRun || !selectedStep) return null;
      return (currentRun.steps || []).find((step) => step.step_id === selectedStep) || null;
    }

    function openFromButton(mode) {
      const step = selectedStepRecord();
      if (!step) {
        alert("Select a node first");
        return;
      }
      openNodeView(step, mode);
    }

    function selectStep(step) {
      selectedStep = step ? step.step_id : null;
      showStepIO(step || null);
      highlightSelection();
      syncStepButtons();
    }

    function bindStepActions(el, step) {
      el.onclick = () => selectStep(step);
      el.ondblclick = (event) => {
        event.preventDefault();
        openNodeView(step, "all");
      };
      bindSparkHovers(el);
    }

    function stepDetail(step) {
      if (step.error) return step.error;
      if (step.reason) return step.reason;
      const update = step.update || {};
      for (const key of ["greeting", "shout", "polished", "output", "result"]) {
        if (update[key]) return String(update[key]);
      }
      return "";
    }

    function graphCap(label) {
      const el = document.createElement("div");
      el.className = "g-cap";
      el.textContent = label;
      return el;
    }

    function graphLine() {
      const el = document.createElement("div");
      el.className = "g-line";
      return el;
    }

    function graphCard(step, skipped) {
      const card = document.createElement("button");
      const kind = skipped ? "" : stepKind(step.node);
      card.type = "button";
      card.className = "g-card" + (kind ? " " + kind : "") + (skipped ? " skipped" : "") + (step.error ? " error" : "");
      if (step.node) card.dataset.node = step.node;
      if (!skipped && step.step_id) card.dataset.stepId = step.step_id;
      const detail = skipped ? (step.reason || "not taken") : stepDetail(step);
      const loc = skipped ? null : nodeSource(step);
      const srcHtml = loc ? '<span class="g-src">' + sourceChipHtml(loc) + "</span>" : "";
      card.innerHTML =
        '<span class="g-dot"></span><span class="g-body">' +
        '<span class="g-name">' + escapeHtml(step.node || "") + "</span>" +
        srcHtml +
        (detail ? '<span class="g-sub">' + escapeHtml(detail) + "</span>" : "") +
        "</span>";
      if (loc) card.title = sourceLabel(loc);
      return card;
    }

    function orderGraphNodes(spec) {
      const skip = { __start__: 1, __end__: 1, START: 1, END: 1 };
      const nodes = (spec && spec.nodes) || [];
      const edges = (spec && spec.edges) || [];
      const seen = [];
      const used = {};
      const queue = [];
      edges.forEach((pair) => {
        if (!pair || pair.length < 2) return;
        if ((pair[0] === "__start__" || pair[0] === "START") && !skip[pair[1]]) queue.push(pair[1]);
      });
      if (!queue.length) nodes.forEach((name) => queue.push(name));
      while (queue.length) {
        const name = queue.shift();
        if (!name || used[name] || skip[name]) continue;
        used[name] = 1;
        seen.push(name);
        edges.forEach((pair) => {
          if (pair && pair[0] === name && pair[1] && !used[pair[1]] && !skip[pair[1]]) queue.push(pair[1]);
        });
      }
      nodes.forEach((name) => { if (!used[name]) seen.push(name); });
      return seen;
    }

    function topoName(name) {
      if (name === "__start__" || name === "START") return "START";
      if (name === "__end__" || name === "END") return "END";
      return name || "";
    }

    function graphSpec(run) {
      const pipe = currentPipeline();
      if (pipe && pipe.graph && (pipe.graph.nodes || []).length) return pipe.graph;
      if (run && run.graph && (run.graph.nodes || []).length) return run.graph;
      return null;
    }

    function nodeCardSel(name) {
      const safe = (window.CSS && CSS.escape) ? CSS.escape(name) : String(name).replace(/"/g, '\\"');
      return '.g-card[data-node="' + safe + '"]';
    }

    function topologyModel(spec, run, overlay) {
      const rawNodes = ((spec && spec.nodes) || []).map(topoName).filter((name) => name && name !== "START" && name !== "END");
      const rawEdges = ((spec && spec.edges) || []).map((pair) => [topoName(pair[0]), topoName(pair[1])]).filter((pair) => pair[0] && pair[1] && pair[0] !== pair[1]);
      const seenEdge = {};
      const edges = [];
      rawEdges.forEach((pair) => {
        const key = pair[0] + "->" + pair[1];
        if (seenEdge[key]) return;
        seenEdge[key] = 1;
        edges.push(pair);
      });
      const names = rawNodes.slice();
      edges.forEach((pair) => {
        pair.forEach((name) => {
          if (name !== "START" && name !== "END" && names.indexOf(name) < 0) names.push(name);
        });
      });
      const adj = {};
      const backKey = {};
      const parent = {};
      edges.forEach((pair) => { (adj[pair[0]] || (adj[pair[0]] = [])).push(pair[1]); });
      const depth = { START: 0 };
      const queue = ["START"];
      const visited = { START: 1 };
      function topoAncestor(anc, node) {
        let cur = node;
        const seen = {};
        while (cur && !seen[cur]) {
          if (cur === anc) return true;
          seen[cur] = 1;
          cur = parent[cur];
        }
        return false;
      }
      while (queue.length) {
        const from = queue.shift();
        (adj[from] || []).forEach((to) => {
          if (visited[to]) {
            if (topoAncestor(to, from)) backKey[from + "->" + to] = 1;
            return;
          }
          visited[to] = 1;
          parent[to] = from;
          depth[to] = (depth[from] || 0) + 1;
          queue.push(to);
        });
      }
      names.forEach((name) => { if (depth[name] == null) depth[name] = 1; });
      if (depth.END == null) depth.END = 1;
      let changed = true;
      let guard = 0;
      while (changed && guard < 24) {
        changed = false;
        guard += 1;
        edges.forEach((pair) => {
          if (backKey[pair[0] + "->" + pair[1]]) return;
          const next = (depth[pair[0]] || 0) + 1;
          if (next > (depth[pair[1]] || 0)) {
            depth[pair[1]] = next;
            changed = true;
          }
        });
      }
      const bodyMax = Math.max(0, ...names.map((name) => depth[name] || 0));
      depth.END = Math.max(depth.END || 0, bodyMax + 1);
      const byLayer = {};
      ["START"].concat(names, ["END"]).forEach((name) => {
        const layer = depth[name] || 0;
        (byLayer[layer] || (byLayer[layer] = [])).push(name);
      });
      const layers = Object.keys(byLayer).sort((a, b) => Number(a) - Number(b)).map((key) => byLayer[key]);
      const steps = (run && run.steps) || [];
      const visits = {};
      const lastStep = {};
      const errored = {};
      const pairCount = {};
      const unusedFrom = {};
      if (overlay) {
        Object.keys(overlay.nodeVisits || {}).forEach((name) => {
          visits[topoName(name)] = overlay.nodeVisits[name] || 0;
        });
        Object.keys(overlay.edgeCount || {}).forEach((key) => {
          pairCount[key] = overlay.edgeCount[key] || 0;
        });
        edges.forEach((pair) => {
          if (pair[0] === "START" && visits[pair[1]] && !pairCount["START->" + pair[1]]) {
            pairCount["START->" + pair[1]] = visits[pair[1]];
          }
        });
      } else {
        steps.forEach((step) => {
          if (!step || step.pending) return;
          const name = topoName(step.node);
          visits[name] = (visits[name] || 0) + 1;
          lastStep[name] = step;
          if (step.error) errored[name] = step;
        });
        const completed = steps.filter((step) => step && !step.pending);
        for (let i = 0; i < completed.length - 1; i += 1) {
          const key = topoName(completed[i].node) + "->" + topoName(completed[i + 1].node);
          pairCount[key] = (pairCount[key] || 0) + 1;
        }
        if (completed.length) {
          pairCount["START->" + topoName(completed[0].node)] = 1;
          const last = topoName(completed[completed.length - 1].node);
          const unusedLast = (completed[completed.length - 1].unused || []).map(topoName);
          if (unusedLast.indexOf("END") < 0) pairCount[last + "->END"] = (pairCount[last + "->END"] || 0) + 1;
        }
        completed.forEach((step) => {
          (step.unused || []).forEach((target) => {
            unusedFrom[topoName(step.node) + "->" + topoName(target)] = 1;
          });
        });
      }
      const live = overlay
        ? Object.keys(visits).some((name) => visits[name]) || Object.keys(pairCount).some((key) => pairCount[key])
        : !!(run && steps.filter((step) => step && !step.pending).length);
      const outTotal = {};
      edges.forEach((pair) => {
        outTotal[pair[0]] = (outTotal[pair[0]] || 0) + (pairCount[pair[0] + "->" + pair[1]] || 0);
      });
      const marked = edges.map((pair) => {
        const key = pair[0] + "->" + pair[1];
        const count = pairCount[key] || 0;
        const back = (depth[pair[1]] || 0) <= (depth[pair[0]] || 0);
        let kind = "idle";
        let statLabel = "";
        if (overlay) {
          const fromSeen = visits[pair[0]] || pair[0] === "START";
          if (count) {
            const siblings = edges.filter((item) => item[0] === pair[0]);
            const maxOut = Math.max.apply(null, siblings.map((item) => pairCount[item[0] + "->" + item[1]] || 0).concat([0]));
            kind = back ? "back" : (count === maxOut ? "taken" : "parallel");
            if (overlay.mode === "routing" && outTotal[pair[0]]) {
              statLabel = Math.round((count / outTotal[pair[0]]) * 100) + "%";
            }
          } else if (fromSeen) {
            kind = "skipped";
          }
        } else if (live && count) {
          kind = count > 1 ? "parallel" : (back ? "back" : "taken");
        } else if (live && (visits[pair[0]] || pair[0] === "START") && (unusedFrom[key] || !visits[pair[1]])) {
          if (pair[1] === "END" && count) kind = "taken";
          else if (pair[1] !== "END" || unusedFrom[key]) kind = "skipped";
        }
        return { from: pair[0], to: pair[1], kind: kind, visits: count, back: back, statLabel: statLabel };
      });
      const nodes = names.map((name) => {
        let status = "idle";
        if (live && visits[name]) status = "taken";
        else if (live) status = "skipped";
        return {
          name: name,
          status: status,
          visits: visits[name] || 0,
          lastStep: lastStep[name] || null,
          error: !!(errored[name]),
          detail: overlay && overlay.nodeDetail ? (overlay.nodeDetail[name] || "") : "",
        };
      });
      return { layers: layers, edges: marked, nodes: nodes, live: live };
    }

    function topoNodeBox(flow, name) {
      const el = flow.querySelector('[data-node="' + name + '"]');
      if (!el) return null;
      const box = flow.getBoundingClientRect();
      const rect = el.getBoundingClientRect();
      return {
        left: rect.left - box.left,
        right: rect.right - box.left,
        top: rect.top - box.top,
        bottom: rect.bottom - box.top,
        cx: rect.left + rect.width / 2 - box.left,
        cy: rect.top + rect.height / 2 - box.top,
      };
    }

    function topoEdgeColor(kind, back) {
      if (kind === "skipped") return "#fbbf24";
      if (kind === "parallel") return "#c4b5fd";
      if (back) return "#4ade80";
      if (kind === "taken") return "#a5b4fc";
      return "#8b8b96";
    }

    function addTopoLabel(svg, x, y, label, kind) {
      const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
      const bg = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
      const width = Math.max(28, label.length * 6.8 + 12);
      const height = 18;
      bg.setAttribute("class", "topo-label-bg " + kind);
      bg.setAttribute("x", String(x - width / 2));
      bg.setAttribute("y", String(y - height / 2));
      bg.setAttribute("width", String(width));
      bg.setAttribute("height", String(height));
      bg.setAttribute("rx", "8");
      text.setAttribute("class", "topo-label " + kind);
      text.setAttribute("x", String(x));
      text.setAttribute("y", String(y + 4));
      text.setAttribute("text-anchor", "middle");
      text.textContent = label;
      g.appendChild(bg);
      g.appendChild(text);
      svg.appendChild(g);
    }

    function drawTopoEdges(flow) {
      const edges = flow._topoEdges || [];
      let svg = flow.querySelector("svg.topo-svg");
      if (!svg) {
        svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
        svg.classList.add("topo-svg");
        flow.insertBefore(svg, flow.firstChild);
      }
      svg.innerHTML = "";
      const width = Math.max(1, flow.clientWidth || flow.scrollWidth);
      const height = Math.max(1, flow.clientHeight || flow.scrollHeight);
      svg.setAttribute("width", String(width));
      svg.setAttribute("height", String(height));
      svg.setAttribute("viewBox", "0 0 " + width + " " + height);
      const defs = document.createElementNS("http://www.w3.org/2000/svg", "defs");
      const markerUid = "t" + (topoMarkerSeq += 1);
      ["taken", "parallel", "skipped", "back", "idle"].forEach((kind) => {
        const marker = document.createElementNS("http://www.w3.org/2000/svg", "marker");
        marker.setAttribute("id", "topo-arrow-" + kind + "-" + markerUid);
        marker.setAttribute("markerWidth", "8");
        marker.setAttribute("markerHeight", "8");
        marker.setAttribute("refX", "7");
        marker.setAttribute("refY", "4");
        marker.setAttribute("orient", "auto");
        const tip = document.createElementNS("http://www.w3.org/2000/svg", "path");
        tip.setAttribute("d", "M0,0 L8,4 L0,8 z");
        tip.setAttribute("fill", topoEdgeColor(kind, kind === "back"));
        marker.appendChild(tip);
        defs.appendChild(marker);
      });
      svg.appendChild(defs);
      let minLeft = width;
      let maxRight = 0;
      edges.forEach((edge) => {
        const from = topoNodeBox(flow, edge.from);
        const to = topoNodeBox(flow, edge.to);
        if (!from || !to) return;
        minLeft = Math.min(minLeft, from.left, to.left);
        maxRight = Math.max(maxRight, from.right, to.right);
      });
      if (!Number.isFinite(minLeft)) minLeft = 0;
      const midGraph = (minLeft + maxRight) / 2;
      const leftRail = Math.max(16, minLeft - 36);
      const rightRail = Math.min(width - 16, maxRight + 36);
      let leftCount = 0;
      let rightCount = 0;
      let forwardCount = 0;
      const labels = [];
      edges.forEach((edge) => {
        const from = topoNodeBox(flow, edge.from);
        const to = topoNodeBox(flow, edge.to);
        if (!from || !to) return;
        const back = !!(edge.back && edge.kind !== "skipped");
        const kind = back ? "back" : edge.kind;
        const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
        path.setAttribute("class", "topo-edge " + kind);
        path.setAttribute("marker-end", "url(#topo-arrow-" + kind + "-" + markerUid + ")");
        let lx;
        let ly;
        if (back) {
          const useLeft = from.cx <= midGraph;
          const offset = (useLeft ? leftCount++ : rightCount++) * 14;
          const rail = useLeft ? leftRail - offset : rightRail + offset;
          const drop = from.bottom + 10;
          const enter = to.top - 10;
          path.setAttribute(
            "d",
            "M " + from.cx + " " + from.bottom +
            " L " + from.cx + " " + drop +
            " L " + rail + " " + drop +
            " L " + rail + " " + enter +
            " L " + to.cx + " " + enter +
            " L " + to.cx + " " + to.top
          );
          lx = rail;
          ly = (drop + enter) / 2;
        } else {
          const midY = (from.bottom + to.top) / 2 + (forwardCount++ % 3) * 5 - 5;
          path.setAttribute(
            "d",
            "M " + from.cx + " " + from.bottom +
            " L " + from.cx + " " + midY +
            " L " + to.cx + " " + midY +
            " L " + to.cx + " " + to.top
          );
          const vertical = Math.abs(from.cx - to.cx) < 8;
          lx = vertical ? from.cx + 20 : (from.cx + to.cx) / 2;
          ly = midY;
        }
        svg.appendChild(path);
        let label = "";
        if (edge.statLabel) label = edge.statLabel;
        else if (edge.kind === "parallel" && edge.visits > 1) label = "×" + edge.visits;
        else if (edge.kind === "skipped") label = "not taken";
        else if (back && edge.visits > 1) label = "loop ×" + edge.visits;
        else if (back) label = "loop";
        if (label) labels.push({ x: lx, y: ly, label: label, kind: kind });
      });
      labels.forEach((item) => addTopoLabel(svg, item.x, item.y, item.label, item.kind));
    }

    function applyGraphZoom() {
      const box = $("diagram");
      const flow = box && box.querySelector(".gflow");
      const wrap = box && box.querySelector(".gzoom");
      const empty = !box || box.classList.contains("empty") || !flow;
      ["zoomIn", "zoomOut", "zoomFit"].forEach((id) => {
        const btn = $(id);
        if (btn) btn.disabled = empty;
      });
      if (empty || !flow) return;
      flow.style.transform =
        "translate(" + graphPanX + "px, " + graphPanY + "px) scale(" + graphZoom + ")";
    }

    function graphPartsRect(flow) {
      const parts = flow.querySelectorAll(".g-cap, .g-card, .g-line, .g-skip, .gantt-head, .gantt-row, .gantt-bar, .topo-svg");
      if (!parts.length) return flow.getBoundingClientRect();
      let left = Infinity;
      let top = Infinity;
      let right = -Infinity;
      let bottom = -Infinity;
      parts.forEach((el) => {
        const rect = el.getBoundingClientRect();
        left = Math.min(left, rect.left);
        top = Math.min(top, rect.top);
        right = Math.max(right, rect.right);
        bottom = Math.max(bottom, rect.bottom);
      });
      return { left, top, right, bottom, width: right - left, height: bottom - top };
    }

    function fitGraphZoom() {
      const box = $("diagram");
      const flow = box && box.querySelector(".gflow");
      if (!box || !flow) return;
      graphPanX = 0;
      graphPanY = 0;
      graphZoom = 1;
      flow.style.transform = "none";
      if (flow.classList.contains("topo")) drawTopoEdges(flow);
      const pad = 24;
      const availW = Math.max(box.clientWidth - pad, 1);
      const availH = Math.max(box.clientHeight - pad, 1);
      const natural = graphPartsRect(flow);
      const width = Math.max(natural.width, 1);
      const height = Math.max(natural.height, 1);
      graphZoom = Math.max(0.25, Math.min(availW / width, availH / height));
      applyGraphZoom();
      const boxRect = box.getBoundingClientRect();
      const graphRect = graphPartsRect(flow);
      graphPanX += (boxRect.left + boxRect.width / 2) - (graphRect.left + graphRect.width / 2);
      graphPanY += (boxRect.top + boxRect.height / 2) - (graphRect.top + graphRect.height / 2);
      applyGraphZoom();
    }

    function mountGraph(target, flow) {
      const wrap = document.createElement("div");
      wrap.className = "gzoom";
      wrap.appendChild(flow);
      target.appendChild(wrap);
      graphPanX = 0;
      graphPanY = 0;
      requestAnimationFrame(() => fitGraphZoom());
    }

    function renderLiveGraph(spec, caption, run) {
      renderTopoGraph(spec, run || null, caption);
    }

    function renderTopoGraph(spec, run, caption) {
      const target = $("diagram");
      const model = topologyModel(spec, run);
      if (!model.layers.length) {
        target.className = "empty";
        target.innerHTML = caption || "Select a run to inspect the graph.";
        applyGraphZoom();
        return;
      }
      const byName = {};
      model.nodes.forEach((node) => { byName[node.name] = node; });
      target.className = "";
      target.innerHTML = caption
        ? '<p class="empty" style="margin:0 0 10px">' + escapeHtml(caption) + "</p>"
        : "";
      const flow = document.createElement("div");
      flow.className = "gflow topo";
      flow._topoEdges = model.edges;
      model.layers.forEach((layer) => {
        const row = document.createElement("div");
        row.className = "topo-layer";
        layer.forEach((name) => {
          if (name === "START" || name === "END") {
            const cap = graphCap(name === "START" ? "Start" : "End");
            cap.dataset.node = name;
            row.appendChild(cap);
            return;
          }
          const info = byName[name] || { name: name, status: "idle", visits: 0, lastStep: null, error: false };
          const skipped = info.status === "skipped";
          const step = info.lastStep || { node: name, reason: skipped ? "not taken" : "", step_id: "" };
          const card = graphCard(step, skipped);
          card.dataset.node = name;
          if (info.status === "idle") card.classList.add("idle");
          if (info.status === "taken") card.classList.add("taken");
          if (info.error) card.classList.add("error");
          if (info.lastStep) bindStepActions(card, info.lastStep);
          const wrap = document.createElement("div");
          wrap.className = "topo-node";
          wrap.appendChild(breakpointDot(name));
          wrap.appendChild(card);
          if (info.visits > 1) {
            const badge = document.createElement("span");
            badge.className = "g-badge";
            badge.textContent = "×" + info.visits;
            wrap.appendChild(badge);
          }
          row.appendChild(wrap);
        });
        flow.appendChild(row);
      });
      mountGraph(target, flow);
      highlightSelection();
      markNextNodes(run);
    }

    function stepSpan(step, fallbackStart) {
      const start = Number(step.started_ms);
      const end = Number(step.ended_ms);
      if (Number.isFinite(start) && Number.isFinite(end) && end >= start) {
        return { start, end };
      }
      const dur = Number(step.elapsed_ms);
      const width = Number.isFinite(dur) && dur > 0 ? dur : 0.1;
      return { start: fallbackStart, end: fallbackStart + width };
    }

    function ganttModel(run) {
      const events = [];
      let cursor = 0;
      (run.steps || []).forEach((step) => {
        const span = stepSpan(step, cursor);
        events.push({ step, start: span.start, end: span.end });
        cursor = Number.isFinite(Number(step.ended_ms)) ? Number(step.ended_ms) : span.end;
      });
      const groups = [];
      const index = {};
      const origin = events.length ? Math.min(...events.map((event) => event.start)) : 0;
      const last = events.length ? Math.max(...events.map((event) => event.end)) : cursor;
      const ordered = events.slice().sort((a, b) => a.start - b.start || a.end - b.end);
      ordered.forEach((event, i) => {
        let nextStart = null;
        for (let j = i + 1; j < ordered.length; j += 1) {
          if (ordered[j].start > event.start) {
            nextStart = ordered[j].start;
            break;
          }
        }
        event.visualStart = event.start;
        event.visualEnd = nextStart != null ? nextStart : Math.max(event.end, last);
      });
      events.forEach((event) => {
        const name = event.step.node || "node";
        if (index[name] == null) {
          index[name] = groups.length;
          groups.push({ node: name, lanes: [[]] });
        }
        const group = groups[index[name]];
        const from = event.visualStart;
        const to = event.visualEnd;
        let lane = group.lanes.find((row) => row.every((other) => to <= other.visualStart || from >= other.visualEnd));
        if (!lane) {
          lane = [];
          group.lanes.push(lane);
        }
        lane.push(event);
      });
      const total = Math.max(0.1, last - origin);
      return { groups, total, origin };
    }

    function ensureGanttTip() {
      let tip = document.getElementById("ganttTip");
      if (!tip) {
        tip = document.createElement("div");
        tip.id = "ganttTip";
        tip.className = "gantt-tip";
        tip.hidden = true;
        document.body.appendChild(tip);
      }
      return tip;
    }

    function hideGanttTip() {
      const tip = document.getElementById("ganttTip");
      if (tip) tip.hidden = true;
    }

    function ganttTipHtml(event, maxes, origin) {
      const step = event.step;
      const kind = step.error ? "error" : stepKind(step.node);
      const kindLabel = kind === "decision" ? "decision" : kind === "loop" ? "loop" : kind === "error" ? "error" : "node";
      const startAt = formatTime(step.started_at);
      const endAt = formatTime(step.ended_at);
      const metrics = stepMetrics(step);
      const rows = [
        ["Start", startAt || "—"],
        ["End", endAt || "—"],
        ["Duration", formatElapsed(event.end - event.start) || formatElapsed(step.elapsed_ms) || "—"],
        ["Memory", formatMemory(metrics.mb)],
        ["Peak", formatMemory(metrics.peak)],
        ["Tokens", formatTokens(metrics)],
        ["Tool tokens", metrics.toolTokens == null ? "—" : String(metrics.toolTokens)],
        ["Tool", metrics.tool != null ? formatElapsed(metrics.tool) : "—"],
        ["Kind", kindLabel],
        ["Source", sourceLabel(nodeSource(step)) || "—"],
      ];
      return (
        '<div class="tip-name">' + escapeHtml(step.node || "") + "</div>" +
        '<div class="tip-id">' + escapeHtml(step.step_id || "") + "</div>" +
        rows.map((row) =>
          '<div class="tip-row"><span>' + row[0] + "</span><b>" + escapeHtml(row[1]) + "</b></div>"
        ).join("") +
        sparkBars(metrics, maxes, "tip-spark") +
        (step.reason ? '<div class="tip-why">' + escapeHtml(step.reason) + "</div>" : "") +
        (step.error ? '<div class="tip-err">' + escapeHtml(step.error) + "</div>" : "")
      );
    }

    function placeGanttTip(mouse, html) {
      const tip = ensureGanttTip();
      tip.innerHTML = html;
      tip.hidden = false;
      const pad = 14;
      const width = tip.offsetWidth;
      const height = tip.offsetHeight;
      let x = mouse.clientX + pad;
      let y = mouse.clientY + pad;
      if (x + width > window.innerWidth - 8) x = mouse.clientX - width - pad;
      if (y + height > window.innerHeight - 8) y = mouse.clientY - height - pad;
      tip.style.left = Math.max(8, x) + "px";
      tip.style.top = Math.max(8, y) + "px";
    }

    function bindGanttHover(bar, event, maxes, origin) {
      const html = ganttTipHtml(event, maxes, origin);
      bar.addEventListener("mouseenter", (mouse) => placeGanttTip(mouse, html));
      bar.addEventListener("mousemove", (mouse) => placeGanttTip(mouse, html));
      bar.addEventListener("mouseleave", hideGanttTip);
    }

    function renderGantt(run) {
      const target = $("diagram");
      hideGanttTip();
      if (!run || !(run.steps || []).length) {
        target.className = "empty";
        target.innerHTML = "Select a run to see the Gantt timeline.";
        applyGraphZoom();
        return;
      }
      const model = ganttModel(run);
      const maxes = runMetricMax(run);
      const ticks = [0, 0.25, 0.5, 0.75, 1].map((part) => formatElapsed(model.total * part));
      const flow = document.createElement("div");
      flow.className = "gflow gantt";
      flow.innerHTML =
        '<div class="gantt-head"><span>Node</span><div class="gantt-ticks">' +
        ticks.map((tick) => "<span>" + escapeHtml(tick) + "</span>").join("") +
        "</div></div>";
      model.groups.forEach((group) => {
        group.lanes.forEach((lane, laneIndex) => {
          const row = document.createElement("div");
          row.className = "gantt-row";
          const label = document.createElement("div");
          label.className = "gantt-label";
          label.textContent = laneIndex === 0 ? group.node : "";
          const track = document.createElement("div");
          track.className = "gantt-track";
          lane.forEach((event) => {
            const kind = event.step.error ? "error" : stepKind(event.step.node);
            const bar = document.createElement("button");
            bar.type = "button";
            bar.className = "gantt-bar" + (kind ? " " + kind : "");
            bar.dataset.stepId = event.step.step_id;
            const from = event.visualStart != null ? event.visualStart : event.start;
            const to = event.visualEnd != null ? event.visualEnd : event.end;
            const width = Math.max(((to - from) / model.total) * 100, 0.8);
            const left = Math.min(
              ((from - model.origin) / model.total) * 100,
              100 - width,
            );
            bar.style.left = Math.max(0, left) + "%";
            bar.style.width = width + "%";
            bindStepActions(bar, event.step);
            bindGanttHover(bar, event, maxes, model.origin);
            track.appendChild(bar);
          });
          row.appendChild(label);
          row.appendChild(track);
          flow.appendChild(row);
        });
      });
      target.className = "";
      target.innerHTML = "";
      mountGraph(target, flow);
      highlightSelection();
    }

    function setGraphView(mode) {
      graphViewMode = mode === "gantt" ? "gantt" : mode === "agents" ? "agents" : "graph";
      hideGanttTip();
      $("viewGraph").classList.toggle("active", graphViewMode === "graph");
      $("viewGantt").classList.toggle("active", graphViewMode === "gantt");
      $("viewAgents").classList.toggle("active", graphViewMode === "agents");
      renderGraph(currentRun);
    }

    function renderGraph(run) {
      if (graphViewMode === "gantt") {
        renderGantt(run);
        return;
      }
      if (graphViewMode === "agents") {
        renderAgents(run);
        return;
      }
      const target = $("diagram");
      const pipe = currentPipeline();
      if (pipe && pipe.error) {
        target.className = "empty";
        target.innerHTML = escapeHtml(pipe.error);
        applyGraphZoom();
        return;
      }
      const spec = graphSpec(run);
      if (spec) {
        const caption = !run
          ? "Current pipeline"
          : isOutdated(run)
            ? "Current pipeline — this run is outdated"
            : "";
        renderTopoGraph(spec, isOutdated(run) ? null : run, caption);
        return;
      }
      if (!run) {
        target.className = "empty";
        target.innerHTML = "Select a run to inspect the graph. Double-click a node to open its view.";
        applyGraphZoom();
        return;
      }
      target.className = "";
      const flow = document.createElement("div");
      flow.className = "gflow";
      flow.appendChild(graphCap("Start"));
      (run.steps || []).forEach((step) => {
        flow.appendChild(graphLine());
        const row = document.createElement("div");
        row.className = "g-row";
        const card = graphCard(step, false);
        bindStepActions(card, step);
        row.appendChild(card);
        const unused = step.unused || [];
        if (unused.length) {
          const skip = document.createElement("div");
          skip.className = "g-skip";
          const arm = document.createElement("div");
          arm.className = "g-skip-arm";
          const stack = document.createElement("div");
          stack.className = "g-skips";
          unused.forEach((name) => {
            stack.appendChild(graphCard({ node: name, reason: "not taken" }, true));
          });
          skip.appendChild(arm);
          skip.appendChild(stack);
          row.appendChild(skip);
        }
        flow.appendChild(row);
      });
      flow.appendChild(graphLine());
      flow.appendChild(graphCap("End"));
      target.innerHTML = "";
      mountGraph(target, flow);
      highlightSelection();
    }

    function logLevel(item) {
      const raw = String((item && item.level) || "print").toLowerCase();
      if (raw === "warn") return "warning";
      if (raw === "fatal") return "critical";
      return raw;
    }

    function logLevelLabel(level) {
      if (level === "warning") return "WARN";
      if (level === "critical") return "CRIT";
      if (level === "print") return "PRINT";
      if (level === "stderr") return "STDERR";
      return String(level || "INFO").toUpperCase();
    }

    function renderLogs(logs) {
      const box = $("runLog");
      if (!box) return;
      box.textContent = "";
      (logs || []).forEach((item) => appendLog(item, false));
    }

    function appendLog(item, scroll) {
      const box = $("runLog");
      if (!box || !item) return;
      const level = logLevel(item);
      const t = item.t == null || item.t === "" ? "" : formatElapsed(item.t);
      const src = item.logger && item.logger !== item.src
        ? (item.src || "run") + " · " + item.logger
        : (item.src || "run");
      const row = document.createElement("div");
      row.className = "log-line log-" + level;
      row.innerHTML =
        '<span class="log-t">' + escapeHtml(t) + "</span>" +
        '<span class="log-src">' + escapeHtml(src) + "</span>" +
        '<span class="log-lvl">' + escapeHtml(logLevelLabel(level)) + "</span>" +
        '<span class="log-msg">' + escapeHtml(item.text || "") + "</span>";
      box.appendChild(row);
      if (scroll !== false) box.scrollTop = box.scrollHeight;
    }

    function renderLiveSteps(run) {
      const steps = $("steps");
      steps.innerHTML = "";
      const wall = run && run.elapsed_ms != null
        ? Number(run.elapsed_ms)
        : Math.max(0, ...((run && run.steps) || []).map((step) => Number(step.ended_ms) || 0));
      $("stepsMs").textContent = run && (run.steps || []).some((step) => !step.pending)
        ? formatElapsed(wall)
        : (run ? "…" : "");
      const maxes = runMetricMax(run);
      (run && run.steps || []).forEach((step) => {
        const row = document.createElement("div");
        row.className = "step"
          + (selectedStep === step.step_id ? " selected" : "")
          + (step.error ? " error" : "")
          + (step.pending ? " pending" : "");
        row.dataset.stepId = step.step_id;
        const kind = step.error ? "error" : stepKind(step.node);
        const elapsed = step.pending ? "…" : formatElapsed(step.elapsed_ms);
        const loc = nodeSource(step);
        const srcHtml = loc ? '<div class="step-src">' + sourceChipHtml(loc) + "</div>" : "";
        row.innerHTML =
          '<div class="bar ' + kind + '"></div><div>' +
          '<div class="step-id">' + escapeHtml(step.step_id) + "</div>" +
          '<div class="step-name">' + escapeHtml(step.node) + "</div>" +
          srcHtml +
          '<div class="step-why">' + escapeHtml(step.pending ? "running" : (step.error || step.reason || "")) + "</div></div>" +
          (step.pending ? "<div></div>" : sparkBars(stepMetrics(step), maxes)) +
          '<div class="step-ms">' + escapeHtml(elapsed) + "</div>";
        if (!step.pending) bindStepActions(row, step);
        steps.appendChild(row);
      });
    }

    function pulseNode(name, on) {
      document.querySelectorAll(nodeCardSel(name)).forEach((card) => {
        card.classList.toggle("running", !!on);
        if (on) card.classList.remove("idle");
      });
    }

    function markLiveCard(step) {
      document.querySelectorAll(nodeCardSel(step.node)).forEach((card) => {
        card.classList.remove("running", "idle");
        card.classList.add("taken");
        if (step.error) card.classList.add("error");
        if (step.step_id) card.dataset.stepId = step.step_id;
        const detail = stepDetail(step);
        let sub = card.querySelector(".g-sub");
        if (detail) {
          if (!sub) {
            sub = document.createElement("span");
            sub.className = "g-sub";
            const body = card.querySelector(".g-body");
            if (body) body.appendChild(sub);
          }
          sub.textContent = detail;
        }
        bindStepActions(card, step);
      });
    }

    function renderRun(run) {
      const steps = $("steps");
      steps.innerHTML = "";
      setJson($("state"), run ? run.result : {}, null, { thread: false });
      renderMemory(run);
      renderLogs(run ? run.logs : []);
      $("ascii").textContent = run ? run.ascii : "";
      const wall = run && run.elapsed_ms != null
        ? Number(run.elapsed_ms)
        : Math.max(0, ...((run && run.steps) || []).map((step) => Number(step.ended_ms) || 0));
      $("stepsMs").textContent = run ? formatElapsed(wall) : "";
      if (!run) {
        selectedStep = null;
        renderGraph(null);
        showStepIO(null);
        closeNodeView();
        syncStepButtons();
        syncPauseControls();
        return;
      }
      if (run.approximate) {
        const note = document.createElement("div");
        note.className = "step-why";
        note.textContent = "Approximate replay — graph reducers and routing were not used"
          + (run.approximate_reason ? " (" + escapeHtml(run.approximate_reason) + ")" : "")
          + ".";
        steps.appendChild(note);
      }
      const selected = (run.steps || []).find((step) => step.step_id === selectedStep);
      showStepIO(selected || null);
      const maxes = runMetricMax(run);
      (run.steps || []).forEach((step) => {
        const row = document.createElement("div");
        row.className = "step" + (selectedStep === step.step_id ? " selected" : "") + (step.error ? " error" : "");
        row.dataset.stepId = step.step_id;
        const kind = step.error ? "error" : stepKind(step.node);
        const elapsed = formatElapsed(step.elapsed_ms);
        const loc = nodeSource(step);
        const srcHtml = loc ? '<div class="step-src">' + sourceChipHtml(loc) + "</div>" : "";
        row.innerHTML =
          '<div class="bar ' + kind + '"></div><div>' +
          '<div class="step-id">' + escapeHtml(step.step_id) + "</div>" +
          '<div class="step-name">' + escapeHtml(step.node) + "</div>" +
          srcHtml +
          '<div class="step-why">' + escapeHtml(step.error || step.reason || "") + "</div></div>" +
          sparkBars(stepMetrics(step), maxes) +
          '<div class="step-ms">' + escapeHtml(elapsed) + "</div>";
        bindStepActions(row, step);
        steps.appendChild(row);
      });
      renderGraph(run);
      syncStepButtons();
      syncPauseControls();
    }

    function newRunId() {
      if (crypto.randomUUID) return crypto.randomUUID().replace(/-/g, "");
      return Math.random().toString(16).slice(2) + Date.now().toString(16);
    }

    function syncRunControls() {
      const runningN = inflightRuns.filter((job) => !job.queued).length;
      const queuedN = inflightRuns.filter((job) => job.queued).length;
      const active = Math.max(slotActive, runningN);
      const waiting = Math.max(slotQueued, queuedN);
      const queueFull = active >= runLimit && waiting >= queueLimit;
      $("runBtn").disabled = queueFull;
      $("runBtn").textContent = "Run";
      const live = $("runLive");
      const cancelingN = inflightRuns.filter((job) => job.canceling).length;
      const n = inflightRuns.length;
      live.hidden = active + waiting === 0 && n === 0;
      live.classList.toggle("canceling", cancelingN > 0 && cancelingN === n && n > 0);
      if (n && cancelingN === n) {
        live.textContent = n === 1 ? "Canceling" : "Canceling " + n + "/" + runLimit;
      } else {
        live.textContent = "running " + active + "/" + runLimit + " · queued " + waiting;
      }
      syncPauseControls();
      paintPipelines();
      renderHistory();
      schedulePipeBoardRefresh();
    }

    async function readSse(res, onEvent) {
      if (!res.body) throw new Error("streaming is not supported");
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      const nl = String.fromCharCode(10);
      let buf = "";
      const consume = (chunk, final) => {
        buf += chunk;
        const parts = buf.split(nl + nl);
        if (!final) buf = parts.pop() || "";
        else buf = "";
        parts.forEach((part) => {
          part.split(nl).forEach((line) => {
            if (line.charCodeAt(line.length - 1) === 13) line = line.slice(0, -1);
            if (line.indexOf("data: ") !== 0) return;
            onEvent(JSON.parse(line.slice(6)));
          });
        });
      };
      while (true) {
        const { value, done } = await reader.read();
        if (done) {
          consume(decoder.decode(), true);
          break;
        }
        consume(decoder.decode(value, { stream: true }), false);
      }
    }

    function viewingJob(job) {
      return Boolean(job && currentRun && currentRun.id === job.id);
    }

    function handleLiveEvent(event, pendingByNode, job) {
      const type = event && event.type;
      const run = job.run || (job.run = { id: job.id, input: job.input, steps: [], result: {}, logs: [] });
      const viewing = viewingJob(job);
      if (type === "ping") return;
      if (type === "queued") {
        job.queued = true;
        job.position = event.position;
        run.status = "queued";
        run.queued = true;
        run.queue_position = event.position;
        if (Number.isFinite(Number(event.active))) slotActive = Math.max(0, Math.floor(Number(event.active)));
        if (Number.isFinite(Number(event.queued))) slotQueued = Math.max(0, Math.floor(Number(event.queued)));
        if (Number.isFinite(Number(event.limit)) && Number(event.limit) >= 1) runLimit = Math.floor(Number(event.limit));
        if (Number.isFinite(Number(event.queue_limit)) && Number(event.queue_limit) >= 0) queueLimit = Math.floor(Number(event.queue_limit));
        if (viewing) currentRun = run;
        renderHistory();
        schedulePipeBoardRefresh();
        syncRunControls();
        return;
      }
      if (type === "start") {
        job.queued = false;
        run.status = "running";
        run.queued = false;
        run.id = event.run_id || run.id;
        job.id = run.id;
        run.input = event.input || run.input;
        run.started_at = event.started_at;
        if (viewing) currentRun = run;
        renderHistory();
        schedulePipeBoardRefresh();
        syncRunControls();
        return;
      }
      if (type === "node_start") {
        if (viewing) pulseNode(event.node, true);
        const pending = {
          step_id: event.node + "#…",
          node: event.node,
          pending: true,
          started_ms: event.started_ms,
        };
        (pendingByNode[event.node] || (pendingByNode[event.node] = [])).push(pending);
        run.steps = (run.steps || []).concat([pending]);
        if (viewing) renderLiveSteps(run);
        schedulePipeBoardRefresh();
        return;
      }
      if (type === "step") {
        const step = event.step;
        if (viewing) {
          pulseNode(step.node, false);
          markLiveCard(step);
        }
        const queue = pendingByNode[step.node] || [];
        const pending = queue.shift();
        const steps = run.steps || [];
        const idx = pending ? steps.indexOf(pending) : -1;
        if (idx >= 0) steps[idx] = step;
        else steps.push(step);
        run.steps = steps;
        if (viewing) {
          renderLiveSteps(run);
          if (graphViewMode === "gantt") renderGantt(run);
          if (graphViewMode === "agents") renderAgents(run);
          renderMemory(run);
        }
        schedulePipeBoardRefresh();
        return;
      }
      if (type === "state") {
        run.result = event.state;
        if (viewing) setJson($("state"), event.state, null, { thread: false });
        return;
      }
      if (type === "log") {
        run.logs = (run.logs || []).concat([{
          t: event.t, src: event.src, text: event.text, level: event.level, logger: event.logger,
        }]);
        if (viewing) appendLog(event, true);
        return;
      }
      if (type === "done" || type === "paused" || type === "canceled") {
        job.queued = false;
        if (event.run) job.run = event.run;
        if (viewing && event.run) {
          currentRun = event.run;
          selectedStep = null;
          renderRun(currentRun);
        }
        renderHistory();
        schedulePipeBoardRefresh();
        return;
      }
      if (type === "error") {
        throw new Error(event.error || "run failed");
      }
    }

    async function streamLive(extra) {
      extra = extra || {};
      const resume = Boolean(extra.resume);
      if (resume && !runIsPaused(currentRun)) {
        alert("Open a paused run first");
        return;
      }
      let input;
      if (!resume) {
        try { input = JSON.parse($("input").value || "{}"); }
        catch (err) { alert("Input must be JSON"); return; }
      }
      const runId = resume ? currentRun.id : newRunId();
      const controller = new AbortController();
      const liveRun = resume
        ? Object.assign({}, currentRun, { status: "queued", queued: true })
        : { id: runId, input: input, steps: [], result: {}, logs: [], status: "queued", queued: true };
      const job = {
        id: runId,
        controller: controller,
        fileId: fileId,
        input: liveRun.input,
        started_at: new Date().toISOString(),
        resume: resume,
        queued: true,
        position: 0,
        run: liveRun,
      };
      inflightRuns.push(job);
      selectedStep = null;
      currentRun = liveRun;
      syncRunControls();
      if (!resume) {
        const logDetails = $("logDetails");
        if (logDetails) logDetails.open = true;
        renderLogs([]);
        setJson($("state"), {}, null, { thread: false });
        $("ascii").textContent = "";
        $("steps").innerHTML = "";
        $("stepsMs").textContent = "…";
        renderMemory(null);
        if (graphViewMode === "graph") {
          const spec = graphSpec(null);
          if (spec) renderTopoGraph(spec, { live: true, steps: [] }, "Running…");
        }
      } else {
        const logDetails = $("logDetails");
        if (logDetails) logDetails.open = true;
        if (graphViewMode === "graph") {
          const spec = graphSpec(currentRun);
          if (spec) renderTopoGraph(spec, currentRun, "Resuming…");
        }
      }
      const payload = {
        file: fileId,
        run_id: runId,
        interrupt_before: Object.prototype.hasOwnProperty.call(extra, "interrupt_before")
          ? extra.interrupt_before
          : interruptBeforeList(),
      };
      if (resume) payload.resume = true;
      else payload.input = input;
      if (extra.interrupt_after && extra.interrupt_after.length) {
        payload.interrupt_after = extra.interrupt_after;
      }
      if (Object.prototype.hasOwnProperty.call(extra, "resume_value")) {
        payload.resume_value = extra.resume_value;
      }
      const pendingByNode = {};
      try {
        const res = await fetch("/api/run/stream", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
          signal: controller.signal,
        });
        if (!res.ok) {
          const data = await res.json().catch(() => ({}));
          if (res.status === 429) {
            throw new Error(
              data.error ||
              ("queue full (" + (data.queued || 0) + "/" + (data.queue_limit || queueLimit) +
                " waiting, " + (data.active || 0) + "/" + (data.limit || runLimit) + " running)")
            );
          }
          throw new Error(data.error || res.statusText);
        }
        let finished = false;
        await readSse(res, (event) => {
          handleLiveEvent(event, pendingByNode, job);
          if (event && (event.type === "done" || event.type === "paused" || event.type === "canceled")) finished = true;
        });
        if (!finished && !(job.run && (job.run.error || job.run.canceled))) {
          throw new Error("run produced no result");
        }
      } catch (err) {
        if (err && err.name === "AbortError") return;
        throw err;
      } finally {
        const keepId = job.run && job.run.id;
        const watching = Boolean(currentRun && (currentRun.id === job.id || currentRun.id === keepId));
        inflightRuns = inflightRuns.filter((item) => item !== job);
        syncRunControls();
        await loadMeta();
        await loadPipelines();
        if (watching && keepId && job.fileId === fileId) {
          try {
            await openRun(keepId);
          } catch (err) {
            await loadHistory();
          }
        } else {
          await loadHistory();
        }
      }
    }

    async function runGraph() {
      await streamLive();
    }

    async function continueRun() {
      const hits = hitlPayloads(currentRun);
      if (hits.length) {
        let value;
        try { value = JSON.parse($("hitlValue").value || "true"); }
        catch (err) { alert("Resume value must be JSON"); return; }
        await streamLive({ resume: true, resume_value: value });
        return;
      }
      await streamLive({ resume: true });
    }

    async function stepRun() {
      const next = ((currentRun && currentRun.next) || [])[0];
      if (!next) {
        alert("Nothing to step");
        return;
      }
      const before = interruptBeforeList().filter((name) => name !== next);
      await streamLive({ resume: true, interrupt_before: before, interrupt_after: [next] });
    }

    async function cancelRuns() {
      const id = currentRun && currentRun.id;
      if (!id) return;
      if (!(currentLiveJob() || runIsBusy(currentRun) || runIsPaused(currentRun))) return;
      const job = inflightRuns.find((item) => item.id === id) || null;
      if (job && job.canceling) return;
      if (!job && (pendingCancel || (currentRun && currentRun.status === "canceling"))) return;
      if (job) job.canceling = true;
      else {
        pendingCancel = { runId: id, fileId: fileId };
        currentRun = Object.assign({}, currentRun, { status: "canceling" });
        historyRuns = historyRuns.map((row) => (
          row.id === id ? Object.assign({}, row, { status: "canceling" }) : row
        ));
      }
      syncRunControls();
      await fetch("/api/run/cancel", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ run_id: id }),
      }).catch(() => null);
      if (!job) {
        try {
          if (currentRun && currentRun.id === id) await openRun(id);
        } finally {
          pendingCancel = null;
          syncRunControls();
        }
      }
    }

    function currentPipeline() {
      return pipelines.find((p) => p.id === fileId) || null;
    }

    function isOutdated(run) {
      if (!run || run.live || run.status === "running" || run.status === "canceling" || run.status === "queued") return false;
      if (inflightRuns.some((job) => job.id === run.id)) return false;
      const pipe = currentPipeline();
      if (!pipe) return false;
      if (pipe.graph_hash && run.graph_hash !== pipe.graph_hash) return true;
      if (pipe.file_sha256 && run.file_sha256 !== pipe.file_sha256) return true;
      return false;
    }

    function graphDiff(oldGraph, newGraph) {
      const oldSpec = oldGraph && typeof oldGraph === "object" ? oldGraph : {};
      const newSpec = newGraph && typeof newGraph === "object" ? newGraph : {};
      const lines = [];
      const asSet = (spec, key) => new Set((spec[key] || []).map((item) => String(item)));
      [["nodes", "node"], ["state", "state"], ["tools", "tool"]].forEach(([key, label]) => {
        const before = asSet(oldSpec, key);
        const after = asSet(newSpec, key);
        [...after].filter((item) => !before.has(item)).sort().forEach((item) => {
          lines.push("+ " + label + " " + item);
        });
        [...before].filter((item) => !after.has(item)).sort().forEach((item) => {
          lines.push("- " + label + " " + item);
        });
      });
      const edgeKey = (item) => Array.isArray(item) && item.length >= 2
        ? String(item[0]) + "\\0" + String(item[1])
        : "";
      const beforeEdges = new Set((oldSpec.edges || []).map(edgeKey).filter(Boolean));
      const afterEdges = new Set((newSpec.edges || []).map(edgeKey).filter(Boolean));
      [...afterEdges].filter((item) => !beforeEdges.has(item)).sort().forEach((item) => {
        const [source, target] = item.split("\\0");
        lines.push("+ edge " + source + " → " + target);
      });
      [...beforeEdges].filter((item) => !afterEdges.has(item)).sort().forEach((item) => {
        const [source, target] = item.split("\\0");
        lines.push("- edge " + source + " → " + target);
      });
      const oldImpl = oldSpec.impl && typeof oldSpec.impl === "object" ? oldSpec.impl : {};
      const newImpl = newSpec.impl && typeof newSpec.impl === "object" ? newSpec.impl : {};
      const implNames = Array.from(new Set([...Object.keys(oldImpl), ...Object.keys(newImpl)])).sort();
      implNames.forEach((name) => {
        if (oldImpl[name] !== newImpl[name]) lines.push("~ node " + name);
      });
      if ((oldSpec.file || "") !== (newSpec.file || "") && !lines.some((line) => line.indexOf("~ node ") === 0)) {
        lines.push("~ pipeline file");
      }
      return lines;
    }

    function confirmOutdatedReplay() {
      return new Promise((resolve) => {
        if (!isOutdated(currentRun)) {
          resolve(true);
          return;
        }
        const pipe = currentPipeline();
        const oldHash = currentRun.graph_hash || "";
        const newHash = (pipe && pipe.graph_hash) || "";
        $("outdatedHashes").innerHTML =
          '<div><span>old</span><code title="' + escapeHtml(oldHash) + '">' +
          escapeHtml(oldHash ? oldHash.slice(0, 12) : "none") + "</code></div>" +
          '<div><span>new</span><code title="' + escapeHtml(newHash) + '">' +
          escapeHtml(newHash ? newHash.slice(0, 12) : "none") + "</code></div>";
        const snap = currentRun.graph;
        const empty = !snap || !(
          (snap.nodes && snap.nodes.length) ||
          (snap.edges && snap.edges.length) ||
          (snap.state && snap.state.length) ||
          (snap.tools && snap.tools.length)
        );
        const diffs = !currentRun.graph_hash && empty
          ? ["no structure snapshot on this run"]
          : graphDiff(snap, pipe && pipe.graph);
        $("outdatedDiffs").innerHTML = (diffs.length ? diffs : ["node code or pipeline file changed"])
          .map((line) => "<li>" + escapeHtml(line) + "</li>").join("");
        const modal = $("outdatedModal");
        modal.hidden = false;
        const done = (ok) => {
          modal.hidden = true;
          $("outdatedCancel").onclick = null;
          $("outdatedGo").onclick = null;
          modal.onclick = null;
          resolve(ok);
        };
        $("outdatedCancel").onclick = () => done(false);
        $("outdatedGo").onclick = () => done(true);
        modal.onclick = (event) => {
          if (event.target === modal) done(false);
        };
      });
    }

    async function rerun(mode) {
      if (!currentRun || !selectedStep) {
        alert("Select a node first");
        return;
      }
      const input = readNodeInput();
      closeNodeView();
      if (!(await confirmOutdatedReplay())) return;
      const parentId = currentRun.id;
      const runId = newRunId();
      const job = {
        id: runId,
        controller: null,
        fileId: fileId,
        input: currentRun.input,
        started_at: new Date().toISOString(),
        resume: false,
        queued: true,
        position: 0,
        run: {
          id: runId,
          input: currentRun.input,
          steps: [],
          result: {},
          logs: [],
          status: "queued",
          queued: true,
          mode: mode === "resume" ? "replay_from" : "replay",
          parent_id: parentId,
        },
      };
      inflightRuns.push(job);
      currentRun = job.run;
      syncRunControls();
      try {
        const saved = await api("/api/rerun", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            run_id: parentId,
            new_run_id: runId,
            step_id: selectedStep,
            mode,
            input,
          }),
        });
        job.run = saved;
        if (currentRun && currentRun.id === runId) {
          currentRun = saved;
          selectedStep = (currentRun.steps && currentRun.steps[0] && currentRun.steps[0].step_id) || null;
          renderRun(currentRun);
        }
      } finally {
        inflightRuns = inflightRuns.filter((item) => item !== job);
        syncRunControls();
        await loadHistory();
        await loadPipelines();
        await loadMeta();
      }
    }

    async function removeRun() {
      if (!currentRun) return;
      if (runIsBusy(currentRun)) {
        throw new Error("cancel the run before deleting");
      }
      await api("/api/runs/" + currentRun.id, { method: "DELETE" });
      await loadHistory();
      await loadPipelines();
      const next = (historyRuns || [])[0];
      if (next) {
        await openRun(next.id);
        return;
      }
      currentRun = null;
      renderRun(null);
      renderHistory();
    }

    async function clearPipelineRuns(p) {
      const page = $("pipelinesView");
      const top = page ? page.scrollTop : 0;
      if (!p) {
        if (page) page.scrollTop = top;
        return;
      }
      if (boardRunsForPipeline(p).some((run) => runIsBusy(run)) || inflightRuns.some((job) => job.fileId === p.id)) {
        if (page) page.scrollTop = top;
        throw new Error("cancel queued or running jobs before clearing");
      }
      if (!confirm("Delete all runs for " + p.stem + "?")) {
        if (page) page.scrollTop = top;
        return;
      }
      await api("/api/runs?file=" + encodeURIComponent(p.id), { method: "DELETE" });
      if (fileId === p.id) {
        currentRun = null;
        renderRun(null);
        await loadHistory();
      }
      await loadPipelines();
      if (page) {
        page.scrollTop = top;
        requestAnimationFrame(() => { page.scrollTop = top; });
      }
    }

    function exportRun() {
      if (!currentRun) return;
      const clean = { ...currentRun };
      delete clean.mermaid;
      delete clean.ascii;
      const blob = new Blob([JSON.stringify(clean, null, 2)], { type: "application/json" });
      const link = document.createElement("a");
      link.href = URL.createObjectURL(blob);
      link.download = (currentRun.pipeline || "run") + "-" + currentRun.id + ".json";
      link.click();
      URL.revokeObjectURL(link.href);
    }

    async function importRunObject(data) {
      const saved = await api("/api/import", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ run: data, file: fileId }),
      });
      if (saved.pipeline && pipelines.some((p) => p.stem === saved.pipeline)) {
        const match = pipelines.find((p) => p.stem === saved.pipeline);
        if (match && match.id !== fileId) await selectPipeline(match.id);
      }
      await openRun(saved.id);
      await loadHistory();
      await loadPipelines();
    }

    async function importFromFile(file) {
      const data = JSON.parse(await file.text());
      await importRunObject(data);
    }

    function runOptionLabel(run) {
      const input = truncateJson(run.input);
      const short = input.length > 36 ? input.slice(0, 33) + "..." : input;
      return [run.pipeline, formatTime(run.created_at), short].filter(Boolean).join("  ·  ");
    }

    function comparePipeFilter() {
      return ($("cmpPipe").value || "all");
    }

    function filteredCompareRuns() {
      const pipe = comparePipeFilter();
      if (pipe === "all") return compareRuns;
      return compareRuns.filter((run) => run.pipeline === pipe);
    }

    function fileIdForStem(stem) {
      const found = (pipelines || []).find((p) => p.stem === stem);
      return found ? found.id : stem;
    }

    function formatTok(value) {
      const n = Number(value) || 0;
      if (!Number.isFinite(n)) return "0";
      const abs = Math.abs(n);
      if (abs >= 1000000) return (n / 1000000).toFixed(1).replace(/\.0$/, "") + "M";
      if (abs >= 1000) return (n / 1000).toFixed(1).replace(/\.0$/, "") + "k";
      if (abs >= 100 || Number.isInteger(n)) return String(Math.round(n));
      return n.toFixed(1).replace(/\.0$/, "");
    }

    function avgTok(total, count) {
      const n = Number(count) || 0;
      if (!n) return 0;
      return (Number(total) || 0) / n;
    }

    function statsPipeFilter() {
      return ($("statsPipe") && $("statsPipe").value) || "";
    }

    function statsMatchesPipe(row) {
      const pipe = statsPipeFilter();
      return !!pipe && row.pipeline === pipe;
    }

    function jumpToRuns(stem) {
      if (stem) runsFocusStem = stem;
      else {
        const pipe = currentPipeline();
        runsFocusStem = (pipe && pipe.stem) || statsPipeFilter() || "";
      }
      showView("runs");
    }

    function jumpToStatistics(stem) {
      if (stem) statsPipePreferred = stem;
      else {
        const pipe = currentPipeline();
        if (pipe && pipe.stem) statsPipePreferred = pipe.stem;
      }
      showView("statistics");
    }

    async function jumpToTrace(pipelineId) {
      if (pipelineId) {
        await openPipeline(pipelineId);
        return;
      }
      showView("trace");
    }

    function fillStatsPipeFilter() {
      const sel = $("statsPipe");
      const keep = statsPipePreferred || sel.value || "";
      const stems = Array.from(new Set([
        ...(pipelines || []).map((p) => p.stem),
        ...(statsData.pipelines || []).map((row) => row.pipeline),
        ...(statsData.runs || []).map((row) => row.pipeline),
      ].filter(Boolean))).sort();
      sel.innerHTML = '<option value="">Select a pipeline</option>';
      stems.forEach((stem) => {
        const opt = document.createElement("option");
        opt.value = stem;
        opt.textContent = stem;
        sel.appendChild(opt);
      });
      sel.value = stems.indexOf(keep) >= 0 ? keep : "";
      statsPipePreferred = sel.value;
    }

    function setStatsTab(name) {
      statsTab = name === "cost" ? "cost" : name === "runtime" ? "runtime" : "routing";
      $("statsTabRouting").classList.toggle("active", statsTab === "routing");
      $("statsTabCost").classList.toggle("active", statsTab === "cost");
      $("statsTabRuntime").classList.toggle("active", statsTab === "runtime");
      renderStats();
    }

    function pipelineGraph(stem) {
      const found = (pipelines || []).find((item) => item.stem === stem);
      return found && found.graph && (found.graph.nodes || []).length ? found.graph : null;
    }

    function statsChoiceTarget(from, choice, spec) {
      let to = topoName(choice);
      if (to === "done" || to === "end") to = "END";
      const edges = ((spec && spec.edges) || []).map((pair) => [topoName(pair[0]), topoName(pair[1])]);
      if (edges.some((pair) => pair[0] === from && pair[1] === to)) return to;
      const lower = String(choice || "").toLowerCase();
      const match = edges.find((pair) => pair[0] === from && topoName(pair[1]).toLowerCase() === lower);
      return match ? match[1] : to;
    }

    function inferStatsEdges(spec, visits, edgeCount, hasPath) {
      const edges = ((spec && spec.edges) || []).map((pair) => [topoName(pair[0]), topoName(pair[1])]);
      const outgoing = {};
      edges.forEach((pair) => {
        (outgoing[pair[0]] || (outgoing[pair[0]] = [])).push(pair[1]);
      });
      if (hasPath) {
        (outgoing.START || []).forEach((to) => {
          if (!edgeCount["START->" + to] && visits[to]) edgeCount["START->" + to] = visits[to];
        });
        return;
      }
      Object.keys(outgoing).forEach((from) => {
        const outs = outgoing[from];
        const fromVisits = from === "START" ? 0 : (visits[from] || 0);
        if (from !== "START" && !fromVisits) return;
        const missing = outs.filter((to) => !edgeCount[from + "->" + to]);
        if (!missing.length) return;
        if (outs.length === 1) {
          const to = outs[0];
          if (from === "START") {
            if (visits[to]) edgeCount[from + "->" + to] = visits[to];
            return;
          }
          if (to === "END") {
            edgeCount[from + "->" + to] = fromVisits;
            return;
          }
          if (visits[to]) edgeCount[from + "->" + to] = Math.min(fromVisits, visits[to]);
          return;
        }
        if (outs.some((to) => edgeCount[from + "->" + to])) return;
        missing.forEach((to) => {
          const destVisits = to === "END" ? fromVisits : (visits[to] || 0);
          if (destVisits) edgeCount[from + "->" + to] = destVisits;
        });
      });
    }

    function statsOverlayFor(stem, mode) {
      const spec = pipelineGraph(stem);
      const edgeCount = {};
      const nodeVisits = {};
      const nodeDetail = {};
      (statsData.nodes || []).filter((row) => row.pipeline === stem).forEach((row) => {
        const name = topoName(row.node);
        nodeVisits[name] = row.visits || 0;
        if (mode === "cost") {
          const billed = Math.max(0, (Number(row.visits) || 0) - (Number(row.unavailable) || 0));
          if (row.unavailable && !row.total) nodeDetail[name] = "no usage";
          else if (billed) {
            nodeDetail[name] = "avg " + formatTok(avgTok(row.prompt, billed)) +
              " in · " + formatTok(avgTok(row.completion, billed)) + " out";
          } else {
            nodeDetail[name] = formatTok(row.prompt) + " in · " + formatTok(row.completion) + " out";
          }
        } else if (mode === "runtime") {
          const timed = Number(row.elapsed_n) || 0;
          const ms = Number(row.elapsed_ms) || 0;
          if (timed && ms) nodeDetail[name] = "avg " + formatElapsed(avgTok(ms, timed));
          else if (ms) nodeDetail[name] = formatElapsed(ms);
          else nodeDetail[name] = "no timing";
        }
      });
      const byNode = {};
      const decisionVisits = {};
      (statsData.decisions || []).filter((row) => row.pipeline === stem).forEach((row) => {
        const from = topoName(row.node);
        (byNode[from] || (byNode[from] = [])).push(row);
        decisionVisits[from] = (decisionVisits[from] || 0) + (Number(row.count) || 0);
      });
      Object.keys(decisionVisits).forEach((name) => {
        if (!nodeVisits[name]) nodeVisits[name] = decisionVisits[name];
      });
      const pathEdges = (statsData.edges || []).filter((row) => row.pipeline === stem);
      if (pathEdges.length) {
        pathEdges.forEach((row) => {
          const from = topoName(row.from || row.src);
          const to = topoName(row.to || row.dst);
          if (!from || !to) return;
          const key = from + "->" + to;
          edgeCount[key] = (edgeCount[key] || 0) + (Number(row.count) || 0);
        });
      } else {
        (statsData.decisions || []).filter((row) => row.pipeline === stem).forEach((row) => {
          const from = topoName(row.node);
          const to = statsChoiceTarget(from, row.choice, spec);
          const key = from + "->" + to;
          edgeCount[key] = (edgeCount[key] || 0) + (Number(row.count) || 0);
        });
      }
      inferStatsEdges(spec, nodeVisits, edgeCount, pathEdges.length > 0);
      if (mode === "routing") {
        Object.keys(byNode).forEach((name) => {
          const rows = byNode[name];
          const total = rows.reduce((sum, row) => sum + (Number(row.count) || 0), 0);
          if (!total) return;
          rows.sort((a, b) => (b.count || 0) - (a.count || 0));
          const top = rows[0];
          nodeDetail[name] = Math.round((top.count / total) * 100) + "% " + top.choice;
        });
      }
      return { mode: mode, spec: spec, edgeCount: edgeCount, nodeVisits: nodeVisits, nodeDetail: nodeDetail };
    }

    function renderStatsTopo(target, spec, overlay, stem) {
      const model = topologyModel(spec, null, overlay);
      if (!model.layers.length) {
        target.innerHTML = '<p class="empty">No graph for this pipeline.</p>';
        return;
      }
      const byName = {};
      model.nodes.forEach((node) => { byName[node.name] = node; });
      const flow = document.createElement("div");
      flow.className = "gflow topo";
      flow._topoEdges = model.edges;
      model.layers.forEach((layer) => {
        const row = document.createElement("div");
        row.className = "topo-layer";
        layer.forEach((name) => {
          if (name === "START" || name === "END") {
            const cap = graphCap(name === "START" ? "Start" : "End");
            cap.dataset.node = name;
            row.appendChild(cap);
            return;
          }
          const info = byName[name] || { name: name, status: "idle", visits: 0, detail: "" };
          const skipped = info.status === "skipped";
          const card = graphCard({
            node: name,
            reason: info.detail || (skipped ? "not taken" : ""),
            step_id: "",
          }, skipped);
          card.dataset.node = name;
          if (info.status === "idle") card.classList.add("idle");
          if (info.status === "taken") card.classList.add("taken");
          if (statsChoice && statsChoice.node === name && statsChoice.pipeline === stem) {
            card.classList.add("selected");
          }
          card.onclick = () => {
            const next = { node: name, choice: "", pipeline: stem };
            if (statsChoice && statsChoice.node === name && statsChoice.pipeline === stem) statsChoice = null;
            else statsChoice = next;
            renderStats();
          };
          const wrap = document.createElement("div");
          wrap.className = "topo-node";
          wrap.appendChild(card);
          if (overlay.mode === "cost" && info.visits) {
            const tok = (statsData.nodes || []).find((row) => row.pipeline === stem && topoName(row.node) === name);
            if (tok && tok.total) {
              const badge = document.createElement("span");
              badge.className = "g-badge";
              badge.title = "node total";
              badge.textContent = "Σ " + formatTok(tok.total);
              wrap.appendChild(badge);
            }
          } else if (info.visits > 1) {
            const badge = document.createElement("span");
            badge.className = "g-badge";
            badge.textContent = "×" + info.visits;
            wrap.appendChild(badge);
          }
          row.appendChild(wrap);
        });
        flow.appendChild(row);
      });
      target.innerHTML = "";
      const wrap = document.createElement("div");
      wrap.className = "gzoom";
      wrap.appendChild(flow);
      target.appendChild(wrap);
      requestAnimationFrame(() => drawTopoEdges(flow));
    }

    function statsReasonGroups(row) {
      if (row.groups && row.groups.length) return row.groups;
      return (row.reasons || []).map((item) => ({
        text: item.text,
        count: item.count,
        unique: 1,
        examples: [item.text],
      }));
    }

    function statsReasonPanel(stem, node) {
      const rows = (statsData.decisions || []).filter((row) =>
        row.pipeline === stem && topoName(row.node) === node
      );
      if (!rows.length) return "";
      rows.sort((a, b) => (b.count || 0) - (a.count || 0));
      const total = rows.reduce((sum, row) => sum + (Number(row.count) || 0), 0) || 1;
      const max = Math.max.apply(null, rows.map((row) => Number(row.count) || 0).concat([1]));
      const body = rows.map((row) => {
        const count = Number(row.count) || 0;
        const pct = Math.round((count / total) * 100);
        const groups = statsReasonGroups(row);
        const skipSub = groups.length === 1 && (groups[0].unique || 1) === 1 &&
          (!groups[0].text || groups[0].text === row.choice);
        const nested = skipSub ? "" : groups.map((item) => {
          const unique = Number(item.unique) || 1;
          const samples = (item.examples || []).slice(0, 2);
          const eg = unique > 1
            ? '<div class="st-why-eg">' + escapeHtml(samples.join(" · ")) +
              (unique > samples.length ? " · +" + (unique - samples.length) + " more" : "") +
              "</div>"
            : "";
          return '<div class="st-why-row sub"><div class="st-why-text" title="' +
            escapeHtml(item.text) + '">' + escapeHtml(item.text) +
            '</div><div class="st-why-n">' + (item.count || 0) + "</div></div>" + eg;
        }).join("");
        return '<div class="st-why-group"><div class="st-why-row choice">' +
          '<div class="st-why-text">' + escapeHtml(row.choice) + "</div>" +
          '<div class="st-why-pct">' + pct + "%</div>" +
          '<div class="st-why-n">' + count + "</div>" +
          '<div class="st-why-bar"><i style="width:' +
          Math.max(4, Math.round((count / max) * 100)) + '%"></i></div></div>' +
          (nested ? '<div class="st-why-sub">' + nested + "</div>" : "") +
          "</div>";
      }).join("");
      return '<div class="st-why-panel"><h3>' + escapeHtml(node) +
        " · " + total + " decisions</h3><div class=\"st-why\">" +
        body + "</div></div>";
    }

    function statsPipeRow(stem) {
      return (statsData.pipelines || []).find((row) => row.pipeline === stem) || {
        pipeline: stem,
        runs: 0,
        prompt: 0,
        completion: 0,
        total: 0,
        usage_runs: 0,
        unavailable_runs: 0,
        elapsed_ms: 0,
        elapsed_n: 0,
        node_elapsed_ms: 0,
      };
    }

    function statsPercentile(sorted, q) {
      if (!sorted.length) return 0;
      const idx = (sorted.length - 1) * q;
      const lo = Math.floor(idx);
      const hi = Math.ceil(idx);
      if (lo === hi) return sorted[lo];
      return sorted[lo] * (hi - idx) + sorted[hi] * (idx - lo);
    }

    function statsBarPercents(values) {
      const nums = values.map((n) => Math.max(0, Number(n) || 0));
      const total = nums.reduce((sum, n) => sum + n, 0);
      if (!total) return nums.map(() => 0);
      const pcts = nums.map((n, index) => (
        index === nums.length - 1 ? 0 : Math.round((n / total) * 100)
      ));
      const used = pcts.slice(0, -1).reduce((sum, n) => sum + n, 0);
      pcts[pcts.length - 1] = Math.max(0, 100 - used);
      return pcts;
    }

    function statsColorTip(rows) {
      return '<div class="st-split-tip">' +
        rows.map((row) =>
          '<div class="st-split-tip-row"><i class="' + row.kind + '"></i>' +
          escapeHtml(row.label) + "</div>"
        ).join("") +
        "</div>";
    }

    function statsRunOutcomes(stem) {
      const rows = (statsData.runs || []).filter((row) => row.pipeline === stem);
      const out = { ok: 0, fail: 0, paused: 0, canceled: 0, total: rows.length };
      rows.forEach((row) => {
        if (row.status === "error") out.fail += 1;
        else if (row.status === "paused") out.paused += 1;
        else if (row.status === "canceled") out.canceled += 1;
        else out.ok += 1;
      });
      return out;
    }

    function statsRoutingSummary(stem) {
      const row = statsPipeRow(stem);
      const runs = Number(row.runs) || 0;
      const visits = (statsData.nodes || [])
        .filter((item) => item.pipeline === stem)
        .reduce((sum, item) => sum + (Number(item.visits) || 0), 0);
      const out = statsRunOutcomes(stem);
      const pcts = statsBarPercents([out.ok, out.fail, out.canceled, out.paused]);
      const meta = out.ok + " success · " + out.fail + " failed · " +
        out.canceled + " canceled · " + out.paused + " paused";
      const el = document.createElement("div");
      el.className = "st-sum";
      el.innerHTML =
        '<div class="fig"><b>' + escapeHtml(String(runs)) + "</b><span>grand total</span></div>" +
        '<div class="fig"><b>' + escapeHtml(formatTok(avgTok(visits, runs))) + "</b><span>avg node count</span></div>" +
        '<div class="split"><div class="st-hero-split">' +
        '<i class="ok" style="width:' + pcts[0] + '%"></i>' +
        '<i class="fail" style="width:' + pcts[1] + '%"></i>' +
        '<i class="canceled" style="width:' + pcts[2] + '%"></i>' +
        '<i class="paused" style="width:' + pcts[3] + '%"></i>' +
        "</div>" + statsColorTip([
          { kind: "ok", label: "success" },
          { kind: "fail", label: "failed" },
          { kind: "canceled", label: "canceled" },
          { kind: "paused", label: "paused" },
        ]) +
        '<div class="meta">' + escapeHtml(meta) + "</div></div>";
      return el;
    }

    function statsCostSummary(stem) {
      const row = statsPipeRow(stem);
      const runs = Number(row.runs) || 0;
      const usage = Number(row.usage_runs) || 0;
      const denom = usage || runs;
      const prompt = Number(row.prompt) || 0;
      const completion = Number(row.completion) || 0;
      const total = Number(row.total) || 0;
      const missing = Number(row.unavailable_runs) || 0;
      const sum = prompt + completion;
      const inPct = sum ? Math.round((prompt / sum) * 100) : 0;
      const outPct = sum ? (100 - inPct) : 0;
      let meta = formatTok(prompt) + " in · " + formatTok(completion) + " out · " +
        runs + " run" + (runs === 1 ? "" : "s");
      if (usage && usage !== runs) meta += " · " + usage + " with usage";
      else if (missing && !usage) meta += " · no usage recorded";
      const el = document.createElement("div");
      el.className = "st-sum";
      el.innerHTML =
        '<div class="fig"><b>' + escapeHtml(formatTok(total)) + "</b><span>grand total</span></div>" +
        '<div class="fig"><b>' + escapeHtml(formatTok(avgTok(total, denom))) + "</b><span>avg / run</span></div>" +
        '<div class="split"><div class="st-hero-split">' +
        '<i class="in" style="width:' + inPct + '%"></i>' +
        '<i class="out" style="width:' + outPct + '%"></i>' +
        "</div>" + statsColorTip([
          { kind: "in", label: "input tokens" },
          { kind: "out", label: "output tokens" },
        ]) +
        '<div class="meta">' + escapeHtml(meta) + "</div></div>";
      return el;
    }

    function statsRuntimeSummary(stem) {
      const row = statsPipeRow(stem);
      const runs = Number(row.runs) || 0;
      const times = (statsData.runs || [])
        .filter((item) => item.pipeline === stem && item.status === "ok")
        .map((item) => Number(item.elapsed_ms))
        .filter((n) => Number.isFinite(n) && n > 0)
        .sort((a, b) => a - b);
      const wall = times.reduce((sum, n) => sum + n, 0);
      const avg = avgTok(wall, times.length);
      const maxMs = times.length ? times[times.length - 1] : 0;
      const pcts = statsBarPercents([avg, maxMs]);
      const others = Math.max(0, runs - times.length);
      let meta = "";
      if (!times.length) meta = "no successful timing";
      else {
        meta = formatElapsed(avg) + " avg · " + formatElapsed(maxMs) + " max · " +
          times.length + " successful";
        if (others) meta += " · " + others + " other";
      }
      const el = document.createElement("div");
      el.className = "st-sum";
      el.innerHTML =
        '<div class="fig"><b>' + escapeHtml(formatElapsed(wall) || "0ms") + "</b><span>grand total</span></div>" +
        '<div class="fig"><b>' + escapeHtml(formatElapsed(avg) || "0ms") + "</b><span>avg / run</span></div>" +
        '<div class="split"><div class="st-hero-split">' +
        '<i class="avg" style="width:' + pcts[0] + '%"></i>' +
        '<i class="max" style="width:' + pcts[1] + '%"></i>' +
        "</div>" + statsColorTip([
          { kind: "avg", label: "average" },
          { kind: "max", label: "max" },
        ]) +
        '<div class="meta">' + escapeHtml(meta) + "</div></div>";
      return el;
    }

    function statsRuntimePanel(stem, node) {
      const row = (statsData.nodes || []).find((item) =>
        item.pipeline === stem && topoName(item.node) === node
      );
      if (!row) return "";
      const visits = Number(row.visits) || 0;
      const timed = Number(row.elapsed_n) || 0;
      const total = Number(row.elapsed_ms) || 0;
      const avg = timed ? avgTok(total, timed) : 0;
      const min = row.elapsed_min == null ? 0 : Number(row.elapsed_min) || 0;
      const max = Number(row.elapsed_max) || 0;
      const pipe = statsPipeRow(stem);
      const pipeMs = Number(pipe.node_elapsed_ms) || 0;
      const share = pipeMs ? Math.round((total / pipeMs) * 100) : 0;
      const maxBar = Math.max(avg, min, max, 1);
      const rows = [
        { label: "average", value: formatElapsed(avg) || "—", width: Math.max(4, Math.round((avg / maxBar) * 100)) },
        { label: "min", value: min ? formatElapsed(min) : "—", width: min ? Math.max(4, Math.round((min / maxBar) * 100)) : 0 },
        { label: "max", value: max ? formatElapsed(max) : "—", width: max ? 100 : 0 },
        { label: "total", value: formatElapsed(total) || "—", width: 0 },
        { label: "share of node time", value: share + "%", width: share ? Math.max(4, share) : 0 },
      ];
      const body = rows.map((item) => {
        return '<div class="st-why-group"><div class="st-why-row">' +
          '<div class="st-why-text">' + escapeHtml(item.label) + "</div>" +
          '<div class="st-why-n">' + escapeHtml(item.value) + "</div>" +
          (item.width
            ? '<div class="st-why-bar"><i style="width:' + item.width + '%"></i></div>'
            : "") +
          "</div></div>";
      }).join("");
      return '<div class="st-why-panel"><h3>' + escapeHtml(node) +
        " · " + visits + " visit" + (visits === 1 ? "" : "s") +
        (timed && timed !== visits ? " · " + timed + " timed" : "") +
        "</h3><div class=\"st-why\">" + body + "</div></div>";
    }

    function renderStatsMaps(mode) {
      const board = $("statsBoard");
      const pipe = statsPipeFilter();
      if (!pipe || !pipelineGraph(pipe)) {
        board.innerHTML = "";
        return;
      }
      board.innerHTML = "";
      const spec = pipelineGraph(pipe);
      const overlay = statsOverlayFor(pipe, mode);
      const map = document.createElement("div");
      map.className = "st-map";
      if (mode === "cost") map.appendChild(statsCostSummary(pipe));
      else if (mode === "runtime") map.appendChild(statsRuntimeSummary(pipe));
      else map.appendChild(statsRoutingSummary(pipe));
      const graph = document.createElement("div");
      graph.className = "st-graph";
      map.appendChild(graph);
      if (mode === "routing" && statsChoice && statsChoice.pipeline === pipe && statsChoice.node) {
        const extra = document.createElement("div");
        extra.innerHTML = statsReasonPanel(pipe, statsChoice.node);
        if (extra.firstChild) map.appendChild(extra.firstChild);
      } else if (mode === "runtime" && statsChoice && statsChoice.pipeline === pipe && statsChoice.node) {
        const extra = document.createElement("div");
        extra.innerHTML = statsRuntimePanel(pipe, statsChoice.node);
        if (extra.firstChild) map.appendChild(extra.firstChild);
      }
      board.appendChild(map);
      renderStatsTopo(graph, spec, overlay, pipe);
    }

    function renderStatsRouting() {
      renderStatsMaps("routing");
    }

    function renderStatsCost() {
      renderStatsMaps("cost");
    }

    function renderStatsRuntime() {
      renderStatsMaps("runtime");
    }

    function renderStats() {
      if (statsTab === "cost") renderStatsCost();
      else if (statsTab === "runtime") renderStatsRuntime();
      else renderStatsRouting();
    }

    async function loadStats() {
      statsData = await api("/api/stats");
      fillStatsPipeFilter();
      renderStats();
    }

    function fillComparePipeFilter() {
      const sel = $("cmpPipe");
      const keep = sel.value || "all";
      const stems = Array.from(new Set([
        ...pipelines.map((p) => p.stem),
        ...compareRuns.map((run) => run.pipeline).filter(Boolean),
      ])).sort();
      sel.innerHTML = '<option value="all">all</option>';
      stems.forEach((stem) => {
        const opt = document.createElement("option");
        opt.value = stem;
        opt.textContent = stem;
        sel.appendChild(opt);
      });
      sel.value = stems.includes(keep) || keep === "all" ? keep : "all";
    }

    function fillCompareSelect(sel, keep) {
      const current = keep || sel.value;
      const rows = filteredCompareRuns();
      sel.innerHTML = '<option value="">Select a run…</option>';
      rows.forEach((run) => {
        const opt = document.createElement("option");
        opt.value = run.id;
        opt.textContent = runOptionLabel(run);
        sel.appendChild(opt);
      });
      sel.value = current && [...sel.options].some((opt) => opt.value === current) ? current : "";
    }

    function jsonEqual(a, b) {
      return JSON.stringify(a) === JSON.stringify(b);
    }

    function fmtDiff(value) {
      if (value === undefined) return "";
      if (typeof value === "string") return value;
      return JSON.stringify(value);
    }

    function copyCell(kind, value) {
      const text = fmtDiff(value);
      const btn = text ? '<button type="button" class="copy-btn">Copy</button>' : "";
      const attr = text ? ' data-copy="' + escapeHtml(text) + '"' : "";
      return '<div class="' + kind + '"' + attr + ">" + escapeHtml(text) + btn + "</div>";
    }

    function diffRowsHtml(rows) {
      return rows.map((row) =>
        '<div class="cmp-row ' + row.kind + '">' +
        '<div class="cmp-side"><div class="cmp-k">' + escapeHtml(row.key) +
        "</div>" + copyCell("cmp-a", row.a) + "</div>" +
        '<div class="cmp-side"><div class="cmp-k">' + escapeHtml(row.key) +
        "</div>" + copyCell("cmp-b", row.b) + "</div>" +
        "</div>"
      ).join("");
    }

    function diffSection(title, rows) {
      return '<section class="cmp-section"><h2>' + escapeHtml(title) + "</h2>" +
        '<div class="cmp-head"><div>Run A</div><div>Run B</div></div>' +
        (rows.length ? diffRowsHtml(rows) : '<p class="empty">No values</p>') +
        "</section>";
    }

    function compareStepBranch(step) {
      const inn = step && step.state_in && typeof step.state_in === "object" && !Array.isArray(step.state_in)
        ? step.state_in : {};
      if (inn.source != null && inn.source !== "" && (typeof inn.source === "string" || typeof inn.source === "number")) {
        return String(inn.source);
      }
      const update = step && step.update && typeof step.update === "object" ? step.update : {};
      const partials = update.partials;
      if (Array.isArray(partials)) {
        for (let i = 0; i < partials.length; i++) {
          const item = partials[i];
          if (item && item.source != null && item.source !== "") return String(item.source);
        }
      }
      return "";
    }

    function stepsForNode(run, node) {
      return (run.steps || []).filter((step) => step && step.node === node);
    }

    function compareNodeOrder(a, b) {
      const names = [];
      function add(run) {
        (run.steps || []).forEach((step) => {
          if (step && step.node && names.indexOf(step.node) < 0) names.push(step.node);
        });
      }
      add(a);
      add(b);
      return names;
    }

    function alignVisits(lefts, rights) {
      const slots = (rights || []).map((step) => ({ step: step, used: false }));
      const pairs = [];
      (lefts || []).forEach((step) => {
        const branch = compareStepBranch(step);
        const j = slots.findIndex((slot) => {
          if (slot.used) return false;
          const other = compareStepBranch(slot.step);
          return branch ? other === branch : !other;
        });
        if (j >= 0) {
          slots[j].used = true;
          pairs.push({ a: step, b: slots[j].step });
        } else {
          pairs.push({ a: step, b: null });
        }
      });
      slots.forEach((slot) => {
        if (!slot.used) pairs.push({ a: null, b: slot.step });
      });
      return pairs;
    }

    function compareVisitLabel(node, pair, index, pairs) {
      const step = pair.a || pair.b || {};
      const branch = compareStepBranch(step);
      if (branch) {
        const same = pairs.filter((item) => compareStepBranch(item.a || item.b) === branch);
        if (same.length > 1) {
          return node + " · " + branch + " #" + (same.indexOf(pair) + 1);
        }
        return node + " · " + branch;
      }
      if (pairs.length > 1) {
        return (pair.a && pair.a.step_id) || (pair.b && pair.b.step_id) || (node + "#" + (index + 1));
      }
      return node;
    }

    function comparePairKind(left, right, equal) {
      if (!left) return "only-b";
      if (!right) return "only-a";
      return equal ? "" : "changed";
    }

    function applyCompareFilter() {
      fillCompareSelect($("cmpA"), $("cmpA").value);
      fillCompareSelect($("cmpB"), $("cmpB").value);
      return renderCompare();
    }

    async function loadCompare() {
      compareRuns = (await api("/api/runs?all=1")).runs;
      fillComparePipeFilter();
      await applyCompareFilter();
    }

    async function renderCompare() {
      const board = $("cmpBoard");
      const idA = $("cmpA").value;
      const idB = $("cmpB").value;
      if (!idA || !idB) {
        board.innerHTML = '<p class="empty">Select runs to compare...</p>';
        return;
      }
      const [a, b] = await Promise.all([api("/api/runs/" + idA), api("/api/runs/" + idB)]);
      const pathA = (a.steps || []).map((step) => step.node);
      const pathB = (b.steps || []).map((step) => step.node);
      const nodes = compareNodeOrder(a, b);
      const inputRows = Array.from(new Set([
        ...Object.keys(a.input || {}),
        ...Object.keys(b.input || {}),
      ])).sort().map((key) => ({
        key,
        a: (a.input || {})[key],
        b: (b.input || {})[key],
        kind: !Object.prototype.hasOwnProperty.call(a.input || {}, key) ? "only-b"
          : !Object.prototype.hasOwnProperty.call(b.input || {}, key) ? "only-a"
          : jsonEqual((a.input || {})[key], (b.input || {})[key]) ? "" : "changed",
      }));
      const pathRows = [];
      for (let i = 0; i < Math.max(pathA.length, pathB.length); i++) {
        pathRows.push({
          key: String(i + 1),
          a: pathA[i],
          b: pathB[i],
          kind: !pathA[i] ? "only-b" : !pathB[i] ? "only-a" : pathA[i] === pathB[i] ? "" : "changed",
        });
      }
      const outRows = [];
      const timeRows = [];
      nodes.forEach((node) => {
        const pairs = alignVisits(stepsForNode(a, node), stepsForNode(b, node));
        pairs.forEach((pair, index) => {
          const key = compareVisitLabel(node, pair, index, pairs);
          const left = pair.a;
          const right = pair.b;
          outRows.push({
            key,
            a: left ? left.update : undefined,
            b: right ? right.update : undefined,
            kind: comparePairKind(left, right, jsonEqual(left && left.update, right && right.update)),
          });
          timeRows.push({
            key,
            a: left ? formatElapsed(left.elapsed_ms) : "",
            b: right ? formatElapsed(right.elapsed_ms) : "",
            kind: comparePairKind(
              left,
              right,
              Number(left && left.elapsed_ms) === Number(right && right.elapsed_ms),
            ),
          });
        });
      });
      timeRows.push({
        key: "total",
        a: formatElapsed(a.elapsed_ms),
        b: formatElapsed(b.elapsed_ms),
        kind: Number(a.elapsed_ms) === Number(b.elapsed_ms) ? "" : "changed",
      });
      board.innerHTML =
        diffSection("Input", inputRows) +
        diffSection("Path", pathRows) +
        diffSection("Per-node output", outRows) +
        diffSection("Timing", timeRows);
    }

    async function refreshAfterFileChange(changedIds) {
      await loadPipelines();
      const files = (pipelines || []).map((p) => p.id);
      if (fileId && files.length && files.indexOf(fileId) < 0) {
        fileId = files[0];
        currentRun = null;
      }
      if (fileId) {
        renderExampleChips();
        await loadHistory();
      } else {
        historyRuns = [];
        renderHistory();
      }
      renderRun(currentRun);
      if (currentView === "runs") renderPipeBoard();
      if (currentView === "compare") renderCompare().catch(() => {});
      if (currentView === "statistics") loadStats().catch(() => {});
    }

    async function pollScan() {
      if (document.hidden) return;
      try {
        const data = await api("/api/scan");
        const files = data.files || [];
        const next = files.map((item) => item.id + ":" + (item.sha256 || "")).join("|");
        if (!scanKey) {
          scanKey = next;
          scanFiles = files;
          return;
        }
        if (next === scanKey) return;
        const prev = scanFiles;
        scanKey = next;
        scanFiles = files;
        const changed = files.filter((item) => {
          const before = prev.find((old) => old.id === item.id);
          return !before || before.sha256 !== item.sha256;
        }).map((item) => item.id);
        prev.forEach((old) => {
          if (!files.some((item) => item.id === old.id)) changed.push(old.id);
        });
        await refreshAfterFileChange(changed);
      } catch (err) {}
    }

    function shortHash(value) {
      return String(value || "").slice(0, 8);
    }

    function fileChangeLabel(ev) {
      if (ev.kind === "added") return ev.stem + ".py appeared";
      if (ev.kind === "removed") return ev.stem + ".py removed";
      if (ev.kind === "hash_changed") return ev.stem + ".py hash changed";
      return ev.stem + ".py modified";
    }

    function renderFileChanges() {
      const panel = $("fileChanges");
      const list = $("fcList");
      if (!fileChangeEvents.length) {
        panel.hidden = true;
        list.innerHTML = "";
        return;
      }
      panel.hidden = false;
      list.innerHTML = fileChangeEvents.map((ev, index) => {
        const hash = ev.kind === "hash_changed" && ev.prev_sha256
          ? shortHash(ev.prev_sha256) + " → " + shortHash(ev.sha256)
          : ev.sha256 ? shortHash(ev.sha256) : "";
        return '<div class="fc-item ' + escapeHtml(ev.kind) + '">' +
          '<div class="fc-row"><span class="fc-name">' + escapeHtml(fileChangeLabel(ev)) +
          '</span><span class="fc-kind">' + escapeHtml(ev.kind.replace("_", " ")) +
          '</span><button type="button" data-fc="' + index + '" aria-label="Dismiss">×</button></div>' +
          (hash ? '<div class="fc-hash">' + escapeHtml(hash) + "</div>" : "") +
          "</div>";
      }).join("");
    }

    async function pollChanges() {
      if (document.hidden) return;
      try {
        const data = await api("/api/changes?since=" + changeSeq);
        const events = data.events || [];
        if (!events.length) return;
        changeSeq = events[events.length - 1].seq;
        fileChangeEvents = events.concat(fileChangeEvents).slice(0, 20);
        renderFileChanges();
        await refreshAfterFileChange(events.map((ev) => ev.id));
      } catch (err) {}
    }

    (function bindInputResize() {
      const handle = $("inputResize");
      const area = $("input");
      if (!handle || !area) return;
      handle.addEventListener("mousedown", (event) => {
        event.preventDefault();
        const startY = event.clientY;
        const startH = area.getBoundingClientRect().height;
        const move = (ev) => {
          area.style.height = Math.max(88, startH + ev.clientY - startY) + "px";
        };
        const up = () => {
          window.removeEventListener("mousemove", move);
          window.removeEventListener("mouseup", up);
        };
        window.addEventListener("mousemove", move);
        window.addEventListener("mouseup", up);
      });
    })();
    $("zoomIn").onclick = () => {
      graphZoom = Math.min(2.5, Math.round(graphZoom * 1.2 * 100) / 100);
      applyGraphZoom();
    };
    $("zoomOut").onclick = () => {
      graphZoom = Math.max(0.25, Math.round(graphZoom / 1.2 * 100) / 100);
      applyGraphZoom();
    };
    $("zoomFit").onclick = () => fitGraphZoom();
    $("viewGraph").onclick = () => setGraphView("graph");
    $("viewGantt").onclick = () => setGraphView("gantt");
    $("viewAgents").onclick = () => setGraphView("agents");
    (function bindGraphPan() {
      const box = $("diagram");
      let dragging = false;
      let lastX = 0;
      let lastY = 0;
      box.addEventListener("pointerdown", (event) => {
        if (box.classList.contains("empty")) return;
        if (event.target.closest("button, a, input, textarea, .bp-dot")) return;
        hideGanttTip();
        dragging = true;
        lastX = event.clientX;
        lastY = event.clientY;
        box.setPointerCapture(event.pointerId);
      });
      box.addEventListener("pointermove", (event) => {
        if (!dragging) return;
        graphPanX += event.clientX - lastX;
        graphPanY += event.clientY - lastY;
        lastX = event.clientX;
        lastY = event.clientY;
        applyGraphZoom();
      });
      const stop = () => { dragging = false; };
      box.addEventListener("pointerup", stop);
      box.addEventListener("pointercancel", stop);
    })();
    $("runBtn").onclick = () => runGraph().catch((e) => alert(e.message));
    $("cancelBtn").onclick = () => cancelRuns().catch((e) => alert(e.message));
    $("continueBtn").onclick = () => continueRun().catch((e) => alert(e.message));
    $("stepBtn").onclick = () => stepRun().catch((e) => alert(e.message));
    $("replayBtn").onclick = () => openFromButton("replay");
    $("replayFromBtn").onclick = () => openFromButton("replay_from");
    $("replayFromHereBtn").onclick = () => rerun("resume").catch((e) => alert(e.message));
    $("callBtn").onclick = () => rerun("replay").catch((e) => alert(e.message));
    $("exportBtn").onclick = () => exportRun();
    $("importBtn").onclick = () => $("importFile").click();
    $("importFile").onchange = () => {
      const file = $("importFile").files && $("importFile").files[0];
      $("importFile").value = "";
      if (file) importFromFile(file).catch((e) => alert(e.message));
    };
    $("deleteBtn").onclick = () => removeRun().catch((e) => alert(e.message));
    $("runFilter").oninput = () => renderHistory();
    $("runFilter").onsearch = () => renderHistory();
    $("runStatus").onchange = () => renderHistory();
    $("cmpPipe").onchange = () => applyCompareFilter().catch((e) => alert(e.message));
    $("cmpA").onchange = () => renderCompare().catch((e) => alert(e.message));
    $("cmpB").onchange = () => renderCompare().catch((e) => alert(e.message));
    const historyBox = $("history");
    historyBox.addEventListener("dragover", (event) => {
      event.preventDefault();
      historyBox.classList.add("drop");
    });
    historyBox.addEventListener("dragleave", () => historyBox.classList.remove("drop"));
    historyBox.addEventListener("drop", (event) => {
      event.preventDefault();
      historyBox.classList.remove("drop");
      const file = event.dataTransfer.files && event.dataTransfer.files[0];
      if (file) importFromFile(file).catch((e) => alert(e.message));
    });
    $("nvClose").onclick = () => closeNodeView();
    $("nodeView").onclick = (event) => {
      if (event.target === $("nodeView")) closeNodeView();
    };
    $("navTrace").onclick = () => showView("trace");
    $("navPipelines").onclick = () => {
      runsFocusStem = "";
      showView("runs");
    };
    $("navCompare").onclick = () => showView("compare");
    $("navStats").onclick = () => showView("statistics");
    $("gotoRunsFromTrace").onclick = () => jumpToRuns();
    $("gotoStatsFromTrace").onclick = () => jumpToStatistics();
    $("gotoTraceFromStats").onclick = () => {
      const stem = statsPipeFilter();
      const id = stem ? fileIdForStem(stem) : fileId;
      jumpToTrace((pipelines || []).some((item) => item.id === id) ? id : "").catch((e) => alert(e.message));
    };
    $("gotoRunsFromStats").onclick = () => jumpToRuns(statsPipeFilter());
    $("statsPipe").onchange = () => {
      statsChoice = null;
      statsPipePreferred = statsPipeFilter();
      renderStats();
    };
    $("statsTabRouting").onclick = () => setStatsTab("routing");
    $("statsTabCost").onclick = () => setStatsTab("cost");
    $("statsTabRuntime").onclick = () => setStatsTab("runtime");
    document.addEventListener("click", async (event) => {
      const src = event.target.closest && event.target.closest(".src-link");
      if (src) {
        event.preventDefault();
        event.stopPropagation();
        const href = src.getAttribute("href");
        if (href) window.location.href = href;
        return;
      }
      const btn = event.target.closest(".copy-btn");
      if (!btn) return;
      event.preventDefault();
      event.stopPropagation();
      const box = btn.closest(".json, .cmp-a, .cmp-b, .copy-wrap");
      let text = "";
      if (box && box.classList.contains("copy-wrap")) {
        const area = box.querySelector("textarea");
        text = area ? area.value : "";
      } else if (box && box._copyPayload != null) {
        text = box._copyPayload;
      } else if (box && box.getAttribute("data-copy") != null) {
        text = box.getAttribute("data-copy");
      }
      try {
        await navigator.clipboard.writeText(text);
        btn.textContent = "Copied";
        setTimeout(() => { btn.textContent = "Copy"; }, 1200);
      } catch (err) {
        alert("Could not copy");
      }
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        if (!$("outdatedModal").hidden) {
          $("outdatedCancel").click();
          return;
        }
        if (isNodeViewOpen()) closeNodeView();
      }
    });
    window.addEventListener("hashchange", () => {
      showView(viewFromHash());
    });
    let pipeResizeTimer = 0;
    window.addEventListener("resize", () => {
      if (currentView !== "runs") return;
      clearTimeout(pipeResizeTimer);
      pipeResizeTimer = setTimeout(renderPipeBoard, 120);
    });
    $("fcClear").onclick = () => {
      fileChangeEvents = [];
      renderFileChanges();
    };
    $("fcList").onclick = (event) => {
      const btn = event.target.closest("[data-fc]");
      if (!btn) return;
      fileChangeEvents.splice(Number(btn.getAttribute("data-fc")), 1);
      renderFileChanges();
    };
    document.querySelectorAll(".spark-legend [data-metric]").forEach((el) => {
      const metric = sparkMetric(el.dataset.metric);
      if (!metric) return;
      bindMetricTip(el, metricTipHtml(metric));
    });
    Promise.all([loadMeta(), loadPipelines()]).then(() => {
      showView(viewFromHash());
    }).catch((e) => alert(e.message));
    setInterval(() => pollScan(), 2000);
    setInterval(() => pollChanges(), 500);
    setInterval(() => {
      if (document.hidden) return;
      loadMeta().then(() => {
        syncRunControls();
        if (slotActive || slotQueued) {
          const tasks = [loadPipelines()];
          if (fileId) tasks.push(loadHistory());
          return Promise.all(tasks);
        }
      }).catch(() => {});
    }, 2000);
  </script>
</body>
</html>
"""


class GraphVIHandler(BaseHTTPRequestHandler):
    workspace: Path
    cache: dict[str, LoadedPipeline]
    config: GVAConfig
    watcher: PipelineWatcher | None = None
    bind_host: str = "127.0.0.1"
    bind_port: int = 8765
    loopback_only: bool = True
    concurrent: int = DEFAULT_CONCURRENT
    node_concurrency: int | None = None
    queue_limit: int = DEFAULT_CONCURRENT * 4
    cli_concurrent: int | None = None
    cli_node_concurrency: int | None = None
    cli_queue_limit: int | None = None
    scheduler: RunScheduler | None = None

    def log_message(self, format: str, *args) -> None:
        print(f"[graphviagent] {args[0]}")

    def _guard(self) -> bool:
        if request_allowed(
            self.command,
            self.headers,
            bind_port=self.bind_port,
            loopback_only=self.loopback_only,
        ):
            return True
        self._json(403, {"error": "blocked origin"})
        return False

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, payload: dict) -> None:
        self._send(code, json.dumps(payload).encode(), "application/json")

    def _sse_begin(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache, no-store")
        self.send_header("Connection", "close")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        self.close_connection = True

    def _sse_data(self, payload: dict) -> None:
        body = f"data: {json.dumps(payload, default=str)}\n\n".encode()
        self.wfile.write(body)
        self.wfile.flush()

    def _normalize_run_id(self, value: object) -> str | None:
        if not value:
            return None
        text = str(value).replace("-", "")
        if not text or len(text) > 64 or any(ch not in "0123456789abcdefABCDEF" for ch in text):
            raise ValueError("run_id must be a hex id")
        return text.lower()

    def _name_list(self, raw: object) -> list[str] | None:
        if raw is None or raw == "":
            return None
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, list):
            raise ValueError("node list must be an array")
        names = [str(item) for item in raw if item]
        return names or None

    def _save_live_run(
        self,
        loaded: LoadedPipeline,
        run: dict,
        previous: dict | None = None,
    ) -> dict:
        tagged = attach_graph_meta(
            run,
            loaded.graph,
            loaded.graph_hash,
            loaded.file_sha256,
        )
        if previous:
            tagged = merge_resume_run(previous, tagged)
        return save_run(self.workspace, loaded.stem, tagged)

    def _run_key(self, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).replace("-", "").strip().lower()
        return text or None

    def _begin_run(self, run_id: str, *aliases: object) -> None:
        sched = self.scheduler
        if sched is None:
            raise RuntimeError("run scheduler is not running")
        if sched.has_job(run_id):
            raise DuplicateRun(run_id)
        for alias in aliases:
            key = self._run_key(alias)
            if key and sched.has_job(key):
                raise DuplicateRun(run_id)

    def _cancel_run(self, run_id: str) -> bool:
        sched = self.scheduler
        if sched is not None and sched.cancel(run_id):
            return True
        run = load_run(self.workspace, run_id)
        if run and (run.get("queued") or run.get("running")):
            stem = run.get("pipeline")
            if stem:
                return (
                    cancel_queue_stub(
                        self.workspace,
                        str(stem),
                        run_id,
                        wait_ms=float(run.get("wait_ms") or 0),
                        resume=bool(run.get("paused")),
                    )
                    is not None
                )
        return cancel_paused_run(self.workspace, run_id) is not None

    @classmethod
    def persist_queue_cancel(cls, job: RunJob) -> None:
        cancel_queue_stub(
            cls.workspace,
            job.stem,
            job.run_id,
            wait_ms=job.wait_ms,
            resume=job.resume,
        )

    def _queue_position(self, run_id: object) -> int | None:
        sched = self.scheduler
        if sched is None or not run_id:
            return None
        return sched.position_of(str(run_id))

    def _annotate_queue(self, run: dict) -> dict:
        if not isinstance(run, dict):
            return run
        if run.get("queued") or run.get("status") == "queued":
            pos = self._queue_position(run.get("id"))
            if pos is not None:
                run = dict(run)
                run["queue_position"] = pos
        return run

    def _build_job(
        self,
        *,
        loaded: LoadedPipeline,
        payload: dict,
        resume: bool,
        previous: dict | None,
        run_id: str,
        thread_id: str,
    ) -> RunJob:
        use_command = resume and "resume_value" in payload
        raw_input = (previous or {}).get("input") if previous else payload.get("input")
        user_input = raw_input if isinstance(raw_input, dict) else {}
        return RunJob(
            run_id=run_id,
            file_id=str(payload.get("file") or ""),
            stem=loaded.stem,
            user_input=user_input,
            resume=resume,
            enqueued_at=time.monotonic(),
            thread_id=thread_id,
            aliases=(run_id, thread_id),
            interrupt_before=self._name_list(payload.get("interrupt_before")),
            interrupt_after=self._name_list(payload.get("interrupt_after")),
            resume_value=payload.get("resume_value") if use_command else None,
            use_command=use_command,
            previous=previous,
            kind="resume" if resume else "run",
        )

    def _try_enqueue(
        self,
        job: RunJob,
        loaded: LoadedPipeline,
        *,
        mode: str | None = None,
        parent_id: str | None = None,
        from_step: str | None = None,
    ) -> None:
        sched = self.scheduler
        if sched is None:
            raise RuntimeError("run scheduler is not running")
        self._begin_run(job.run_id, *job.aliases)
        snapshot = dict(job.previous) if job.previous else None
        save_queued_stub(
            self.workspace,
            job.stem,
            run_id=job.run_id,
            user_input=job.user_input,
            previous=job.previous,
            graph_hash=loaded.graph_hash,
            file_sha256=loaded.file_sha256,
            mode=mode or job.kind,
            parent_id=parent_id or job.source_run_id,
            from_step=from_step or job.step_id,
        )
        try:
            sched.submit(job)
        except (QueueFull, DuplicateRun):
            if snapshot is not None:
                if not sched.has_job(job.run_id):
                    save_run(self.workspace, job.stem, snapshot)
            else:
                delete_run(self.workspace, job.run_id)
            raise

    def _sse_pump(self, job: RunJob) -> None:
        while True:
            try:
                event = job.events.get(timeout=HEARTBEAT_S)
            except queue.Empty:
                self._sse_data({"type": "ping"})
                continue
            if event is SENTINEL:
                return
            kind = event.get("type") if isinstance(event, dict) else None
            if kind == "canceled":
                saved = load_run(self.workspace, job.run_id)
                if saved is None:
                    saved = cancel_queue_stub(
                        self.workspace,
                        job.stem,
                        job.run_id,
                        wait_ms=float(event.get("wait_ms") or job.wait_ms or 0),
                        resume=job.resume,
                    )
                if saved is not None:
                    event = {"type": "done", "run": self._with_render(saved)}
            self._sse_data(event)

    def _drain_job(self, job: RunJob) -> dict:
        final = None
        error = None
        while True:
            try:
                event = job.events.get(timeout=HEARTBEAT_S)
            except queue.Empty:
                continue
            if event is SENTINEL:
                break
            if not isinstance(event, dict):
                continue
            kind = event.get("type")
            if kind in {"done", "paused"}:
                final = event.get("run")
            elif kind == "canceled":
                final = load_run(self.workspace, job.run_id)
            elif kind == "error":
                error = str(event.get("error") or "run failed")
        if error:
            raise RuntimeError(error)
        if not isinstance(final, dict):
            final = load_run(self.workspace, job.run_id)
        if not isinstance(final, dict):
            raise RuntimeError("run produced no result")
        return final

    @classmethod
    def execute_job(cls, job: RunJob) -> None:
        handler = cls.__new__(cls)
        wait_ms = job.wait_ms
        mark_run_started(cls.workspace, job.stem, job.run_id, wait_ms=wait_ms)
        job.events.put({
            "type": "start",
            "run_id": job.run_id,
            "input": job.user_input,
            "wait_ms": wait_ms,
        })
        try:
            loaded = handler._get_loaded(job.file_id)
            if job.kind in {"replay", "replay_from"}:
                source = job.previous or load_run(cls.workspace, job.source_run_id or "")
                if source is None:
                    raise RuntimeError("run not found")
                replay = resume_from_step if job.kind == "replay_from" else replay_step
                new_run = replay(
                    loaded.app,
                    source,
                    job.step_id or "",
                    job.state_patch,
                    job.replay_input,
                    context=loaded.context,
                    thread_id=job.run_id,
                    cancel=job.cancel,
                    max_concurrency=cls.node_concurrency,
                )
                new_run["id"] = job.run_id
                timed = attach_run_timing(new_run, wait_ms=wait_ms)
                saved = handler._save_live_run(loaded, timed, None)
                job.events.put({"type": "done", "run": handler._with_render(saved)})
                return
            for event in iter_run_events(
                loaded.app,
                job.previous.get("input") if job.previous else job.user_input,
                thread_id=job.thread_id,
                has_checkpointer=loaded.has_checkpointer,
                cancel=job.cancel,
                max_concurrency=cls.node_concurrency,
                resume=job.resume,
                interrupt_before=job.interrupt_before,
                interrupt_after=job.interrupt_after,
                pause=True,
                resume_value=job.resume_value if job.use_command else None,
                use_command=job.use_command,
                context=loaded.context,
            ):
                kind = event.get("type")
                if kind in {"done", "paused"}:
                    run = attach_run_timing(event.get("run") or {}, wait_ms=wait_ms)
                    saved = handler._save_live_run(loaded, run, job.previous)
                    event = {"type": kind, "run": handler._with_render(saved)}
                job.events.put(event)
        except Exception as exc:
            run = load_run(cls.workspace, job.run_id) or {
                "id": job.run_id,
                "input": job.user_input,
                "steps": [],
                "result": {},
                "canceled": False,
                "paused": False,
            }
            run["error"] = str(exc)
            timed = attach_run_timing(run, wait_ms=wait_ms)
            save_run(cls.workspace, job.stem, timed)
            job.events.put({"type": "error", "error": str(exc)})

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        return json.loads(raw.decode() or "{}")

    def _rel_id(self, path: Path) -> str:
        resolved = path.resolve()
        workspace = self.workspace.resolve()
        try:
            return resolved.relative_to(workspace).as_posix()
        except ValueError:
            return resolved.as_posix()

    def _pipelines(self) -> list[Path]:
        return discover_pipelines(self.workspace, self.config)

    def _pipeline_stem(self, path: Path) -> str:
        return self.config.stem_for(path)

    def _inside_workspace(self, path: Path) -> bool:
        workspace = self.workspace.resolve()
        try:
            resolved = path.resolve()
        except OSError:
            return False
        return resolved == workspace or workspace in resolved.parents

    def _resolve_file_id(self, file_id: str) -> Path | None:
        if not file_id:
            return None
        for path in self._pipelines():
            resolved = path.resolve()
            if self._rel_id(resolved) == file_id:
                return resolved
        spec = self.config.pipelines.get(file_id)
        if spec is not None:
            return spec.file.resolve()
        raw = Path(file_id)
        try:
            path = raw.resolve() if raw.is_absolute() else (self.workspace / file_id).resolve()
        except OSError:
            return None
        if self._inside_workspace(path):
            return path
        for known in self._pipelines():
            if known.resolve() == path:
                return path
        return None

    def _stem_for_file_id(self, file_id: str) -> str:
        path = self._resolve_file_id(file_id)
        if path is not None:
            return self._pipeline_stem(path)
        spec = self.config.pipelines.get(file_id)
        if spec is not None:
            return spec.id
        return Path(file_id).stem if file_id else ""

    def _known_stems(self) -> list[str]:
        return [self._pipeline_stem(path) for path in self._pipelines()]

    def _scan_files(self) -> list[dict]:
        if self.watcher is not None:
            self.watcher.refresh(emit=True)
            return self.watcher.files()
        return list(snapshot_files(self.workspace, self.config).values())

    def _cached_pipeline(self, file_id: str) -> LoadedPipeline:
        path = self._resolve_file_id(file_id)
        if path is None:
            loaded = LoadedPipeline(path=Path(file_id), stem=Path(file_id).stem)
            loaded.error = "path outside workspace"
            return loaded
        try:
            sha = file_sha256(path)
        except OSError:
            sha = None
        cached = self.cache.get(file_id)
        if (
            cached is not None
            and not cached.error
            and sha is not None
            and getattr(cached, "_sha256", None) == sha
        ):
            return cached
        loaded = load_pipeline(path, self.config)
        loaded._sha256 = sha  # type: ignore[attr-defined]
        if loaded.error:
            self.cache.pop(file_id, None)
        else:
            self.cache[file_id] = loaded
        return loaded

    def _load_listed(self, file_id: str) -> LoadedPipeline:
        return self._cached_pipeline(file_id)

    def _get_loaded(self, file_id: str) -> LoadedPipeline:
        cached = self._cached_pipeline(file_id)
        if cached.error:
            raise RuntimeError(cached.error)
        if cached.app is None:
            raise RuntimeError("pipeline has no graph")
        return cached

    def _with_render(self, run: dict) -> dict:
        run = dict(run)
        run["mermaid"] = unrolled_mermaid(run.get("steps") or [])
        run["ascii"] = ascii_tree(run.get("pipeline") or "run", run.get("steps") or [])
        return self._annotate_queue(run)

    def do_GET(self) -> None:
        if not self._guard():
            return
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send(200, PAGE.encode(), "text/html; charset=utf-8")
            return
        if parsed.path in {"/favicon.svg", "/favicon.ico"}:
            self._send(200, FAVICON_SVG.encode(), "image/svg+xml")
            return
        if parsed.path == "/api/meta":
            payload = {
                "version": __version__,
                "concurrent": self.concurrent,
                "node_concurrency": self.node_concurrency,
                "queue_limit": self.queue_limit,
                "active": 0,
                "queued": 0,
                "limit": self.concurrent,
            }
            if self.scheduler is not None:
                payload.update(self.scheduler.snapshot())
            self._json(200, payload)
            return
        if parsed.path == "/api/scan":
            self._json(200, {"files": self._scan_files()})
            return
        if parsed.path == "/api/changes":
            since = 0
            try:
                since = int((parse_qs(parsed.query).get("since") or ["0"])[0] or 0)
            except ValueError:
                since = 0
            if self.watcher is not None:
                self.watcher.refresh(emit=True)
                events = self.watcher.changes(since)
            else:
                events = []
            self._json(200, {"events": events})
            return
        if parsed.path == "/api/pipelines":
            items = []
            for path in self._pipelines():
                file_id = self._rel_id(path)
                loaded = self._load_listed(file_id)
                stem = self._pipeline_stem(path)
                runs = [
                    self._annotate_queue(item)
                    for item in list_runs(self.workspace, stem, include_steps=True, limit=30)
                ]
                items.append(
                    {
                        "id": file_id,
                        "stem": stem,
                        "examples": loaded.examples,
                        "error": loaded.error,
                            "graph": loaded.graph,
                            "graph_hash": loaded.graph_hash,
                            "file_sha256": loaded.file_sha256,
                        "last_run": runs[0] if runs else None,
                        "recent": runs,
                    }
                )
            self._json(200, {"pipelines": items})
            return
        if parsed.path == "/api/runs":
            query = parse_qs(parsed.query)
            if (query.get("all") or [""])[0] in {"1", "true", "yes"}:
                self._json(
                    200,
                    {
                        "runs": [
                            self._annotate_queue(item)
                            for item in list_all_runs(self.workspace, include_steps=True)
                        ]
                    },
                )
                return
            file_id = (query.get("file") or [""])[0]
            stem = self._stem_for_file_id(file_id)
            self._json(
                200,
                {
                    "runs": [
                        self._annotate_queue(item)
                        for item in list_runs(self.workspace, stem, include_steps=True)
                    ]
                },
            )
            return
        if parsed.path.startswith("/api/runs/"):
            run_id = parsed.path.rsplit("/", 1)[-1]
            try:
                run = load_run(self.workspace, run_id)
            except ValueError as exc:
                self._json(409, {"error": str(exc)})
                return
            if run is None:
                self._json(404, {"error": "run not found"})
                return
            self._json(200, self._with_render(run))
            return
        if parsed.path == "/api/stats":
            query = parse_qs(parsed.query)
            file_id = (query.get("file") or [""])[0]
            stem = self._stem_for_file_id(file_id) if file_id else None
            self._json(200, collect_stats(self.workspace, stem or None))
            return
        self._json(404, {"error": "not found"})

    def do_DELETE(self) -> None:
        if not self._guard():
            return
        parsed = urlparse(self.path)
        if parsed.path == "/api/runs":
            file_id = (parse_qs(parsed.query).get("file") or [""])[0]
            if not file_id:
                self._json(400, {"error": "file is required"})
                return
            stem = self._stem_for_file_id(file_id)
            if (self.scheduler is not None and self.scheduler.has_job_for(stem=stem, file_id=file_id)) or pipeline_has_busy_runs(
                self.workspace, stem
            ):
                self._json(409, {"error": "cancel queued or running jobs before clearing"})
                return
            count = delete_runs(self.workspace, stem)
            self._json(200, {"ok": True, "deleted": count})
            return
        if parsed.path.startswith("/api/runs/"):
            run_id = parsed.path.rsplit("/", 1)[-1]
            run = load_run(self.workspace, run_id)
            if run is None:
                self._json(404, {"error": "run not found"})
                return
            if (self.scheduler is not None and self.scheduler.has_job(run_id)) or run_is_busy(run):
                self._json(409, {"error": "cancel the run before deleting"})
                return
            ok = delete_run(self.workspace, run_id)
            self._json(200 if ok else 404, {"ok": ok})
            return
        self._json(404, {"error": "not found"})

    def do_POST(self) -> None:
        if not self._guard():
            return
        parsed = urlparse(self.path)
        try:
            payload = self._read_json()
            if parsed.path == "/api/run/cancel":
                run_id = self._run_key(payload.get("run_id"))
                if not run_id:
                    raise ValueError("run_id is required")
                self._json(200, {"ok": self._cancel_run(run_id)})
                return
            if parsed.path == "/api/run/stream":
                loaded = self._get_loaded(payload["file"])
                resume = bool(payload.get("resume"))
                previous = None
                client_run_id = self._normalize_run_id(payload.get("run_id")) or uuid4().hex
                thread_id = client_run_id
                if resume:
                    previous = load_run(self.workspace, client_run_id)
                    if previous is None:
                        self._json(404, {"error": "run not found"})
                        return
                    if previous.get("queued") or previous.get("running"):
                        if self.scheduler is not None and self.scheduler.has_job(client_run_id):
                            raise DuplicateRun(client_run_id)
                        previous = dict(previous)
                        previous["queued"] = False
                        previous["running"] = False
                    if not previous.get("paused"):
                        raise ValueError("run is not paused")
                    thread = previous.get("thread_id") or previous.get("id")
                    if not thread:
                        raise RuntimeError("paused run has no thread_id")
                    thread_id = str(thread)
                job = self._build_job(
                    loaded=loaded,
                    payload=payload,
                    resume=resume,
                    previous=previous,
                    run_id=client_run_id,
                    thread_id=thread_id,
                )
                self._try_enqueue(job, loaded)
                started = False
                try:
                    self._sse_begin()
                    started = True
                    self._sse_pump(job)
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    self._cancel_run(client_run_id)
                except Exception as exc:
                    if started:
                        try:
                            self._sse_data({"type": "error", "error": str(exc)})
                        except Exception:
                            self._cancel_run(client_run_id)
                    else:
                        self._cancel_run(client_run_id)
                        raise
                return
            if parsed.path == "/api/run":
                loaded = self._get_loaded(payload["file"])
                run_id = self._normalize_run_id(payload.get("run_id")) or uuid4().hex
                job = self._build_job(
                    loaded=loaded,
                    payload=payload,
                    resume=False,
                    previous=None,
                    run_id=run_id,
                    thread_id=run_id,
                )
                self._try_enqueue(job, loaded)
                try:
                    saved = self._drain_job(job)
                    self._json(200, self._with_render(saved))
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    self._cancel_run(run_id)
                    raise
                return
            if parsed.path == "/api/rerun":
                run = load_run(self.workspace, payload["run_id"])
                if run is None:
                    self._json(404, {"error": "run not found"})
                    return
                matches = [
                    path
                    for path in self._pipelines()
                    if self._pipeline_stem(path) == run["pipeline"]
                ]
                if not matches:
                    raise RuntimeError(f"pipeline {run['pipeline']} not found")
                file_id = self._rel_id(matches[0])
                loaded = self._get_loaded(file_id)
                mode = payload.get("mode") or "replay"
                kind = "replay_from" if mode == "resume" else "replay"
                patch = payload.get("state_patch") or None
                incoming = payload.get("input")
                if incoming is not None and not isinstance(incoming, dict):
                    raise ValueError("input must be a JSON object")
                new_id = self._normalize_run_id(payload.get("new_run_id")) or uuid4().hex
                raw_input = run.get("input")
                job = RunJob(
                    run_id=new_id,
                    file_id=file_id,
                    stem=loaded.stem,
                    user_input=raw_input if isinstance(raw_input, dict) else {},
                    resume=False,
                    enqueued_at=time.monotonic(),
                    thread_id=new_id,
                    aliases=(new_id,),
                    previous=None,
                    kind=kind,
                    step_id=str(payload["step_id"]),
                    state_patch=patch if isinstance(patch, dict) else None,
                    replay_input=incoming if isinstance(incoming, dict) else None,
                    source_run_id=str(run.get("id") or ""),
                )
                self._try_enqueue(
                    job,
                    loaded,
                    mode=kind,
                    parent_id=str(run.get("id") or ""),
                    from_step=str(payload["step_id"]),
                )
                saved = self._drain_job(job)
                self._json(200, self._with_render(saved))
                return
            if parsed.path == "/api/import":
                fallback = self._stem_for_file_id(str(payload.get("file") or ""))
                saved = import_run(
                    self.workspace,
                    payload.get("run") or {},
                    fallback_stem=fallback,
                    known_stems=self._known_stems(),
                )
                self._json(200, self._with_render(saved))
                return
            self._json(404, {"error": "not found"})
        except QueueFull as exc:
            self._json(429, {"error": str(exc), **exc.payload()})
        except DuplicateRun as exc:
            self._json(409, {"error": str(exc)})
        except Exception as exc:
            self._json(400, {"error": str(exc)})


def serve(
    workspace: Path,
    host: str = "127.0.0.1",
    port: int = 8765,
    open_browser: bool = False,
    config: GVAConfig | None = None,
    expose: bool = False,
    cli_concurrent: int | None = None,
    cli_node_concurrency: int | None = None,
    cli_queue_limit: int | None = None,
) -> None:
    if is_public_bind(host) and not expose:
        raise ValueError(
            f"refusing to bind {host}: this exposes pipeline execution on the network. "
            "Use 127.0.0.1 (default), or pass --expose if you intend this."
        )
    if config is None:
        config = load_config(workspace)
        activate_config(config)
        apply_limit_overrides(
            config,
            concurrent=cli_concurrent,
            node_concurrency=cli_node_concurrency,
            queue_limit=cli_queue_limit,
        )
    workspace = config.root
    handler = partial(GraphVIHandler)
    GraphVIHandler.workspace = workspace
    GraphVIHandler.config = config
    GraphVIHandler.cache = {}
    GraphVIHandler.bind_host = host
    GraphVIHandler.bind_port = port
    GraphVIHandler.loopback_only = not is_public_bind(host)
    GraphVIHandler.cli_concurrent = cli_concurrent
    GraphVIHandler.cli_node_concurrency = cli_node_concurrency
    GraphVIHandler.cli_queue_limit = cli_queue_limit
    GraphVIHandler.concurrent = config.concurrent
    GraphVIHandler.node_concurrency = config.node_concurrency
    GraphVIHandler.queue_limit = effective_queue_limit(config)

    def invalidate(file_ids: list[str]) -> None:
        for file_id in file_ids:
            GraphVIHandler.cache.pop(file_id, None)

    def on_config(updated: GVAConfig) -> None:
        apply_limit_overrides(
            updated,
            concurrent=GraphVIHandler.cli_concurrent,
            node_concurrency=GraphVIHandler.cli_node_concurrency,
            queue_limit=GraphVIHandler.cli_queue_limit,
        )
        GraphVIHandler.config = updated
        GraphVIHandler.workspace = updated.root
        GraphVIHandler.concurrent = updated.concurrent
        GraphVIHandler.node_concurrency = updated.node_concurrency
        GraphVIHandler.queue_limit = effective_queue_limit(updated)
        GraphVIHandler.cache.clear()

    watcher = PipelineWatcher(
        GraphVIHandler.workspace,
        on_change=invalidate,
        config=config,
        on_config=on_config,
    )
    watcher.start()
    GraphVIHandler.watcher = watcher
    scheduler = RunScheduler(
        concurrent_fn=lambda: GraphVIHandler.concurrent,
        queue_limit_fn=lambda: GraphVIHandler.queue_limit,
        execute_fn=GraphVIHandler.execute_job,
        persist_cancel=GraphVIHandler.persist_queue_cancel,
    )
    GraphVIHandler.scheduler = scheduler
    scheduler.start()
    server = ThreadingHTTPServer((host, port), handler)
    url = f"http://{host}:{port}"
    extras = f"concurrent={config.concurrent}  queue_limit={GraphVIHandler.queue_limit}"
    if config.node_concurrency is not None:
        extras += f"  node_concurrency={config.node_concurrency}"
    print(
        f"GraphVIAgent {__version__}  {url}  workspace={workspace}  {extras}",
        flush=True,
    )
    if is_public_bind(host):
        print(
            "WARNING: reachable on the network. Anyone who can open this URL "
            "can run your pipelines and read stored runs.",
            flush=True,
        )
    if open_browser:
        browse_host = "127.0.0.1" if host.strip("[]") in _WILDCARD_BINDS else host
        browse = f"http://{browse_host}:{port}"
        threading.Timer(0.4, lambda: webbrowser.open(browse)).start()
    try:
        server.serve_forever()
    finally:
        scheduler.stop()
        GraphVIHandler.scheduler = None
        watcher.stop()
        GraphVIHandler.watcher = None
