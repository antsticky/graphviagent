from __future__ import annotations

import json
import threading
import webbrowser
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from graphviagent.discover import discover_pipelines
from graphviagent.graph_hash import attach_graph_meta
from graphviagent.load import LoadedPipeline, load_pipeline
from graphviagent.record import record_run, replay_step, resume_from_step
from graphviagent.render import ascii_tree, unrolled_mermaid
from graphviagent.store import (
    delete_run,
    delete_runs,
    import_run,
    list_all_runs,
    list_runs,
    load_run,
    save_run,
)
from graphviagent.watch import PipelineWatcher, file_sha256

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
    .status-dot.gray { background: #5a5a64; }
    .page { height: calc(100vh - 48px); overflow: auto; padding: 24px 28px; }
    .page[hidden], .layout[hidden] { display: none; }
    .page-inner { width: 100%; max-width: none; }
    .page h1 { font-size: 22px; font-weight: 600; margin: 0 0 6px; }
    .lede { color: var(--muted); margin: 0 0 22px; }
    .af-card {
      background: #17171c; border: 1px solid var(--line);
      border-radius: 10px; padding: 16px 18px 18px; margin: 0 0 16px;
      overflow: auto;
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
    .af-grid { width: max-content; min-width: min-content; }
    .af-bars, .af-plays, .af-row, .af-days, .af-times {
      display: grid; align-items: end; column-gap: 3px;
    }
    .af-bars { height: 58px; margin-bottom: 4px; }
    .af-days { margin-bottom: 1px; }
    .af-times { margin-bottom: 2px; }
    .af-day, .af-time {
      border: 0; background: transparent; padding: 0; cursor: pointer;
      font-family: "IBM Plex Mono", ui-monospace, monospace;
      text-align: center; line-height: 1.2; letter-spacing: -0.02em;
      overflow: hidden; text-overflow: clip; white-space: nowrap;
    }
    .af-day { color: #7d7d86; font-size: 9px; font-weight: 500; }
    .af-time { color: #5c5c66; font-size: 9px; }
    .af-day:hover, .af-time:hover { color: #a1a1aa; }
    .af-bar {
      width: 100%; border: 0; border-radius: 2px 2px 0 0; padding: 0;
      cursor: pointer; min-height: 4px; align-self: end;
    }
    .af-bar.ok { background: #22c55e; }
    .af-bar.error { background: #ef4444; }
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
    #history .pill.mode-error { color: #fca5a5; background: rgba(240, 113, 120, 0.16); }
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
    button.primary, button.ghost {
      height: 30px; padding: 0 12px; border-radius: 7px;
      font: 500 12px Inter, sans-serif; cursor: pointer;
    }
    button.primary { background: var(--accent); color: #fff; border: 0; }
    button.ghost { background: transparent; color: var(--ink); border: 1px solid var(--line); }
    button.ghost:hover { background: #1f1f26; }
    button.ghost:disabled, button.primary:disabled {
      opacity: 0.4; cursor: not-allowed;
    }
    button.ghost:disabled:hover { background: transparent; }
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
      display: grid; grid-template-columns: 14px 1fr auto; gap: 8px;
      padding: 8px 8px 8px 0; border-radius: 8px; cursor: pointer;
    }
    .step:hover { background: #22222a; }
    .step.selected { background: var(--accent-dim); }
    .step.error .step-why { color: var(--danger); }
    .bar { width: 3px; border-radius: 99px; background: #6366f1; margin: 2px 0 2px 6px; }
    .bar.decision { background: #f59e0b; }
    .bar.loop { background: #22c55e; }
    .bar.error { background: var(--danger); }
    .step-id { color: var(--muted); font-size: 11px; }
    .step-name { font-weight: 500; }
    .step-why { color: var(--muted); margin-top: 2px; }
    .step-ms, .run-ms, #stepsMs {
      color: var(--muted); font-variant-numeric: tabular-nums;
      font-size: 11px; white-space: nowrap;
    }
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
      display: grid; grid-template-columns: 88px minmax(0, 1fr);
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
        <button class="nav-link" id="navPipelines" type="button">Pipelines</button>
        <button class="nav-link" id="navCompare" type="button">Compare</button>
      </nav>
    </div>
  </header>
  <section id="compareView" class="page" hidden>
    <div class="page-inner">
      <h1>Compare</h1>
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
      <h1>Pipelines</h1>
      <div id="pipeBoard"></div>
    </div>
  </section>
  <div id="traceView" class="layout">
    <aside>
      <h2>Pipelines</h2>
      <div id="pipelines"></div>
      <h2>Runs</h2>
      <div class="hist-tools">
        <input id="runFilter" type="search" placeholder="input…"/>
        <select id="runStatus">
          <option value="all">all</option>
          <option value="ok">success</option>
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
        <button class="ghost" id="replayBtn" disabled>Replay step</button>
        <button class="ghost" id="replayFromBtn" disabled>Replay from</button>
        <button class="ghost" id="exportBtn" disabled>Export</button>
        <button class="ghost" id="importBtn">Import</button>
        <button class="ghost" id="deleteBtn">Delete run</button>
      </div>
      <div class="grid">
        <div class="pane">
          <h2 class="pane-title">Graph
            <span class="graph-zoom">
              <button type="button" class="ghost" id="zoomOut" title="Zoom out">−</button>
              <button type="button" class="ghost" id="zoomFit" title="Fit to window">Fit</button>
              <button type="button" class="ghost" id="zoomIn" title="Zoom in">+</button>
            </span>
          </h2>
          <div id="diagram" class="empty">Select a run to inspect the unrolled path. Double-click a node to open its view.</div>
        </div>
        <div class="pane">
          <h2 class="pane-title">Steps <span id="stepsMs"></span></h2>
          <div id="steps" class="waterfall"></div>
          <h2>Final state</h2>
          <div id="state" class="json"></div>
          <details class="tree-details">
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
    let changeSeq = 0;
    let fileChangeEvents = [];
    let compareRuns = [];
    let graphZoom = 1;
    let graphPanX = 0;
    let graphPanY = 0;

    const $ = (id) => document.getElementById(id);

    function escapeHtml(value) {
      return String(value ?? "").replace(/[&<>"']/g, (ch) => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
      }[ch]));
    }

    function formatTime(iso) {
      if (!iso) return "";
      const d = new Date(iso);
      if (Number.isNaN(d.getTime())) return iso.slice(0, 16);
      const time = d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
      const date = d.toLocaleDateString([], { month: "short", day: "numeric" });
      return time + "  " + date;
    }

    function formatElapsed(ms) {
      const value = Number(ms);
      if (!Number.isFinite(value)) return "";
      if (value < 1000) return (Math.round(value * 10) / 10) + "ms";
      return (value / 1000).toFixed(2) + "s";
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
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || res.statusText);
      return data;
    }

    function pipelineStatus(p) {
      if (p && p.error) return "error";
      if (p && p.last_run && p.last_run.status === "error") return "error";
      if (p && p.last_run) return "ok";
      return "gray";
    }

    function runDotClass(run) {
      return run && run.status === "error" ? "error" : "ok";
    }

    function runDateParts(run) {
      const date = run.created_at ? new Date(run.created_at) : null;
      if (!date || Number.isNaN(date.getTime())) return { day: "", time: "", key: "" };
      return {
        day: date.toLocaleDateString([], { month: "short", day: "numeric" }),
        time: date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
        key: date.getFullYear() + "-" + date.getMonth() + "-" + date.getDate(),
      };
    }

    function runHoverText(run) {
      const when = run.created_at ? new Date(run.created_at) : null;
      const date = when && !Number.isNaN(when.getTime())
        ? when.toLocaleString()
        : (run.created_at || "");
      return [date, run.mode || "run", formatElapsed(run.elapsed_ms)].filter(Boolean).join("  ·  ");
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
      if (!hits.length) return "skip";
      if (hits.some((step) => step.status === "error")) return "error";
      return "ok";
    }

    function cellLabel(status) {
      if (status === "error") return "✕";
      if (status === "skip") return "○";
      return "✓";
    }

    function gridTemplate(count) {
      return "200px repeat(" + Math.max(count, 1) + ", 24px)";
    }

    function gridSlotCount(runCount) {
      const view = $("pipelinesView");
      const width = (view && view.clientWidth) || document.body.clientWidth || 960;
      const avail = width - 28 * 2 - 18 * 2 - 200;
      const fit = Math.floor((avail + 3) / 27);
      return Math.max(runCount, Math.min(Math.max(fit, 12), 48));
    }

    function skelNode(tag, className) {
      const el = document.createElement(tag);
      el.className = className;
      if (tag === "button") el.type = "button";
      el.tabIndex = -1;
      el.setAttribute("aria-hidden", "true");
      return el;
    }

    function renderPipeBoard() {
      const board = $("pipeBoard");
      board.innerHTML = "";
      if (!pipelines.length) {
        board.innerHTML = '<p class="empty">No pipelines</p>';
        return;
      }
      const slots = gridSlotCount(0);
      pipelines.forEach((p) => {
        const card = document.createElement("div");
        card.className = "af-card" + (p.id === fileId ? " active" : "");
        const newestFirst = p.recent || [];
        const columns = newestFirst.slice(0, 30).slice().reverse();
        const placeholders = Math.max(0, Math.max(slots, columns.length) - columns.length);
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
        const clear = document.createElement("button");
        clear.type = "button";
        clear.className = "ghost";
        clear.textContent = "Clear";
        clear.disabled = !newestFirst.length;
        clear.onclick = () => clearPipelineRuns(p).catch((e) => alert(e.message));
        actions.appendChild(lastEl);
        actions.appendChild(clear);
        head.appendChild(title);
        head.appendChild(actions);
        card.appendChild(head);
        const grid = document.createElement("div");
        grid.className = "af-grid";
        const total = columns.length + placeholders;
        const template = gridTemplate(total);
        const maxMs = Math.max(...columns.map((run) => Number(run.elapsed_ms) || 0), 1);
        const bars = document.createElement("div");
        bars.className = "af-bars";
        bars.style.gridTemplateColumns = template;
        bars.appendChild(document.createElement("div"));
        columns.forEach((run) => {
          const bar = document.createElement("button");
          bar.type = "button";
          bar.className = "af-bar " + runDotClass(run);
          bar.style.height = Math.max(6, Math.round(((Number(run.elapsed_ms) || 0) / maxMs) * 52)) + "px";
          bar.title = runHoverText(run);
          bar.onclick = () => openPipelineRun(p.id, run.id);
          bars.appendChild(bar);
        });
        for (let i = 0; i < placeholders; i++) bars.appendChild(skelNode("div", "af-bar skel"));
        const days = document.createElement("div");
        days.className = "af-days";
        days.style.gridTemplateColumns = template;
        days.appendChild(document.createElement("div"));
        const times = document.createElement("div");
        times.className = "af-times";
        times.style.gridTemplateColumns = template;
        times.appendChild(document.createElement("div"));
        let previousDay = "";
        columns.forEach((run) => {
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
        for (let i = 0; i < placeholders; i++) {
          days.appendChild(skelNode("div", "af-day skel"));
          times.appendChild(skelNode("div", "af-time skel"));
        }
        const plays = document.createElement("div");
        plays.className = "af-plays";
        plays.style.gridTemplateColumns = template;
        plays.appendChild(document.createElement("div"));
        columns.forEach((run) => {
          const play = document.createElement("button");
          play.type = "button";
          play.className = "af-play";
          play.textContent = "▶";
          play.title = runHoverText(run);
          play.onclick = () => openPipelineRun(p.id, run.id);
          plays.appendChild(play);
        });
        for (let i = 0; i < placeholders; i++) plays.appendChild(skelNode("div", "af-play skel"));
        grid.appendChild(bars);
        grid.appendChild(days);
        grid.appendChild(times);
        grid.appendChild(plays);
        const tasks = taskOrder(newestFirst);
        const rows = tasks.length ? tasks : [null, null, null];
        rows.forEach((node) => {
          const row = document.createElement("div");
          row.className = "af-row";
          row.style.gridTemplateColumns = template;
          const label = document.createElement("div");
          label.className = node ? "af-task" : "af-task skel";
          if (node) label.textContent = node;
          row.appendChild(label);
          columns.forEach((run) => {
            if (!node) {
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
          for (let i = 0; i < placeholders; i++) {
            const cell = skelNode("div", "af-cell skel");
            cell.appendChild(skelNode("span", "af-mark skel"));
            row.appendChild(cell);
          }
          grid.appendChild(row);
        });
        card.appendChild(grid);
        board.appendChild(card);
      });
    }

    function viewFromHash() {
      if (location.hash === "#/pipelines") return "pipelines";
      if (location.hash === "#/compare") return "compare";
      return "trace";
    }

    function showView(name) {
      currentView = name === "pipelines" || name === "compare" ? name : "trace";
      $("traceView").hidden = currentView !== "trace";
      $("pipelinesView").hidden = currentView !== "pipelines";
      $("compareView").hidden = currentView !== "compare";
      $("navTrace").classList.toggle("active", currentView === "trace");
      $("navPipelines").classList.toggle("active", currentView === "pipelines");
      $("navCompare").classList.toggle("active", currentView === "compare");
      if (currentView === "pipelines") renderPipeBoard();
      if (currentView === "compare") loadCompare().catch((e) => alert(e.message));
      const hash = "#/" + currentView;
      if (location.hash !== hash) location.hash = hash;
    }

    async function openPipeline(pipelineId) {
      await selectPipeline(pipelineId);
      showView("trace");
    }

    async function openPipelineRun(pipelineId, runId) {
      if (pipelineId !== fileId) await selectPipeline(pipelineId);
      await openRun(runId);
      showView("trace");
    }

    async function loadPipelines() {
      pipelines = (await api("/api/pipelines")).pipelines;
      const box = $("pipelines");
      box.innerHTML = "";
      pipelines.forEach((p) => {
        const btn = document.createElement("button");
        btn.className = "item" + (p.error ? " error" : "") + (p.id === fileId ? " active" : "");
        btn.innerHTML =
          '<span class="status-dot ' + pipelineStatus(p) + '"></span> ' +
          escapeHtml(p.stem + (p.error ? " (error)" : ""));
        btn.title = p.error || p.id;
        btn.onclick = () => selectPipeline(p.id);
        box.appendChild(btn);
      });
      if (currentView === "pipelines") renderPipeBoard();
      if (fileId) renderExampleChips();
      if (!fileId && pipelines.length) selectPipeline(pipelines[0].id);
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
      const p = pipeline();
      const example = (p && p.examples && p.examples[0]) || null;
      $("input").value = JSON.stringify(exampleInput(example), null, 2);
      renderExampleChips();
      await loadPipelines();
      renderExampleChips();
      await loadHistory();
      renderRun(null);
    }

    function runMatchesFilter(run) {
      const status = $("runStatus").value;
      if (status !== "all" && run.status !== status) return false;
      const q = ($("runFilter").value || "").trim().toLowerCase();
      if (!q) return true;
      if (q === "has error" || q === "error" || q === "failed") return run.status === "error";
      if (q === "ok" || q === "success") return run.status === "ok";
      const input = JSON.stringify(run.input || {}).toLowerCase();
      const nodes = (run.steps || []).map((step) => String(step.node || "").toLowerCase()).join(" ");
      const blob = [run.status, run.mode, run.pipeline, input, nodes, run.id].join(" ").toLowerCase();
      return blob.includes(q);
    }

    function renderHistory() {
      const box = $("history");
      if (!fileId) { box.textContent = "Select a pipeline"; return; }
      const rows = historyRuns.filter(runMatchesFilter);
      if (!historyRuns.length) { box.innerHTML = '<p class="empty">No runs — drop a JSON file to import</p>'; return; }
      if (!rows.length) { box.innerHTML = '<p class="empty">No runs match</p>'; return; }
      box.innerHTML = "";
      rows.forEach((r) => {
        const btn = document.createElement("button");
        btn.className = "run" + (currentRun && currentRun.id === r.id ? " active" : "");
        const mode = r.mode || "run";
        const pill = r.status === "error" ? "error" : mode;
        const known = { run: 1, replay: 1, replay_from: 1, error: 1 };
        const pillClass = known[pill] ? pill : "run";
        const outdated = isOutdated(r)
          ? '<span class="pill mode-outdated">outdated</span>'
          : "";
        btn.innerHTML =
          '<div class="run-top"><span>' + escapeHtml(formatTime(r.created_at)) +
          '</span><span class="run-pills"><span class="pill mode-' + pillClass + '">' +
          escapeHtml(pill) + "</span>" + outdated + "</span></div>" +
          '<div class="meta">' + escapeHtml(truncateJson(r.input)) +
          (r.elapsed_ms != null ? "  ·  " + escapeHtml(formatElapsed(r.elapsed_ms)) : "") +
          "</div>";
        btn.onclick = () => openRun(r.id);
        box.appendChild(btn);
      });
    }

    async function loadHistory() {
      if (!fileId) { historyRuns = []; renderHistory(); return; }
      const { runs } = await api("/api/runs?file=" + encodeURIComponent(fileId));
      historyRuns = runs;
      renderHistory();
    }

    async function openRun(id) {
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

    function setJson(el, value, emptyText) {
      el.innerHTML = "";
      if (emptyText) {
        el.classList.add("empty");
        el.textContent = emptyText;
        return;
      }
      el.classList.remove("empty");
      const messages = extractMessages(value);
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
        return;
      }
      $("nvTitle").textContent = step.node || "Node";
      $("nvMeta").textContent = [
        step.step_id,
        formatElapsed(step.elapsed_ms),
        step.error ? "error" : "",
      ].filter(Boolean).join("  ·  ");
      $("ioName").textContent = formatElapsed(step.elapsed_ms);
      empty.style.display = "none";
      editor.classList.add("visible");
      actions.classList.add("visible");
      editor.value = JSON.stringify(step.state_in ?? {}, null, 2);
      setJson($("stepOut"), step.update);
    }

    function isNodeViewOpen() {
      return !$("nodeView").hidden;
    }

    function setNodeViewMode(mode) {
      nodeViewMode = mode || "all";
      const call = $("callBtn");
      const from = $("replayFromHereBtn");
      call.hidden = nodeViewMode === "replay_from";
      from.hidden = nodeViewMode === "replay";
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
      selectStep(step);
      setNodeViewMode(mode || "all");
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
      document.querySelectorAll(".step, .g-card[data-step-id]").forEach((el) => {
        el.classList.toggle("selected", el.dataset.stepId === selectedStep);
      });
    }

    function syncStepButtons() {
      const disabled = !selectedStep;
      $("replayBtn").disabled = disabled;
      $("replayFromBtn").disabled = disabled;
      $("exportBtn").disabled = !currentRun;
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
    }

    function stepDetail(step) {
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
      if (!skipped && step.step_id) card.dataset.stepId = step.step_id;
      const detail = skipped ? (step.reason || "not taken") : stepDetail(step);
      card.innerHTML =
        '<span class="g-dot"></span><span class="g-body">' +
        '<span class="g-name">' + escapeHtml(step.node || "") + "</span>" +
        (detail ? '<span class="g-sub">' + escapeHtml(detail) + "</span>" : "") +
        "</span>";
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
      const parts = flow.querySelectorAll(".g-cap, .g-card, .g-line, .g-skip");
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

    function renderLiveGraph(spec, caption) {
      const target = $("diagram");
      const names = orderGraphNodes(spec);
      if (!names.length) {
        target.className = "empty";
        target.innerHTML = caption || "Select a run to inspect the unrolled path.";
        applyGraphZoom();
        return;
      }
      target.className = "";
      target.innerHTML = caption
        ? '<p class="empty" style="margin:0 0 10px">' + escapeHtml(caption) + "</p>"
        : "";
      const flow = document.createElement("div");
      flow.className = "gflow";
      flow.appendChild(graphCap("Start"));
      names.forEach((name) => {
        flow.appendChild(graphLine());
        const row = document.createElement("div");
        row.className = "g-row";
        row.appendChild(graphCard({ node: name, reason: "", step_id: name }, false));
        flow.appendChild(row);
      });
      flow.appendChild(graphLine());
      flow.appendChild(graphCap("End"));
      mountGraph(target, flow);
    }

    function renderGraph(run) {
      const target = $("diagram");
      const pipe = currentPipeline();
      if (pipe && pipe.error) {
        target.className = "empty";
        target.innerHTML = escapeHtml(pipe.error);
        applyGraphZoom();
        return;
      }
      if (pipe && pipe.graph && (!run || isOutdated(run))) {
        renderLiveGraph(
          pipe.graph,
          run ? "Current pipeline — this run is outdated" : "Current pipeline",
        );
        return;
      }
      if (!run) {
        target.className = "empty";
        target.innerHTML = "Select a run to inspect the unrolled path. Double-click a node to open its view.";
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

    function renderRun(run) {
      const steps = $("steps");
      steps.innerHTML = "";
      setJson($("state"), run ? run.result : {});
      $("ascii").textContent = run ? run.ascii : "";
      $("stepsMs").textContent = run && run.elapsed_ms != null ? formatElapsed(run.elapsed_ms) : "";
      if (!run) {
        selectedStep = null;
        renderGraph(null);
        showStepIO(null);
        closeNodeView();
        syncStepButtons();
        return;
      }
      const selected = (run.steps || []).find((step) => step.step_id === selectedStep);
      showStepIO(selected || null);
      (run.steps || []).forEach((step) => {
        const row = document.createElement("div");
        row.className = "step" + (selectedStep === step.step_id ? " selected" : "") + (step.error ? " error" : "");
        row.dataset.stepId = step.step_id;
        const kind = step.error ? "error" : stepKind(step.node);
        const elapsed = formatElapsed(step.elapsed_ms);
        row.innerHTML =
          '<div class="bar ' + kind + '"></div><div>' +
          '<div class="step-id">' + escapeHtml(step.step_id) + "</div>" +
          '<div class="step-name">' + escapeHtml(step.node) + "</div>" +
          '<div class="step-why">' + escapeHtml(step.reason || "") + "</div></div>" +
          '<div class="step-ms">' + escapeHtml(elapsed) + "</div>";
        bindStepActions(row, step);
        steps.appendChild(row);
      });
      renderGraph(run);
      syncStepButtons();
    }

    async function runGraph() {
      let input;
      try { input = JSON.parse($("input").value || "{}"); }
      catch (err) { alert("Input must be JSON"); return; }
      currentRun = await api("/api/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ file: fileId, input }),
      });
      selectedStep = null;
      renderRun(currentRun);
      await loadHistory();
      await loadPipelines();
    }

    function currentPipeline() {
      return pipelines.find((p) => p.id === fileId) || null;
    }

    function isOutdated(run) {
      const pipe = currentPipeline();
      if (!run || !pipe) return false;
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
      if (!(await confirmOutdatedReplay())) return;
      const input = readNodeInput();
      currentRun = await api("/api/rerun", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          run_id: currentRun.id,
          step_id: selectedStep,
          mode,
          input,
        }),
      });
      const keepView = isNodeViewOpen();
      selectedStep = (currentRun.steps && currentRun.steps[0] && currentRun.steps[0].step_id) || null;
      renderRun(currentRun);
      if (keepView && selectedStep) $("nodeView").hidden = false;
      await loadHistory();
      await loadPipelines();
    }

    async function removeRun() {
      if (!currentRun) return;
      await api("/api/runs/" + currentRun.id, { method: "DELETE" });
      currentRun = null;
      renderRun(null);
      await loadHistory();
      await loadPipelines();
    }

    async function clearPipelineRuns(p) {
      if (!p || !confirm("Delete all runs for " + p.stem + "?")) return;
      await api("/api/runs?file=" + encodeURIComponent(p.id), { method: "DELETE" });
      if (fileId === p.id) {
        currentRun = null;
        renderRun(null);
        await loadHistory();
      }
      await loadPipelines();
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

    function firstByNode(run) {
      const map = new Map();
      (run.steps || []).forEach((step) => {
        if (step.node && !map.has(step.node)) map.set(step.node, step);
      });
      return map;
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
      const mapA = firstByNode(a);
      const mapB = firstByNode(b);
      const nodes = Array.from(new Set([...mapA.keys(), ...mapB.keys()]));
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
      const outRows = nodes.map((node) => {
        const left = mapA.get(node);
        const right = mapB.get(node);
        return {
          key: node,
          a: left ? left.update : undefined,
          b: right ? right.update : undefined,
          kind: !left ? "only-b" : !right ? "only-a" : jsonEqual(left.update, right.update) ? "" : "changed",
        };
      });
      const timeRows = nodes.map((node) => {
        const left = mapA.get(node);
        const right = mapB.get(node);
        return {
          key: node,
          a: left ? formatElapsed(left.elapsed_ms) : "",
          b: right ? formatElapsed(right.elapsed_ms) : "",
          kind: !left ? "only-b" : !right ? "only-a" : Number(left.elapsed_ms) === Number(right.elapsed_ms) ? "" : "changed",
        };
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
      if (currentView === "pipelines") renderPipeBoard();
      if (currentView === "compare") renderCompare().catch(() => {});
    }

    async function pollScan() {
      if (document.hidden) return;
      try {
        const data = await api("/api/scan");
        const next = JSON.stringify(data.files || []);
        if (!scanKey) {
          scanKey = next;
          return;
        }
        if (next === scanKey) return;
        const prev = JSON.parse(scanKey);
        scanKey = next;
        const changed = (data.files || []).filter((item) => {
          const before = prev.find((old) => old.id === item.id);
          return !before || before.sha256 !== item.sha256 || before.mtime !== item.mtime;
        }).map((item) => item.id);
        prev.forEach((old) => {
          if (!(data.files || []).some((item) => item.id === old.id)) changed.push(old.id);
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
    (function bindGraphPan() {
      const box = $("diagram");
      let dragging = false;
      let lastX = 0;
      let lastY = 0;
      box.addEventListener("pointerdown", (event) => {
        if (box.classList.contains("empty")) return;
        if (event.target.closest("button, a, input, textarea")) return;
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
    $("navPipelines").onclick = () => showView("pipelines");
    $("navCompare").onclick = () => showView("compare");
    document.addEventListener("click", async (event) => {
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
      if (currentView !== "pipelines") return;
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
    loadPipelines().then(() => {
      showView(viewFromHash());
    }).catch((e) => alert(e.message));
    setInterval(() => pollScan(), 2000);
    setInterval(() => pollChanges(), 500);
  </script>
</body>
</html>
"""


class GraphVIHandler(BaseHTTPRequestHandler):
    workspace: Path
    cache: dict[str, LoadedPipeline]
    watcher: PipelineWatcher | None = None

    def log_message(self, format: str, *args) -> None:
        print(f"[graphviagent] {args[0]}")

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

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        return json.loads(raw.decode() or "{}")

    def _rel_id(self, path: Path) -> str:
        return path.resolve().relative_to(self.workspace.resolve()).as_posix()

    def _known_stems(self) -> list[str]:
        return [path.stem for path in discover_pipelines(self.workspace)]

    def _scan_files(self) -> list[dict]:
        if self.watcher is not None:
            self.watcher.refresh(emit=True)
            return self.watcher.files()
        items = []
        for path in discover_pipelines(self.workspace):
            try:
                sha = file_sha256(path)
                mtime = path.stat().st_mtime
            except OSError:
                continue
            items.append(
                {
                    "id": self._rel_id(path),
                    "stem": path.stem,
                    "mtime": mtime,
                    "sha256": sha,
                }
            )
        return items

    def _load_listed(self, file_id: str) -> LoadedPipeline:
        path = (self.workspace / file_id).resolve()
        if self.workspace.resolve() not in path.parents and path != self.workspace.resolve():
            loaded = LoadedPipeline(path=path, stem=path.stem)
            loaded.error = "path outside workspace"
            return loaded
        try:
            sha = file_sha256(path)
        except OSError:
            sha = None
        loaded = load_pipeline(path)
        loaded._sha256 = sha  # type: ignore[attr-defined]
        self.cache[file_id] = loaded
        return loaded

    def _get_loaded(self, file_id: str) -> LoadedPipeline:
        path = (self.workspace / file_id).resolve()
        if self.workspace.resolve() not in path.parents and path != self.workspace.resolve():
            raise ValueError("path outside workspace")
        try:
            sha = file_sha256(path)
        except OSError:
            sha = None
        cached = self.cache.get(file_id)
        if cached is None or sha is None or getattr(cached, "_sha256", None) != sha:
            loaded = load_pipeline(path)
            loaded._sha256 = sha  # type: ignore[attr-defined]
            self.cache[file_id] = loaded
            cached = loaded
        if cached.error:
            raise RuntimeError(cached.error)
        if cached.app is None:
            raise RuntimeError("pipeline has no graph")
        return cached

    def _with_render(self, run: dict) -> dict:
        run = dict(run)
        run["mermaid"] = unrolled_mermaid(run.get("steps") or [])
        run["ascii"] = ascii_tree(run.get("pipeline") or "run", run.get("steps") or [])
        return run

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send(200, PAGE.encode(), "text/html; charset=utf-8")
            return
        if parsed.path in {"/favicon.svg", "/favicon.ico"}:
            self._send(200, FAVICON_SVG.encode(), "image/svg+xml")
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
            for path in discover_pipelines(self.workspace):
                file_id = self._rel_id(path)
                loaded = self._load_listed(file_id)
                runs = list_runs(self.workspace, path.stem, include_steps=True, limit=30)
                items.append(
                    {
                        "id": file_id,
                        "stem": path.stem,
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
                self._json(200, {"runs": list_all_runs(self.workspace, include_steps=True)})
                return
            file_id = (query.get("file") or [""])[0]
            stem = Path(file_id).stem
            self._json(200, {"runs": list_runs(self.workspace, stem, include_steps=True)})
            return
        if parsed.path.startswith("/api/runs/"):
            run_id = parsed.path.rsplit("/", 1)[-1]
            run = load_run(self.workspace, run_id)
            if run is None:
                self._json(404, {"error": "run not found"})
                return
            self._json(200, self._with_render(run))
            return
        self._json(404, {"error": "not found"})

    def do_DELETE(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/runs":
            file_id = (parse_qs(parsed.query).get("file") or [""])[0]
            if not file_id:
                self._json(400, {"error": "file is required"})
                return
            count = delete_runs(self.workspace, Path(file_id).stem)
            self._json(200, {"ok": True, "deleted": count})
            return
        if parsed.path.startswith("/api/runs/"):
            run_id = parsed.path.rsplit("/", 1)[-1]
            ok = delete_run(self.workspace, run_id)
            self._json(200 if ok else 404, {"ok": ok})
            return
        self._json(404, {"error": "not found"})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            payload = self._read_json()
            if parsed.path == "/api/run":
                loaded = self._get_loaded(payload["file"])
                run = record_run(
                    loaded.app,
                    payload.get("input") or {},
                    has_checkpointer=loaded.has_checkpointer,
                )
                saved = save_run(
                    self.workspace,
                    loaded.stem,
                    attach_graph_meta(run, loaded.graph, loaded.graph_hash, loaded.file_sha256),
                )
                self._json(200, self._with_render(saved))
                return
            if parsed.path == "/api/rerun":
                run = load_run(self.workspace, payload["run_id"])
                if run is None:
                    self._json(404, {"error": "run not found"})
                    return
                matches = [
                    path
                    for path in discover_pipelines(self.workspace)
                    if path.stem == run["pipeline"]
                ]
                if not matches:
                    raise RuntimeError(f"pipeline {run['pipeline']} not found")
                loaded = self._get_loaded(self._rel_id(matches[0]))
                mode = payload.get("mode") or "replay"
                patch = payload.get("state_patch") or None
                incoming = payload.get("input")
                if incoming is not None and not isinstance(incoming, dict):
                    raise ValueError("input must be a JSON object")
                if mode == "resume":
                    new_run = resume_from_step(
                        loaded.app,
                        run,
                        payload["step_id"],
                        patch,
                        incoming,
                    )
                else:
                    new_run = replay_step(
                        loaded.app,
                        run,
                        payload["step_id"],
                        patch,
                        incoming,
                    )
                saved = save_run(
                    self.workspace,
                    loaded.stem,
                    attach_graph_meta(new_run, loaded.graph, loaded.graph_hash, loaded.file_sha256),
                )
                self._json(200, self._with_render(saved))
                return
            if parsed.path == "/api/import":
                fallback = Path(payload.get("file") or "").stem
                saved = import_run(
                    self.workspace,
                    payload.get("run") or {},
                    fallback_stem=fallback,
                    known_stems=self._known_stems(),
                )
                self._json(200, self._with_render(saved))
                return
            self._json(404, {"error": "not found"})
        except Exception as exc:
            self._json(400, {"error": str(exc)})


def serve(
    workspace: Path,
    host: str = "127.0.0.1",
    port: int = 8765,
    open_browser: bool = False,
) -> None:
    handler = partial(GraphVIHandler)
    GraphVIHandler.workspace = workspace.resolve()
    GraphVIHandler.cache = {}

    def invalidate(file_ids: list[str]) -> None:
        for file_id in file_ids:
            GraphVIHandler.cache.pop(file_id, None)

    watcher = PipelineWatcher(GraphVIHandler.workspace, on_change=invalidate)
    watcher.start()
    GraphVIHandler.watcher = watcher
    server = ThreadingHTTPServer((host, port), handler)
    url = f"http://{host}:{port}"
    print(f"GraphVIAgent {url}  workspace={workspace}", flush=True)
    if open_browser:
        threading.Timer(0.4, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    finally:
        watcher.stop()
        GraphVIHandler.watcher = None
