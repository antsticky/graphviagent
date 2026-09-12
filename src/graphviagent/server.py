from __future__ import annotations

import json
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from graphviagent.discover import discover_pipelines
from graphviagent.load import LoadedPipeline, load_pipeline
from graphviagent.record import record_run, replay_step, resume_from_step
from graphviagent.render import ascii_tree, unrolled_mermaid
from graphviagent.store import delete_run, list_runs, load_run, save_run

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
    .layout { display: grid; grid-template-columns: 280px 1fr; height: calc(100vh - 48px); }
    aside {
      background: var(--raised);
      border-right: 1px solid var(--line);
      padding: 12px;
      overflow: auto;
    }
    main { padding: 16px 18px; overflow: auto; }
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
    }
    .meta {
      color: var(--muted); font-size: 11px; margin-top: 3px;
      white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
      font-family: "IBM Plex Mono", ui-monospace, monospace;
    }
    .toolbar { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin: 10px 0 8px; }
    textarea {
      width: 100%; min-height: 88px; resize: vertical;
      background: var(--panel); color: var(--ink);
      border: 1px solid var(--line); border-radius: 8px;
      font-family: "IBM Plex Mono", ui-monospace, monospace;
      font-size: 12px; padding: 10px; outline: none;
    }
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
    .grid { display: grid; grid-template-columns: 1.15fr 1fr; gap: 12px; }
    .pane {
      background: var(--panel); border: 1px solid var(--line);
      border-radius: 8px; padding: 12px; min-height: 240px;
    }
    #diagram {
      min-height: 320px;
      overflow: auto;
      border-radius: 8px;
      background:
        radial-gradient(circle at 1px 1px, #26262e 1px, transparent 0) 0 0 / 18px 18px;
    }
    #diagram.empty, .empty { color: var(--muted); background: transparent; }
    .gflow {
      display: flex; flex-direction: column; align-items: center;
      padding: 18px 36px 22px; min-width: min-content;
    }
    .g-cap {
      height: 22px; padding: 0 10px; border-radius: 999px;
      border: 1px solid #2e2e36; background: #121216; color: #8b8b96;
      font-size: 10px; font-weight: 500; letter-spacing: 0.08em;
      text-transform: uppercase; display: inline-flex; align-items: center;
    }
    .g-line { width: 1px; height: 18px; background: #32323c; }
    .g-row { position: relative; display: flex; justify-content: center; }
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
    .waterfall { display: flex; flex-direction: column; gap: 4px; }
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
    .json { line-height: 1.55; }
    .json.empty { color: var(--muted); }
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
    @media (max-width: 900px) { .layout, .grid, .io { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <span class="mark">GVA</span> GraphVIAgent
      <nav class="nav">
        <button class="nav-link active" id="navTrace" type="button">Trace</button>
        <button class="nav-link" id="navPipelines" type="button">Pipelines</button>
      </nav>
    </div>
  </header>
  <section id="pipelinesView" class="page" hidden>
    <div class="page-inner">
      <h1>Pipelines</h1>
      <p class="lede">Airflow-style grid: duration bars, then tasks by run. Hover for the date. Click a cell or bar to open that run.</p>
      <div id="pipeBoard"></div>
    </div>
  </section>
  <div id="traceView" class="layout">
    <aside>
      <h2>Pipelines</h2>
      <div id="pipelines"></div>
      <h2>Runs</h2>
      <div id="history" class="empty">Select a pipeline</div>
    </aside>
    <main>
      <h2>Input</h2>
      <textarea id="input">{}</textarea>
      <div class="toolbar">
        <button class="primary" id="runBtn">Run</button>
        <button class="ghost" id="replayBtn" disabled>Replay step</button>
        <button class="ghost" id="replayFromBtn" disabled>Replay from</button>
        <button class="ghost" id="deleteBtn">Delete run</button>
      </div>
      <p class="warn">Replay, Replay from, and Call node run node functions again. Side effects will fire.</p>
      <div class="grid">
        <div class="pane">
          <h2>Graph</h2>
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
          <textarea id="nodeInput" spellcheck="false"></textarea>
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
  <script>
    let pipelines = [];
    let fileId = null;
    let currentRun = null;
    let selectedStep = null;
    let nodeViewMode = "all";
    let currentView = "trace";

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
      const res = await fetch(path, opts);
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
        head.appendChild(title);
        head.appendChild(lastEl);
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

    function showView(name) {
      currentView = name === "pipelines" ? "pipelines" : "trace";
      $("traceView").hidden = currentView !== "trace";
      $("pipelinesView").hidden = currentView !== "pipelines";
      $("navTrace").classList.toggle("active", currentView === "trace");
      $("navPipelines").classList.toggle("active", currentView === "pipelines");
      if (currentView === "pipelines") renderPipeBoard();
      const hash = currentView === "pipelines" ? "#/pipelines" : "#/trace";
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
      if (!fileId && pipelines.length) selectPipeline(pipelines[0].id);
    }

    function pipeline() {
      return pipelines.find((p) => p.id === fileId);
    }

    async function selectPipeline(id) {
      fileId = id;
      currentRun = null;
      selectedStep = null;
      const p = pipeline();
      const example = (p && p.examples && p.examples[0]) || {};
      $("input").value = JSON.stringify(example, null, 2);
      await loadPipelines();
      await loadHistory();
      renderRun(null);
    }

    async function loadHistory() {
      const box = $("history");
      if (!fileId) { box.textContent = "Select a pipeline"; return; }
      const { runs } = await api("/api/runs?file=" + encodeURIComponent(fileId));
      if (!runs.length) { box.innerHTML = '<p class="empty">No runs</p>'; return; }
      box.innerHTML = "";
      runs.forEach((r) => {
        const btn = document.createElement("button");
        btn.className = "run" + (currentRun && currentRun.id === r.id ? " active" : "");
        const mode = r.mode || "run";
        btn.innerHTML =
          '<div class="run-top"><span>' + escapeHtml(formatTime(r.created_at)) +
          '</span><span class="pill">' + escapeHtml(mode) + "</span></div>" +
          '<div class="meta">' + escapeHtml(truncateJson(r.input)) +
          (r.elapsed_ms != null ? "  ·  " + escapeHtml(formatElapsed(r.elapsed_ms)) : "") +
          "</div>";
        btn.onclick = () => openRun(r.id);
        box.appendChild(btn);
      });
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

    function setJson(el, value, emptyText) {
      el.innerHTML = "";
      if (emptyText) {
        el.classList.add("empty");
        el.textContent = emptyText;
        return;
      }
      el.classList.remove("empty");
      el.appendChild(renderJsonTree(value ?? {}, undefined, 0));
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

    function renderGraph(run) {
      const target = $("diagram");
      if (!run) {
        target.className = "empty";
        target.innerHTML = "Select a run to inspect the unrolled path. Double-click a node to open its view.";
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
      target.appendChild(flow);
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
        $("diagram").className = "empty";
        $("diagram").innerHTML = "Select a run to inspect the unrolled path. Double-click a node to open its view.";
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

    async function rerun(mode) {
      if (!currentRun || !selectedStep) {
        alert("Select a node first");
        return;
      }
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

    $("runBtn").onclick = () => runGraph().catch((e) => alert(e.message));
    $("replayBtn").onclick = () => openFromButton("replay");
    $("replayFromBtn").onclick = () => openFromButton("replay_from");
    $("replayFromHereBtn").onclick = () => rerun("resume").catch((e) => alert(e.message));
    $("callBtn").onclick = () => rerun("replay").catch((e) => alert(e.message));
    $("deleteBtn").onclick = () => removeRun().catch((e) => alert(e.message));
    $("nvClose").onclick = () => closeNodeView();
    $("nodeView").onclick = (event) => {
      if (event.target === $("nodeView")) closeNodeView();
    };
    $("navTrace").onclick = () => showView("trace");
    $("navPipelines").onclick = () => showView("pipelines");
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && isNodeViewOpen()) closeNodeView();
    });
    window.addEventListener("hashchange", () => {
      showView(location.hash === "#/pipelines" ? "pipelines" : "trace");
    });
    let pipeResizeTimer = 0;
    window.addEventListener("resize", () => {
      if (currentView !== "pipelines") return;
      clearTimeout(pipeResizeTimer);
      pipeResizeTimer = setTimeout(renderPipeBoard, 120);
    });
    loadPipelines().then(() => {
      showView(location.hash === "#/pipelines" ? "pipelines" : "trace");
    }).catch((e) => alert(e.message));
  </script>
</body>
</html>
"""


class GraphVIHandler(BaseHTTPRequestHandler):
    workspace: Path
    cache: dict[str, LoadedPipeline]

    def log_message(self, format: str, *args) -> None:
        print(f"[graphviagent] {args[0]}")

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
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

    def _get_loaded(self, file_id: str) -> LoadedPipeline:
        path = (self.workspace / file_id).resolve()
        if self.workspace.resolve() not in path.parents and path != self.workspace.resolve():
            raise ValueError("path outside workspace")
        cached = self.cache.get(file_id)
        mtime = path.stat().st_mtime
        if cached is None or getattr(cached, "_mtime", None) != mtime:
            loaded = load_pipeline(path)
            loaded._mtime = mtime  # type: ignore[attr-defined]
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
        if parsed.path == "/api/pipelines":
            items = []
            for path in discover_pipelines(self.workspace):
                file_id = self._rel_id(path)
                try:
                    loaded = self._get_loaded(file_id)
                    runs = list_runs(self.workspace, path.stem, include_steps=True, limit=30)
                    items.append(
                        {
                            "id": file_id,
                            "stem": path.stem,
                            "examples": loaded.examples,
                            "error": None,
                            "last_run": runs[0] if runs else None,
                            "recent": runs,
                        }
                    )
                except Exception as exc:
                    runs = list_runs(self.workspace, path.stem, include_steps=True, limit=30)
                    items.append(
                        {
                            "id": file_id,
                            "stem": path.stem,
                            "examples": [],
                            "error": str(exc),
                            "last_run": runs[0] if runs else None,
                            "recent": runs,
                        }
                    )
            self._json(200, {"pipelines": items})
            return
        if parsed.path == "/api/runs":
            query = parse_qs(parsed.query)
            file_id = (query.get("file") or [""])[0]
            stem = Path(file_id).stem
            self._json(200, {"runs": list_runs(self.workspace, stem)})
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
                saved = save_run(self.workspace, loaded.stem, run)
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
                saved = save_run(self.workspace, loaded.stem, new_run)
                self._json(200, self._with_render(saved))
                return
            self._json(404, {"error": "not found"})
        except Exception as exc:
            self._json(400, {"error": str(exc)})


def serve(workspace: Path, host: str = "127.0.0.1", port: int = 8765) -> None:
    handler = partial(GraphVIHandler)
    GraphVIHandler.workspace = workspace.resolve()
    GraphVIHandler.cache = {}
    server = ThreadingHTTPServer((host, port), handler)
    print(f"GraphVIAgent http://{host}:{port}  workspace={workspace}", flush=True)
    server.serve_forever()
