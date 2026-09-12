from __future__ import annotations


def _reason(step: dict) -> str:
    return step.get("reason") or ""


def format_elapsed(ms: object) -> str:
    try:
        value = float(ms)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return ""
    if value < 1000:
        text = f"{value:.1f}ms"
        return text.replace(".0ms", "ms")
    return f"{value / 1000:.2f}s"


def _detail(step: dict) -> str:
    update = step.get("update") or {}
    if not isinstance(update, dict):
        return str(update)
    reason = _reason(step)
    if reason:
        return reason
    for key in ("greeting", "shout", "polished", "output", "result"):
        if update.get(key) not in (None, ""):
            return str(update[key])
    if not update:
        return ""
    parts = [f"{key}={update[key]}" for key in list(update)[:3]]
    return ", ".join(parts)


def ascii_tree(title: str, steps: list[dict]) -> str:
    lines = [title]
    indent = 0
    for step in steps:
        extra = _detail(step)
        elapsed = format_elapsed(step.get("elapsed_ms"))
        bits = [part for part in (extra, elapsed) if part]
        suffix = f"  {'  '.join(bits)}" if bits else ""
        lines.append(f"{'   ' * indent}└─ {step['node']}{suffix}")
        indent += 1
    if steps:
        lines.append(f"{'   ' * indent}└─ END")
    return "\n".join(lines)


def _esc(text: str) -> str:
    return (
        text.replace('"', "#quot;")
        .replace("<", " ")
        .replace(">", " ")
        .replace("\n", "<br/>")
    )


def unrolled_mermaid(steps: list[dict]) -> str:
    lines = ["flowchart TD", "    startNode([START])"]
    prev = "startNode"
    for index, step in enumerate(steps):
        nid = f"n{index}"
        detail = _detail(step)
        label = f"{step['node']}<br/>{detail}" if detail else step["node"]
        lines.append(f'    {nid}["{_esc(label)}"]')
        lines.append(f"    {prev} --> {nid}")
        for unused_i, unused in enumerate(step.get("unused") or []):
            other_id = f"{nid}u{unused_i}"
            lines.append(f'    {other_id}["{_esc(str(unused))}<br/>not taken"]:::skipped')
            lines.append(f"    {nid} -.-> {other_id}")
        prev = nid
    lines.append("    endNode([END])")
    lines.append(f"    {prev} --> endNode")
    lines.append("    classDef skipped fill:#1c1c22,stroke:#3a3a44,color:#8b8b96")
    return "\n".join(lines)
