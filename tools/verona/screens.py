# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
"""Record the verona week's screen entries in headless Chromium.

Each screen script becomes a plain, unbranded page (a document, a task board,
a code review or an email draft) whose content is typed out over the entry's
target duration, then recorded to ``screen.mp4``.
"""

from __future__ import annotations

import html
import json
import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

from tools.verona import story

log = logging.getLogger("verona.screens")

WIDTH, HEIGHT = 1440, 900
HOLD_S = 6

_CSS = """
* { box-sizing: border-box; }
body { margin: 0; font: 17px/1.55 -apple-system, "Segoe UI", Helvetica, Arial, sans-serif;
  color: #1f2328; background: #eef0f3; }
.chrome { height: 44px; background: #dfe3e8; display: flex; align-items: center;
  padding: 0 16px; gap: 8px; border-bottom: 1px solid #c9ced6; }
.dot { width: 12px; height: 12px; border-radius: 50%; background: #c3c8cf; }
.tab { margin-left: 18px; padding: 6px 14px; background: #fff; border-radius: 8px 8px 0 0;
  font-size: 14px; color: #444; }
.page { width: 900px; margin: 28px auto; background: #fff; min-height: 780px;
  padding: 48px 64px; box-shadow: 0 1px 3px rgba(0,0,0,.12); }
h1 { font-size: 30px; margin: 0 0 18px; } h2 { font-size: 21px; margin: 26px 0 8px; }
p { margin: 0 0 12px; } li { margin: 0 0 6px; }
.caret::after { content: "|"; color: #2f6feb; animation: b 1s steps(1) infinite; }
@keyframes b { 50% { opacity: 0; } }
.board { display: flex; gap: 18px; padding: 28px; }
.col { flex: 1; background: #e3e7ec; border-radius: 10px; padding: 14px; min-height: 760px; }
.col h3 { margin: 0 0 12px; font-size: 16px; color: #555; }
.card { background: #fff; border-radius: 8px; padding: 12px 14px; margin-bottom: 10px;
  box-shadow: 0 1px 2px rgba(0,0,0,.12); transition: outline .3s; }
.card small { display: block; color: #667; margin-top: 4px; }
.card.moved { outline: 3px solid #2f6feb; }
.code { font: 14px/1.6 ui-monospace, Menlo, Consolas, monospace; }
.file { margin: 18px 0; border: 1px solid #d0d7de; border-radius: 8px; overflow: hidden; }
.file .name { background: #f6f8fa; padding: 8px 12px; border-bottom: 1px solid #d0d7de; }
.ln { padding: 0 12px; white-space: pre; }
.add { background: #e6ffec; } .del { background: #ffebe9; }
.cmt { margin: 6px 12px 10px; padding: 10px 12px; border: 1px solid #d0d7de;
  border-radius: 8px; background: #fff; font-family: -apple-system, Helvetica, Arial, sans-serif; }
.badge { display: inline-block; padding: 4px 12px; border-radius: 14px; background: #1f883d;
  color: #fff; font-weight: 600; }
.mailhdr div { padding: 8px 0; border-bottom: 1px solid #e3e6ea; color: #444; }
.hidden { display: none; }
"""

_TYPE_JS = """
async function run(cfg) {
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const nodes = [...document.querySelectorAll('[data-type]')];
  for (const el of nodes) {
    const text = el.dataset.type; el.textContent = ''; el.classList.remove('hidden');
    el.classList.add('caret');
    for (const ch of text) { el.textContent += ch; await sleep(cfg.msPerChar); }
    el.classList.remove('caret');
    await sleep(cfg.msPerBlock);
    el.scrollIntoView({block: 'nearest'});
  }
  for (const step of cfg.steps || []) { await sleep(cfg.msPerStep); step(); }
  window.__done = true;
}
"""


def _chrome(tab: str) -> str:
    return (
        '<div class="chrome"><span class="dot"></span><span class="dot"></span>'
        f'<span class="dot"></span><span class="tab">{html.escape(tab)}</span></div>'
    )


def _typed(tag: str, text: str, cls: str = "") -> str:
    attr = f' class="hidden {cls}"' if cls else ' class="hidden"'
    return f'<{tag}{attr} data-type="{html.escape(text, quote=True)}"></{tag}>'


def _doc_page(script: dict) -> tuple[str, int, str]:
    body = [_typed("h1", script["doc_title"])]
    in_list = False
    for block in script["blocks"]:
        if block["type"] == "li" and not in_list:
            body.append("<ul>")
            in_list = True
        if block["type"] != "li" and in_list:
            body.append("</ul>")
            in_list = False
        body.append(_typed(block["type"], block["text"]))
    if in_list:
        body.append("</ul>")
    chars = len(script["doc_title"]) + sum(len(b["text"]) for b in script["blocks"])
    return (
        _chrome(script["doc_title"]) + '<div class="page">' + "".join(body) + "</div>",
        chars,
        "[]",
    )


def _email_page(script: dict) -> tuple[str, int, str]:
    header = (
        '<div class="mailhdr">'
        f"<div>To: {html.escape(script['to'])}</div>"
        f"<div>Subject: {html.escape(script['subject'])}</div></div><br>"
    )
    body = [
        _typed(
            b["type"] if b["type"] == "p" else "p",
            ("- " if b["type"] == "li" else "") + b["text"],
        )
        for b in script["blocks"]
    ]
    chars = sum(len(b["text"]) for b in script["blocks"])
    return (
        _chrome("New message")
        + '<div class="page">'
        + header
        + "".join(body)
        + "</div>",
        chars,
        "[]",
    )


def _board_page(script: dict) -> tuple[str, int, str]:
    cards: dict[str, str] = {}
    cols = []
    for index, column in enumerate(script["columns"]):
        items = []
        for card in script["cards"]:
            if card["column"] != column:
                continue
            card_id = f"c{len(cards)}"
            cards[card["title"]] = card_id
            items.append(
                f'<div class="card" id="{card_id}">{html.escape(card["title"])}'
                f"<small>{html.escape(card.get('assignee', ''))}</small></div>"
            )
        cols.append(
            f'<div class="col" id="col{index}"><h3>{html.escape(column)}</h3>'
            + "".join(items)
            + "</div>"
        )
    steps = []
    for move in script["moves"]:
        if "add" in move:
            add = move["add"]
            col = script["columns"].index(add["column"])
            card_html = json.dumps(
                f'<div class="card moved">{html.escape(add["title"])}'
                f"<small>{html.escape(add.get('assignee', ''))}</small></div>"
            )
            steps.append(
                f"() => document.getElementById('col{col}').insertAdjacentHTML('beforeend', {card_html})"
            )
        else:
            col = script["columns"].index(move["to"])
            card_id = cards[move["card"]]
            steps.append(
                f"() => {{ const c = document.getElementById('{card_id}'); c.classList.add('moved');"
                f" document.getElementById('col{col}').appendChild(c); }}"
            )
    page = (
        _chrome(script["board_title"])
        + '<div class="board">'
        + "".join(cols)
        + "</div>"
    )
    return page, 0, "[" + ",".join(steps) + "]"


def _code_page(script: dict) -> tuple[str, int, str]:
    parts = [
        f"<h1>{html.escape(script['pr_title'])}</h1>",
        f"<p>{html.escape(script['author'])} wants to merge into main · {html.escape(script['repo'])}</p>",
    ]
    comments: dict[tuple[str, int], list[str]] = {}
    for comment in script["comments"]:
        comments.setdefault((comment["file"], comment["line_index"]), []).append(
            comment["text"]
        )
    chars = 0
    for diff in script["diff"]:
        lines = []
        for index, line in enumerate(diff["lines"]):
            cls = (
                "add" if line.startswith("+") else "del" if line.startswith("-") else ""
            )
            lines.append(f'<div class="ln {cls}">{html.escape(line)}</div>')
            for text in comments.get((diff["file"], index), []):
                chars += len(text)
                lines.append(
                    '<div class="cmt"><b>Juliet Capulet</b><br>'
                    + _typed("span", text)
                    + "</div>"
                )
        parts.append(
            f'<div class="file code"><div class="name">{html.escape(diff["file"])}</div>{"".join(lines)}</div>'
        )
    parts.append(
        '<p id="verdict" class="hidden"><span class="badge">Approved</span></p>'
    )
    steps = "[() => { const v = document.getElementById('verdict'); v.classList.remove('hidden'); v.scrollIntoView(); }]"
    page = (
        _chrome(script["pr_title"]) + '<div class="page">' + "".join(parts) + "</div>"
    )
    return page, chars, steps


_PAGES = {
    "doc": _doc_page,
    "email": _email_page,
    "board": _board_page,
    "code": _code_page,
}


def render_screen(
    entry: dict, script: dict, journal_dir: Path, cache_dir: Path
) -> Path:
    """Record one screen entry; return its segment directory."""
    from playwright.sync_api import sync_playwright

    duration = int(entry["target_seconds"])
    body, chars, steps = _PAGES[script["app"]](script)
    active_ms = max(duration - HOLD_S, 10) * 1000
    step_count = steps.count("() =>")
    ms_per_step = (
        0
        if not step_count
        else (active_ms * (0.3 if chars else 0.9)) // (step_count + 1)
    )
    typing_ms = active_ms - ms_per_step * step_count
    blocks = max(body.count("data-type"), 1)
    ms_per_block = 250
    ms_per_char = (
        max((typing_ms - ms_per_block * blocks) // max(chars, 1), 8) if chars else 0
    )
    page_html = (
        f"<!doctype html><html><head><meta charset='utf-8'><style>{_CSS}</style></head>"
        f"<body>{body}<script>{_TYPE_JS}</script></body></html>"
    )

    cache_dir.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(dir=cache_dir))
    try:
        (work / "page.html").write_text(page_html, encoding="utf-8")
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            context = browser.new_context(
                viewport={"width": WIDTH, "height": HEIGHT},
                record_video_dir=str(work),
                record_video_size={"width": WIDTH, "height": HEIGHT},
            )
            page = context.new_page()
            page.goto((work / "page.html").as_uri())
            page.evaluate(
                f"run({{msPerChar: {ms_per_char}, msPerBlock: {ms_per_block},"
                f" msPerStep: {ms_per_step}, steps: {steps}}})"
            )
            page.wait_for_function(
                "window.__done === true", timeout=(duration + 60) * 1000
            )
            page.wait_for_timeout(HOLD_S * 1000)
            video = page.video
            context.close()
            browser.close()
            if video is None:
                raise RuntimeError(f"{entry['id']}: Playwright produced no video")
            webm = Path(video.path())
        seg_dir = story.segment_dir(journal_dir, entry, "screen", duration)
        story.clear_entry(journal_dir, entry, "screen")
        seg_dir.mkdir(parents=True, exist_ok=True)
        # Keep the last `duration` seconds: the page load at the start is blank.
        subprocess.run(
            [
                "ffmpeg",
                "-loglevel",
                "error",
                "-y",
                "-sseof",
                f"-{duration}",
                "-i",
                str(webm),
                "-t",
                str(duration),
                "-an",
                "-c:v",
                "libx264",
                "-preset",
                "slow",
                "-crf",
                "30",
                "-pix_fmt",
                "yuv420p",
                "-r",
                "10",
                "-movflags",
                "+faststart",
                str(seg_dir / "screen.mp4"),
            ],
            check=True,
        )
        return seg_dir
    finally:
        shutil.rmtree(work, ignore_errors=True)
