# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
"""Load the verona week's story files and map entries to journal segments."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

STORY_DIR = Path(__file__).resolve().parent / "story"
INDEX_PATH = Path(__file__).resolve().parent / "rendered.json"
STREAMS = {"audio": "verona.audio", "screen": "verona.screen"}
MEDIA = {"audio": "audio.wav", "screen": "screen.mp4"}
LICENSE = "CC-BY-4.0"


def load_cast() -> dict:
    return json.loads((STORY_DIR / "cast.json").read_text(encoding="utf-8"))


def load_week() -> dict:
    return json.loads((STORY_DIR / "week.json").read_text(encoding="utf-8"))


def load_entries() -> list[dict]:
    """Every entry in the week, with its day attached, in time order."""
    entries: list[dict] = []
    for day in load_week()["days"]:
        for entry in day["entries"]:
            entries.append({**entry, "day": day["day"], "weekday": day["weekday"]})
    return entries


def load_script(entry: dict) -> dict:
    path = STORY_DIR / "days" / f"{entry['id']}.json"
    script = json.loads(path.read_text(encoding="utf-8"))
    if script.get("id") != entry["id"] or script.get("kind") != entry["kind"]:
        raise ValueError(f"{path}: id/kind do not match week.json entry {entry['id']}")
    return script


def segment_dir(journal_dir: Path, entry: dict, kind: str, duration: int) -> Path:
    return journal_dir / entry["day"] / STREAMS[kind] / f"{entry['time']}_{duration}"


def clear_entry(journal_dir: Path, entry: dict, kind: str) -> None:
    """Remove any earlier render of this entry (its duration may change)."""
    stream_dir = journal_dir / entry["day"] / STREAMS[kind]
    if not stream_dir.exists():
        return
    for seg in stream_dir.glob(f"{entry['time']}_*"):
        shutil.rmtree(seg)


def write_index(journal_dir: Path) -> list[dict]:
    """Record every rendered segment so build.py can list it without rendering."""
    rendered: list[dict] = []
    for entry in load_entries():
        kind = entry["kind"]
        stream_dir = journal_dir / entry["day"] / STREAMS[kind]
        matches = sorted(stream_dir.glob(f"{entry['time']}_*/{MEDIA[kind]}"))
        if len(matches) > 1:
            raise RuntimeError(f"{entry['id']}: more than one rendered segment")
        if not matches:
            continue
        segment = matches[0].parent.name
        rendered.append(
            {
                "id": entry["id"],
                "day": entry["day"],
                "stream": STREAMS[kind],
                "time": entry["time"],
                "duration_seconds": int(segment.split("_")[1]),
                "title": entry["title"],
            }
        )
    INDEX_PATH.write_text(json.dumps(rendered, indent=2) + "\n", encoding="utf-8")
    return rendered


def load_index() -> list[dict]:
    if not INDEX_PATH.exists():
        return []
    return json.loads(INDEX_PATH.read_text(encoding="utf-8"))
