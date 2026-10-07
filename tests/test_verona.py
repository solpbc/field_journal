# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from tools.verona import demo, story

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = REPO_ROOT / "manifest.json"
JOURNAL_DIR = REPO_ROOT / "journal"


def _manifest() -> list[dict]:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))["segments"]


def test_every_entry_has_a_matching_script() -> None:
    cast = story.load_cast()["people"]
    for entry in story.load_entries():
        script = story.load_script(entry)
        if entry["kind"] == "audio":
            speakers = {line["speaker"] for line in script["lines"]}
            assert speakers <= set(cast), f"{entry['id']}: unknown speakers"
            assert speakers <= set(entry["people"]), (
                f"{entry['id']}: speaker not listed"
            )


def test_every_entry_is_rendered_into_the_manifest() -> None:
    verona = {s["source_id"]: s for s in _manifest() if s["source"] == "verona"}
    assert set(verona) == {entry["id"] for entry in story.load_entries()}
    for entry in story.load_entries():
        seg = verona[entry["id"]]
        media = (
            JOURNAL_DIR
            / seg["day"]
            / seg["stream"]
            / seg["segment"]
            / story.MEDIA[entry["kind"]]
        )
        assert media.exists(), f"missing {media}"
        assert seg["license"] == story.LICENSE


def test_audio_entries_carry_their_exact_transcript() -> None:
    for entry in story.load_entries():
        if entry["kind"] != "audio":
            continue
        rows = (
            (REPO_ROOT / "reference" / "verona" / entry["id"] / "transcript.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        )
        assert [json.loads(r)["text"] for r in rows] == [
            line["text"] for line in story.load_script(entry)["lines"]
        ]


def test_demo_takes_only_synthetic_segments() -> None:
    """The demo journal can never include the corpus's real recordings."""
    assert demo.DEMO_SOURCES == {"verona"}
    chosen = demo.demo_segments()
    assert chosen and {s["source"] for s in chosen} == {"verona"}
    real = [s for s in _manifest() if s["source"] != "verona"]
    assert real, "control: the manifest does hold real-recording segments"
    assert not {(s["day"], s["stream"], s["segment"]) for s in real} & {
        (s["day"], s["stream"], s["segment"]) for s in chosen
    }


def test_week_lands_on_the_friday_before_the_end_date() -> None:
    days = demo.day_map(date(2026, 10, 7))  # a Wednesday
    assert list(days.values()) == [
        "20260928",
        "20260929",
        "20260930",
        "20261001",
        "20261002",
    ]
    assert (
        demo.day_map(date(2026, 10, 10))["20260213"] == "20261009"
    )  # the Saturday after
    assert (
        demo.day_map(date(2026, 10, 9))["20260213"] == "20261002"
    )  # a Friday itself is not yet over
