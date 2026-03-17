# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
JOURNAL_DIR = REPO_ROOT / "journal"
MANIFEST_PATH = REPO_ROOT / "manifest.json"
REQUIRED_SEGMENT_KEYS = {
    "day",
    "stream",
    "segment",
    "source",
    "source_id",
    "license",
    "duration_seconds",
    "description",
    "exercises",
    "has_reference",
}


def test_manifest_is_valid_json() -> None:
    """manifest.json exists and is a valid JSON object."""
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    assert isinstance(data, dict), "manifest.json should be a JSON object"
    assert "version" in data
    assert "built" in data
    assert "segments" in data


def test_manifest_version() -> None:
    """manifest version is fixed at 1."""
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    assert data["version"] == 1


def test_manifest_segments_have_required_fields() -> None:
    """Every manifest segment contains the required keys."""
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    segments = data["segments"]
    if not segments:
        pytest.skip("No segments in manifest (journal not built)")

    for entry in segments:
        assert set(entry.keys()) == REQUIRED_SEGMENT_KEYS, (
            f"Wrong keys in manifest entry: {set(entry.keys())}"
        )


def test_manifest_segment_count() -> None:
    """The built journal contains the expected total segment count."""
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    segments = data["segments"]
    if not segments:
        pytest.skip("No segments in manifest (journal not built)")
    assert len(segments) == 28


def test_manifest_stream_counts() -> None:
    """Audio and screen stream counts match the allocation."""
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    segments = data["segments"]
    if not segments:
        pytest.skip("No segments in manifest (journal not built)")

    audio = [segment for segment in segments if segment["stream"] == "field.audio"]
    screen = [segment for segment in segments if segment["stream"] == "field.screen"]
    assert len(audio) == 17
    assert len(screen) == 11


def test_manifest_paths_exist_on_disk() -> None:
    """Every manifest segment directory exists on disk."""
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    segments = data["segments"]
    if not segments:
        pytest.skip("No segments in manifest (journal not built)")

    for entry in segments:
        seg_dir = JOURNAL_DIR / entry["day"] / entry["stream"] / entry["segment"]
        assert seg_dir.exists(), f"Segment dir missing: {seg_dir}"


def test_manifest_both_streams_all_days() -> None:
    """Each stream appears on all five simulated days."""
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    segments = data["segments"]
    if not segments:
        pytest.skip("No segments in manifest (journal not built)")

    days = {"20260201", "20260202", "20260203", "20260204", "20260205"}
    for stream in ["field.audio", "field.screen"]:
        stream_days = {
            segment["day"] for segment in segments if segment["stream"] == stream
        }
        assert days == stream_days, f"{stream} missing days: {days - stream_days}"
