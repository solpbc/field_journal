# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.sources import dipco, voxconverse

REPO_ROOT = Path(__file__).resolve().parent.parent
JOURNAL_DIR = REPO_ROOT / "journal"
MANIFEST_PATH = REPO_ROOT / "manifest.json"


def test_segment_directories_have_media() -> None:
    """Every segment has the expected media file."""
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    segments = data.get("segments", [])
    if not segments:
        pytest.skip("Journal not built")

    for entry in segments:
        seg_dir = JOURNAL_DIR / entry["day"] / entry["stream"] / entry["segment"]
        if entry["stream"] == "field.audio":
            assert (seg_dir / "audio.wav").exists(), f"Missing audio.wav in {seg_dir}"
        elif entry["stream"] == "field.screen":
            assert (seg_dir / "screen.mp4").exists(), f"Missing screen.mp4 in {seg_dir}"


def test_stream_json_exists_and_valid() -> None:
    """Every segment has a valid stream.json."""
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    segments = data.get("segments", [])
    if not segments:
        pytest.skip("Journal not built")

    required_keys = {"stream", "prev_day", "prev_segment", "seq"}
    for entry in segments:
        seg_dir = JOURNAL_DIR / entry["day"] / entry["stream"] / entry["segment"]
        marker_path = seg_dir / "stream.json"
        assert marker_path.exists(), f"Missing stream.json in {seg_dir}"
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
        assert set(marker.keys()) == required_keys, (
            f"Wrong keys in {marker_path}: {set(marker.keys())}"
        )
        assert marker["stream"] == entry["stream"]


def test_stream_sequencing() -> None:
    """Stream sequence numbers are contiguous and chaining is valid."""
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    segments = data.get("segments", [])
    if not segments:
        pytest.skip("Journal not built")

    for stream_name in ["field.audio", "field.screen"]:
        stream_segs = [
            segment for segment in segments if segment["stream"] == stream_name
        ]
        markers: list[tuple[dict, dict]] = []
        for entry in stream_segs:
            seg_dir = JOURNAL_DIR / entry["day"] / entry["stream"] / entry["segment"]
            marker = json.loads((seg_dir / "stream.json").read_text(encoding="utf-8"))
            markers.append((entry, marker))

        seqs = [marker["seq"] for _, marker in markers]
        assert seqs == list(range(1, len(markers) + 1)), (
            f"Sequence gap in {stream_name}: {seqs}"
        )

        for index, (entry, marker) in enumerate(markers):
            if index == 0:
                assert marker["prev_day"] is None
                assert marker["prev_segment"] is None
            else:
                prev_entry = markers[index - 1][0]
                assert marker["prev_day"] == prev_entry["day"]
                assert marker["prev_segment"] == prev_entry["segment"]


def test_stream_state_files() -> None:
    """Stream state files exist with correct final state."""
    streams_dir = JOURNAL_DIR / "streams"
    if not streams_dir.exists():
        pytest.skip("Journal not built")

    for name, expected_seq, expected_last_day, expected_last_seg in [
        ("field.audio", 79, "20260205", "141500_540"),
        ("field.screen", 11, "20260205", "144500_360"),
    ]:
        state_path = streams_dir / f"{name}.json"
        assert state_path.exists(), f"Missing {state_path}"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        expected_keys = {
            "name",
            "type",
            "host",
            "platform",
            "created_at",
            "last_day",
            "last_segment",
            "seq",
        }
        assert set(state.keys()) == expected_keys, (
            f"Wrong keys in {state_path}: {set(state.keys())}"
        )
        assert state["name"] == name
        assert state["seq"] == expected_seq
        assert state["last_day"] == expected_last_day
        assert state["last_segment"] == expected_last_seg
        assert state["host"] is None
        assert state["platform"] is None
        assert state["created_at"] == 1769904000
        assert state["type"] == "import"


def test_ami_reference_data() -> None:
    """AMI reference files exist for required meetings."""
    ref_dir = REPO_ROOT / "reference"
    if not ref_dir.exists():
        pytest.skip("Reference data not generated")

    for meeting_id in ["ES2002a", "ES2005a"]:
        meeting_dir = ref_dir / "ami" / meeting_id
        assert meeting_dir.exists(), f"Missing reference dir: {meeting_dir}"
        assert (meeting_dir / "transcript.txt").exists()
        assert (meeting_dir / "speakers.json").exists()


def test_chime6_reference_data() -> None:
    """CHiME-6 reference files exist for locked sessions."""
    ref_dir = REPO_ROOT / "reference"
    if not ref_dir.exists():
        pytest.skip("Reference data not generated")

    for session_id in ["S01", "S21"]:
        session_dir = ref_dir / "chime6" / session_id
        assert session_dir.exists(), f"Missing reference dir: {session_dir}"
        assert (session_dir / "transcript.txt").exists()
        assert (session_dir / "speakers.json").exists()


def test_icsi_reference_data() -> None:
    """ICSI reference files exist for locked meetings."""
    ref_dir = REPO_ROOT / "reference"
    if not ref_dir.exists():
        pytest.skip("Reference data not generated")

    for meeting_id in ["Bmr005", "Bmr006", "Bmr007"]:
        meeting_dir = ref_dir / "icsi" / meeting_id
        assert meeting_dir.exists(), f"Missing reference dir: {meeting_dir}"
        assert (meeting_dir / "transcript.txt").exists()
        assert (meeting_dir / "speakers.json").exists()


def test_dipco_reference_data() -> None:
    """DiPCo reference files exist and are non-trivial."""
    ref_dir = REPO_ROOT / "reference"
    if not ref_dir.exists():
        pytest.skip("Reference data not generated")

    for session_id in dipco.SESSIONS:
        session_dir = ref_dir / "dipco" / session_id
        transcript_path = session_dir / "transcript.txt"
        speakers_path = session_dir / "speakers.json"
        assert session_dir.exists(), f"Missing reference dir: {session_dir}"
        assert transcript_path.exists()
        assert speakers_path.exists()
        assert transcript_path.read_text(encoding="utf-8").strip()
        speakers = json.loads(speakers_path.read_text(encoding="utf-8"))
        assert len(speakers) >= 2
        assert any(data.get("word_count", 0) > 0 for data in speakers.values()), (
            speakers
        )


def test_voxconverse_reference_data() -> None:
    """VoxConverse reference files contain speaker counts only."""
    ref_dir = REPO_ROOT / "reference"
    if not ref_dir.exists():
        pytest.skip("Reference data not generated")

    for clip_id in voxconverse.CLIPS:
        clip_dir = ref_dir / "voxconverse" / clip_id
        speakers_path = clip_dir / "speakers.json"
        assert clip_dir.exists(), f"Missing reference dir: {clip_dir}"
        assert speakers_path.exists()
        assert not (clip_dir / "transcript.txt").exists()
        speakers = json.loads(speakers_path.read_text(encoding="utf-8"))
        assert len(speakers) >= 2
        assert any(data.get("word_count", 0) > 0 for data in speakers.values()), (
            speakers
        )
