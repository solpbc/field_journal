# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
from __future__ import annotations

import json
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools.sources import ami, hpr, loc, nasa, psai

REPO_ROOT = Path(__file__).resolve().parent.parent
JOURNAL_DIR = REPO_ROOT / "journal"
MANIFEST_PATH = REPO_ROOT / "manifest.json"
CACHE_DIR = Path(__file__).resolve().parent / ".cache"
STREAMS_DIR = JOURNAL_DIR / "streams"
REFERENCE_DIR = REPO_ROOT / "reference"
DAYS = ["20260201", "20260202", "20260203", "20260204", "20260205"]
CREATED_AT = 1769904000
SOURCES = [ami, psai, loc, nasa, hpr]


def download_all() -> None:
    """Download all source files to cache."""
    for source in SOURCES:
        source.download(CACHE_DIR)


def _collect_segments() -> list[dict]:
    """Gather segments from all sources, sorted deterministically."""
    segments: list[dict] = []
    for source in SOURCES:
        segments.extend(source.segments())
    segments.sort(
        key=lambda segment: (segment["day"], segment["stream"], segment["time"])
    )
    return segments


def _slice_audio(
    src_path: Path, dest_path: Path, start_s: int, duration_s: int
) -> None:
    """Slice audio to WAV: mono, 16kHz, 16-bit PCM."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(src_path),
            "-ss",
            str(start_s),
            "-t",
            str(duration_s),
            "-vn",
            "-acodec",
            "pcm_s16le",
            "-ar",
            "16000",
            "-ac",
            "1",
            str(dest_path),
        ],
        check=True,
        capture_output=True,
    )


def _slice_screen(
    src_path: Path, dest_path: Path, start_s: int, duration_s: int
) -> None:
    """Slice/trim video to MP4."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(src_path),
            "-ss",
            str(start_s),
            "-t",
            str(duration_s),
            "-c",
            "copy",
            str(dest_path),
        ],
        check=True,
        capture_output=True,
    )


def _source_path(seg: dict) -> Path:
    """Resolve cached source file path for a segment."""
    source = seg["source"]
    source_id = seg["source_id"]
    if source == "ami":
        return CACHE_DIR / "ami" / f"{source_id}.Mix-Headset.wav"
    if source == "psai":
        return CACHE_DIR / "psai" / f"{source_id}.mp4"
    if source == "loc":
        return CACHE_DIR / "loc" / f"{source_id}.mp3"
    if source == "nasa":
        return CACHE_DIR / "nasa" / f"{source_id}.mp4"
    if source == "hpr":
        return CACHE_DIR / "hpr" / f"{source_id}.mp3"
    raise ValueError(f"Unknown source: {source}")


def _write_stream_json(
    seg_dir: Path,
    stream: str,
    prev_day: str | None,
    prev_segment: str | None,
    seq: int,
) -> None:
    """Write stream.json marker into segment directory."""
    marker = {
        "stream": stream,
        "prev_day": prev_day,
        "prev_segment": prev_segment,
        "seq": seq,
    }
    (seg_dir / "stream.json").write_text(json.dumps(marker) + "\n", encoding="utf-8")


def _write_stream_state(name: str, last_day: str, last_segment: str, seq: int) -> None:
    """Write stream state file to journal/streams/."""
    STREAMS_DIR.mkdir(parents=True, exist_ok=True)
    state = {
        "name": name,
        "type": "import",
        "host": None,
        "platform": None,
        "created_at": CREATED_AT,
        "last_day": last_day,
        "last_segment": last_segment,
        "seq": seq,
    }
    (STREAMS_DIR / f"{name}.json").write_text(
        json.dumps(state, indent=2) + "\n",
        encoding="utf-8",
    )


def _extract_ami_reference(cache_dir: Path) -> bool:
    """Extract AMI reference transcripts and speaker labels for ES2002a and ES2005a."""
    zip_path = cache_dir / "ami" / "ami_public_manual_1.6.2.zip"
    if not zip_path.exists():
        print("Warning: AMI annotations ZIP not found, skipping reference extraction")
        return False

    for meeting_id in ["ES2002a", "ES2005a"]:
        ref_dir = REFERENCE_DIR / "ami" / meeting_id
        ref_dir.mkdir(parents=True, exist_ok=True)

        transcript_lines: list[dict] = []
        speakers_data: dict[str, dict] = {}

        with zipfile.ZipFile(zip_path) as archive:
            for speaker in ["A", "B", "C", "D"]:
                xml_name = f"words/{meeting_id}.{speaker}.words.xml"
                try:
                    xml_bytes = archive.read(xml_name)
                except KeyError:
                    continue

                root = ET.fromstring(xml_bytes)
                words: list[dict] = []
                for word in root.iter("w"):
                    text = (word.text or "").strip()
                    start = word.get("starttime")
                    end = word.get("endtime")
                    if text and start and end:
                        words.append(
                            {
                                "word": text,
                                "start": float(start),
                                "end": float(end),
                                "speaker": speaker,
                            }
                        )

                speakers_data[speaker] = {
                    "label": f"Speaker {speaker}",
                    "word_count": len(words),
                }
                transcript_lines.extend(words)

        transcript_lines.sort(key=lambda word: word["start"])

        transcript_text = ""
        current_speaker: str | None = None
        current_line: list[str] = []
        for word in transcript_lines:
            if word["speaker"] != current_speaker:
                if current_line and current_speaker:
                    transcript_text += (
                        f"[Speaker {current_speaker}] {' '.join(current_line)}\n"
                    )
                current_speaker = word["speaker"]
                current_line = [word["word"]]
            else:
                current_line.append(word["word"])

        if current_line and current_speaker:
            transcript_text += f"[Speaker {current_speaker}] {' '.join(current_line)}\n"

        (ref_dir / "transcript.txt").write_text(transcript_text, encoding="utf-8")
        (ref_dir / "speakers.json").write_text(
            json.dumps(speakers_data, indent=2) + "\n",
            encoding="utf-8",
        )
    return True


_TEST_FACETS = [
    {
        "slug": "meetings",
        "title": "Meetings",
        "description": "Conversations, calls, and collaborative sessions",
        "emoji": "🗓",
        "color": "#4285f4",
    },
    {
        "slug": "computer-work",
        "title": "Computer Work",
        "description": "Software development, system administration, and technical tasks",
        "emoji": "💻",
        "color": "#34a853",
    },
    {
        "slug": "research",
        "title": "Research",
        "description": "Information gathering, reading, and learning",
        "emoji": "🔬",
        "color": "#ea4335",
    },
]


def _setup_facets() -> None:
    """Create test facets so dream agents exercise facet-dependent pipeline stages."""
    facets_dir = JOURNAL_DIR / "facets"
    for facet in _TEST_FACETS:
        facet_dir = facets_dir / facet["slug"]
        facet_dir.mkdir(parents=True, exist_ok=True)
        facet_data = {
            "title": facet["title"],
            "description": facet["description"],
            "emoji": facet["emoji"],
            "color": facet["color"],
        }
        (facet_dir / "facet.json").write_text(
            json.dumps(facet_data, indent=2) + "\n", encoding="utf-8"
        )
    print(
        f"Created {len(_TEST_FACETS)} test facets: {[f['slug'] for f in _TEST_FACETS]}"
    )


def _clean_generated() -> None:
    """Remove only generated journal content for the 5 simulated days."""
    for day in DAYS:
        day_dir = JOURNAL_DIR / day
        if day_dir.exists():
            shutil.rmtree(day_dir)

    for name in ["field.audio.json", "field.screen.json"]:
        path = STREAMS_DIR / name
        if path.exists():
            path.unlink()

    for meeting_id in ["ES2002a", "ES2005a"]:
        ref_dir = REFERENCE_DIR / "ami" / meeting_id
        if ref_dir.exists():
            shutil.rmtree(ref_dir)

    for facet in _TEST_FACETS:
        facet_dir = JOURNAL_DIR / "facets" / facet["slug"]
        if facet_dir.exists():
            shutil.rmtree(facet_dir)


def build() -> None:
    """Download, slice, and organize all sources into the journal."""
    download_all()
    _clean_generated()
    _setup_facets()

    segments = _collect_segments()
    stream_state: dict[str, dict[str, str | int | None]] = {}
    manifest_segments: list[dict] = []

    for seg in segments:
        stream = seg["stream"]
        day = seg["day"]
        time = seg["time"]
        duration = seg["duration_seconds"]
        segment_key = f"{time}_{duration}"

        seg_dir = JOURNAL_DIR / day / stream / segment_key
        seg_dir.mkdir(parents=True, exist_ok=True)

        src_path = _source_path(seg)
        slice_info = seg["slice"]
        start_s = slice_info["start_seconds"]
        duration_s = slice_info["duration_seconds"]

        if stream == "field.audio":
            _slice_audio(src_path, seg_dir / "audio.wav", start_s, duration_s)
        elif stream == "field.screen":
            _slice_screen(src_path, seg_dir / "screen.mp4", start_s, duration_s)
        else:
            raise ValueError(f"Unknown stream: {stream}")

        if stream not in stream_state:
            stream_state[stream] = {"prev_day": None, "prev_segment": None, "seq": 1}
        else:
            stream_state[stream]["seq"] = int(stream_state[stream]["seq"]) + 1

        state = stream_state[stream]
        _write_stream_json(
            seg_dir,
            stream,
            state["prev_day"],
            state["prev_segment"],
            int(state["seq"]),
        )

        state["prev_day"] = day
        state["prev_segment"] = segment_key

        manifest_segments.append(
            {
                "day": day,
                "stream": stream,
                "segment": segment_key,
                "source": seg["source"],
                "source_id": seg["source_id"],
                "license": seg["license"],
                "duration_seconds": duration,
                "description": seg["description"],
                "exercises": seg["exercises"],
                "has_reference": seg["has_reference"],
            }
        )

    for stream_name, state in stream_state.items():
        _write_stream_state(
            stream_name,
            str(state["prev_day"]),
            str(state["prev_segment"]),
            int(state["seq"]),
        )

    manifest = {
        "version": 1,
        "built": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "segments": manifest_segments,
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Manifest written: {len(manifest_segments)} segments")

    if _extract_ami_reference(CACHE_DIR):
        print("AMI reference data extracted")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "build"
    if command == "download":
        download_all()
    elif command == "build":
        build()
    elif command == "clean":
        _clean_generated()
    else:
        print(
            f"Unknown command: {command}. Valid: download, build, clean",
            file=sys.stderr,
        )
        sys.exit(1)
