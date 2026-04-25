# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools.sources import (
    ami,
    chime6,
    dipco,
    hpr,
    icsi,
    loc,
    nasa,
    psai,
    voices,
    voxconverse,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
JOURNAL_DIR = REPO_ROOT / "journal"
MANIFEST_PATH = REPO_ROOT / "manifest.json"
CACHE_DIR = Path(__file__).resolve().parent / ".cache"
STREAMS_DIR = JOURNAL_DIR / "streams"
REFERENCE_DIR = REPO_ROOT / "reference"
DAYS = ["20260201", "20260202", "20260203", "20260204", "20260205"]
CREATED_AT = 1769904000
SOURCES = [ami, psai, loc, nasa, hpr, chime6, icsi, voices, dipco, voxconverse]


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
    if source == "chime6":
        return CACHE_DIR / "chime6" / f"{source_id}.wav"
    if source == "icsi":
        return CACHE_DIR / "icsi" / f"{source_id}.wav"
    if source == "voices":
        return CACHE_DIR / "voices" / f"{source_id}.wav"
    if source == "dipco":
        return CACHE_DIR / "dipco" / "clips" / f"{source_id}.wav"
    if source == "voxconverse":
        return CACHE_DIR / "voxconverse" / "clips" / f"{source_id}.wav"
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


def _write_reference_files(ref_dir: Path, ordered_words: list[dict]) -> None:
    """Write transcript.txt and speakers.json from already ordered word records."""
    ref_dir.mkdir(parents=True, exist_ok=True)

    transcript_text = ""
    current_speaker: str | None = None
    current_line: list[str] = []
    speakers_data: dict[str, dict[str, str | int]] = {}
    for word in ordered_words:
        speaker = str(word["speaker"])
        token = str(word["word"]).strip()
        if not token:
            continue

        if speaker not in speakers_data:
            speakers_data[speaker] = {"label": f"Speaker {speaker}", "word_count": 0}
        speakers_data[speaker]["word_count"] = (
            int(speakers_data[speaker]["word_count"]) + 1
        )

        if speaker != current_speaker:
            if current_line and current_speaker:
                transcript_text += (
                    f"[Speaker {current_speaker}] {' '.join(current_line)}\n"
                )
            current_speaker = speaker
            current_line = [token]
        else:
            current_line.append(token)

    if current_line and current_speaker:
        transcript_text += f"[Speaker {current_speaker}] {' '.join(current_line)}\n"

    (ref_dir / "transcript.txt").write_text(transcript_text, encoding="utf-8")
    (ref_dir / "speakers.json").write_text(
        json.dumps(speakers_data, indent=2) + "\n",
        encoding="utf-8",
    )


def _extract_dipco_reference(cache_dir: Path) -> bool:
    """Extract DiPCo slice transcript.txt and speakers.json files."""
    segments = [segment for segment in dipco.segments() if segment["source"] == "dipco"]
    if not segments:
        return False

    for segment in segments:
        session = str(segment["source_id"])
        offset_s = int(segment["reference_offset_seconds"])
        window_end = offset_s + int(segment["duration_seconds"])
        transcript_path = cache_dir / "dipco" / "transcriptions" / f"{session}.json"
        utterances = json.loads(transcript_path.read_text(encoding="utf-8"))
        overlapping: list[tuple[float, int, dict]] = []
        for order, utterance in enumerate(utterances):
            start_s, end_s = dipco._utterance_window(utterance)
            if end_s <= offset_s or start_s >= window_end:
                continue
            overlapping.append((start_s, order, utterance))

        ordered_words: list[dict[str, str]] = []
        for _, _, utterance in sorted(overlapping):
            speaker = str(utterance["speaker_id"])
            words = str(utterance["words"]).split()
            ordered_words.extend({"speaker": speaker, "word": token} for token in words)

        _write_reference_files(REFERENCE_DIR / "dipco" / session, ordered_words)
    return True


def _read_chime6_reference_row(
    cache_dir: Path, session: str
) -> tuple[list[str], list[str]]:
    """Read the CHiME-6 transcript and speaker-token arrays for one locked session."""
    target_name = f"{session}_U02.CH1.wav"
    match_count = 0
    transcript: list[str] | None = None
    word_speakers: list[str] | None = None

    for shard in chime6.SHARDS:
        parquet_path = cache_dir / "chime6" / shard
        if not parquet_path.exists():
            raise FileNotFoundError(f"Missing CHiME-6 parquet shard: {parquet_path}")

        table = pq.ParquetFile(parquet_path).read(
            columns=["audio", "transcript", "word_speakers"]
        )
        flat = table.flatten()
        for index, audio_path in enumerate(flat["audio.path"].to_pylist()):
            if Path(audio_path).name != target_name:
                continue
            match_count += 1
            if match_count > 1:
                raise RuntimeError(f"Multiple CHiME-6 rows found for {session}")
            transcript = table["transcript"][index].as_py()
            word_speakers = table["word_speakers"][index].as_py()

    if match_count != 1 or transcript is None or word_speakers is None:
        raise RuntimeError(f"Unable to locate CHiME-6 reference row for {session}")
    if len(transcript) != len(word_speakers):
        raise RuntimeError(
            f"CHiME-6 transcript/speaker length mismatch for {session}: "
            f"{len(transcript)} != {len(word_speakers)}"
        )
    return transcript, word_speakers


def _extract_chime6_reference(cache_dir: Path) -> bool:
    """Extract CHiME-6 whole-session transcript.txt and speakers.json files."""
    for session in chime6.SESSIONS:
        transcript, word_speakers = _read_chime6_reference_row(cache_dir, session)
        ordered_words = [
            {"speaker": speaker_id, "word": token}
            for token, speaker_id in zip(transcript, word_speakers, strict=True)
            if token and speaker_id
        ]
        _write_reference_files(REFERENCE_DIR / "chime6" / session, ordered_words)
    return True


def _nite_attr(element: ET.Element, attr_name: str) -> str | None:
    """Return namespaced NITE attributes by local name."""
    for key, value in element.attrib.items():
        if (
            key == attr_name
            or key.endswith(f"}}{attr_name}")
            or key.endswith(f":{attr_name}")
        ):
            return value
    return None


def _parse_float(value: str | None) -> float | None:
    if value in {None, ""}:
        return None
    return float(value)


def _parse_icsi_href(href: str) -> tuple[str, str]:
    """Return the start/end IDs from a NITE href range."""
    match = re.search(r"#id\(([^)]+)\)(?:\.\.id\(([^)]+)\))?$", href)
    if not match:
        raise RuntimeError(f"Unexpected ICSI href format: {href}")
    start_id = match.group(1)
    end_id = match.group(2) or start_id
    return start_id, end_id


def _parse_icsi_words_document(
    xml_bytes: bytes,
) -> tuple[list[dict[str, str | float | None]], dict[str, int]]:
    """Parse an ICSI Words XML document into ordered element records plus an ID index."""
    root = ET.fromstring(xml_bytes)
    ordered_entries: list[dict[str, str | float | None]] = []
    index_by_id: dict[str, int] = {}

    for element in list(root):
        element_id = _nite_attr(element, "id")
        if not element_id:
            continue
        ordered_entries.append(
            {
                "id": element_id,
                "tag": element.tag.rsplit("}", 1)[-1],
                "word": (element.text or "").strip(),
                "start": _parse_float(element.get("starttime")),
                "end": _parse_float(element.get("endtime")),
            }
        )
        index_by_id[element_id] = len(ordered_entries) - 1

    return ordered_entries, index_by_id


def _extract_icsi_meeting_words(archive: zipfile.ZipFile, meeting: str) -> list[dict]:
    """Extract ordered words from ICSI NXT XML.

    Words come from `ICSI/Words/<meeting>.<agent>.words.xml`. Speaker identity comes
    from the paired `ICSI/Segments/<meeting>.<agent>.segs.xml`, where each
    `<segment ... participant="me011">` contains a NITE child href range like
    `Bmr005.A.words.xml#id(Bmr005.w.95)..id(Bmr005.w.100)`. Segment ranges can end
    on comment IDs, so the parser walks the ordered Words XML element stream and
    includes only `<w>` elements that fall inside each referenced range.
    """
    word_names = sorted(
        name
        for name in archive.namelist()
        if name.startswith(f"ICSI/Words/{meeting}.") and name.endswith(".words.xml")
    )
    if not word_names:
        raise RuntimeError(f"No ICSI word XML files found for {meeting}")

    meeting_words: list[dict] = []
    for word_name in word_names:
        agent = Path(word_name).name.split(".")[1]
        segment_name = f"ICSI/Segments/{meeting}.{agent}.segs.xml"
        try:
            words_xml = archive.read(word_name)
            segments_xml = archive.read(segment_name)
        except KeyError as exc:
            raise RuntimeError(
                f"Missing paired ICSI XML for {meeting}.{agent}"
            ) from exc

        ordered_entries, index_by_id = _parse_icsi_words_document(words_xml)
        segments_root = ET.fromstring(segments_xml)
        seen_word_ids: set[str] = set()

        for segment in segments_root.iter():
            if segment.tag.rsplit("}", 1)[-1] != "segment":
                continue

            participant = segment.get("participant")
            segment_start = _parse_float(segment.get("starttime"))
            segment_end = _parse_float(segment.get("endtime"))
            if participant is None or segment_start is None or segment_end is None:
                raise RuntimeError(
                    f"Incomplete ICSI segment metadata in {segment_name}"
                )

            href: str | None = None
            for child in list(segment):
                if child.tag.rsplit("}", 1)[-1] == "child":
                    href = child.get("href")
                    break
            if href is None:
                raise RuntimeError(f"Missing NITE child href in {segment_name}")

            start_id, end_id = _parse_icsi_href(href)
            if start_id not in index_by_id or end_id not in index_by_id:
                raise RuntimeError(
                    f"ICSI href IDs not found in ordered Words XML: {href}"
                )

            start_index = index_by_id[start_id]
            end_index = index_by_id[end_id]
            if start_index > end_index:
                start_index, end_index = end_index, start_index

            for entry in ordered_entries[start_index : end_index + 1]:
                if entry["tag"] != "w":
                    continue
                word_id = str(entry["id"])
                if word_id in seen_word_ids:
                    continue
                token = str(entry["word"]).strip()
                if not token:
                    continue
                meeting_words.append(
                    {
                        "speaker": participant,
                        "word": token,
                        "start": (
                            float(entry["start"])
                            if entry["start"] is not None
                            else segment_start
                        ),
                    }
                )
                seen_word_ids.add(word_id)

    for order, word in enumerate(meeting_words):
        word["order"] = order
    meeting_words.sort(key=lambda word: (float(word["start"]), int(word["order"])))
    return meeting_words


def _extract_icsi_reference(cache_dir: Path) -> bool:
    """Extract ICSI whole-meeting transcript.txt and speakers.json files."""
    zip_path = cache_dir / "icsi" / "ICSI_core_NXT.zip"
    if not zip_path.exists():
        raise FileNotFoundError(f"Missing ICSI annotations ZIP: {zip_path}")

    with zipfile.ZipFile(zip_path) as archive:
        for meeting in icsi.MEETINGS:
            ordered_words = _extract_icsi_meeting_words(archive, meeting)
            _write_reference_files(REFERENCE_DIR / "icsi" / meeting, ordered_words)
    return True


def _extract_voxconverse_reference(cache_dir: Path) -> bool:
    """Extract VoxConverse slice speakers.json files."""
    segments = [
        segment
        for segment in voxconverse.segments()
        if segment["source"] == "voxconverse"
    ]
    if not segments:
        return False

    for segment in segments:
        clip = str(segment["source_id"])
        offset_s = int(segment["reference_offset_seconds"])
        window_end = offset_s + int(segment["duration_seconds"])
        rttm_path = cache_dir / "voxconverse" / "rttm" / f"{clip}.rttm"
        speaker_counts: dict[str, int] = {}

        for line in rttm_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            parts = line.split()
            if len(parts) < 8 or parts[0] != "SPEAKER":
                raise RuntimeError(f"Unexpected VoxConverse RTTM line: {line}")
            start_s = float(parts[3])
            end_s = start_s + float(parts[4])
            if end_s <= offset_s or start_s >= window_end:
                continue
            speaker = parts[7]
            speaker_counts[speaker] = speaker_counts.get(speaker, 0) + 1

        ref_dir = REFERENCE_DIR / "voxconverse" / clip
        ref_dir.mkdir(parents=True, exist_ok=True)
        speakers_data = {
            speaker: {"label": f"Speaker {speaker}", "word_count": count}
            for speaker, count in sorted(speaker_counts.items())
        }
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
    for segment in _collect_segments():
        segment_key = f"{segment['time']}_{segment['duration_seconds']}"
        seg_dir = JOURNAL_DIR / segment["day"] / segment["stream"] / segment_key
        for filename in ["audio.wav", "screen.mp4", "stream.json"]:
            path = seg_dir / filename
            if path.exists():
                path.unlink()

    for name in ["field.audio.json", "field.screen.json"]:
        path = STREAMS_DIR / name
        if path.exists():
            path.unlink()

    for meeting_id in ["ES2002a", "ES2005a"]:
        ref_dir = REFERENCE_DIR / "ami" / meeting_id
        if ref_dir.exists():
            shutil.rmtree(ref_dir)

    for session_id in ["S01", "S21"]:
        ref_dir = REFERENCE_DIR / "chime6" / session_id
        if ref_dir.exists():
            shutil.rmtree(ref_dir)

    for meeting_id in ["Bmr005", "Bmr006", "Bmr007"]:
        ref_dir = REFERENCE_DIR / "icsi" / meeting_id
        if ref_dir.exists():
            shutil.rmtree(ref_dir)

    for session_id in dipco.ALL_SESSIONS:
        ref_dir = REFERENCE_DIR / "dipco" / session_id
        if ref_dir.exists():
            shutil.rmtree(ref_dir)

    for clip_id in voxconverse.ALL_CLIPS:
        ref_dir = REFERENCE_DIR / "voxconverse" / clip_id
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
    if _extract_chime6_reference(CACHE_DIR):
        print("CHiME-6 reference data extracted")
    if _extract_icsi_reference(CACHE_DIR):
        print("ICSI reference data extracted")
    if _extract_dipco_reference(CACHE_DIR):
        print("DiPCo reference data extracted")
    if _extract_voxconverse_reference(CACHE_DIR):
        print("VoxConverse reference data extracted")


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
