# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
"""DiPCo dinner-party close-talk mix selection."""

from __future__ import annotations

import io
import json
import re
import shutil
import subprocess
import tarfile
import wave
from array import array
from pathlib import Path
from typing import cast

from tools.sources import download_file

LICENSE = "CDLA-Permissive-1.0"
TARBALL_URL = "https://zenodo.org/api/records/8122551/files/DipCo.tgz/content"
TARBALL_NAME = "DipCo.tgz"
SESSIONS = ["S01", "S02", "S03", "S04", "S05", "S06"]
BACKUP_SESSIONS = ["S07", "S08", "S09"]
ALL_SESSIONS = SESSIONS + BACKUP_SESSIONS
SLICE_OFFSET_SECONDS = 600
DURATION_SECONDS = 180
PARTICIPANTS_PER_SESSION = 4
SELECTION_FILE = "selection.json"
EXTRACTED_MARKER = ".extracted"
OFFSET_CANDIDATES = [
    SLICE_OFFSET_SECONDS,
    SLICE_OFFSET_SECONDS + 120,
    SLICE_OFFSET_SECONDS + 240,
]
DEFAULT_CACHE_DIR = Path(__file__).resolve().parent.parent / ".cache" / "dipco"
SLOTS = [
    {"day": "20260201", "time": "074500", "session": "S01"},
    {"day": "20260202", "time": "074500", "session": "S02"},
    {"day": "20260203", "time": "074500", "session": "S03"},
    {"day": "20260204", "time": "074500", "session": "S04"},
    {"day": "20260205", "time": "074500", "session": "S05"},
    {"day": "20260201", "time": "075500", "session": "S06"},
]
_AUDIO_RE = re.compile(
    r"(?:^|.*/)[^/]+/audio/(?:dev|eval)/(?P<session>S\d{2})_(?P<participant>P\d{2})\.wav$"
)
_TRANSCRIPT_RE = re.compile(
    r"(?:^|.*/)[^/]+/transcriptions/(?:dev|eval)/(?P<session>S\d{2})\.json$"
)


def _selection_path(cache_dir: Path) -> Path:
    return cache_dir / SELECTION_FILE


def _load_selection(cache_dir: Path | None = None) -> list[dict]:
    source_dir = cache_dir or DEFAULT_CACHE_DIR
    selection_path = _selection_path(source_dir)
    if selection_path.exists():
        loaded = json.loads(selection_path.read_text(encoding="utf-8"))
        if not isinstance(loaded, list):
            raise RuntimeError(f"Unexpected DiPCo selection payload: {selection_path}")
        return cast(list[dict], loaded)
    return [
        {
            "day": slot["day"],
            "time": slot["time"],
            "source_id": slot["session"],
            "reference_offset_seconds": SLICE_OFFSET_SECONDS,
        }
        for slot in SLOTS
    ]


def _close_talk_paths(cache_dir: Path, session: str) -> list[Path]:
    paths = sorted((cache_dir / "audio").glob(f"{session}_P*.wav"))
    if len(paths) != PARTICIPANTS_PER_SESSION:
        raise FileNotFoundError(
            f"Expected {PARTICIPANTS_PER_SESSION} close-talk WAVs for {session}, "
            f"found {len(paths)} in {cache_dir / 'audio'}"
        )
    return paths


def _missing_assets(source_dir: Path, sessions: list[str]) -> list[str]:
    audio_dir = source_dir / "audio"
    transcripts_dir = source_dir / "transcriptions"
    missing: list[str] = []

    for session in sessions:
        for participant in range(1, PARTICIPANTS_PER_SESSION + 1):
            audio_path = audio_dir / f"{session}_P{participant:02d}.wav"
            if not audio_path.exists():
                missing.append(str(audio_path.relative_to(source_dir)))
        transcript_path = transcripts_dir / f"{session}.json"
        if not transcript_path.exists():
            missing.append(str(transcript_path.relative_to(source_dir)))

    return missing


def _assets_complete(source_dir: Path, sessions: list[str]) -> bool:
    return not _missing_assets(source_dir, sessions)


def _parse_timecode(value: str) -> float:
    hours, minutes, seconds = value.split(":")
    return (int(hours) * 3600) + (int(minutes) * 60) + float(seconds)


def _utterance_window(utterance: dict) -> tuple[float, float]:
    start_times = utterance.get("start_time")
    end_times = utterance.get("end_time")
    if not isinstance(start_times, dict) or not isinstance(end_times, dict):
        raise RuntimeError(f"Unexpected DiPCo utterance timing payload: {utterance}")

    reference = str(utterance.get("ref") or "close-talk")
    start_value = (
        start_times.get(reference)
        or start_times.get("close-talk")
        or next(iter(start_times.values()), None)
    )
    end_value = (
        end_times.get(reference)
        or end_times.get("close-talk")
        or next(iter(end_times.values()), None)
    )
    if not isinstance(start_value, str) or not isinstance(end_value, str):
        raise RuntimeError(f"Missing DiPCo utterance timing values: {utterance}")
    return _parse_timecode(start_value), _parse_timecode(end_value)


def _overlap_density(cache_dir: Path, session: str, offset_s: int) -> tuple[int, int]:
    transcript_path = cache_dir / "transcriptions" / f"{session}.json"
    utterances = json.loads(transcript_path.read_text(encoding="utf-8"))
    window_end = offset_s + DURATION_SECONDS
    speakers: set[str] = set()
    utterance_count = 0

    for utterance in utterances:
        start_s, end_s = _utterance_window(utterance)
        if end_s <= offset_s or start_s >= window_end:
            continue
        utterance_count += 1
        speakers.add(str(utterance["speaker_id"]))

    return len(speakers), utterance_count


def _read_wav_window(path: Path, offset_s: int, duration_s: int) -> array:
    with wave.open(str(path), "rb") as handle:
        channels = handle.getnchannels()
        sample_width = handle.getsampwidth()
        sample_rate = handle.getframerate()
        total_frames = handle.getnframes()

        start_frame = int(offset_s * sample_rate)
        frame_count = int(duration_s * sample_rate)
        if total_frames < start_frame + frame_count:
            raise RuntimeError(
                f"DiPCo source {path} is too short for slice "
                f"{offset_s}-{offset_s + duration_s}s"
            )

        if sample_width == 2 and sample_rate == 16000:
            handle.setpos(start_frame)
            frame_bytes = handle.readframes(frame_count)
            samples = array("h")
            samples.frombytes(frame_bytes)
            if channels > 1:
                samples = array("h", samples[::channels])
            if len(samples) != 16000 * duration_s:
                raise RuntimeError(
                    f"Unexpected DiPCo frame count for {path}: "
                    f"{len(samples)} != {16000 * duration_s}"
                )
            return samples

    process = subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-ss",
            str(offset_s),
            "-t",
            str(duration_s),
            "-i",
            str(path),
            "-vn",
            "-acodec",
            "pcm_s16le",
            "-ar",
            "16000",
            "-ac",
            "1",
            "-f",
            "wav",
            "pipe:1",
        ],
        capture_output=True,
        check=False,
    )
    if process.returncode != 0:
        stderr = process.stderr.decode("utf-8", errors="replace")
        raise RuntimeError(f"Unable to transcode DiPCo audio for {path}: {stderr}")

    with wave.open(io.BytesIO(process.stdout), "rb") as handle:
        if (
            handle.getnchannels() != 1
            or handle.getsampwidth() != 2
            or handle.getframerate() != 16000
        ):
            raise RuntimeError(f"Unexpected transcoded DiPCo WAV params for {path}")
        samples = array("h")
        samples.frombytes(handle.readframes(handle.getnframes()))
    if len(samples) != 16000 * duration_s:
        raise RuntimeError(
            f"Unexpected transcoded DiPCo frame count for {path}: "
            f"{len(samples)} != {16000 * duration_s}"
        )
    return samples


def _mix_and_slice(
    close_talk_paths: list[Path], offset_s: int, duration_s: int
) -> bytes:
    tracks = [_read_wav_window(path, offset_s, duration_s) for path in close_talk_paths]
    # Average the close-talk channels instead of hard summing to avoid clipping.
    mixed = array(
        "h",
        (
            max(-32768, min(32767, sum(samples) // len(tracks)))
            for samples in zip(*tracks, strict=True)
        ),
    )
    wav_buffer = io.BytesIO()
    with wave.open(wav_buffer, "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(16000)
        handle.writeframes(mixed.tobytes())
    return wav_buffer.getvalue()


def _extract_assets(source_dir: Path, tarball_path: Path) -> None:
    transcripts_dir = source_dir / "transcriptions"
    audio_dir = source_dir / "audio"
    extracted_marker = source_dir / EXTRACTED_MARKER
    transcripts_dir.mkdir(parents=True, exist_ok=True)
    audio_dir.mkdir(parents=True, exist_ok=True)

    if extracted_marker.exists() and _assets_complete(source_dir, ALL_SESSIONS):
        return

    with tarfile.open(tarball_path, "r:gz") as archive:
        for member in archive:
            if not member.isfile():
                continue

            audio_match = _AUDIO_RE.match(member.name)
            if audio_match and audio_match.group("session") in ALL_SESSIONS:
                dest = audio_dir / Path(member.name).name
            else:
                transcript_match = _TRANSCRIPT_RE.match(member.name)
                if (
                    transcript_match
                    and transcript_match.group("session") in ALL_SESSIONS
                ):
                    dest = transcripts_dir / Path(member.name).name
                else:
                    continue

            if dest.exists():
                continue

            extracted = archive.extractfile(member)
            if extracted is None:
                raise RuntimeError(
                    f"Unable to extract DiPCo archive member {member.name}"
                )
            with open(dest, "wb") as handle:
                shutil.copyfileobj(extracted, handle)

    missing_assets = _missing_assets(source_dir, ALL_SESSIONS)
    if missing_assets:
        raise RuntimeError(f"DiPCo extraction incomplete: missing {missing_assets}")
    extracted_marker.write_text("ok\n", encoding="utf-8")


def _resolve_selection(cache_dir: Path) -> list[dict]:
    selected_ids: set[str] = set()
    selected: list[dict] = []

    for slot in SLOTS:
        primary = str(slot["session"])
        candidates = [primary] + [
            session
            for session in BACKUP_SESSIONS
            if session not in selected_ids and session != primary
        ]
        chosen_session: str | None = None
        chosen_offset: int | None = None

        for candidate in candidates:
            if candidate in selected_ids:
                continue
            try:
                _close_talk_paths(cache_dir, candidate)
            except FileNotFoundError as exc:
                print(str(exc))
                continue
            for offset_s in OFFSET_CANDIDATES:
                speaker_count, utterance_count = _overlap_density(
                    cache_dir,
                    candidate,
                    offset_s,
                )
                print(
                    f"DiPCo {candidate} @ {offset_s}s: "
                    f"{speaker_count} speakers, {utterance_count} utterances"
                )
                if speaker_count >= 3 and utterance_count >= 5:
                    chosen_session = candidate
                    chosen_offset = offset_s
                    break
            if chosen_session is not None:
                break

        if chosen_session is None or chosen_offset is None:
            raise RuntimeError(
                f"Unable to find overlap-rich DiPCo slice for slot "
                f"{slot['day']} {slot['time']} (primary {primary})"
            )

        if chosen_session != primary:
            print(f"DiPCo fallback: {primary} -> {chosen_session}")

        selected_ids.add(chosen_session)
        selected.append(
            {
                "day": slot["day"],
                "time": slot["time"],
                "source_id": chosen_session,
                "reference_offset_seconds": chosen_offset,
            }
        )

    return selected


def download(cache_dir: Path) -> Path:
    """Download DiPCo assets and cache the selected 180-second clips."""
    source_dir = cache_dir / "dipco"
    source_dir.mkdir(parents=True, exist_ok=True)

    tarball_path = download_file(TARBALL_URL, source_dir / TARBALL_NAME)
    _extract_assets(source_dir, tarball_path)

    clip_dir = source_dir / "clips"
    clip_dir.mkdir(parents=True, exist_ok=True)

    selected = _resolve_selection(source_dir)
    for selection in selected:
        session = str(selection["source_id"])
        clip_path = clip_dir / f"{session}.wav"
        if clip_path.exists():
            continue
        clip_path.write_bytes(
            _mix_and_slice(
                _close_talk_paths(source_dir, session),
                int(selection["reference_offset_seconds"]),
                DURATION_SECONDS,
            )
        )

    _selection_path(source_dir).write_text(
        json.dumps(selected, indent=2) + "\n",
        encoding="utf-8",
    )
    return tarball_path


def segments() -> list[dict]:
    """Return DiPCo segment definitions."""
    return [
        {
            "day": str(selection["day"]),
            "stream": "field.audio",
            "time": str(selection["time"]),
            "duration_seconds": DURATION_SECONDS,
            "source": "dipco",
            "source_id": str(selection["source_id"]),
            "license": LICENSE,
            "description": (
                f"DiPCo dinner-party session {selection['source_id']} close-talk mix"
            ),
            "exercises": ["transcription", "diarization", "entity_extraction"],
            "has_reference": True,
            "slice": {"start_seconds": 0, "duration_seconds": DURATION_SECONDS},
            "reference_offset_seconds": int(selection["reference_offset_seconds"]),
        }
        for selection in _load_selection()
    ]
