# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
"""VoxConverse overlap-dense dev clip selection."""

from __future__ import annotations

import io
import json
import shutil
import subprocess
import wave
import zipfile
from array import array
from pathlib import Path
from typing import cast

from tools.sources import download_file

LICENSE = "CC-BY-4.0"
WAV_ZIP_URL = (
    "https://www.robots.ox.ac.uk/~vgg/data/voxconverse/data/voxconverse_dev_wav.zip"
)
WAV_ZIP_NAME = "voxconverse_dev_wav.zip"
RTTM_URL_TEMPLATE = (
    "https://raw.githubusercontent.com/joonson/voxconverse/master/dev/{clip}.rttm"
)
CLIPS = ["kdfqk", "vmaiq", "cjfer", "ldnro", "czlvt", "xxwgv"]
BACKUP_CLIPS = ["epdpg", "nrogz", "ufpel"]
ALL_CLIPS = CLIPS + BACKUP_CLIPS
SLICE_OFFSET_SECONDS = 0
DURATION_SECONDS = 180
SELECTION_FILE = "selection.json"
OFFSET_CANDIDATES = [SLICE_OFFSET_SECONDS, 60]
DEFAULT_CACHE_DIR = Path(__file__).resolve().parent.parent / ".cache" / "voxconverse"
SLOTS = [
    {"day": "20260201", "time": "075000", "clip": "kdfqk"},
    {"day": "20260202", "time": "075000", "clip": "vmaiq"},
    {"day": "20260203", "time": "075000", "clip": "cjfer"},
    {"day": "20260204", "time": "075000", "clip": "ldnro"},
    {"day": "20260205", "time": "075000", "clip": "czlvt"},
    {"day": "20260202", "time": "075500", "clip": "xxwgv"},
]


def _selection_path(cache_dir: Path) -> Path:
    return cache_dir / SELECTION_FILE


def _load_selection(cache_dir: Path | None = None) -> list[dict]:
    source_dir = cache_dir or DEFAULT_CACHE_DIR
    selection_path = _selection_path(source_dir)
    if selection_path.exists():
        loaded = json.loads(selection_path.read_text(encoding="utf-8"))
        if not isinstance(loaded, list):
            raise RuntimeError(
                f"Unexpected VoxConverse selection payload: {selection_path}"
            )
        return cast(list[dict], loaded)
    return [
        {
            "day": slot["day"],
            "time": slot["time"],
            "source_id": slot["clip"],
            "reference_offset_seconds": SLICE_OFFSET_SECONDS,
        }
        for slot in SLOTS
    ]


def _extract_audio(source_dir: Path, zip_path: Path) -> None:
    audio_dir = source_dir / "audio"
    audio_dir.mkdir(parents=True, exist_ok=True)
    if all((audio_dir / f"{clip}.wav").exists() for clip in ALL_CLIPS):
        return

    targets = {f"{clip}.wav": clip for clip in ALL_CLIPS}
    member_map: dict[str, str] = {}
    with zipfile.ZipFile(zip_path) as archive:
        for member_name in archive.namelist():
            if member_name.endswith("/"):
                continue
            base_name = Path(member_name).name
            if base_name not in targets:
                continue
            clip = targets[base_name]
            if clip in member_map:
                raise RuntimeError(f"Duplicate VoxConverse ZIP member for {clip}")
            member_map[clip] = member_name

        missing = sorted(set(ALL_CLIPS) - set(member_map))
        if missing:
            raise RuntimeError(f"Missing VoxConverse WAVs in ZIP: {missing}")

        for clip, member_name in member_map.items():
            dest = audio_dir / f"{clip}.wav"
            if dest.exists():
                continue
            with archive.open(member_name) as source, open(dest, "wb") as handle:
                shutil.copyfileobj(source, handle)


def _download_rttm(source_dir: Path) -> None:
    rttm_dir = source_dir / "rttm"
    rttm_dir.mkdir(parents=True, exist_ok=True)
    for clip in ALL_CLIPS:
        download_file(
            RTTM_URL_TEMPLATE.format(clip=clip),
            rttm_dir / f"{clip}.rttm",
        )


def _density_for_clip(cache_dir: Path, clip: str, offset_s: int) -> tuple[int, int]:
    rttm_path = cache_dir / "rttm" / f"{clip}.rttm"
    window_end = offset_s + DURATION_SECONDS
    speakers: set[str] = set()
    turns = 0

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
        turns += 1
        speakers.add(parts[7])

    return len(speakers), turns


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
                f"VoxConverse source {path} is too short for slice "
                f"{offset_s}-{offset_s + duration_s}s"
            )

        if sample_width == 2 and sample_rate == 16000 and channels == 1:
            handle.setpos(start_frame)
            frame_bytes = handle.readframes(frame_count)
            samples = array("h")
            samples.frombytes(frame_bytes)
            if len(samples) != 16000 * duration_s:
                raise RuntimeError(
                    f"Unexpected VoxConverse frame count for {path}: "
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
        raise RuntimeError(
            f"Unable to transcode VoxConverse audio for {path}: {stderr}"
        )

    with wave.open(io.BytesIO(process.stdout), "rb") as handle:
        if (
            handle.getnchannels() != 1
            or handle.getsampwidth() != 2
            or handle.getframerate() != 16000
        ):
            raise RuntimeError(
                f"Unexpected transcoded VoxConverse WAV params for {path}"
            )
        samples = array("h")
        samples.frombytes(handle.readframes(handle.getnframes()))
    if len(samples) != 16000 * duration_s:
        raise RuntimeError(
            f"Unexpected transcoded VoxConverse frame count for {path}: "
            f"{len(samples)} != {16000 * duration_s}"
        )
    return samples


def _slice_clip(wav_path: Path, offset_s: int, duration_s: int) -> bytes:
    samples = _read_wav_window(wav_path, offset_s, duration_s)
    wav_buffer = io.BytesIO()
    with wave.open(wav_buffer, "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(16000)
        handle.writeframes(samples.tobytes())
    return wav_buffer.getvalue()


def _resolve_selection(cache_dir: Path) -> list[dict]:
    selected_ids: set[str] = set()
    selected: list[dict] = []

    for slot in SLOTS:
        primary = str(slot["clip"])
        candidates = [primary] + [
            clip
            for clip in BACKUP_CLIPS
            if clip not in selected_ids and clip != primary
        ]
        chosen_clip: str | None = None
        chosen_offset: int | None = None

        for candidate in candidates:
            if candidate in selected_ids:
                continue
            for offset_s in OFFSET_CANDIDATES:
                speaker_count, turn_count = _density_for_clip(
                    cache_dir,
                    candidate,
                    offset_s,
                )
                print(
                    f"VoxConverse {candidate} @ {offset_s}s: "
                    f"{speaker_count} speakers, {turn_count} turns"
                )
                if speaker_count >= 2:
                    chosen_clip = candidate
                    chosen_offset = offset_s
                    break
            if chosen_clip is not None:
                break

        if chosen_clip is None or chosen_offset is None:
            raise RuntimeError(
                f"Unable to find multi-speaker VoxConverse slice for slot "
                f"{slot['day']} {slot['time']} (primary {primary})"
            )

        if chosen_clip != primary:
            print(f"VoxConverse fallback: {primary} -> {chosen_clip}")

        selected_ids.add(chosen_clip)
        selected.append(
            {
                "day": slot["day"],
                "time": slot["time"],
                "source_id": chosen_clip,
                "reference_offset_seconds": chosen_offset,
            }
        )

    return selected


def download(cache_dir: Path) -> Path:
    """Download VoxConverse assets and cache the selected 180-second clips."""
    source_dir = cache_dir / "voxconverse"
    source_dir.mkdir(parents=True, exist_ok=True)

    zip_path = download_file(WAV_ZIP_URL, source_dir / WAV_ZIP_NAME)
    _extract_audio(source_dir, zip_path)
    _download_rttm(source_dir)

    clip_dir = source_dir / "clips"
    clip_dir.mkdir(parents=True, exist_ok=True)

    selected = _resolve_selection(source_dir)
    for selection in selected:
        clip = str(selection["source_id"])
        clip_path = clip_dir / f"{clip}.wav"
        if clip_path.exists():
            continue
        clip_path.write_bytes(
            _slice_clip(
                source_dir / "audio" / f"{clip}.wav",
                int(selection["reference_offset_seconds"]),
                DURATION_SECONDS,
            )
        )

    _selection_path(source_dir).write_text(
        json.dumps(selected, indent=2) + "\n",
        encoding="utf-8",
    )
    return zip_path


def segments() -> list[dict]:
    """Return VoxConverse segment definitions."""
    return [
        {
            "day": str(selection["day"]),
            "stream": "field.audio",
            "time": str(selection["time"]),
            "duration_seconds": DURATION_SECONDS,
            "source": "voxconverse",
            "source_id": str(selection["source_id"]),
            "license": LICENSE,
            "description": f"VoxConverse dev clip {selection['source_id']}",
            "exercises": ["transcription", "diarization"],
            "has_reference": True,
            "slice": {"start_seconds": 0, "duration_seconds": DURATION_SECONDS},
            "reference_offset_seconds": int(selection["reference_offset_seconds"]),
        }
        for selection in _load_selection()
    ]
