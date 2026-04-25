# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
from __future__ import annotations

import subprocess
from pathlib import Path

import pyarrow.parquet as pq
from huggingface_hub import hf_hub_download

from tools.sources import download_file

# Locked picks: Bmr005, Bmr006, and Bmr007 share six recurring participants
# (fe008, fe016, me011, me013, me018, mn005). Bmr001/002/003 were rejected
# because they share only two speakers.
MEETINGS = ["Bmr005", "Bmr006", "Bmr007"]
HF_REPO = "argmaxinc/icsi-meetings"
ANNOTATIONS_URL = (
    "https://groups.inf.ed.ac.uk/ami/ICSICorpusAnnotations/ICSI_core_NXT.zip"
)
SHARDS = [f"data/test-{index:05d}-of-00017.parquet" for index in range(17)]


def _meeting_audio_name(meeting: str) -> str:
    return f"{meeting}.interaction.wav"


def _ensure_wav_bytes(audio_bytes: bytes) -> bytes:
    """Return WAV bytes, transcoding from stdin if the payload is not WAV."""
    if audio_bytes[:4] == b"RIFF" and audio_bytes[8:12] == b"WAVE":
        return audio_bytes

    process = subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            "pipe:0",
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
        input=audio_bytes,
        capture_output=True,
        check=False,
    )
    if process.returncode != 0:
        stderr = process.stderr.decode("utf-8", errors="replace")
        raise RuntimeError(f"Unable to transcode ICSI audio bytes to WAV: {stderr}")
    return process.stdout


def _find_audio_names(parquet_path: Path, target_names: set[str]) -> list[str]:
    table = pq.ParquetFile(parquet_path).read(columns=["audio.path"])
    flat = table.flatten()
    matches = [Path(path).name for path in flat["audio.path"].to_pylist()]
    return [name for name in matches if name in target_names]


def _read_audio_bytes(parquet_path: Path, target_name: str) -> bytes:
    table = pq.ParquetFile(parquet_path).read(columns=["audio"])
    flat = table.flatten()
    matches = [
        index
        for index, audio_path in enumerate(flat["audio.path"].to_pylist())
        if Path(audio_path).name == target_name
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected one ICSI row for {target_name} in {parquet_path}, found {len(matches)}"
        )
    audio_bytes = flat["audio.bytes"][matches[0]].as_py()
    if not isinstance(audio_bytes, (bytes, bytearray)):
        raise RuntimeError(
            f"Unexpected ICSI audio payload type for {target_name}: "
            f"{type(audio_bytes).__name__}"
        )
    return bytes(audio_bytes)


def download(cache_dir: Path) -> None:
    """Download ICSI annotations and extract the locked meeting WAVs."""
    source_dir = cache_dir / "icsi"
    source_dir.mkdir(parents=True, exist_ok=True)
    download_file(ANNOTATIONS_URL, source_dir / "ICSI_core_NXT.zip")

    remaining = {
        meeting: _meeting_audio_name(meeting)
        for meeting in MEETINGS
        if not (source_dir / f"{meeting}.wav").exists()
    }
    if not remaining:
        return

    for shard in SHARDS:
        if not remaining:
            break
        shard_path = Path(
            hf_hub_download(repo_id=HF_REPO, repo_type="dataset", filename=shard)
        )
        matches = _find_audio_names(shard_path, set(remaining.values()))
        for meeting, target_name in list(remaining.items()):
            if target_name not in matches:
                continue
            wav_path = source_dir / f"{meeting}.wav"
            wav_path.write_bytes(
                _ensure_wav_bytes(_read_audio_bytes(shard_path, target_name))
            )
            del remaining[meeting]

    if remaining:
        raise RuntimeError(
            f"Missing ICSI meetings in parquet shards: {sorted(remaining)}"
        )


def segments() -> list[dict]:
    """Return ICSI segment definitions."""
    return [
        {
            "day": "20260203",
            "stream": "field.audio",
            "time": "080000",
            "duration_seconds": 600,
            "source": "icsi",
            "source_id": "Bmr005",
            "license": "CC-BY-4.0",
            "description": "lab meeting, recurring participants across sessions",
            "exercises": ["transcription", "diarization", "entity_extraction"],
            "has_reference": True,
            "slice": {"start_seconds": 0, "duration_seconds": 600},
        },
        {
            "day": "20260204",
            "stream": "field.audio",
            "time": "080000",
            "duration_seconds": 600,
            "source": "icsi",
            "source_id": "Bmr006",
            "license": "CC-BY-4.0",
            "description": "lab meeting, recurring participants across sessions",
            "exercises": ["transcription", "diarization", "entity_extraction"],
            "has_reference": True,
            "slice": {"start_seconds": 0, "duration_seconds": 600},
        },
        {
            "day": "20260205",
            "stream": "field.audio",
            "time": "080000",
            "duration_seconds": 600,
            "source": "icsi",
            "source_id": "Bmr007",
            "license": "CC-BY-4.0",
            "description": "lab meeting, recurring participants across sessions",
            "exercises": ["transcription", "diarization", "entity_extraction"],
            "has_reference": True,
            "slice": {"start_seconds": 0, "duration_seconds": 600},
        },
    ]
