# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
from __future__ import annotations

import subprocess
from pathlib import Path

import pyarrow.parquet as pq
from huggingface_hub import hf_hub_download

# Locked picks: S01 and S21 are the only sessions exposed by the
# argmaxinc/chime-6 Hugging Face mirror as of 2026-04.
SESSIONS = ["S01", "S21"]
HF_REPO = "argmaxinc/chime-6"
SHARDS = [
    "data/test-00000-of-00002.parquet",
    "data/test-00001-of-00002.parquet",
]


def _session_audio_name(session: str) -> str:
    return f"{session}_U02.CH1.wav"


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
        raise RuntimeError(f"Unable to transcode CHiME-6 audio bytes to WAV: {stderr}")
    return process.stdout


def _read_matching_audio_bytes(
    parquet_path: Path, target_names: set[str]
) -> dict[str, bytes]:
    table = pq.ParquetFile(parquet_path).read(columns=["audio"])
    flat = table.flatten()
    matches: dict[str, bytes] = {}
    for index, audio_path in enumerate(flat["audio.path"].to_pylist()):
        audio_name = Path(audio_path).name
        if audio_name not in target_names:
            continue
        if audio_name in matches:
            raise RuntimeError(
                f"Multiple CHiME-6 rows found for {audio_name} in {parquet_path}"
            )
        audio_bytes = flat["audio.bytes"][index].as_py()
        if not isinstance(audio_bytes, (bytes, bytearray)):
            raise RuntimeError(
                f"Unexpected CHiME-6 audio payload type for {audio_name}: "
                f"{type(audio_bytes).__name__}"
            )
        matches[audio_name] = bytes(audio_bytes)
    return matches


def download(cache_dir: Path) -> None:
    """Download CHiME-6 parquet shards and extract the locked session WAVs."""
    source_dir = cache_dir / "chime6"
    source_dir.mkdir(parents=True, exist_ok=True)

    for shard in SHARDS:
        local_shard = source_dir / shard
        if not local_shard.exists():
            hf_hub_download(
                repo_id=HF_REPO,
                repo_type="dataset",
                filename=shard,
                local_dir=source_dir,
            )

    if all((source_dir / f"{session}.wav").exists() for session in SESSIONS):
        return

    target_to_session = {_session_audio_name(session): session for session in SESSIONS}
    found_sessions: set[str] = set()
    for shard in SHARDS:
        shard_path = source_dir / shard
        matches = _read_matching_audio_bytes(shard_path, set(target_to_session))
        for target_name, audio_bytes in matches.items():
            session = target_to_session[target_name]
            if session in found_sessions:
                raise RuntimeError(f"Duplicate CHiME-6 row found for session {session}")
            wav_path = source_dir / f"{session}.wav"
            if not wav_path.exists():
                wav_path.write_bytes(_ensure_wav_bytes(audio_bytes))
            found_sessions.add(session)

    missing = [session for session in SESSIONS if session not in found_sessions]
    if missing:
        raise RuntimeError(f"Missing CHiME-6 sessions in parquet shards: {missing}")


def segments() -> list[dict]:
    """Return CHiME-6 segment definitions."""
    return [
        {
            "day": "20260201",
            "stream": "field.audio",
            "time": "080000",
            "duration_seconds": 600,
            "source": "chime6",
            "source_id": "S01",
            "license": "CC-BY-SA-4.0",
            "description": "4-speaker dinner party, far-field overlap",
            "exercises": ["transcription", "diarization", "entity_extraction"],
            "has_reference": True,
            "slice": {"start_seconds": 0, "duration_seconds": 600},
        },
        {
            "day": "20260202",
            "stream": "field.audio",
            "time": "080000",
            "duration_seconds": 600,
            "source": "chime6",
            "source_id": "S21",
            "license": "CC-BY-SA-4.0",
            "description": "4-speaker dinner party, far-field overlap",
            "exercises": ["transcription", "diarization", "entity_extraction"],
            "has_reference": True,
            "slice": {"start_seconds": 0, "duration_seconds": 600},
        },
    ]
