# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
from __future__ import annotations

from pathlib import Path

from tools.sources import download_file

BASE_URL = "https://tile.loc.gov/storage-services/media/afc/cal"

RECORDINGS = {
    "afc1986022_sr58a04": {
        "day": "20260203",
        "time": "133000",
        "duration_seconds": 360,
        "description": "Sociolinguistic interview, Washington DC dialect",
    },
    "afc1986022_sr58a03": {
        "day": "20260203",
        "time": "150000",
        "duration_seconds": 420,
        "description": "Sociolinguistic interview, Washington DC dialect",
    },
    "afc1986022_sr13b01": {
        "day": "20260204",
        "time": "110000",
        "duration_seconds": 420,
        "description": "American English dialect field recording",
    },
}


def download(cache_dir: Path) -> None:
    """Download LOC recordings to cache_dir/loc/."""
    source_dir = cache_dir / "loc"
    for recording_id in RECORDINGS:
        url = f"{BASE_URL}/{recording_id}.mp3"
        download_file(url, source_dir / f"{recording_id}.mp3")


def segments() -> list[dict]:
    """Return LOC segment definitions."""
    items: list[dict] = []
    for recording_id, metadata in sorted(
        RECORDINGS.items(),
        key=lambda item: (item[1]["day"], item[1]["time"]),
    ):
        items.append(
            {
                "day": metadata["day"],
                "stream": "field.audio",
                "time": metadata["time"],
                "duration_seconds": metadata["duration_seconds"],
                "source": "loc",
                "source_id": recording_id,
                "license": "Unrestricted",
                "description": metadata["description"],
                "exercises": ["transcription"],
                "has_reference": False,
                "slice": {
                    "start_seconds": 0,
                    "duration_seconds": metadata["duration_seconds"],
                },
            }
        )
    return items
