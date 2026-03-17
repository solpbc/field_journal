# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
from __future__ import annotations

from pathlib import Path

from tools.sources import download_file

BASE_URL = "https://hub.hackerpublicradio.org/ccdn.php"

EPISODES = {
    "hpr4297": {
        "day": "20260201",
        "time": "133000",
        "duration_seconds": 300,
        "description": "Solo tech commentary",
    },
    "hpr4591": {
        "day": "20260201",
        "time": "143000",
        "duration_seconds": 480,
        "description": "Solo tech commentary - version control basics",
    },
    "hpr4593": {
        "day": "20260202",
        "time": "111500",
        "duration_seconds": 360,
        "description": "Solo tech commentary - nuclear reactor technology",
    },
    "hpr4596": {
        "day": "20260204",
        "time": "140000",
        "duration_seconds": 300,
        "description": "Solo tech commentary - audio production",
    },
    "hpr4597": {
        "day": "20260205",
        "time": "110000",
        "duration_seconds": 360,
        "description": "Solo tech commentary - UNIX command exploration",
    },
}


def download(cache_dir: Path) -> None:
    """Download HPR episodes to cache_dir/hpr/."""
    source_dir = cache_dir / "hpr"
    for episode_id in EPISODES:
        url = f"{BASE_URL}?filename=/eps/{episode_id}/{episode_id}.mp3"
        download_file(url, source_dir / f"{episode_id}.mp3")


def segments() -> list[dict]:
    """Return HPR segment definitions."""
    items: list[dict] = []
    for episode_id, metadata in sorted(
        EPISODES.items(),
        key=lambda item: (item[1]["day"], item[1]["time"]),
    ):
        items.append(
            {
                "day": metadata["day"],
                "stream": "field.audio",
                "time": metadata["time"],
                "duration_seconds": metadata["duration_seconds"],
                "source": "hpr",
                "source_id": episode_id,
                "license": "CC-BY-SA-4.0",
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
