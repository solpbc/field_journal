# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
from __future__ import annotations

import json
from pathlib import Path

import requests  # type: ignore[import-untyped]

from tools.sources import download_file

VIDEOS = {
    "20230830_OSIRIS_Briefing_hbr": {
        "day": "20260202",
        "time": "140000",
        "duration_seconds": 420,
        "description": "OSIRIS-REx sample return press conference, multi-speaker briefing",
    },
    "KSC-20240624-VP-CDC01-0001-GOES_U_Prelaunch_News_Conference_720p-M7205": {
        "day": "20260203",
        "time": "103000",
        "duration_seconds": 480,
        "description": "GOES-U prelaunch news conference, multi-speaker briefing",
    },
    "KSC-19690715-MH-NAS01-0001-Historical_Video_Apollo_11_T-2_Day_Press_Conference-B_1493": {
        "day": "20260205",
        "time": "141500",
        "duration_seconds": 540,
        "description": "Apollo 11 prelaunch press conference, historical briefing",
    },
}


def _collection_url(nasa_id: str) -> str:
    return f"https://images-assets.nasa.gov/video/{nasa_id}/collection.json"


def _select_asset(assets: list[str]) -> str:
    for suffix in ("~mobile.mp4", "~medium.mp4"):
        for url in assets:
            if url.endswith(suffix):
                return url.replace("http://", "https://", 1)
    raise ValueError("No downloadable NASA MP4 asset found")


def download(cache_dir: Path) -> None:
    """Download NASA briefing videos to cache_dir/nasa/."""
    source_dir = cache_dir / "nasa"
    source_dir.mkdir(parents=True, exist_ok=True)

    for nasa_id in VIDEOS:
        json_path = source_dir / f"{nasa_id}.collection.json"
        if json_path.exists():
            assets = json.loads(json_path.read_text(encoding="utf-8"))
        else:
            response = requests.get(_collection_url(nasa_id), timeout=300)
            response.raise_for_status()
            assets = response.json()
            json_path.write_text(json.dumps(assets, indent=2) + "\n", encoding="utf-8")

        asset_url = _select_asset(assets)
        download_file(asset_url, source_dir / f"{nasa_id}.mp4")


def segments() -> list[dict]:
    """Return NASA segment definitions."""
    items: list[dict] = []
    for nasa_id, metadata in sorted(
        VIDEOS.items(),
        key=lambda item: (item[1]["day"], item[1]["time"]),
    ):
        items.append(
            {
                "day": metadata["day"],
                "stream": "field.audio",
                "time": metadata["time"],
                "duration_seconds": metadata["duration_seconds"],
                "source": "nasa",
                "source_id": nasa_id,
                "license": "Public domain",
                "description": metadata["description"],
                "exercises": ["transcription", "diarization", "entity_extraction"],
                "has_reference": False,
                "slice": {
                    "start_seconds": 0,
                    "duration_seconds": metadata["duration_seconds"],
                },
            }
        )
    return items
