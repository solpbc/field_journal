# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
from __future__ import annotations

from pathlib import Path

from tools.sources import download_file

HF_RESOLVE = (
    "https://huggingface.co/datasets/anaisleila/computer-use-data-psai/"
    "resolve/main/videos"
)

VIDEOS = {
    "cmcc8u6yc00va1p1ydsdu52zy": {
        "day": "20260201",
        "time": "094500",
        "duration_seconds": 300,
        "description": "Web browsing - privacy policy research",
    },
    "cmbcvas3300m3yl0xx41f307d": {
        "day": "20260201",
        "time": "140000",
        "duration_seconds": 360,
        "description": "E-commerce - product search and filtering",
    },
    "cm9u1c073001c150wyn2p48t5": {
        "day": "20260202",
        "time": "093000",
        "duration_seconds": 300,
        "description": "Document editing - structured AI report",
    },
    "cmb02icyd0083yc0wldyf5t1p": {
        "day": "20260202",
        "time": "113000",
        "duration_seconds": 240,
        "description": "Web utilities - hyperlink extraction",
    },
    "cmb02icyd007zyc0wzjayhdbj": {
        "day": "20260202",
        "time": "150000",
        "duration_seconds": 300,
        "description": "Developer tools - mobile device emulation",
    },
    "cma09228f0096wu0vzkr7ikpw": {
        "day": "20260203",
        "time": "100000",
        "duration_seconds": 360,
        "description": "Desktop task - general computer use",
    },
    "cma092ib300ewwu0v49ewbkpu": {
        "day": "20260204",
        "time": "091500",
        "duration_seconds": 300,
        "description": "Browser settings - ad blocking configuration",
    },
    "cmcc8u6y900rc1p1ya3utqt74": {
        "day": "20260204",
        "time": "113000",
        "duration_seconds": 240,
        "description": "Web research - encyclopedia navigation",
    },
    "cmbcvas3p01gjyl0x7w09l7nt": {
        "day": "20260204",
        "time": "143000",
        "duration_seconds": 360,
        "description": "Scientific computing - thermal analysis",
    },
    "cmcc8u6yc00vm1p1yhjl1u0bf": {
        "day": "20260205",
        "time": "093000",
        "duration_seconds": 300,
        "description": "Web browsing - career page exploration",
    },
    "cmcc8u6y900ra1p1yxavqqaho": {
        "day": "20260205",
        "time": "144500",
        "duration_seconds": 360,
        "description": "Web research - art history exploration",
    },
}


def download(cache_dir: Path) -> None:
    """Download PSAI screen recordings to cache_dir/psai/."""
    source_dir = cache_dir / "psai"
    for video_id in VIDEOS:
        url = f"{HF_RESOLVE}/{video_id}.mp4?download=true"
        download_file(url, source_dir / f"{video_id}.mp4")


def segments() -> list[dict]:
    """Return PSAI segment definitions."""
    items: list[dict] = []
    sorted_ids = sorted(
        VIDEOS, key=lambda video_id: (VIDEOS[video_id]["day"], VIDEOS[video_id]["time"])
    )
    for video_id in sorted_ids:
        metadata = VIDEOS[video_id]
        items.append(
            {
                "day": metadata["day"],
                "stream": "field.screen",
                "time": metadata["time"],
                "duration_seconds": metadata["duration_seconds"],
                "source": "psai",
                "source_id": video_id,
                "license": "MIT",
                "description": metadata["description"],
                "exercises": ["screen_capture"],
                "has_reference": False,
                "slice": {
                    "start_seconds": 0,
                    "duration_seconds": metadata["duration_seconds"],
                },
            }
        )
    return items
