# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
from __future__ import annotations

from pathlib import Path

from tools.sources import download_file

MEETINGS = ["ES2002a", "ES2004a", "ES2005a", "ES2008a", "ES2014a"]
BASE_URL = "https://groups.inf.ed.ac.uk/ami/AMICorpusMirror/amicorpus"
ANNOTATIONS_URL = (
    "https://groups.inf.ed.ac.uk/ami/AMICorpusAnnotations/ami_public_manual_1.6.2.zip"
)


def download(cache_dir: Path) -> None:
    """Download AMI mix-headset WAVs and annotation ZIP to cache_dir/ami/."""
    source_dir = cache_dir / "ami"
    for meeting in MEETINGS:
        url = f"{BASE_URL}/{meeting}/audio/{meeting}.Mix-Headset.wav"
        download_file(url, source_dir / f"{meeting}.Mix-Headset.wav")

    download_file(ANNOTATIONS_URL, source_dir / "ami_public_manual_1.6.2.zip")


def segments() -> list[dict]:
    """Return AMI segment definitions."""
    return [
        {
            "day": "20260201",
            "stream": "field.audio",
            "time": "091500",
            "duration_seconds": 420,
            "source": "ami",
            "source_id": "ES2002a",
            "license": "CC-BY-4.0",
            "description": "4-speaker design team meeting, scenario-driven",
            "exercises": ["transcription", "diarization", "entity_extraction"],
            "has_reference": True,
            "slice": {"start_seconds": 0, "duration_seconds": 420},
        },
        {
            "day": "20260201",
            "stream": "field.audio",
            "time": "100500",
            "duration_seconds": 360,
            "source": "ami",
            "source_id": "ES2004a",
            "license": "CC-BY-4.0",
            "description": "4-speaker design team meeting, scenario-driven",
            "exercises": ["transcription", "diarization", "entity_extraction"],
            "has_reference": False,
            "slice": {"start_seconds": 0, "duration_seconds": 360},
        },
        {
            "day": "20260202",
            "stream": "field.audio",
            "time": "090000",
            "duration_seconds": 480,
            "source": "ami",
            "source_id": "ES2005a",
            "license": "CC-BY-4.0",
            "description": "4-speaker design team meeting, scenario-driven",
            "exercises": ["transcription", "diarization", "entity_extraction"],
            "has_reference": True,
            "slice": {"start_seconds": 0, "duration_seconds": 480},
        },
        {
            "day": "20260203",
            "stream": "field.audio",
            "time": "091000",
            "duration_seconds": 540,
            "source": "ami",
            "source_id": "ES2002a",
            "license": "CC-BY-4.0",
            "description": "4-speaker design team meeting, continued session",
            "exercises": ["transcription", "diarization"],
            "has_reference": True,
            "slice": {"start_seconds": 420, "duration_seconds": 540},
        },
        {
            "day": "20260204",
            "stream": "field.audio",
            "time": "090000",
            "duration_seconds": 600,
            "source": "ami",
            "source_id": "ES2008a",
            "license": "CC-BY-4.0",
            "description": "4-speaker design team meeting, scenario-driven",
            "exercises": ["transcription", "diarization", "entity_extraction"],
            "has_reference": False,
            "slice": {"start_seconds": 0, "duration_seconds": 600},
        },
        {
            "day": "20260205",
            "stream": "field.audio",
            "time": "091500",
            "duration_seconds": 480,
            "source": "ami",
            "source_id": "ES2014a",
            "license": "CC-BY-4.0",
            "description": "4-speaker design team meeting, scenario-driven",
            "exercises": ["transcription", "diarization", "entity_extraction"],
            "has_reference": False,
            "slice": {"start_seconds": 0, "duration_seconds": 480},
        },
    ]
