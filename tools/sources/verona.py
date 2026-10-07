# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
"""verona: a synthetic working week written by sol pbc.

Unlike every other source, verona is authored rather than downloaded. Its
scripts live in ``tools/verona/story/`` and are rendered once into committed
media by ``tools/verona/render.py`` (stock TTS voices for audio, a headless
browser for screen). Nobody in it is a real person. Because the media is
already in place, ``build.py`` lists these segments without slicing them.
"""

from __future__ import annotations

from pathlib import Path

from tools.verona import story

SYNTHETIC = True


def download(cache_dir: Path) -> None:
    """Nothing to download: the rendered media is committed."""


def segments() -> list[dict]:
    result = []
    for item in story.load_index():
        kind = "audio" if item["stream"] == story.STREAMS["audio"] else "screen"
        result.append(
            {
                "day": item["day"],
                "stream": item["stream"],
                "time": item["time"],
                "duration_seconds": item["duration_seconds"],
                "source": "verona",
                "source_id": item["id"],
                "license": story.LICENSE,
                "description": f"verona (synthetic, sol pbc): {item['title']}",
                "exercises": (
                    [
                        "transcription",
                        "diarization",
                        "entity_extraction",
                        "facet_classification",
                    ]
                    if kind == "audio"
                    else ["screen_description", "entity_extraction"]
                ),
                "has_reference": kind == "audio",
                "slice": None,
            }
        )
    return result
