# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
"""Build the field journal from upstream sources.

Orchestrates: download → slice → organize into journal/ structure.
Updates manifest.json with per-segment metadata.
"""

from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
JOURNAL_DIR = REPO_ROOT / "journal"
MANIFEST_PATH = REPO_ROOT / "manifest.json"


def build() -> None:
    """Download, slice, and organize all sources into the journal."""
    segments: list[dict] = []

    # TODO: implement per-source download and slicing
    # from tools.sources import ami, psai, loc, nasa, hpr

    MANIFEST_PATH.write_text(json.dumps(segments, indent=2) + "\n")
    print(f"Manifest written: {len(segments)} segments")


if __name__ == "__main__":
    build()
