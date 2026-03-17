# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
"""Verify manifest.json matches actual files on disk."""

from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
JOURNAL_DIR = REPO_ROOT / "journal"
MANIFEST_PATH = REPO_ROOT / "manifest.json"


def test_manifest_is_valid_json() -> None:
    """manifest.json exists and is valid JSON."""
    assert MANIFEST_PATH.exists(), "manifest.json not found"
    data = json.loads(MANIFEST_PATH.read_text())
    assert isinstance(data, list), "manifest.json should be a JSON array"


def test_manifest_entries_have_required_fields() -> None:
    """Each manifest entry has the required metadata fields."""
    data = json.loads(MANIFEST_PATH.read_text())
    required = {"path", "source", "license", "duration_s"}
    for entry in data:
        missing = required - set(entry.keys())
        assert not missing, f"Entry missing fields {missing}: {entry}"


def test_manifest_paths_exist_on_disk() -> None:
    """Every path in the manifest points to a real file."""
    data = json.loads(MANIFEST_PATH.read_text())
    for entry in data:
        p = REPO_ROOT / entry["path"]
        assert p.exists(), f"Manifest references missing file: {entry['path']}"
