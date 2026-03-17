# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
from __future__ import annotations

import os
from pathlib import Path

import requests  # type: ignore[import-untyped]


def download_file(
    url: str,
    dest: Path,
    *,
    allow_redirects: bool = True,
) -> Path:
    """Download url to dest. Skip if dest already exists. Atomic write."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        return dest

    tmp_path = dest.with_suffix(dest.suffix + ".tmp")
    with requests.get(
        url,
        stream=True,
        allow_redirects=allow_redirects,
        timeout=300,
    ) as response:
        response.raise_for_status()
        with open(tmp_path, "wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)

    os.rename(tmp_path, dest)
    return dest
