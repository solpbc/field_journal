# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
"""Render the verona week from its scripts into journal media.

The verona week is authored, not downloaded: every word is written for the
corpus in ``story/``. This renders it once into committed media:

- audio entries: each line is spoken by the speaker's stock text-to-speech
  voice (Gemini TTS; nobody's voice is cloned), joined with short pauses,
  written as 16 kHz mono ``audio.wav``. The exact line timings are written to
  ``reference/verona/<id>/transcript.jsonl`` as ground truth.
- screen entries: the scripted page is typed out in headless Chromium and
  recorded to ``screen.mp4``.

Rendering needs ``GOOGLE_API_KEY`` (TTS) and the ``demo`` dependency group
(Playwright). Re-run only when a script changes; ``tools/build.py`` organizes
the committed result without calling any service.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tools.verona import story  # noqa: E402
from tools.verona.screens import render_screen  # noqa: E402

log = logging.getLogger("verona.render")

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
JOURNAL_DIR = REPO_ROOT / "journal"
REFERENCE_DIR = REPO_ROOT / "reference" / "verona"
CACHE_DIR = Path(__file__).resolve().parent / ".cache"
TTS_MODEL = "gemini-2.5-flash-preview-tts"
TTS_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    f"{TTS_MODEL}:generateContent"
)
TTS_RATE = 24000
OUT_RATE = 16000
LEAD_IN_S = 0.6
TAIL_S = 0.8


def _gap_seconds(text: str, same_speaker: bool) -> float:
    """A deterministic pause between lines, shorter within one speaker."""
    jitter = int(hashlib.sha256(text.encode()).hexdigest()[:4], 16) / 0xFFFF
    return (0.25 if same_speaker else 0.45) + 0.4 * jitter


def _tts(text: str, voice: str, api_key: str) -> bytes:
    """Return raw 24 kHz s16le PCM for one line, cached by content."""
    digest = hashlib.sha256(f"{TTS_MODEL}|{voice}|{text}".encode()).hexdigest()
    cached = CACHE_DIR / "tts" / f"{digest}.pcm"
    if cached.exists():
        return cached.read_bytes()
    body = json.dumps(
        {
            "contents": [
                {
                    "parts": [
                        {
                            "text": "Read this line naturally, as one person "
                            "speaking in an ordinary work conversation: " + text
                        }
                    ]
                }
            ],
            "generationConfig": {
                "responseModalities": ["AUDIO"],
                "speechConfig": {
                    "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": voice}}
                },
            },
        }
    ).encode()
    last_error: Exception | None = None
    for attempt in range(5):
        request = urllib.request.Request(
            TTS_URL,
            data=body,
            headers={"Content-Type": "application/json", "x-goog-api-key": api_key},
        )
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                payload = json.load(response)
            part = payload["candidates"][0]["content"]["parts"][0]["inlineData"]
            if f"rate={TTS_RATE}" not in part["mimeType"]:
                raise RuntimeError(f"unexpected TTS audio format {part['mimeType']}")
            pcm = base64.b64decode(part["data"])
            if not pcm:
                raise RuntimeError("TTS returned empty audio")
            cached.parent.mkdir(parents=True, exist_ok=True)
            cached.write_bytes(pcm)
            return pcm
        except (urllib.error.URLError, KeyError, RuntimeError) as exc:
            last_error = exc
            log.warning(
                "TTS attempt %d failed for voice %s: %s", attempt + 1, voice, exc
            )
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"TTS failed for voice {voice}: {last_error}")


def _silence(seconds: float) -> bytes:
    return b"\x00\x00" * int(TTS_RATE * seconds)


def render_audio(entry: dict, script: dict, cast: dict, api_key: str) -> Path:
    """Render one audio entry; return its segment directory."""
    people = cast["people"]
    pcm = bytearray(_silence(LEAD_IN_S))
    reference: list[dict] = []
    previous: str | None = None
    for line in script["lines"]:
        speaker = line["speaker"]
        if speaker not in people:
            raise ValueError(f"{entry['id']}: unknown speaker {speaker}")
        if previous is not None:
            pcm += _silence(_gap_seconds(line["text"], previous == speaker))
        start = len(pcm) / 2 / TTS_RATE
        pcm += _tts(line["text"], people[speaker]["voice"], api_key)
        end = len(pcm) / 2 / TTS_RATE
        reference.append(
            {
                "start": round(start, 3),
                "end": round(end, 3),
                "speaker": people[speaker]["name"],
                "text": line["text"],
            }
        )
        previous = speaker
    pcm += _silence(TAIL_S)
    duration = int(len(pcm) / 2 / TTS_RATE + 0.999)

    seg_dir = story.segment_dir(JOURNAL_DIR, entry, "audio", duration)
    story.clear_entry(JOURNAL_DIR, entry, "audio")
    seg_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "ffmpeg",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "s16le",
            "-ar",
            str(TTS_RATE),
            "-ac",
            "1",
            "-i",
            "pipe:0",
            "-acodec",
            "pcm_s16le",
            "-ar",
            str(OUT_RATE),
            "-ac",
            "1",
            "-bitexact",
            str(seg_dir / "audio.wav"),
        ],
        input=bytes(pcm),
        check=True,
    )
    ref_dir = REFERENCE_DIR / entry["id"]
    ref_dir.mkdir(parents=True, exist_ok=True)
    (ref_dir / "transcript.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in reference), encoding="utf-8"
    )
    return seg_dir


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ids", nargs="*", help="entry ids to render (default: all)")
    parser.add_argument("--kind", choices=["audio", "screen"])
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    cast = story.load_cast()
    entries = story.load_entries()
    wanted = set(args.ids)
    unknown = wanted - {entry["id"] for entry in entries}
    if unknown:
        parser.error(f"unknown entry ids: {sorted(unknown)}")

    api_key = os.environ.get("GOOGLE_API_KEY", "")
    for entry in entries:
        if wanted and entry["id"] not in wanted:
            continue
        if args.kind and entry["kind"] != args.kind:
            continue
        script = story.load_script(entry)
        if entry["kind"] == "audio":
            if not api_key:
                raise SystemExit("GOOGLE_API_KEY is required to render audio")
            seg_dir = render_audio(entry, script, cast, api_key)
        else:
            seg_dir = render_screen(entry, script, JOURNAL_DIR, CACHE_DIR)
        log.info("rendered %s -> %s", entry["id"], seg_dir.relative_to(REPO_ROOT))
    story.write_index(JOURNAL_DIR)
    return 0


if __name__ == "__main__":
    sys.exit(main())
