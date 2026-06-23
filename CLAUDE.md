# field_journal

A curated collection of public domain and permissively-licensed media organized as a [solstone](https://github.com/solpbc/solstone) journal. Used for pipeline validation: transcription, speaker diarization, entity extraction, facet classification, and media retention.

## Project Structure

```
field_journal/
├── journal/              # day/stream/segment/ media tree (committed media files)
│   └── YYYYMMDD/
│       ├── field.audio/
│       │   └── HHMMSS_duration/
│       │       └── audio.wav
│       └── field.screen/
│           └── HHMMSS_duration/
│               └── screen.mp4
├── manifest.json         # per-segment metadata (source, license, duration, what it exercises)
├── ATTRIBUTION.md        # detailed license info per source
├── tools/                # download/build scripts (python)
│   ├── sources/          # one module per source (ami, chime6, dipco, icsi, psai, loc, nasa, hpr, voices, voxconverse)
│   └── build.py          # orchestrates download → slice → organize into journal structure
└── tests/                # validation tests
```

## Key Concepts

- **Journal structure**: follows solstone's `day/stream/segment/` pattern
- **manifest.json**: source of truth for what segments exist, their sources, licenses, and what pipeline features they exercise
- **tools/**: reproducible build — download upstream sources, slice into segments, organize into journal structure
- **Ten sources**: AMI Meeting Corpus (CC-BY 4.0), CHiME-6 Dinner Parties (CC-BY-SA 4.0), ICSI Meeting Corpus (CC-BY 4.0), PSAI Computer Use Dataset (MIT), LOC Dialect Recordings (unrestricted), NASA Press Conferences (public domain), Hacker Public Radio (CC-BY-SA 4.0), VOiCES (CC-BY 4.0), DiPCo Dinner Party Corpus (CDLA-Permissive-1.0), VoxConverse (CC-BY 4.0)

## Development

```bash
make install    # set up dev environment
make test       # run validation tests
make ci         # full pre-commit check (format + lint + types + test)
make format     # auto-fix formatting
make clean      # remove build artifacts
```

## Coding Standards

- AGPL-3.0-only license
- All source files must begin with: `# SPDX-License-Identifier: AGPL-3.0-only` and `# Copyright (c) 2026 sol pbc`
- Python 3.10+, snake_case naming, absolute imports
- ruff for formatting/linting, mypy for type checking, pytest for tests
- uv for dependency management

## Engineering Principles

sol pbc's coding standards, distilled — inlined because a coding agent here can't
read the private org standards. Most apply to the `tools/` build scripts (the
only real code surface in this corpus repo):

- **Fail loudly, never silently.** The download/build scripts fetch from ten external sources — a fetch, checksum, slice, or license-metadata failure must raise a clear, specific error (which source, which step), not produce a silently truncated or mislabeled segment. Use the `logging` module, not `print`. A corpus that's quietly wrong is worse than one that fails the build.
- **Verify before you claim.** Source URLs, licenses, and dataset shapes are facts to verify against the live source, not recall — `manifest.json` and `ATTRIBUTION.md` must reflect what was actually downloaded and its real license, never an assumed one.
- **The build is reproducible and idempotent.** Re-running `build.py` on an unchanged source set is a no-op; downloads are cached/skipped, not re-fetched blindly. `manifest.json` is the source of truth for what exists — keep it in sync in the same change.
- **KISS / YAGNI.** One small module per source under `tools/sources/`; don't add abstraction for sources that don't exist yet. No backwards-compat shims.
