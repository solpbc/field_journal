# field_journal

A curated collection of public domain and permissively-licensed media organized as a [solstone](https://github.com/solpbc/solstone) journal. Used for pipeline validation: transcription, speaker diarization, entity extraction, facet classification, and media retention.

## what's in here

The `journal/` directory follows solstone's `day/stream/segment/` structure — a multi-day fixture journal built from real-world audio and screen recordings. Media files are committed directly to git so anyone can clone and use the journal without downloading external resources.

## sources

| Source | License | What it provides |
|--------|---------|-----------------|
| [AMI Meeting Corpus](https://groups.inf.ed.ac.uk/ami/corpus/) | CC-BY 4.0 | Multi-party meeting recordings with ground-truth speaker labels and transcripts |
| [PSAI Computer Use Dataset](https://huggingface.co/datasets/anaisleila/computer-use-data-psai) | MIT | Real desktop screen capture — shopping, research, document editing |
| [LOC American English Dialect Recordings](https://citizen-dj.labs.loc.gov/loc-american-english-dialect-recordings/use/) | Unrestricted | Natural conversation, dialect interviews, oral histories |
| [NASA Press Conferences](https://images.nasa.gov/) | Public domain | Entity-dense multi-speaker briefings |
| [Hacker Public Radio](https://hackerpublicradio.org/) | CC-BY-SA 4.0 | Casual solo tech commentary, voice-memo texture |

## directory structure

```
field_journal/
├── journal/              # day/stream/segment/ media tree
│   └── YYYYMMDD/
│       ├── fixture.audio/
│       │   └── HHMMSS_duration/
│       │       └── audio.wav (or .mp3, .mp4)
│       └── fixture.screen/
│           └── HHMMSS_duration/
│               └── screen.mp4
├── manifest.json         # per-segment metadata (source, license, duration, what it exercises)
├── ATTRIBUTION.md        # detailed license info per source
├── tools/                # download/build scripts (python)
│   ├── sources/          # one module per source (ami, psai, loc, nasa, hpr)
│   └── build.py          # orchestrates download → slice → organize
└── tests/                # validation tests
```

## usage

Clone the repo and point solstone at `journal/` as the journal path:

```bash
git clone https://github.com/solpbc/field_journal.git
```

## building the journal from source

The `tools/` directory contains the scripts used to construct the journal from upstream sources. This provides full provenance and rebuildability:

```bash
make install
python tools/build.py
```

## development

```bash
make install    # set up dev environment
make test       # run validation tests
make ci         # full pre-commit check
make format     # auto-fix formatting
make clean      # remove build artifacts
```

## license

AGPL-3.0-only. See [LICENSE](LICENSE) for the project license.

Media files in `journal/` are subject to their individual source licenses — see [ATTRIBUTION.md](ATTRIBUTION.md) for details.
