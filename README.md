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
| [CHiME-6 Dinner Parties](https://www.chimechallenge.org/challenges/chime6/) | CC-BY-SA 4.0 | Real-home far-field dinner-party recordings with overlap and cross-talk |
| [ICSI Meeting Corpus](https://groups.inf.ed.ac.uk/ami/icsi/) | CC-BY 4.0 | Recurring lab meetings with shared speakers across sessions |
| [VOiCES](https://iqtlabs.github.io/voices/) | CC-BY 4.0 | Far-field replayed LibriSpeech utterances at multiple mic distances for cross-device drift |
| [DiPCo Dinner Party Corpus](https://zenodo.org/records/8122551) | CDLA-Permissive-1.0 | Close-talk dinner-party speaker mixes with overlap-rich transcripts |
| verona (written by sol pbc) | CC-BY 4.0 | A synthetic working week: invented people, meetings, writing and projects, with exact reference transcripts |

The first nine sources are real recordings of real people. **verona is the exception: it is fiction**, written for this corpus, and nobody in it is a real person (see below).

## verona: the synthetic week

`journal/20260209` to `journal/20260213` (streams `verona.audio` and `verona.screen`) hold one invented working week from the journal of **Juliet Capulet**, head of product at the fictional Capulet Industries. Her team and its partners recur across the days: Romeo and Benvolio at Montague Tech, Friar at Verona Ventures, Paris Duke at Ducal Freight, and the Balcony App, Schema Bridge and Mesh Routing projects. There are 20 entries: meetings, calls and voice memos, plus her screen while she writes a spec, triages a board, reviews code and drafts an email. It continues the fictional Verona cast in the solstone journal's own test fixture.

- **Written, not recorded.** The scripts live in `tools/verona/story/` (`cast.json`, `week.json`, one file per entry in `days/`).
- **Rendered once, then committed.** `make verona-render` speaks each line with a stock text-to-speech voice (Gemini TTS, one voice per character, nobody's voice cloned), types each screen script out in headless Chromium, and refreshes the manifest. You only need it after changing a script.
- **Exact ground truth.** Every audio entry has `reference/verona/<id>/transcript.jsonl`, with each line's speaker, text and timing.

### the demo journal

`make demo` builds a journal containing **only** the verona week, so it never includes the real recordings. A test enforces this. The build needs `GOOGLE_API_KEY` and `minisign`. It:

1. installs the latest released solstone journal into an isolated home under `.demo/runtime/`, so your own install and service are untouched;
2. moves the week so that its Friday is the most recent Friday before today;
3. imports the week's calendar, lets that journal's own supervisor process it now rather than overnight (transcription, speakers, thinking, daily outputs), then builds the index;
4. writes `.demo/latest/demo-build.json`, recording the journal version, this repo's commit and the provider and model used.

`make demo-serve` opens the latest build's web app at `http://127.0.0.1:5115`. It also opens its local agent door at `http://127.0.0.1:7659/mcp`, with a read-only demo token written to `.demo/latest/mcp.json`, so any MCP client can ask the week a question. Port 7659 is fixed, so stop your own journal first if one runs on this computer.

## directory structure

```
field_journal/
├── journal/              # day/stream/segment/ media tree
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
│   ├── sources/          # one module per source (ami, chime6, dipco, icsi, psai, loc, nasa, hpr, voices, verona)
│   ├── verona/           # the synthetic week: story, renderer, demo builder
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

### Configuration

`journal/config/` is gitignored — it holds live secrets. To (re)materialize a
minimal already-onboarded config for local testing:

```bash
make config         # write journal/config/journal.json from flags + env + .env
make config-force   # overwrite existing config (rotates convey.secret)
```

Provider keys are resolved in order: explicit flags to `tools/make_config.py`,
a local `.env` file in the repo root, then the process environment. `.env` is
gitignored. All three provider keys (`GOOGLE_API_KEY`, `OPENAI_API_KEY`,
`ANTHROPIC_API_KEY`) are optional; whichever are present get written into
`journal/config/journal.json`'s `env` block.

The generated config always sets `retention.raw_media` to `"keep"` — solstone's
default is day-based retention (7 days), which would sweep away this repo's
tracked fixture media on every supervisor boot. This is not overridable by
flag or env; the whole point of `make config` is configuring for fixture
preservation.

The generated config also pins `providers.generate` and `providers.cogitate`
to `"google"` with `backup: "google"` — so every provider resolution lands on
Gemini and no fallback to Anthropic or OpenAI engages, even if those keys
leak in via the process environment. This is not overridable by flag or env;
`make config` exists to configure field_journal for Google-only pipeline
validation.

The generated config is pre-onboarded (`setup.completed_at` set, localhost
trusted, no password) so the `solstone-field` sandbox boots without the
convey `/init` redirect.

## license

AGPL-3.0-only. See [LICENSE](LICENSE) for the project license.

Media files in `journal/` are subject to their individual source licenses — see [ATTRIBUTION.md](ATTRIBUTION.md) for details.
