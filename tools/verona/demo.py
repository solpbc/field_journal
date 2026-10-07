# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
"""Build the verona demo journal with a released solstone journal.

The demo journal holds the verona week and nothing else: only segments whose
manifest source is in ``DEMO_SOURCES`` are copied, so none of the corpus's
real recordings can enter it. The week is re-dated so its Friday is the most
recent Friday before ``--end`` (default: today), then processed by the
journal binary on ``PATH``:

    importer (the week's calendar) -> sense -> think -> indexer

and ``demo-build.json`` records the journal version and inputs. Run it with a
released journal installed in an isolated ``HOME`` (``make demo`` does this).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tools.verona import story  # noqa: E402

log = logging.getLogger("verona.demo")

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
JOURNAL_DIR = REPO_ROOT / "journal"
MANIFEST_PATH = REPO_ROOT / "manifest.json"
DEMO_SOURCES = frozenset({"verona"})
MODEL = "gemini-3.5-flash"
TIMEZONE = "America/Denver"
FACETS = {
    "capulet": {
        "title": "Capulet",
        "description": "Work at Capulet Industries: the Balcony App and the team",
        "color": "#b5523b",
        "emoji": "🏛️",
    },
    "schema-bridge": {
        "title": "Schema Bridge",
        "description": "The Schema Bridge project with Montague Tech and the Ducal Freight pilot",
        "color": "#3b6fb5",
        "emoji": "🌉",
    },
}
EMAIL_DOMAINS = {
    "juliet_capulet": "capulet.example",
    "tybalt_capulet": "capulet.example",
    "nurse_angela": "capulet.example",
    "balthasar_davi": "capulet.example",
    "romeo_montague": "montague.example",
    "benvolio_montague": "montague.example",
    "mercutio_escalus": "montague.example",
    "friar_lawrence": "verona-ventures.example",
    "paris_duke": "ducal-freight.example",
}


def demo_segments() -> list[dict]:
    """Manifest segments eligible for the demo, refusing anything else."""
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    chosen = [s for s in manifest["segments"] if s["source"] in DEMO_SOURCES]
    if not chosen:
        raise SystemExit("manifest has no verona segments; run tools/build.py verona")
    return chosen


def day_map(end: date) -> dict[str, str]:
    """Map the authored week's days onto the week ending the Friday before ``end``."""
    friday = end - timedelta(days=((end.weekday() - 4) % 7) or 7)
    days = [d["day"] for d in story.load_week()["days"]]
    first = friday - timedelta(days=len(days) - 1)
    return {
        old: (first + timedelta(days=i)).strftime("%Y%m%d")
        for i, old in enumerate(days)
    }


def _run(cmd: list[str], env: dict[str, str], log_path: Path) -> None:
    log.info("$ %s", " ".join(cmd))
    with log_path.open("a", encoding="utf-8") as out:
        out.write(f"\n$ {' '.join(cmd)}\n")
        out.flush()
        result = subprocess.run(cmd, env=env, stdout=out, stderr=subprocess.STDOUT)
    if result.returncode != 0:
        raise SystemExit(
            f"command failed ({result.returncode}): {' '.join(cmd)}; see {log_path}"
        )


def _email(key: str) -> str:
    """Invented addresses on reserved ``.example`` domains (RFC 2606)."""
    first = story.load_cast()["people"][key]["name"].split()[0].lower()
    return f"{first}@{EMAIL_DOMAINS[key]}"


def write_calendar(path: Path, days: dict[str, str]) -> None:
    """The week's meetings as an owner's calendar export (ICS)."""
    cast = story.load_cast()["people"]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    lines = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//sol pbc//verona demo//EN"]
    for entry in story.load_entries():
        if entry["kind"] != "audio" or len(entry["people"]) < 2:
            continue
        start = days[entry["day"]] + "T" + entry["time"]
        minutes = 15 if entry["id"].endswith("standup") else 30
        end_dt = datetime.strptime(start, "%Y%m%dT%H%M%S") + timedelta(minutes=minutes)
        lines += [
            "BEGIN:VEVENT",
            f"UID:{entry['id']}@verona.example",
            f"DTSTAMP:{stamp}",
            f"DTSTART;TZID={TIMEZONE}:{start}",
            f"DTEND;TZID={TIMEZONE}:{end_dt.strftime('%Y%m%dT%H%M%S')}",
            f"SUMMARY:{entry['title']}",
            f"ORGANIZER;CN=Juliet Capulet:mailto:{_email('juliet_capulet')}",
        ]
        for person in entry["people"]:
            lines.append(f"ATTENDEE;CN={cast[person]['name']}:mailto:{_email(person)}")
        lines.append("END:VEVENT")
    lines.append("END:VCALENDAR")
    path.write_text("\r\n".join(lines) + "\r\n", encoding="utf-8")


def seed_journal(journal: Path, days: dict[str, str]) -> list[dict]:
    """Copy the demo segments into ``journal/chronicle`` at their new dates."""
    placed: list[dict] = []
    streams: dict[str, dict] = {}
    for seg in sorted(
        demo_segments(), key=lambda s: (s["day"], s["stream"], s["segment"])
    ):
        new_day = days[seg["day"]]
        src = JOURNAL_DIR / seg["day"] / seg["stream"] / seg["segment"]
        dest = journal / "chronicle" / new_day / seg["stream"] / seg["segment"]
        media = [p for p in src.iterdir() if p.name in story.MEDIA.values()]
        if len(media) != 1:
            raise SystemExit(f"{src}: expected exactly one media file")
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy2(media[0], dest / media[0].name)
        state = streams.setdefault(
            seg["stream"], {"prev_day": None, "prev_segment": None, "seq": 0}
        )
        state["seq"] += 1
        marker = {
            "stream": seg["stream"],
            "prev_day": state["prev_day"],
            "prev_segment": state["prev_segment"],
            "seq": state["seq"],
        }
        (dest / "stream.json").write_text(json.dumps(marker) + "\n", encoding="utf-8")
        state["prev_day"], state["prev_segment"] = new_day, seg["segment"]
        placed.append({**seg, "demo_day": new_day})
    for name, state in streams.items():
        (journal / "streams").mkdir(parents=True, exist_ok=True)
        (journal / "streams" / f"{name}.json").write_text(
            json.dumps(
                {
                    "name": name,
                    "type": "import",
                    "host": None,
                    "platform": None,
                    "created_at": int(datetime.now(timezone.utc).timestamp()),
                    "last_day": state["prev_day"],
                    "last_segment": state["prev_segment"],
                    "seq": state["seq"],
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    for slug, facet in FACETS.items():
        facet_dir = journal / "facets" / slug
        facet_dir.mkdir(parents=True, exist_ok=True)
        (facet_dir / "facet.json").write_text(
            json.dumps(facet, indent=2) + "\n", encoding="utf-8"
        )
    return placed


def configure(journal: Path, api_key: str) -> None:
    path = journal / "config" / "journal.json"
    config = json.loads(path.read_text(encoding="utf-8"))
    config.setdefault("env", {})["GOOGLE_API_KEY"] = api_key
    config["identity"].update(
        {
            "name": "Juliet Capulet",
            "preferred": "Juliet",
            "timezone": TIMEZONE,
            "email_addresses": [_email("juliet_capulet")],
        }
    )
    config.setdefault("retention", {})["raw_media"] = "keep"
    # The owner finished first-run setup; the web app opens on the journal, not /init.
    config.setdefault("setup", {})["completed_at"] = int(time.time() * 1000)
    # Process now rather than in the overnight window an owner's journal waits for.
    config["processing"]["gate"]["time_window"]["enabled"] = False
    path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")


CONVEY_PORT = 5115
DIRECT_PORT = 7757


def _journal_env(journal: Path) -> dict[str, str]:
    return {**os.environ, "SOLSTONE_JOURNAL": str(journal)}


def start_supervisor(journal: Path, log_path: Path) -> subprocess.Popen:
    """Run the journal's own supervisor: it processes segments as an owner's would."""
    out = log_path.open("a", encoding="utf-8")
    proc = subprocess.Popen(
        [
            "journal",
            "supervisor",
            str(CONVEY_PORT),
            "--no-spl",
            "--direct-port",
            str(DIRECT_PORT),
        ],
        env=_journal_env(journal),
        stdout=out,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    time.sleep(10)
    if proc.poll() is not None:
        raise SystemExit(f"supervisor exited ({proc.returncode}); see {log_path}")
    return proc


def stop_supervisor(proc: subprocess.Popen) -> None:
    """Stop the supervisor; it shuts down its own children (web app, doors)."""
    proc.terminate()
    try:
        proc.wait(timeout=120)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        busy = subprocess.run(
            ["ss", "-ltnH", f"sport = :{CONVEY_PORT}"], capture_output=True, text=True
        ).stdout
        if not busy.strip():
            return
        time.sleep(2)
    log.warning("port %d is still held after the supervisor stopped", CONVEY_PORT)


def _segments_done(journal: Path, placed: list[dict]) -> int:
    pending = 0
    for seg in placed:
        talents = (
            journal
            / "chronicle"
            / seg["demo_day"]
            / seg["stream"]
            / seg["segment"]
            / "talents"
        )
        if not (talents / "sense.json").exists():
            pending += 1
    return pending


def wait_for_processing(
    journal: Path, placed: list[dict], days: dict[str, str], timeout_s: int
) -> None:
    """Wait until every segment has been thought about and no daily output is owed."""
    env = _journal_env(journal)
    first, last = min(days.values()), max(days.values())
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        pending = _segments_done(journal, placed)
        owed = (
            subprocess.run(
                ["journal", "reprocess", first, "--through", last, "--owed"],
                env=env,
                capture_output=True,
                text=True,
            )
            .stdout.strip()
            .splitlines()
        )
        summary = owed[-1] if owed else "?"
        log.info("segments pending: %d · %s", pending, summary)
        if pending == 0 and summary.startswith("0 owed"):
            return
        time.sleep(30)
    raise SystemExit("processing did not finish in time; the supervisor log says why")


def _version(env: dict[str, str]) -> str:
    out = subprocess.run(
        ["journal", "--version"], env=env, capture_output=True, text=True, check=True
    )
    return out.stdout.strip()


def _git_head() -> str:
    out = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--", "journal", "tools", "manifest.json"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    return out.stdout.strip() + ("-dirty" if dirty else "")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--out", type=Path, required=True, help="empty directory for the demo journal"
    )
    parser.add_argument("--end", type=date.fromisoformat, default=date.today())
    parser.add_argument(
        "--timeout", type=int, default=4 * 3600, help="seconds to wait for processing"
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    api_key = os.environ.get("GOOGLE_API_KEY", "")
    if not api_key:
        raise SystemExit(
            "GOOGLE_API_KEY is required: the demo is processed on a Google key"
        )
    out = args.out.resolve()
    if out.exists() and any(out.iterdir()):
        raise SystemExit(f"{out} is not empty")
    journal = out / "journal"
    out.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "SOLSTONE_JOURNAL": str(journal)}
    log_path = out / "build.log"

    _run(
        [
            "journal",
            "setup",
            "--journal",
            str(journal),
            "--skip-service",
            "--skip-wrapper",
            "--skip-path",
            "--skip-skills",
            "--skip-brain",
            "-y",
        ],
        env,
        log_path,
    )
    configure(journal, api_key)
    _run(
        [
            "journal",
            "thinking",
            "set-lane",
            "byo",
            "--provider",
            "google",
            "--model",
            MODEL,
        ],
        env,
        log_path,
    )

    days = day_map(args.end)
    placed = seed_journal(journal, days)
    calendar = out / "verona-week.ics"
    write_calendar(calendar, days)

    supervisor = start_supervisor(journal, out / "supervisor.log")
    try:
        _run(["journal", "importer", str(calendar), "--source", "ics"], env, log_path)
        wait_for_processing(journal, placed, days, args.timeout)
        _run(["journal", "indexer", "--rescan-full"], env, log_path)
    finally:
        stop_supervisor(supervisor)

    record = {
        "journal_version": _version(env),
        "field_journal_commit": _git_head(),
        "provider": "google",
        "model": MODEL,
        "built_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "days": days,
        "segments": len(placed),
        "sources": sorted({s["source"] for s in placed}),
        "owner_or_real_person_data": "none: every segment is sol pbc-authored fiction (source verona)",
    }
    (out / "demo-build.json").write_text(
        json.dumps(record, indent=2) + "\n", encoding="utf-8"
    )
    log.info("demo journal ready at %s (%s)", journal, record["journal_version"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
