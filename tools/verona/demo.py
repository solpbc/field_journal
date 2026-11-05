# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
"""Build the verona demo journal with a released solstone journal.

The demo journal holds the verona week and nothing else: only segments whose
manifest source is in ``DEMO_SOURCES`` are copied, so none of the corpus's
real recordings can enter it. The week is re-dated so its Friday is the most
recent Friday before ``--end`` (default: today), or, with ``--today``, so its
hero day falls on that date and the days after it are left out; the journal's
first page then shows the hero day as today.

The build sets up a journal with the journal binary on ``PATH``, locks its
mark, and pairs one invented device (``juliets-laptop``) the way an owner pairs a
computer. No segment is written into the journal directly: the device's own
bridge delivers each one through the journal's ingest, in the order it was
recorded, and the journal processes each one as it arrives:

    importer (the week's calendar) -> ingest -> sense -> think -> indexer

Afterwards Juliet adds the people, companies and projects she works with, as an
owner does, choosing from the names the journal detected. ``demo-build.json``
records the journal version, the inputs, the device and those owner actions.
Run it with a released journal installed in an isolated ``HOME`` (``make demo``
does this).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import secrets
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tools.verona import story  # noqa: E402

log = logging.getLogger("verona.demo")

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
JOURNAL_DIR = REPO_ROOT / "journal"
MANIFEST_PATH = REPO_ROOT / "manifest.json"
DEMO_SOURCES = frozenset({"verona"})
MODEL = "gemini-3.8-flash"
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


def hero_day_map(today: date) -> dict[str, str]:
    """Map the hero day onto ``today`` and the days before it onto the days before.

    Authored days after the hero are left out: they would be in the future.
    """
    week = story.load_week()["days"]
    hero = next(i for i, d in enumerate(week) if d.get("hero"))
    return {
        d["day"]: (today - timedelta(days=hero - i)).strftime("%Y%m%d")
        for i, d in enumerate(week[: hero + 1])
    }


def _run(cmd: list[str], env: dict[str, str], log_path: Path) -> None:
    log.info("$ %s", " ".join(cmd))
    with log_path.open("a", encoding="utf-8") as out:
        out.write(f"\n$ {' '.join(cmd)}\n")
        out.flush()
        result = subprocess.run(
            cmd, env=env, stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT
        )
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
        if entry["day"] not in days:
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


def deliveries(days: dict[str, str]) -> list[dict]:
    """The demo segments to deliver, at their new dates, in the order recorded."""
    chosen: list[dict] = []
    for seg in demo_segments():
        if seg["day"] not in days:
            continue
        kind = seg["stream"].split(".", 1)[1]
        media = (
            JOURNAL_DIR
            / seg["day"]
            / seg["stream"]
            / seg["segment"]
            / story.MEDIA[kind]
        )
        if not media.is_file():
            raise SystemExit(f"{media}: missing; run tools/build.py verona")
        chosen.append(
            {**seg, "demo_day": days[seg["day"]], "kind": kind, "media": media}
        )
    return sorted(chosen, key=lambda s: (s["demo_day"], s["segment"], s["kind"]))


def write_facets(journal: Path) -> None:
    for slug, facet in FACETS.items():
        facet_dir = journal / "facets" / slug
        facet_dir.mkdir(parents=True, exist_ok=True)
        (facet_dir / "facet.json").write_text(
            json.dumps(facet, indent=2) + "\n", encoding="utf-8"
        )


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


def set_ports(convey: int, direct: int) -> None:
    """Use other ports, so a build can run while another build is being served."""
    global CONVEY_PORT, DIRECT_PORT
    CONVEY_PORT, DIRECT_PORT = convey, direct


def _journal_env(journal: Path) -> dict[str, str]:
    # `solstone call` reaches a journal by port, never by SOLSTONE_JOURNAL: name
    # this one, or it reaches whatever journal holds the client's default port.
    return {
        **os.environ,
        "SOLSTONE_JOURNAL": str(journal),
        "SOLSTONE_CONVEY_PORT": str(CONVEY_PORT),
    }


def start_supervisor(journal: Path, log_path: Path) -> subprocess.Popen:
    """Run the journal's own supervisor: it processes segments as an owner's would."""
    out = log_path.open("a", encoding="utf-8")
    proc = subprocess.Popen(
        [
            "solstone",
            "journal",
            "supervisor",
            str(CONVEY_PORT),
            "--no-spl",
            "--direct-port",
            str(DIRECT_PORT),
        ],
        env=_journal_env(journal),
        stdin=subprocess.DEVNULL,
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


DEVICE_LABEL = "juliets-laptop"


def _local(method: str, path: str, body: dict | None = None) -> dict:
    """One JSON request to this journal's web app, from this computer."""
    request = urllib.request.Request(
        f"http://127.0.0.1:{CONVEY_PORT}{path}",
        method=method,
        data=None if body is None else json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            answer: dict = json.loads(response.read() or b"{}")
            return answer
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", "replace")[:500]
        raise SystemExit(f"{method} {path}: HTTP {error.code}: {detail}") from error


def lock_mark() -> list[str]:
    """Lock the journal's mark, as first-run setup does, and return its two words."""
    mark = _local("GET", "/init/mark")
    if not mark.get("locked"):
        mark = _local("POST", "/init/mark/lock", {"proposal_id": mark["proposal_id"]})
    return list(mark["mark"]["words"])


def wait_for_door(journal: Path, timeout_s: int = 120) -> None:
    """Wait until the journal's direct door is open for devices on this computer."""
    path = journal / "health" / "direct-door.json"
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.exists():
            if json.loads(path.read_text(encoding="utf-8")).get("state") == "bound":
                return
        time.sleep(2)
    raise SystemExit(f"the journal's direct door did not open; see {path}")


def _device_env(out: Path) -> dict[str, str]:
    # The device keeps its own credentials and reports its own (invented) name,
    # never this computer's.
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in ("SOLSTONE_JOURNAL", "SOLSTONE_CONVEY_PORT")
    }
    env.update({"XDG_CONFIG_HOME": str(out / "device"), "HOSTNAME": DEVICE_LABEL})
    return env


def pair_device(out: Path, words: list[str], log_path: Path) -> None:
    """Pair the invented device with the journal, as an owner pairs a computer."""
    minted = _local(
        "POST",
        "/app/network/pair-start",
        {"role": "observer", "same_machine": True, "device_label": DEVICE_LABEL},
    )
    log.info("$ solstone link join --as observer --label %s", DEVICE_LABEL)
    with log_path.open("a", encoding="utf-8") as log_file:
        # The pair link stays out of the log: it is a credential while it lives.
        log_file.write(f"\n$ solstone link join <pair link> --label {DEVICE_LABEL}\n")
        log_file.flush()
        result = subprocess.run(
            [
                "solstone",
                "link",
                "join",
                "--code",
                minted["pair_link"],
                "--as",
                "observer",
                "--label",
                DEVICE_LABEL,
                "--mark",
                " ".join(words),
            ],
            env=_device_env(out),
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            timeout=120,
        )
    if result.returncode != 0:
        raise SystemExit(f"the device did not pair; see {log_path}")


def start_bridge(out: Path) -> tuple[subprocess.Popen, int]:
    """Start the device's bridge to the journal and return it with its local port."""
    log_path = out / "device.log"
    proc = subprocess.Popen(
        [
            "solstone",
            "link",
            "serve",
            "--label",
            DEVICE_LABEL,
            "--port",
            "0",
            "--direct",
        ],
        env=_device_env(out),
        stdin=subprocess.DEVNULL,
        stdout=log_path.open("a", encoding="utf-8"),
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        found = re.search(
            r"forwarding 127\.0\.0\.1:(\d+) ", log_path.read_text(encoding="utf-8")
        )
        if found:
            return proc, int(found.group(1))
        if proc.poll() is not None:
            break
        time.sleep(1)
    proc.terminate()
    raise SystemExit(f"the device's bridge did not start; see {log_path}")


def stop_bridge(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def deliver(bridge_port: int, segment: dict) -> None:
    """Send one recorded segment through the device's bridge to the journal's ingest."""
    name = segment["media"].name
    envelope = json.dumps(
        {
            "day": segment["demo_day"],
            "segment": segment["segment"],
            "source": segment["kind"],
            "files": [{"submitted": name}],
        }
    ).encode("utf-8")
    boundary = f"verona-{secrets.token_hex(12)}"
    body = b"".join(
        [
            f'--{boundary}\r\nContent-Disposition: form-data; name="envelope"\r\n'
            "Content-Type: application/json\r\n\r\n".encode(),
            envelope,
            f'\r\n--{boundary}\r\nContent-Disposition: form-data; name="files"; '
            f'filename="{name}"\r\nContent-Type: application/octet-stream\r\n\r\n'.encode(),
            segment["media"].read_bytes(),
            f"\r\n--{boundary}--\r\n".encode(),
        ]
    )
    request = urllib.request.Request(
        f"http://127.0.0.1:{bridge_port}/app/devices/ingest",
        method="POST",
        data=body,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "X-Solstone-Protocol-Version": "3",
            "Accept": "application/json",
        },
    )
    where = f"{segment['demo_day']}/{segment['segment']} ({segment['kind']})"
    try:
        with urllib.request.urlopen(request, timeout=300) as response:
            answer = json.loads(response.read())
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", "replace")[:500]
        raise SystemExit(
            f"ingest refused {where}: HTTP {error.code}: {detail}"
        ) from error
    if answer.get("status") != "ok":
        raise SystemExit(f"ingest did not take {where}: {answer}")


def _segments_done(journal: Path, placed: list[dict]) -> int:
    pending = 0
    for seg in placed:
        # The journal names the device's streams itself; find the segment in any.
        day = journal / "chronicle" / seg["demo_day"]
        if not any(day.glob(f"*/{seg['segment']}/talents/sense.json")):
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
                [
                    "solstone",
                    "journal",
                    "reprocess",
                    first,
                    "--through",
                    last,
                    "--owed",
                ],
                env=env,
                stdin=subprocess.DEVNULL,
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


def wait_for_activities(journal: Path, timeout_s: int) -> None:
    """Wait until the activities the journal wrote as segments arrived have settled."""
    pending_dir = journal / "awareness" / "activity_settle"
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        pending = 0
        for state in pending_dir.glob("*.json"):
            data = json.loads(state.read_text(encoding="utf-8"))
            pending += sum(len(c) for c in data.get("days", {}).values())
        log.info("activities waiting to settle: %d", pending)
        if pending == 0:
            return
        time.sleep(30)
    raise SystemExit("activities did not settle in time; the supervisor log says why")


def _detected(journal: Path) -> dict[str, list[tuple[str, str]]]:
    """Per facet, the (type, name) pairs the journal detected this week.

    That is its entity detections, plus each person it named as a speaker in a
    segment, counted in the facets it placed that segment in with high
    confidence: someone who only speaks is not always also listed as mentioned.
    """
    found: dict[str, list[tuple[str, str]]] = {slug: [] for slug in FACETS}
    for slug, rows in found.items():
        for path in sorted((journal / "facets" / slug / "entities").glob("*.jsonl")):
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    row = json.loads(line)
                    rows.append((row.get("type", ""), row.get("name", "")))
    for speakers in sorted(journal.glob("chronicle/*/*/*/talents/speakers.json")):
        facets = speakers.parent / "facets.json"
        if not facets.exists():
            continue
        names = json.loads(speakers.read_text(encoding="utf-8"))
        for placed in json.loads(facets.read_text(encoding="utf-8")):
            if placed.get("level") == "high" and placed.get("facet") in found:
                found[placed["facet"]] += [("Person", name) for name in names]
    return found


def owner_entities(journal: Path) -> list[dict]:
    """What Juliet adds to her journal: the people, companies and projects she
    works with, each in a facet where the journal itself detected them.

    An owner adds people she knows by their full name; a person is offered only
    once the journal has detected them by that name or one part of it. The
    journal's review never counts the day it reviews, so in a ``--today`` build
    it can't promote anyone before the hero day is over.
    """
    cast = story.load_cast()
    wanted: list[dict] = []
    for key, person in cast["people"].items():
        if key == cast["owner"]:
            continue
        parts = {part.lower() for part in person["name"].split()}
        wanted.append(
            {
                "type": "Person",
                "name": person["name"],
                "description": person["role"],
                "matches": lambda t, n, parts=parts, full=person["name"].lower(): (
                    t == "Person" and (n == full or n in parts)
                ),
            }
        )
    for text in cast["companies"].values():
        name, _, about = text.partition(", ")
        first = name.split()[0].lower()
        wanted.append(
            {
                "type": "Company",
                "name": name,
                "description": about[:1].upper() + about[1:] if about else name,
                "matches": lambda t, n, full=name.lower(), first=first: (
                    t == "Company" and (n == full or n == first)
                ),
            }
        )
    for key in ("schema_bridge", "balcony_app"):
        text = cast["projects"][key]
        name, _, about = text.partition(": ")
        head = " ".join(name.split()[:2]).lower()
        wanted.append(
            {
                "type": "Project",
                "name": " ".join(name.split()[:2]),
                "description": about,
                "matches": lambda t, n, head=head: n.startswith(head),
            }
        )
    actions: list[dict] = []
    for slug, rows in _detected(journal).items():
        for item in wanted:
            seen = sorted({n for t, n in rows if item["matches"](t, n.lower())})
            if seen:
                actions.append(
                    {
                        "facet": slug,
                        "type": item["type"],
                        "name": item["name"],
                        "description": item["description"],
                        "detected_as": seen,
                    }
                )
    return actions


def add_owner_entities(journal: Path, log_path: Path) -> list[dict]:
    """Add them through the journal's own attach verb, as an owner or her agent does."""
    actions = owner_entities(journal)
    for action in actions:
        _run(
            [
                "solstone",
                "call",
                "entities",
                "attach",
                action["type"],
                action["name"],
                action["description"],
                "--facet",
                action["facet"],
            ],
            _journal_env(journal),
            log_path,
        )
    return actions


def _version(env: dict[str, str]) -> str:
    out = subprocess.run(
        ["solstone", "journal", "--version"],
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=True,
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
        "--today",
        type=date.fromisoformat,
        help="put the hero day on this date (usually today) instead of using --end",
    )
    parser.add_argument(
        "--timeout", type=int, default=4 * 3600, help="seconds to wait for processing"
    )
    parser.add_argument(
        "--port", type=int, default=CONVEY_PORT, help="the journal's web app port"
    )
    parser.add_argument(
        "--direct-port",
        type=int,
        default=DIRECT_PORT,
        help="the journal's door for devices",
    )
    args = parser.parse_args(argv)
    set_ports(args.port, args.direct_port)
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
    env = _journal_env(journal)
    log_path = out / "build.log"

    _run(
        [
            "solstone",
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
            "solstone",
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

    days = hero_day_map(args.today) if args.today else day_map(args.end)
    placed = deliveries(days)
    write_facets(journal)
    calendar = out / "verona-week.ics"
    write_calendar(calendar, days)

    supervisor_log = out / "supervisor.log"
    supervisor = start_supervisor(journal, supervisor_log)
    bridge = None
    try:
        # Locking the mark gives the journal its identity; its door for devices
        # opens when the web app next starts.
        words = lock_mark()
        stop_supervisor(supervisor)
        supervisor = start_supervisor(journal, supervisor_log)
        wait_for_door(journal)
        pair_device(out, words, log_path)
        bridge, bridge_port = start_bridge(out)
        _run(
            ["solstone", "journal", "importer", str(calendar), "--source", "ics"],
            env,
            log_path,
        )
        for segment in placed:
            deliver(bridge_port, segment)
        log.info("delivered %d segments from %s", len(placed), DEVICE_LABEL)
        wait_for_processing(journal, placed, days, args.timeout)
        wait_for_activities(journal, args.timeout)
        owner_actions = add_owner_entities(journal, log_path)
        _run(["solstone", "journal", "indexer", "--rescan-full"], env, log_path)
    finally:
        if bridge is not None:
            stop_bridge(bridge)
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
        "device": {
            "label": DEVICE_LABEL,
            "simulated": True,
            "how": (
                "an invented device: this build's own `solstone link` bridge, paired "
                "with the journal as an observer through same-computer pairing, "
                "delivered every segment through the journal's ingest in recorded "
                "order. It reports this computer's real platform (so the journal "
                "says 'your computer') and the invented name above, never this "
                "computer's host or user name."
            ),
        },
        "owner_actions": [
            {"action": "entities attach", **action} for action in owner_actions
        ],
    }
    (out / "demo-build.json").write_text(
        json.dumps(record, indent=2) + "\n", encoding="utf-8"
    )
    log.info("demo journal ready at %s (%s)", journal, record["journal_version"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
