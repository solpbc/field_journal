# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
"""Serve a built verona demo journal: its web app and its local agent door.

Starts the journal's supervisor on the demo build (web app at
http://127.0.0.1:5115, agent door at http://127.0.0.1:7659/mcp), gives one
bearer token read access to the whole journal, and writes ``mcp.json`` beside
the build so an MCP client can ask the journal a question, for example:

    claude -p --mcp-config .demo/latest/mcp.json "<question>"

Every call the agent makes is then listed by ``journal mcp activity``.
Stop with Ctrl-C.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tools.verona import demo  # noqa: E402

log = logging.getLogger("verona.serve")

LABEL = "demo-agent"
DOOR = "http://127.0.0.1:7659/mcp"


def grant_agent(journal: Path, out: Path) -> Path:
    """Mint the demo agent's token once and grant it whole-journal read."""
    env = demo._journal_env(journal)
    config_path = out / "mcp.json"
    if not config_path.exists():
        created = subprocess.run(
            ["journal", "mcp", "token", "create", "--label", LABEL],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        secrets = re.findall(r"^\s*(\S{30,})\s*$", created, flags=re.MULTILINE)
        if len(secrets) != 1:
            raise SystemExit(
                "could not read the new token from `journal mcp token create`"
            )
        config = {
            "mcpServers": {
                "verona-journal": {
                    "type": "http",
                    "url": DOOR,
                    "headers": {"Authorization": f"Bearer {secrets[0]}"},
                }
            }
        }
        config_path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
        config_path.chmod(0o600)
    subprocess.run(
        [
            "journal",
            "mcp",
            "permission",
            "set",
            "--token",
            LABEL,
            "--category",
            "transcripts",
            "--category",
            "entities",
            "--category",
            "facets",
        ],
        env=env,
        check=True,
    )
    return config_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--build", type=Path, required=True, help="a demo build directory"
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    out = args.build.resolve()
    journal = out / "journal"
    if not (out / "demo-build.json").exists():
        raise SystemExit(f"{out} is not a finished demo build (no demo-build.json)")
    supervisor = demo.start_supervisor(journal, out / "serve.log")
    try:
        config_path = grant_agent(journal, out)
        log.info("web app: http://127.0.0.1:%d", demo.CONVEY_PORT)
        log.info("agent door: %s (client config: %s)", DOOR, config_path)
        supervisor.wait()
    except KeyboardInterrupt:
        pass
    finally:
        demo.stop_supervisor(supervisor)
    return 0


if __name__ == "__main__":
    sys.exit(main())
