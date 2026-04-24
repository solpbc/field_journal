# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = REPO_ROOT / "journal" / "config" / "journal.json"
DEFAULT_ENV_FILE = REPO_ROOT / ".env"
PROVIDER_KEYS = ("GOOGLE_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY")


def _parse_env_file(path: Path) -> dict[str, str]:
    """Parse simple KEY=VALUE lines without shell features.

    Lines beginning with ``export`` are not treated specially, so ``export
    FOO=bar`` produces the literal key ``export FOO``.
    """

    if not path.exists():
        return {}

    result: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        result[key] = value
    return result


def _resolve_source(
    flag_value: str | None,
    env_file_dict: dict[str, str],
    env_name: str,
    process_env: dict[str, str],
) -> str | None:
    if flag_value is not None:
        return flag_value
    if env_name in env_file_dict:
        # Prefer repo-local .env over process env for headless build reproducibility.
        return env_file_dict[env_name]
    if env_name in process_env:
        return process_env[env_name]
    return None


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f".{path.name}.tmp.{os.getpid()}"
    fd = -1
    created = False
    try:
        fd = os.open(tmp_path, os.O_CREAT | os.O_WRONLY | os.O_EXCL, 0o600)
        created = True
        total_written = 0
        while total_written < len(data):
            written = os.write(fd, data[total_written:])
            if written == 0:
                raise OSError("short write while writing config")
            total_written += written
        os.fsync(fd)
        os.close(fd)
        fd = -1
        os.replace(tmp_path, path)
    except BaseException:
        if fd != -1:
            os.close(fd)
        if created:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--google-api-key")
    parser.add_argument("--openai-api-key")
    parser.add_argument("--anthropic-api-key")
    parser.add_argument("--convey-secret")
    parser.add_argument("--name", default="Field")
    parser.add_argument("--preferred", default="Field")
    parser.add_argument("--timezone", default="America/Denver")
    parser.add_argument("--no-keys", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--path", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help=".env path override (test hook)",
    )
    args = parser.parse_args(argv)

    output_path = args.path.resolve()
    try:
        path_str = str(output_path.relative_to(REPO_ROOT))
    except ValueError:
        path_str = str(output_path)

    try:
        env_file_dict = _parse_env_file(args.env_file.resolve())
    except (OSError, PermissionError) as exc:
        print(
            f"error: could not read env file {args.env_file}: {exc}\n"
            "Fix the file permissions, remove the file, or pass --env-file PATH.",
            file=sys.stderr,
        )
        return 1

    process_env = dict(os.environ)
    env_block = {
        key: value
        for key, value in {
            "GOOGLE_API_KEY": _resolve_source(
                args.google_api_key,
                env_file_dict,
                "GOOGLE_API_KEY",
                process_env,
            ),
            "OPENAI_API_KEY": _resolve_source(
                args.openai_api_key,
                env_file_dict,
                "OPENAI_API_KEY",
                process_env,
            ),
            "ANTHROPIC_API_KEY": _resolve_source(
                args.anthropic_api_key,
                env_file_dict,
                "ANTHROPIC_API_KEY",
                process_env,
            ),
        }.items()
        if value is not None
    }
    if args.no_keys:
        env_block = {}

    convey_secret = _resolve_source(
        args.convey_secret,
        env_file_dict,
        "CONVEY_SECRET",
        process_env,
    )
    if convey_secret is None:
        convey_secret = secrets.token_hex(32)

    if output_path.exists() and not args.force:
        print(
            f"{path_str} already exists.\n\n"
            "Running make config again would rotate convey.secret and invalidate any\n"
            "existing convey session cookies or paired-device tokens.\n\n"
            "To proceed anyway, choose one:\n"
            "  make config-force\n"
            "  python3 tools/make_config.py --force",
            file=sys.stderr,
        )
        return 1

    if not env_block and not args.no_keys:
        print(
            "warning: no provider API keys found\n\n"
            "GOOGLE_API_KEY: not set\n"
            "OPENAI_API_KEY: not set\n"
            "ANTHROPIC_API_KEY: not set\n\n"
            "env block will be empty.\n\n"
            "Set keys with flags such as --google-api-key, --openai-api-key, or\n"
            "--anthropic-api-key; a repo-root .env file; or the process environment.",
            file=sys.stderr,
        )

    data = {
        "setup": {"completed_at": int(time.time() * 1000)},
        "convey": {"secret": convey_secret, "trust_localhost": True},
        "identity": {
            "name": args.name,
            "preferred": args.preferred,
            "timezone": args.timezone,
        },
        "providers": {
            "generate": {"provider": "google", "backup": "google"},
            "cogitate": {"provider": "google", "backup": "google"},
        },
        "retention": {"raw_media": "keep"},
        "env": env_block,
    }
    payload = (json.dumps(data, indent=2, sort_keys=False) + "\n").encode("utf-8")

    _atomic_write(output_path, payload)
    print(
        f"wrote {path_str} ({len(env_block)} API keys, "
        f"convey.secret rotated, identity={args.name})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
