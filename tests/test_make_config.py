# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest

from tools.make_config import DEFAULT_OUTPUT, PROVIDER_KEYS, _parse_env_file, main


@pytest.fixture(autouse=True)
def scrub_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("GOOGLE_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY"):
        monkeypatch.delenv(key, raising=False)


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_baseline_no_keys(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    out = tmp_path / "out.json"

    assert main(["--path", str(out)]) == 0
    captured = capsys.readouterr()

    data = _read_json(out)
    assert list(data.keys()) == ["setup", "convey", "identity", "env"]
    assert isinstance(data["setup"]["completed_at"], int)
    assert data["setup"]["completed_at"] > 0
    assert len(data["convey"]["secret"]) == 64
    assert set(data["convey"]["secret"]) <= set("0123456789abcdef")
    assert data["convey"]["trust_localhost"] is True
    assert data["identity"] == {
        "name": "Field",
        "preferred": "Field",
        "timezone": "America/Denver",
    }
    assert data["env"] == {}
    assert stat.S_IMODE(out.stat().st_mode) == 0o600
    assert "warning: no provider API keys found" in captured.err
    assert "GOOGLE_API_KEY: not set" in captured.err
    assert "OPENAI_API_KEY: not set" in captured.err
    assert "ANTHROPIC_API_KEY: not set" in captured.err
    assert "env block will be empty" in captured.err
    assert "--google-api-key" in captured.err


def test_env_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out = tmp_path / "out.json"
    monkeypatch.setenv("OPENAI_API_KEY", "env-val")

    assert main(["--path", str(out)]) == 0

    data = _read_json(out)
    assert data["env"] == {"OPENAI_API_KEY": "env-val"}


def test_dotenv_only(tmp_path: Path) -> None:
    out = tmp_path / "out.json"
    env_file = tmp_path / ".env"
    env_file.write_text("GOOGLE_API_KEY=dotenv-val\n", encoding="utf-8")

    assert main(["--path", str(out), "--env-file", str(env_file)]) == 0

    data = _read_json(out)
    assert data["env"] == {"GOOGLE_API_KEY": "dotenv-val"}


def test_dotenv_beats_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out = tmp_path / "out.json"
    env_file = tmp_path / ".env"
    env_file.write_text("OPENAI_API_KEY=dotenv-val\n", encoding="utf-8")
    monkeypatch.setenv("OPENAI_API_KEY", "env-val")

    assert main(["--path", str(out), "--env-file", str(env_file)]) == 0

    data = _read_json(out)
    assert data["env"] == {"OPENAI_API_KEY": "dotenv-val"}


def test_flag_beats_both(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out = tmp_path / "out.json"
    env_file = tmp_path / ".env"
    env_file.write_text("GOOGLE_API_KEY=dotenv-val\n", encoding="utf-8")
    monkeypatch.setenv("GOOGLE_API_KEY", "env-val")

    assert (
        main(
            [
                "--path",
                str(out),
                "--env-file",
                str(env_file),
                "--google-api-key",
                "flag-val",
            ]
        )
        == 0
    )

    data = _read_json(out)
    assert data["env"] == {"GOOGLE_API_KEY": "flag-val"}


def test_refuse_on_exists(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    out = tmp_path / "out.json"

    assert main(["--path", str(out), "--no-keys"]) == 0
    original_bytes = out.read_bytes()
    original_mtime = out.stat().st_mtime_ns

    assert main(["--path", str(out), "--no-keys"]) == 1

    captured = capsys.readouterr()
    assert "already exists." in captured.err
    assert "rotate convey.secret" in captured.err
    assert "To proceed anyway, choose one:" in captured.err
    assert "make config-force" in captured.err
    assert "python3 tools/make_config.py --force" in captured.err
    assert out.read_bytes() == original_bytes
    assert out.stat().st_mtime_ns == original_mtime


def test_force_rotates(tmp_path: Path) -> None:
    out = tmp_path / "out.json"

    assert main(["--path", str(out), "--no-keys"]) == 0
    first_secret = _read_json(out)["convey"]["secret"]

    assert main(["--path", str(out), "--no-keys", "--force"]) == 0
    second_secret = _read_json(out)["convey"]["secret"]

    assert first_secret != second_secret
    assert len(first_secret) == 64
    assert len(second_secret) == 64
    assert set(first_secret) <= set("0123456789abcdef")
    assert set(second_secret) <= set("0123456789abcdef")


def test_no_keys_flag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out = tmp_path / "out.json"
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "GOOGLE_API_KEY=dotenv-google",
                "OPENAI_API_KEY=dotenv-openai",
                "ANTHROPIC_API_KEY=dotenv-anthropic",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("GOOGLE_API_KEY", "env-google")
    monkeypatch.setenv("OPENAI_API_KEY", "env-openai")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "env-anthropic")

    assert main(["--path", str(out), "--env-file", str(env_file), "--no-keys"]) == 0

    data = _read_json(out)
    assert data["env"] == {}


def test_path_respected(tmp_path: Path) -> None:
    out = tmp_path / "custom.json"

    assert main(["--path", str(out), "--no-keys"]) == 0

    data = _read_json(out)
    assert out.exists()
    assert list(data.keys()) == ["setup", "convey", "identity", "env"]


def test_env_parser_edges(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# comment",
                "",
                "GOOGLE_API_KEY=plain",
                'OPENAI_API_KEY="double-quoted"',
                "ANTHROPIC_API_KEY='single-quoted'",
                "export FOO=bar",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = _parse_env_file(env_file)

    assert result["GOOGLE_API_KEY"] == "plain"
    assert result["OPENAI_API_KEY"] == "double-quoted"
    assert result["ANTHROPIC_API_KEY"] == "single-quoted"
    assert "export FOO" in result

    out = tmp_path / "out.json"
    assert main(["--path", str(out), "--env-file", str(env_file)]) == 0

    data = _read_json(out)
    assert set(data["env"]) <= set(PROVIDER_KEYS)
    assert not any(key.startswith("export") for key in data["env"])
    assert DEFAULT_OUTPUT.name == "journal.json"
