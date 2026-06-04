import os

import pytest

from quantilica.core.config import EnvSettings, load_dotenv
from quantilica.core.exceptions import ConfigError


def test_env_settings_reads_prefixed_values():
    settings = EnvSettings(prefix="QUANTILICA_", environ={"QUANTILICA_TOKEN": "abc"})

    assert settings.get("token") == "abc"
    assert settings.require("token") == "abc"


def test_env_settings_require_raises_for_missing_value():
    settings = EnvSettings(prefix="QUANTILICA_", environ={})

    with pytest.raises(ConfigError):
        settings.require("token")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1", True),
        ("true", True),
        ("yes", True),
        ("0", False),
        ("false", False),
        ("no", False),
    ],
)
def test_env_settings_get_bool(raw, expected):
    settings = EnvSettings(environ={"FEATURE": raw})

    assert settings.get_bool("feature") is expected


def test_env_settings_get_bool_raises_for_invalid_value():
    settings = EnvSettings(environ={"FEATURE": "maybe"})

    with pytest.raises(ConfigError):
        settings.get_bool("feature")


def test_env_settings_get_int():
    settings = EnvSettings(environ={"LIMIT": "42"})

    assert settings.get_int("limit") == 42


def test_load_dotenv_loads_simple_file(tmp_path, monkeypatch):
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        """
        # comment
        QUANTILICA_FOO=bar
        QUANTILICA_QUOTED="baz"
        """,
        encoding="utf-8",
    )
    monkeypatch.delenv("QUANTILICA_FOO", raising=False)
    monkeypatch.delenv("QUANTILICA_QUOTED", raising=False)

    loaded = load_dotenv(dotenv)

    assert loaded == 2
    assert os.environ["QUANTILICA_FOO"] == "bar"
    assert os.environ["QUANTILICA_QUOTED"] == "baz"


def test_load_dotenv_does_not_override_by_default(tmp_path, monkeypatch):
    dotenv = tmp_path / ".env"
    dotenv.write_text("QUANTILICA_FOO=new\n", encoding="utf-8")
    monkeypatch.setenv("QUANTILICA_FOO", "old")

    loaded = load_dotenv(dotenv)

    assert loaded == 0
    assert os.environ["QUANTILICA_FOO"] == "old"
