"""Minimal configuration helpers for Quantilica projects."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .exceptions import ConfigError


@dataclass(frozen=True)
class EnvSettings:
    """Read settings from an environment mapping using an optional prefix."""

    prefix: str = ""
    environ: Mapping[str, str] | None = None

    def _source(self) -> Mapping[str, str]:
        return os.environ if self.environ is None else self.environ

    def _key(self, name: str) -> str:
        return f"{self.prefix}{name}".upper()

    def get(self, name: str, default: str | None = None) -> str | None:
        """Return a setting value or a default."""
        return self._source().get(self._key(name), default)

    def require(self, name: str) -> str:
        """Return a setting value or raise ConfigError."""
        value = self.get(name)
        if value is None or value == "":
            raise ConfigError(f"Missing required setting: {self._key(name)}")
        return value

    def get_bool(self, name: str, default: bool = False) -> bool:
        """Return a boolean setting."""
        value = self.get(name)
        if value is None:
            return default
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
        raise ConfigError(f"Invalid boolean setting: {self._key(name)}={value!r}")

    def get_int(self, name: str, default: int | None = None) -> int | None:
        """Return an integer setting."""
        value = self.get(name)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError as exc:
            message = f"Invalid integer setting: {self._key(name)}={value!r}"
            raise ConfigError(message) from exc

    def path(self, name: str, default: str | Path | None = None) -> Path | None:
        """Return a setting as an expanded Path."""
        value = self.get(name)
        if value is None:
            if default is None:
                return None
            return Path(default).expanduser()
        return Path(value).expanduser()


def load_dotenv(
    path: str | os.PathLike[str] = ".env",
    *,
    override: bool = False,
) -> int:
    """Load a simple dotenv file without adding a runtime dependency.

    This parser intentionally supports only ``KEY=VALUE`` lines, comments, and
    optional single or double quotes. It returns the number of variables loaded.
    """
    dotenv_path = Path(path).expanduser()
    if not dotenv_path.exists():
        return 0

    loaded = 0
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if not key:
            continue
        if override or key not in os.environ:
            os.environ[key] = value
            loaded += 1
    return loaded
