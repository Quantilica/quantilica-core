"""Logging helpers for command-line tools, jobs, and services."""

from __future__ import annotations

import logging
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a logger with a NullHandler attached if needed."""
    logger = logging.getLogger("quantilica" if name is None else name)
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger


def configure_logging(
    level: int | str = logging.INFO,
    *,
    stream: Any = sys.stderr,
    force: bool = False,
) -> None:
    """Configure root logging for CLIs and scripts."""
    logging.basicConfig(
        level=level,
        format=DEFAULT_LOG_FORMAT,
        datefmt=DEFAULT_DATE_FORMAT,
        stream=stream,
        force=force,
    )


def configure_cli_logging(
    verbose: bool = False,
    *,
    stream: Any = sys.stderr,
    force: bool = True,
) -> None:
    """Configure logging for a CLI entry point.

    ``verbose=True`` selects ``DEBUG``, otherwise ``INFO``. ``force`` defaults
    to ``True`` so CLIs re-invoked in the same interpreter (notebooks, tests)
    reconfigure cleanly instead of silently inheriting previous handlers.
    """
    configure_logging(
        level=logging.DEBUG if verbose else logging.INFO,
        stream=stream,
        force=force,
    )


def bind_context(message: str, **context: object) -> str:
    """Append structured context to a human-readable log message."""
    if not context:
        return message
    fields = " ".join(f"{key}={value}" for key, value in sorted(context.items()))
    return f"{message} {fields}"


@contextmanager
def log_step(
    logger: logging.Logger,
    step: str,
    *,
    level: int = logging.INFO,
    **context: object,
) -> Iterator[None]:
    """Log start/end/failure messages around a block."""
    start = time.perf_counter()
    logger.log(level, bind_context(f"Starting {step}", **context))
    try:
        yield
    except Exception:
        elapsed = time.perf_counter() - start
        logger.exception(
            bind_context(f"Failed {step}", elapsed=f"{elapsed:.3f}s", **context)
        )
        raise
    elapsed = time.perf_counter() - start
    logger.log(
        level,
        bind_context(f"Finished {step}", elapsed=f"{elapsed:.3f}s", **context),
    )
