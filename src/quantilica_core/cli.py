"""Shared Rich-based helpers for quantilica fetcher CLI plugins.

These helpers are host-only: ``plugin.py`` modules run inside the
``quantilica-cli`` host, which pulls in ``rich`` via the ``cli`` extra
(``quantilica-core[cli]``). Standalone argparse CLIs should keep using
:func:`quantilica_core.logging.configure_cli_logging` instead.
"""

from __future__ import annotations

import logging

try:
    from rich.console import Console
    from rich.logging import RichHandler
    from rich.progress import (
        BarColumn,
        DownloadColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
        TimeRemainingColumn,
        TransferSpeedColumn,
    )
except ModuleNotFoundError as exc:  # pragma: no cover
    raise ModuleNotFoundError(
        "quantilica_core.cli requires the 'cli' extra; "
        "install quantilica-core[cli]"
    ) from exc


_console: Console | None = None


def get_console() -> Console:
    """Return a process-wide shared Rich console."""
    global _console
    if _console is None:
        _console = Console()
    return _console


def setup_rich_logging(
    verbose: bool,
    *,
    console: Console | None = None,
) -> None:
    """Configure logging via ``RichHandler`` without breaking progress bars.

    ``verbose=False`` → WARNING only; ``verbose=True`` → DEBUG.
    """
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(console=console or get_console(), show_path=False)
        ],
        force=True,
    )


def make_batch_progress(console: Console | None = None) -> Progress:
    """Build a Progress for overall/batch tracking (file counts)."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console or get_console(),
    )


def make_download_progress(console: Console | None = None) -> Progress:
    """Build a Progress for individual file downloads (bytes/speed)."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[dim]{task.description}[/dim]"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console or get_console(),
    )


def expand_years_cli(
    years: list[str] | None,
    default_range: str | None = None,
    console: Console | None = None,
) -> list[int]:
    """Expand CLI year/range arguments (e.g. ``["2020:2022", "2024"]``).

    If ``years`` is empty and ``default_range`` is provided, it expands the default range.
    Prints a warning to the console/stderr for any invalid specs.
    """
    from quantilica_core.dates import expand_year_range

    con = console or get_console()
    specs = years if years else ([default_range] if default_range else [])
    result: list[int] = []
    for arg in specs:
        try:
            result.extend(expand_year_range(arg))
        except ValueError:
            con.print(f"[yellow]Aviso:[/yellow] ano/intervalo inválido '{arg}'")
    return result

