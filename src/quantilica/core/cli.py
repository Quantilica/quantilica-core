"""Shared Rich-based helpers for quantilica fetcher CLI plugins.

These helpers are host-only: ``plugin.py`` modules run inside the
``quantilica-cli`` host, which pulls in ``rich`` via the ``cli`` extra
(``quantilica-core[cli]``). Standalone argparse CLIs should keep using
:func:`quantilica.core.logging.configure_cli_logging` instead.
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import logging
import threading
from collections.abc import Callable, Generator

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
        "quantilica.core.cli requires the 'cli' extra; install quantilica-core[cli]"
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
        handlers=[RichHandler(console=console or get_console(), show_path=False)],
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

    If ``years`` is empty and ``default_range`` is provided, it expands the
    default range. Prints a warning to the console/stderr for any invalid specs.
    """
    from quantilica.core.dates import expand_year_range

    con = console or get_console()
    specs = years if years else ([default_range] if default_range else [])
    result: list[int] = []
    for arg in specs:
        try:
            result.extend(expand_year_range(arg))
        except ValueError:
            con.print(f"[yellow]Aviso:[/yellow] ano/intervalo inválido '{arg}'")
    return result


class ProgressPool:
    """Manages a fixed pool of rich progress bars for concurrent workers."""

    def __init__(self, workers: int, file_prog: Progress):
        self.lock = threading.Lock()
        self.file_prog = file_prog
        self.available = [
            file_prog.add_task("[dim]Inativo[/dim]", total=1) for _ in range(workers)
        ]

    @contextlib.contextmanager
    def acquire(
        self, description: str
    ) -> Generator[Callable[[int, int], None], None, None]:
        with self.lock:
            task_id = self.available.pop(0)
        self.file_prog.update(task_id, description=description, completed=0, total=None)

        def update_cb(downloaded: int, total: int) -> None:
            if downloaded == 0 and total == 0:
                self.file_prog.update(task_id, completed=0)
                return
            self.file_prog.update(task_id, completed=downloaded, total=total or None)

        try:
            yield update_cb
        finally:
            with self.lock:
                self.file_prog.update(
                    task_id,
                    description="[dim]Inativo[/dim]",
                    completed=0,
                    total=1,
                )
                self.available.append(task_id)


@contextlib.contextmanager
def graceful_executor(
    max_workers: int,
) -> Generator[concurrent.futures.ThreadPoolExecutor, None, None]:
    """ThreadPoolExecutor that cancels futures and shuts down on KeyboardInterrupt."""
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    # Python 3.9+ supports cancel_futures=True, which automatically
    # cancels pending futures during shutdown.
    try:
        yield executor
        executor.shutdown(wait=True)
    except KeyboardInterrupt:
        executor.shutdown(wait=False, cancel_futures=True)
        raise
