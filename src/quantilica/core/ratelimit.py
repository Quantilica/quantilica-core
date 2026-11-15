"""Per-host traffic guards: token bucket, shared 429 backoff and circuit breaker.

These are the building blocks behind :class:`quantilica.core.http.HostPolicy`.
They are plain, thread-safe objects with injectable clock and sleep, so they
can be unit-tested without a network and without real waiting.

- :class:`TokenBucket`: paces requests at ``1 / interval`` per second with a
  configurable burst. ``burst=1`` is exactly the classic fixed spacing.
- :class:`HostGuard`: a bucket plus a host-wide *penalty* (set by a ``429``
  so every thread waits, not only the one that got the refusal) plus an
  optional :class:`CircuitBreaker`.
- :class:`CircuitBreaker`: opens after N consecutive "host is down" signals
  (``405`` on a non-``HEAD`` request, an HTML maintenance/WAF page) and
  rejects further requests for a cooldown, then lets one probe through.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Literal

from .exceptions import FetchError

__all__ = [
    "CircuitBreaker",
    "CircuitOpenError",
    "HostGuard",
    "TokenBucket",
    "looks_like_maintenance_html",
]

# Markers (lower-case) of maintenance / WAF block pages served by public hosts,
# usually with status 200 or 5xx and ``text/html``.
_MAINTENANCE_MARKERS = (
    "em manutenção",
    "em manutencao",
    "manutenção programada",
    "serviço indisponível",
    "servico indisponivel",
    "temporarily unavailable",
    "under maintenance",
    "request rejected",
    "access denied",
)
_HTML_PREFIXES = ("<!doctype html", "<html", "<head", "<body", "<script")


class CircuitOpenError(FetchError):
    """The circuit breaker of a host is open: the request was not sent.

    Deliberately **not** retryable: retrying against a host that is in
    maintenance only prolongs the block. ``retry_in`` is the number of
    seconds until the breaker lets a probe through.
    """

    def __init__(self, host: str, retry_in: float) -> None:
        super().__init__(
            f"Circuit breaker open for {host}; next probe in {retry_in:.0f}s"
        )
        self.host = host
        self.retry_in = retry_in


def looks_like_maintenance_html(
    content_type: str | None, body_prefix: bytes | str | None
) -> bool:
    """Tell whether a response is an HTML maintenance or WAF block page.

    Args:
        content_type: The ``Content-Type`` header (may be ``None``).
        body_prefix: The first bytes of the body (a few KiB are enough).

    Returns:
        bool: True when the body is HTML and carries a maintenance marker.
    """
    if not body_prefix:
        return False
    if isinstance(body_prefix, bytes):
        text = body_prefix[:4096].decode("utf-8", errors="ignore")
    else:
        text = body_prefix[:4096]
    head = text.lstrip("﻿ \t\r\n").lower()
    is_html = "html" in (content_type or "").lower() or head.startswith(_HTML_PREFIXES)
    if not is_html:
        return False
    return any(marker in head for marker in _MAINTENANCE_MARKERS)


class TokenBucket:
    """Thread-safe token bucket with reserved (never ``now``-based) slots.

    Tokens refill at ``1 / interval`` per second up to ``burst``. Each
    :meth:`acquire` takes one token; when the bucket is empty the caller
    reserves a *future* slot (the balance goes negative) and sleeps outside
    the lock. Advancing from the reserved slot, not from a pre-sleep ``now``,
    keeps N consecutive acquires at least ``(N - burst) * interval`` apart.
    """

    def __init__(
        self,
        interval: float,
        burst: int = 1,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Initialize the bucket.

        Args:
            interval: Seconds to refill one token. ``<= 0`` disables pacing.
            burst: Bucket capacity (requests allowed back to back). ``>= 1``.
            clock: Monotonic clock (injectable for tests).
            sleep: Sleep function (injectable for tests).
        """
        self.interval = interval
        self.burst = max(1, int(burst))
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._tokens = float(self.burst)
        self._updated = clock()

    def reserve(self) -> float:
        """Take one token and return how long the caller must wait (seconds)."""
        if self.interval <= 0:
            return 0.0
        with self._lock:
            now = self._clock()
            elapsed = max(0.0, now - self._updated)
            self._updated = now
            self._tokens = min(
                float(self.burst), self._tokens + elapsed / self.interval
            )
            self._tokens -= 1.0
            if self._tokens >= 0:
                return 0.0
            return -self._tokens * self.interval

    def acquire(self) -> float:
        """Reserve a token, sleeping if needed. Returns the time slept."""
        delay = self.reserve()
        if delay > 0:
            self._sleep(delay)
        return delay


class CircuitBreaker:
    """Consecutive-failure circuit breaker (closed, open, half-open)."""

    def __init__(
        self,
        threshold: int,
        cooldown: float,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Initialize the breaker.

        Args:
            threshold: Consecutive failures that open the circuit. ``<= 0``
                disables the breaker (it never opens).
            cooldown: Seconds the circuit stays open before a probe.
            clock: Monotonic clock (injectable for tests).
        """
        self.threshold = int(threshold)
        self.cooldown = float(cooldown)
        self._clock = clock
        self._lock = threading.Lock()
        self._failures = 0
        self._opened_at: float | None = None
        self._probing = False

    @property
    def state(self) -> Literal["closed", "open", "half-open"]:
        """Current state (``half-open`` once the cooldown elapsed)."""
        with self._lock:
            return self._state_locked()

    def _state_locked(self) -> Literal["closed", "open", "half-open"]:
        if self._opened_at is None:
            return "closed"
        if self._clock() - self._opened_at >= self.cooldown:
            return "half-open"
        return "open"

    def before_request(self) -> float:
        """Return ``0`` to proceed, or the seconds left while open.

        In ``half-open`` exactly one caller (the probe) gets ``0``; the others
        are rejected until the probe reports back.
        """
        if self.threshold <= 0:
            return 0.0
        with self._lock:
            state = self._state_locked()
            if state == "closed":
                return 0.0
            if state == "half-open":
                if self._probing:
                    return max(self.cooldown, 1.0)
                self._probing = True
                return 0.0
            assert self._opened_at is not None
            return max(0.0, self.cooldown - (self._clock() - self._opened_at))

    def record_success(self) -> None:
        """A normal response closes the circuit and clears the count."""
        with self._lock:
            self._failures = 0
            self._opened_at = None
            self._probing = False

    def record_failure(self) -> None:
        """A "host is down" signal; opens (or re-opens) at the threshold."""
        if self.threshold <= 0:
            return
        with self._lock:
            self._failures += 1
            if self._probing or self._failures >= self.threshold:
                self._opened_at = self._clock()
                self._probing = False


class HostGuard:
    """Everything the client enforces for one host: pacing, penalty, breaker."""

    def __init__(
        self,
        interval: float,
        *,
        burst: int = 1,
        backoff_base: float = 5.0,
        backoff_max: float = 300.0,
        breaker_threshold: int = 0,
        breaker_cooldown: float = 300.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Initialize the guard.

        Args:
            interval: Seconds between requests (token refill time).
            burst: Token bucket capacity.
            backoff_base: First backoff (seconds) for a ``429`` without
                ``Retry-After``; doubles per consecutive ``429``.
            backoff_max: Cap for any backoff, ``Retry-After`` included.
            breaker_threshold: Consecutive "host is down" signals that open
                the circuit; ``0`` disables the breaker.
            breaker_cooldown: Seconds the circuit stays open.
            clock: Monotonic clock (injectable for tests).
            sleep: Sleep function (injectable for tests).
        """
        self.backoff_base = max(0.0, backoff_base)
        self.backoff_max = max(0.0, backoff_max)
        self._clock = clock
        self._sleep = sleep
        self.bucket = TokenBucket(interval, burst, clock=clock, sleep=sleep)
        self.breaker = CircuitBreaker(breaker_threshold, breaker_cooldown, clock=clock)
        self._lock = threading.Lock()
        self._blocked_until = 0.0
        self._consecutive_429 = 0

    def acquire(self, host: str) -> None:
        """Wait for the host-wide penalty and the bucket; raise if breaker open.

        Raises:
            CircuitOpenError: The circuit breaker is open for this host.
        """
        retry_in = self.breaker.before_request()
        if retry_in > 0:
            raise CircuitOpenError(host, retry_in)
        with self._lock:
            penalty = self._blocked_until - self._clock()
        if penalty > 0:
            self._sleep(penalty)
        self.bucket.acquire()

    def register_429(self, retry_after: float | None) -> float:
        """Record a ``429`` and block the whole host for the backoff.

        Args:
            retry_after: Parsed ``Retry-After`` seconds, if the server sent one
                (an HTML 429 body usually comes without it).

        Returns:
            float: The backoff applied, in seconds (already capped).
        """
        with self._lock:
            self._consecutive_429 += 1
            if retry_after is not None and retry_after > 0:
                delay = retry_after
            else:
                delay = self.backoff_base * (2 ** (self._consecutive_429 - 1))
            delay = min(delay, self.backoff_max)
            self._blocked_until = max(self._blocked_until, self._clock() + delay)
        return delay

    def register_ok(self) -> None:
        """A normal response resets the 429 streak and the breaker."""
        with self._lock:
            self._consecutive_429 = 0
        self.breaker.record_success()

    def register_down(self) -> None:
        """A maintenance/405 signal: feed the circuit breaker."""
        self.breaker.record_failure()
