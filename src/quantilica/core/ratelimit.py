"""Per-host traffic guards: token bucket, shared 429 backoff and circuit breaker.

These are the building blocks behind :class:`quantilica.core.http.HostPolicy`.
They are plain, thread-safe objects with injectable clock and sleep, so they
can be unit-tested without a network and without real waiting.

- :class:`TokenBucket`: paces requests at ``1 / interval`` per second with a
  configurable burst. ``burst=1`` is exactly the classic fixed spacing.
- :class:`HostGuard`: a bucket plus an optional host-wide *penalty* (opt-in:
  a ``429`` then makes every thread wait, not only the one that got the
  refusal) plus an optional :class:`CircuitBreaker`.
- :class:`CircuitBreaker`: opens after N consecutive "host is down" signals
  (``405`` on a non-``HEAD`` request, an HTML maintenance/WAF page) and
  rejects further requests for a cooldown, then lets one probe through. A
  probe that ends without a clear success (5xx, 429, network error) counts as
  a failure and re-opens the circuit.
"""

from __future__ import annotations

import re
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


_BODY_LIMIT = 8192
"""Bytes of body the detector reads; a body that fills it is not "short"."""
_VISIBLE_TEXT_LIMIT = 1500
"""A maintenance/WAF page is a few lines; longer visible text is a real page."""
_HIDDEN_BLOCK = re.compile(
    r"<(?P<tag>[a-z][a-z0-9]*)\b[^>]*"
    r"(?:\bhidden\b|display\s*:\s*none|visibility\s*:\s*hidden)[^>]*>"
    r".*?</(?P=tag)\s*>",
    re.DOTALL,
)
_INVISIBLE_BLOCK = re.compile(
    r"<(script|style|noscript|template)\b.*?</\1\s*>|<!--.*?-->", re.DOTALL
)
_TAG = re.compile(r"<[^>]*>")


def _visible_text(html: str) -> str:
    """Best-effort visible text: no scripts, comments or hidden elements."""
    html = _INVISIBLE_BLOCK.sub(" ", html)
    html = _HIDDEN_BLOCK.sub(" ", html)
    return " ".join(_TAG.sub(" ", html).split()).lower()


def looks_like_maintenance_html(
    content_type: str | None,
    body_prefix: bytes | str | None,
    *,
    status: int | None = None,
    expect_html: bool = False,
) -> bool:
    """Tell whether a response is an HTML maintenance or WAF block page.

    The heuristic is deliberately strict, because a false positive feeds the
    circuit breaker and can lock a healthy host out. All of these must hold:

    - the body is HTML;
    - the body is short (it fits in the inspected prefix and has little
      visible text): a real page is not a one-paragraph block notice;
    - a maintenance marker appears in the *visible* text (the title counts;
      scripts, styles, comments and ``hidden``/``display:none`` elements do
      not, so a template with a hidden "Access denied" string is ignored);
    - the response is suspicious: an error ``status`` (``>= 400``), or HTML
      where the request did not expect HTML (``expect_html=False``).

    Args:
        content_type: The ``Content-Type`` header (may be ``None``).
        body_prefix: The first bytes of the body (``8192`` are inspected; a
            body that fills them is considered long).
        status: HTTP status of the response, when known. An error status makes
            the response suspicious even if HTML was expected.
        expect_html: The request legitimately expects an HTML page (a
            ``.html`` URL, say), so a ``2xx`` HTML body alone is not
            suspicious.

    Returns:
        bool: True when the response looks like a maintenance/WAF page.
    """
    if not body_prefix:
        return False
    if len(body_prefix) >= _BODY_LIMIT:
        return False
    if isinstance(body_prefix, bytes):
        text = body_prefix.decode("utf-8", errors="ignore")
    else:
        text = body_prefix
    head = text.lstrip("\ufeff \t\r\n").lower()
    is_html = "html" in (content_type or "").lower() or head.startswith(_HTML_PREFIXES)
    if not is_html:
        return False
    if expect_html and not (status is not None and status >= 400):
        return False
    visible = _visible_text(head)
    if len(visible) > _VISIBLE_TEXT_LIMIT:
        return False
    return any(marker in visible for marker in _MAINTENANCE_MARKERS)


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
        self._probe_gen = 0

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

    def enter(self) -> tuple[float, int | None]:
        """Ask to send a request.

        Returns:
            tuple[float, int | None]: ``(0.0, None)`` to proceed normally (a
            closed circuit), ``(0.0, ticket)`` when this caller is the
            half-open *probe* (it must report back with :meth:`record_success`,
            :meth:`record_failure`, :meth:`release` or :meth:`abandon`), or
            ``(seconds_left, None)`` when the request must not be sent.
        """
        if self.threshold <= 0:
            return 0.0, None
        with self._lock:
            state = self._state_locked()
            if state == "closed":
                return 0.0, None
            if state == "half-open":
                if self._probing:
                    return max(self.cooldown, 1.0), None
                self._probing = True
                self._probe_gen += 1
                return 0.0, self._probe_gen
            assert self._opened_at is not None
            left = max(0.0, self.cooldown - (self._clock() - self._opened_at))
            return left, None

    def before_request(self) -> float:
        """Return ``0`` to proceed, or the seconds left while open.

        In ``half-open`` exactly one caller (the probe) gets ``0``; the others
        are rejected until the probe reports back. Prefer :meth:`enter`, which
        also hands the probe its ticket.
        """
        return self.enter()[0]

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

    def abandon(self, ticket: int | None) -> None:
        """The probe ended without a verdict (5xx, 429, network error).

        Anything that is not a success counts as a failure of the probe and
        re-opens the circuit; without this the probe slot would stay taken
        forever and every later request would be refused. A no-op when
        ``ticket`` is ``None`` or the probe already reported.
        """
        if ticket is None:
            return
        with self._lock:
            if self._probing and ticket == self._probe_gen:
                self._failures += 1
                self._opened_at = self._clock()
                self._probing = False

    def release(self, ticket: int | None) -> None:
        """The probe got a *neutral* answer: free the slot, keep half-open.

        The next caller probes again. A no-op when ``ticket`` is ``None`` or
        the probe already reported.
        """
        if ticket is None:
            return
        with self._lock:
            if self._probing and ticket == self._probe_gen:
                self._probing = False


class HostGuard:
    """Everything the client enforces for one host: pacing, penalty, breaker.

    The host-wide ``429`` penalty is opt-in: it is on only when
    ``backoff_base`` is given. Without it a ``429`` changes nothing here and
    the caller keeps its own retry delay (``Retry-After`` honoured as sent, or
    its own exponential delay), exactly as before this guard existed.
    """

    DEFAULT_BACKOFF_MAX = 300.0
    """Cap of the doubling backoff when ``backoff_max`` is not set."""

    def __init__(
        self,
        interval: float,
        *,
        burst: int = 1,
        backoff_base: float | None = None,
        backoff_max: float | None = None,
        breaker_threshold: int = 0,
        breaker_cooldown: float = 300.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Initialize the guard.

        Args:
            interval: Seconds between requests (token refill time).
            burst: Token bucket capacity.
            backoff_base: ``None`` (default) leaves ``429`` handling to the
                caller. A number turns on the host-wide pause: the first
                backoff (seconds) for a ``429`` without ``Retry-After``,
                doubling per consecutive ``429``.
            backoff_max: Only with ``backoff_base``. ``None`` caps the
                doubling backoff at :attr:`DEFAULT_BACKOFF_MAX` and never cuts
                a server ``Retry-After``; a number caps every pause,
                ``Retry-After`` included.
            breaker_threshold: Consecutive "host is down" signals that open
                the circuit; ``0`` disables the breaker.
            breaker_cooldown: Seconds the circuit stays open.
            clock: Monotonic clock (injectable for tests).
            sleep: Sleep function (injectable for tests).
        """
        self.backoff_base = None if backoff_base is None else max(0.0, backoff_base)
        self.backoff_max = None if backoff_max is None else max(0.0, backoff_max)
        self._clock = clock
        self._sleep = sleep
        self.bucket = TokenBucket(interval, burst, clock=clock, sleep=sleep)
        self.breaker = CircuitBreaker(breaker_threshold, breaker_cooldown, clock=clock)
        self._lock = threading.Lock()
        self._blocked_until = 0.0
        self._consecutive_429 = 0

    def acquire(self, host: str) -> int | None:
        """Wait for the host-wide penalty and the bucket; raise if breaker open.

        Returns:
            int | None: The probe ticket when this request is the half-open
            probe of the circuit breaker, else ``None``. The caller must
            settle it with :meth:`settle` (or by registering the response).

        Raises:
            CircuitOpenError: The circuit breaker is open for this host.
        """
        retry_in, ticket = self.breaker.enter()
        if retry_in > 0:
            raise CircuitOpenError(host, retry_in)
        try:
            with self._lock:
                penalty = self._blocked_until - self._clock()
            if penalty > 0:
                self._sleep(penalty)
            self.bucket.acquire()
        except BaseException:
            self.breaker.release(ticket)
            raise
        return ticket

    def settle(self, ticket: int | None) -> None:
        """Close the books on a request: a probe with no verdict is a failure."""
        self.breaker.abandon(ticket)

    def register_429(self, retry_after: float | None) -> float | None:
        """Record a ``429``; with the pause on, block the whole host.

        Concurrent ``429`` of one burst count once: a refusal that arrives
        while the pause it triggered is still running (``now`` before the
        current block) does not escalate the streak, it only returns the time
        left (or extends the pause when its ``Retry-After`` is longer).

        Args:
            retry_after: Parsed ``Retry-After`` seconds, if the server sent one
                (an HTML 429 body usually comes without it).

        Returns:
            float | None: The seconds to wait before retrying. With the pause
            off (no ``backoff_base``): the ``Retry-After`` as sent, or
            ``None`` when there is none (the caller picks its own delay).
        """
        has_header = retry_after is not None and retry_after > 0
        if self.backoff_base is None:
            return float(retry_after) if has_header and retry_after else None
        with self._lock:
            now = self._clock()
            if has_header:
                header = float(retry_after or 0.0)
                if self.backoff_max is not None:
                    header = min(header, self.backoff_max)
            else:
                header = 0.0
            if now < self._blocked_until:  # same burst: do not escalate
                if now + header > self._blocked_until:
                    self._blocked_until = now + header
                return self._blocked_until - now
            self._consecutive_429 += 1
            if has_header:
                delay = header
            else:
                delay = self.backoff_base * (2 ** (self._consecutive_429 - 1))
                delay = min(
                    delay,
                    self.DEFAULT_BACKOFF_MAX
                    if self.backoff_max is None
                    else self.backoff_max,
                )
            self._blocked_until = max(self._blocked_until, now + delay)
        return delay

    def register_ok(self) -> None:
        """A normal response resets the 429 streak and the breaker."""
        with self._lock:
            self._consecutive_429 = 0
        self.breaker.record_success()

    def register_down(self) -> None:
        """A maintenance/405 signal: feed the circuit breaker."""
        self.breaker.record_failure()

    def register_neutral(self, ticket: int | None = None) -> None:
        """A response that says nothing about the host (e.g. ``HEAD`` ``405``).

        Neither resets the failure count nor closes a half-open circuit; a
        half-open probe that got it frees its slot for the next caller.
        """
        self.breaker.release(ticket)
