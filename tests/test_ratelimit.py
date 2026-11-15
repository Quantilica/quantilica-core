"""Token bucket, shared 429 backoff and circuit breaker (no network, no sleeping)."""

import httpx2
import pytest

from quantilica.core.http import (
    HostPolicy,
    HttpClient,
    HttpStatusError,
    MaintenancePageError,
    RetryableHttpStatusError,
)
from quantilica.core.ratelimit import (
    CircuitBreaker,
    CircuitOpenError,
    HostGuard,
    TokenBucket,
    looks_like_maintenance_html,
)

HOST = "api.example.com"
URL = f"https://{HOST}/data"
HTML_429 = b"<html><body><h1>429 Too Many Requests</h1></body></html>"
MAINTENANCE = "<!DOCTYPE html><html>Serviço em manutenção</html>".encode()


class FakeClock:
    """Monotonic clock whose ``sleep`` just advances it and records the call."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


# --- TokenBucket ------------------------------------------------------------


def test_bucket_burst_then_spacing():
    clock = FakeClock()
    bucket = TokenBucket(1.0, burst=3, clock=clock, sleep=clock.sleep)
    waits = [bucket.acquire() for _ in range(6)]
    assert waits[:3] == [0.0, 0.0, 0.0]
    assert waits[3:] == [1.0, 1.0, 1.0]


def test_bucket_burst_one_is_fixed_spacing():
    clock = FakeClock()
    bucket = TokenBucket(2.0, clock=clock, sleep=clock.sleep)
    start = clock.now
    for _ in range(5):
        bucket.acquire()
    # N acquires cost at least (N - 1) * interval: no collapse into a burst.
    assert clock.now - start == pytest.approx(8.0)


def test_bucket_refills_while_idle_up_to_burst():
    clock = FakeClock()
    bucket = TokenBucket(1.0, burst=2, clock=clock, sleep=clock.sleep)
    bucket.acquire()
    bucket.acquire()
    clock.now += 10  # long idle: refills only up to the burst
    assert [bucket.acquire() for _ in range(3)] == [0.0, 0.0, 1.0]


def test_bucket_disabled_for_non_positive_interval():
    clock = FakeClock()
    bucket = TokenBucket(0.0, clock=clock, sleep=clock.sleep)
    assert [bucket.acquire() for _ in range(5)] == [0.0] * 5
    assert clock.slept == []


# --- CircuitBreaker ---------------------------------------------------------


def test_breaker_opens_then_probes_then_closes():
    clock = FakeClock()
    breaker = CircuitBreaker(2, 60.0, clock=clock)
    breaker.record_failure()
    assert breaker.state == "closed"
    breaker.record_failure()
    assert breaker.state == "open"
    assert breaker.before_request() == pytest.approx(60.0)
    clock.now += 61
    assert breaker.state == "half-open"
    assert breaker.before_request() == 0.0  # the probe
    assert breaker.before_request() > 0  # others wait for the probe
    breaker.record_success()
    assert breaker.state == "closed"


def test_breaker_failed_probe_reopens():
    clock = FakeClock()
    breaker = CircuitBreaker(1, 30.0, clock=clock)
    breaker.record_failure()
    clock.now += 31
    assert breaker.before_request() == 0.0
    breaker.record_failure()
    assert breaker.state == "open"


def test_breaker_success_resets_the_streak():
    breaker = CircuitBreaker(2, 30.0)
    breaker.record_failure()
    breaker.record_success()
    breaker.record_failure()
    assert breaker.state == "closed"


def test_breaker_disabled_with_zero_threshold():
    breaker = CircuitBreaker(0, 30.0)
    for _ in range(10):
        breaker.record_failure()
    assert breaker.before_request() == 0.0


# --- HostGuard --------------------------------------------------------------


def test_guard_429_blocks_every_caller_and_doubles():
    clock = FakeClock()
    guard = HostGuard(
        0.0, backoff_base=5.0, backoff_max=100.0, clock=clock, sleep=clock.sleep
    )
    assert guard.register_429(None) == 5.0
    assert guard.register_429(None) == 10.0
    assert guard.register_429(None) == 20.0
    guard.acquire(HOST)
    assert clock.slept == [pytest.approx(20.0)]  # a *different* caller waits too
    guard.register_ok()
    assert guard.register_429(None) == 5.0  # streak reset by a good response


def test_guard_retry_after_wins_and_is_capped():
    guard = HostGuard(0.0, backoff_base=5.0, backoff_max=60.0)
    assert guard.register_429(7.0) == 7.0
    assert guard.register_429(3600.0) == 60.0


def test_guard_raises_when_breaker_open():
    clock = FakeClock()
    guard = HostGuard(
        0.0, breaker_threshold=1, breaker_cooldown=50.0, clock=clock, sleep=clock.sleep
    )
    guard.register_down()
    with pytest.raises(CircuitOpenError) as info:
        guard.acquire(HOST)
    assert info.value.host == HOST
    assert info.value.retry_in == pytest.approx(50.0)


# --- maintenance page detection ---------------------------------------------


def test_looks_like_maintenance_html():
    assert looks_like_maintenance_html("text/html; charset=utf-8", MAINTENANCE)
    assert looks_like_maintenance_html(None, b"<html>Request Rejected</html>")
    assert not looks_like_maintenance_html("application/json", b'{"a": 1}')
    assert not looks_like_maintenance_html("text/html", b"<html>tudo certo</html>")
    assert not looks_like_maintenance_html("text/html", None)


# --- HttpClient integration (fake transport) ----------------------------------


def _client(handler, policy: HostPolicy, clock: FakeClock, **kwargs) -> HttpClient:
    client = HttpClient(
        transport=httpx2.MockTransport(handler),
        host_policies={HOST: policy},
        attempts=kwargs.pop("attempts", 3),
        jitter=0.0,
        **kwargs,
    )
    client._sleep = clock.sleep
    client._clock = clock
    return client


def test_html_429_without_retry_after_backs_off_then_succeeds():
    clock = FakeClock()
    calls = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        calls.append(request)
        if len(calls) <= 2:
            return httpx2.Response(
                429, content=HTML_429, headers={"Content-Type": "text/html"}
            )
        return httpx2.Response(200, json={"ok": True})

    policy = HostPolicy(min_interval=0.0, backoff_base=4.0)
    client = _client(handler, policy, clock)
    assert client.get_json(URL) == {"ok": True}
    assert len(calls) == 3
    # 4 s after the first 429, 8 s after the second.
    assert sum(clock.slept) == pytest.approx(12.0)


def test_429_retry_after_header_is_honoured():
    clock = FakeClock()
    calls = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        calls.append(request)
        if len(calls) == 1:
            return httpx2.Response(429, headers={"Retry-After": "9"})
        return httpx2.Response(200, json={"ok": True})

    client = _client(handler, HostPolicy(min_interval=0.0), clock)
    assert client.get_json(URL) == {"ok": True}
    assert sum(clock.slept) == pytest.approx(9.0)


def test_429_exhausts_attempts_with_retryable_error():
    clock = FakeClock()

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(429, content=HTML_429)

    client = _client(handler, HostPolicy(min_interval=0.0), clock, attempts=2)
    with pytest.raises(Exception) as info:
        client.get(URL)
    cause = info.value.__cause__
    assert isinstance(cause, RetryableHttpStatusError)
    assert cause.status_code == 429


def test_policy_does_not_change_hosts_without_policy():
    clock = FakeClock()

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, json=[])

    client = _client(handler, HostPolicy(min_interval=5.0), clock)
    client.get("https://other.example.org/x")
    client.get("https://other.example.org/x")
    assert clock.slept == []


def test_burst_allows_back_to_back_requests_then_spaces():
    clock = FakeClock()
    client = HttpClient(host_policies={HOST: HostPolicy(min_interval=2.0, burst=2)})
    client._sleep = clock.sleep
    client._clock = clock
    client._acquire_rate_limits(URL)
    client._acquire_rate_limits(URL)
    assert clock.slept == []
    client._acquire_rate_limits(URL)
    assert clock.slept == [pytest.approx(2.0)]


def test_breaker_opens_on_405_and_stops_sending():
    clock = FakeClock()
    calls = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        calls.append(request)
        return httpx2.Response(405)

    policy = HostPolicy(min_interval=0.0, breaker_threshold=2, breaker_cooldown=60.0)
    client = _client(handler, policy, clock)
    for _ in range(2):
        with pytest.raises(HttpStatusError):
            client.get(URL)
    with pytest.raises(CircuitOpenError):
        client.get(URL)
    assert len(calls) == 2  # the third never left the process


def test_405_on_head_does_not_feed_the_breaker():
    clock = FakeClock()

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(405)

    policy = HostPolicy(min_interval=0.0, breaker_threshold=1)
    client = _client(handler, policy, clock)
    for _ in range(3):
        with pytest.raises(HttpStatusError):
            client.head(URL)
    assert client._guard_for(URL).breaker.state == "closed"


def test_breaker_counts_html_maintenance_page_and_opens():
    clock = FakeClock()
    calls = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        calls.append(request)
        return httpx2.Response(
            200, content=MAINTENANCE, headers={"Content-Type": "text/html"}
        )

    policy = HostPolicy(min_interval=0.0, breaker_threshold=2)
    client = _client(handler, policy, clock)
    for _ in range(2):
        with pytest.raises(MaintenancePageError):
            client.get(URL)
    with pytest.raises(CircuitOpenError):
        client.get(URL)
    assert len(calls) == 2


def test_breaker_recovers_after_cooldown_with_good_response():
    clock = FakeClock()
    state = {"down": True}

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(405) if state["down"] else httpx2.Response(200, json=1)

    policy = HostPolicy(min_interval=0.0, breaker_threshold=1, breaker_cooldown=30.0)
    client = _client(handler, policy, clock)
    with pytest.raises(HttpStatusError):
        client.get(URL)
    with pytest.raises(CircuitOpenError):
        client.get(URL)
    guard = client._guard_for(URL)
    clock.now += 31  # cooldown elapsed
    state["down"] = False
    assert client.get_json(URL) == 1
    assert guard.breaker.state == "closed"


def test_breaker_off_by_default_html_page_is_returned():
    clock = FakeClock()

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            200, content=MAINTENANCE, headers={"Content-Type": "text/html"}
        )

    client = _client(handler, HostPolicy(min_interval=0.0), clock)
    assert client.get(URL).status_code == 200


def test_stream_429_registers_host_backoff():
    clock = FakeClock()

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(429, content=HTML_429)

    client = _client(handler, HostPolicy(min_interval=0.0, backoff_base=3.0), clock)
    with pytest.raises(RetryableHttpStatusError) as info:
        with client.stream("GET", URL):
            pass
    assert info.value.retry_after == 3.0
    client._guard_for(URL).acquire(HOST)
    assert clock.slept  # the next caller waits for the penalty
