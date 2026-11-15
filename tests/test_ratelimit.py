"""Token bucket, shared 429 backoff and circuit breaker (no network, no sleeping)."""

import httpx2
import pytest

from quantilica.core.exceptions import QuantilicaError
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
    clock.now += 5  # the pause ended: the next refusal is a new one
    assert guard.register_429(None) == 10.0
    clock.now += 10
    assert guard.register_429(None) == 20.0
    guard.acquire(HOST)
    assert clock.slept == [pytest.approx(20.0)]  # a *different* caller waits too
    guard.register_ok()
    assert guard.register_429(None) == 5.0  # streak reset by a good response


def test_guard_retry_after_wins_and_is_capped():
    clock = FakeClock()
    guard = HostGuard(0.0, backoff_base=5.0, backoff_max=60.0, clock=clock)
    assert guard.register_429(7.0) == 7.0
    clock.now += 7
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
    with pytest.raises(QuantilicaError) as info:
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


# --- A1: a half-open probe that is not a success must not wedge the breaker ---


def _open_breaker_client(handler, clock, **policy_kwargs):
    policy = HostPolicy(
        min_interval=0.0, breaker_threshold=1, breaker_cooldown=30.0, **policy_kwargs
    )
    client = _client(handler, policy, clock, attempts=1)
    return client, client._guard_for(URL).breaker


def _trip_and_wait(client, clock):
    """Open the breaker with a 405, then let the cooldown elapse."""
    with pytest.raises(QuantilicaError):
        client.get(URL)
    assert client._guard_for(URL).breaker.state == "open"
    clock.now += 31
    assert client._guard_for(URL).breaker.state == "half-open"


@pytest.mark.parametrize("probe_answer", ["503", "429", "network"])
def test_failed_probe_reopens_instead_of_wedging(probe_answer):
    clock = FakeClock()
    answers = ["405", probe_answer, "200"]

    def handler(request: httpx2.Request) -> httpx2.Response:
        answer = answers.pop(0)
        if answer == "network":
            raise httpx2.ConnectError("boom", request=request)
        if answer == "200":
            return httpx2.Response(200, json=1)
        return httpx2.Response(int(answer))

    client, breaker = _open_breaker_client(handler, clock)
    _trip_and_wait(client, clock)
    with pytest.raises(QuantilicaError):
        client.get(URL)  # the probe
    assert breaker.state == "open"  # re-opened, not stuck "half-open, probing"
    clock.now += 31
    assert client.get_json(URL) == 1  # the next probe is allowed and closes it
    assert breaker.state == "closed"


def test_failed_probe_on_stream_reopens():
    clock = FakeClock()
    answers = [405, 503]

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(answers.pop(0))

    client, breaker = _open_breaker_client(handler, clock)
    _trip_and_wait(client, clock)
    with pytest.raises(QuantilicaError):
        with client.stream("GET", URL):
            pass
    assert breaker.state == "open"


def test_breaker_abandon_and_release_only_touch_their_own_probe():
    clock = FakeClock()
    breaker = CircuitBreaker(1, 30.0, clock=clock)
    breaker.record_failure()
    clock.now += 31
    _, ticket = breaker.enter()
    assert ticket is not None
    breaker.release(ticket)  # neutral answer: slot freed, still half-open
    assert breaker.state == "half-open"
    _, second = breaker.enter()
    breaker.abandon(ticket)  # stale ticket of the first probe: ignored
    assert breaker.enter()[0] > 0  # the second probe is still in flight
    breaker.abandon(second)
    assert breaker.state == "open"


# --- A2: HEAD 405 is neutral ---------------------------------------------------


def test_head_405_does_not_reset_the_failure_count():
    clock = FakeClock()

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(405)

    policy = HostPolicy(min_interval=0.0, breaker_threshold=2)
    client = _client(handler, policy, clock, attempts=1)
    with pytest.raises(QuantilicaError):
        client.get(URL)
    with pytest.raises(QuantilicaError):
        client.head(URL)  # neutral: must not zero the streak
    with pytest.raises(QuantilicaError):
        client.get(URL)
    assert client._guard_for(URL).breaker.state == "open"


def test_head_or_get_pairs_still_open_the_breaker():
    clock = FakeClock()

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(405)

    policy = HostPolicy(min_interval=0.0, breaker_threshold=2)
    client = _client(handler, policy, clock, attempts=1)
    for _ in range(2):  # HEAD 405 then GET 405 per file
        with pytest.raises(QuantilicaError):
            client.head_or_get(URL)
    assert client._guard_for(URL).breaker.state == "open"


def test_head_405_probe_is_neutral_and_does_not_close_half_open():
    clock = FakeClock()
    seen = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        seen.append(request.method)
        return httpx2.Response(405)

    client, breaker = _open_breaker_client(handler, clock)
    _trip_and_wait(client, clock)
    with pytest.raises(QuantilicaError):
        client.head(URL)
    assert breaker.state == "half-open"  # not closed by the HEAD
    assert breaker.enter()[1] is not None  # and the probe slot is free again


def test_head_403_is_neutral_when_403_trips_the_breaker():
    clock = FakeClock()

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(403)

    policy = HostPolicy(
        min_interval=0.0, breaker_threshold=1, breaker_statuses=frozenset({403, 405})
    )
    client = _client(handler, policy, clock, attempts=1)
    for _ in range(3):
        with pytest.raises(QuantilicaError):
            client.head(URL)
    assert client._guard_for(URL).breaker.state == "closed"
    with pytest.raises(QuantilicaError):
        client.get(URL)
    assert client._guard_for(URL).breaker.state == "open"


# --- A3: defaults keep the pre-guard 429 behaviour ------------------------------


def _waits_for_429(url: str, headers: dict[str, str], policy: HostPolicy | None):
    """Seconds slept between 3 attempts that end in 200, for ``url``."""
    clock = FakeClock()
    calls = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        calls.append(request)
        if len(calls) <= 2:
            return httpx2.Response(429, headers=headers)
        return httpx2.Response(200, json=1)

    client = HttpClient(
        transport=httpx2.MockTransport(handler),
        host_policies={HOST: policy} if policy else {},
        attempts=3,
        retry_base_delay=1.0,
        jitter=0.0,
    )
    client._sleep = clock.sleep
    client._clock = clock
    assert client.get_json(url) == 1
    return clock.slept, client


def test_default_policy_429_waits_exactly_like_no_policy():
    policy = HostPolicy(min_interval=0.0)
    with_policy, client = _waits_for_429(URL, {}, policy)
    without_policy, _ = _waits_for_429("https://nopolicy.example.org/x", {}, None)
    assert with_policy == without_policy == [1.0, 2.0]  # retry_base_delay, doubling
    # No host-wide pause was set: another caller does not wait.
    client._guard_for(URL).acquire(HOST)


def test_default_policy_never_cuts_server_retry_after():
    policy = HostPolicy(min_interval=0.0)
    with_policy, _ = _waits_for_429(URL, {"Retry-After": "3600"}, policy)
    without_policy, _ = _waits_for_429(
        "https://nopolicy.example.org/x", {"Retry-After": "3600"}, None
    )
    assert with_policy == without_policy == [3600.0, 3600.0]


def test_default_policy_does_not_pause_the_whole_host():
    clock = FakeClock()
    guard = HostGuard(0.0, clock=clock, sleep=clock.sleep)
    assert guard.register_429(None) is None
    assert guard.register_429(12.0) == 12.0
    guard.acquire(HOST)
    assert clock.slept == []


def test_opt_in_backoff_cap_applies_to_retry_after_only_when_explicit():
    clock = FakeClock()
    open_cap = HostGuard(0.0, backoff_base=5.0, clock=clock)
    assert open_cap.register_429(3600.0) == 3600.0  # not cut without backoff_max
    clock.now += 4000
    assert open_cap.register_429(None) == 10.0  # the second step of the doubling
    clock.now += 10
    closed = HostGuard(0.0, backoff_base=5.0, backoff_max=60.0, clock=clock)
    assert closed.register_429(3600.0) == 60.0


def test_opt_in_doubling_backoff_has_a_default_ceiling():
    clock = FakeClock()
    guard = HostGuard(0.0, backoff_base=100.0, clock=clock)
    delays = []
    for _ in range(4):
        delays.append(guard.register_429(None))
        clock.now += 1000
    assert delays == [100.0, 200.0, 300.0, 300.0]


# --- A4: one burst of 429 is one step of the backoff ----------------------------


def test_simultaneous_429_of_one_burst_count_once():
    clock = FakeClock()
    guard = HostGuard(0.0, backoff_base=5.0, backoff_max=300.0, clock=clock)
    delays = [guard.register_429(None) for _ in range(8)]  # same instant
    assert delays == [5.0] * 8
    clock.now += 5  # the pause is over: the next refusal is a genuine second one
    assert guard.register_429(None) == 10.0


def test_late_429_in_a_burst_returns_the_time_left_and_longer_retry_after_wins():
    clock = FakeClock()
    guard = HostGuard(0.0, backoff_base=10.0, clock=clock)
    assert guard.register_429(None) == 10.0
    clock.now += 4
    assert guard.register_429(None) == pytest.approx(6.0)
    assert guard.register_429(30.0) == pytest.approx(30.0)  # extends the pause
    assert guard._blocked_until == pytest.approx(clock.now + 30.0)


# --- A5: stricter maintenance detection -------------------------------------------

HIDDEN_TEMPLATE = (
    "<!DOCTYPE html><html><head><title>Painel</title></head><body>"
    '<div style="display:none">Access denied</div>'
    '<template id="err"><p>Request rejected</p></template>'
    "<script>var m = 'access denied';</script>"
    "<h1>Painel de dados</h1></body></html>"
)


def test_hidden_markers_in_a_real_page_are_not_maintenance():
    assert not looks_like_maintenance_html("text/html", HIDDEN_TEMPLATE)
    assert not looks_like_maintenance_html("text/html", HIDDEN_TEMPLATE, status=200)


def test_long_page_with_a_marker_is_not_maintenance():
    page = "<html><body><p>Access denied</p>" + "<p>conteúdo</p>" * 2000 + "</body>"
    assert not looks_like_maintenance_html("text/html", page.encode())
    wordy = "<html><body><p>access denied</p>" + "palavra " * 400 + "</body></html>"
    assert not looks_like_maintenance_html("text/html", wordy, status=503)


def test_expected_html_needs_an_error_status():
    page = MAINTENANCE
    assert not looks_like_maintenance_html(
        "text/html", page, status=200, expect_html=True
    )
    assert looks_like_maintenance_html("text/html", page, status=503, expect_html=True)
    assert looks_like_maintenance_html("text/html", page, status=200, expect_html=False)


def test_client_ignores_a_short_html_page_on_an_html_url():
    clock = FakeClock()

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            200, content=MAINTENANCE, headers={"Content-Type": "text/html"}
        )

    policy = HostPolicy(min_interval=0.0, breaker_threshold=1)
    client = _client(handler, policy, clock)
    assert client.get(f"https://{HOST}/status.html").status_code == 200
    assert client._guard_for(URL).breaker.state == "closed"


def test_detect_maintenance_html_can_be_turned_off_keeping_the_405_breaker():
    clock = FakeClock()
    answers = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        if answers:
            return httpx2.Response(405)
        return httpx2.Response(
            200, content=MAINTENANCE, headers={"Content-Type": "text/html"}
        )

    policy = HostPolicy(
        min_interval=0.0, breaker_threshold=1, detect_maintenance_html=False
    )
    client = _client(handler, policy, clock, attempts=1)
    assert client.get(URL).status_code == 200  # page returned, breaker closed
    assert client._guard_for(URL).breaker.state == "closed"
    answers.append(1)
    with pytest.raises(QuantilicaError):
        client.get(URL)
    assert client._guard_for(URL).breaker.state == "open"


def test_stream_does_not_inspect_the_body():
    clock = FakeClock()

    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            200, content=MAINTENANCE, headers={"Content-Type": "text/html"}
        )

    policy = HostPolicy(min_interval=0.0, breaker_threshold=1)
    client = _client(handler, policy, clock)
    with client.stream("GET", URL) as response:
        assert response.status_code == 200
    assert client._guard_for(URL).breaker.state == "closed"
