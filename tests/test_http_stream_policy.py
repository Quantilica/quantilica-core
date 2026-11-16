"""``HostPolicy`` em ``stream()``: concorrência, ritmo e 429 num servidor local."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from quantilica.core.http import (
    HostPolicy,
    HttpClient,
    HttpStatusError,
    RetryableHttpStatusError,
)

HOST = "127.0.0.1"
BODY = b"x" * 4096
# Intervalo longo e tolerância relativa: o relógio do Windows tem 15,6 ms de
# resolução e a máquina sob carga atrasa as threads.
INTERVALO = 0.2
TOLERANCIA = 0.5


class FakeServer:
    """Servidor HTTP local que mede simultaneidade e instantes de chegada."""

    def __init__(self, *, body_delay: float = 0.15) -> None:
        self.body_delay = body_delay
        self.arrivals: list[tuple[float, str]] = []
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()
        self.responder: Callable[[BaseHTTPRequestHandler, str], bool] | None = None
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args: object) -> None:  # silencia o stderr
                pass

            def do_GET(self) -> None:  # noqa: N802
                with outer.lock:
                    outer.arrivals.append((time.monotonic(), self.path))
                    outer.active += 1
                    outer.max_active = max(outer.max_active, outer.active)
                try:
                    if outer.responder is not None and outer.responder(self, self.path):
                        return
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(BODY)))
                    self.end_headers()
                    self.wfile.write(BODY[:1024])
                    self.wfile.flush()
                    time.sleep(outer.body_delay)
                    self.wfile.write(BODY[1024:])
                finally:
                    with outer.lock:
                        outer.active -= 1

        self.httpd = ThreadingHTTPServer((HOST, 0), Handler)
        self.httpd.daemon_threads = True
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def url(self, path: str = "/f") -> str:
        return f"http://{HOST}:{self.port}{path}"

    def __enter__(self) -> FakeServer:
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)


def consume(client: HttpClient, url: str) -> int:
    with client.stream("GET", url) as response:
        return len(b"".join(response.iter_bytes()))


def run_parallel(fn: Callable[[int], object], n: int, workers: int = 8) -> list:
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(fn, range(n)))


def gaps(arrivals: list[tuple[float, str]]) -> list[float]:
    times = sorted(t for t, _ in arrivals)
    return [b - a for a, b in zip(times, times[1:], strict=False)]


def test_stream_respeita_max_concurrency():
    with FakeServer() as server:
        client = HttpClient(
            host_policies={HOST: HostPolicy(min_interval=0.0, max_concurrency=2)}
        )
        sizes = run_parallel(lambda i: consume(client, server.url(f"/f{i}")), 8)
    assert sizes == [len(BODY)] * 8
    assert server.max_active == 2


def test_stream_respeita_min_interval():
    with FakeServer(body_delay=0.5) as server:
        client = HttpClient(
            host_policies={HOST: HostPolicy(min_interval=INTERVALO, max_concurrency=3)}
        )
        run_parallel(lambda i: consume(client, server.url(f"/f{i}")), 6)
    assert len(server.arrivals) == 6
    assert min(gaps(server.arrivals)) >= TOLERANCIA * INTERVALO
    assert server.max_active <= 3


def test_request_e_stream_dividem_o_mesmo_limite():
    """HEAD/GET simples e stream disputam as mesmas vagas e o mesmo ritmo."""
    with FakeServer(body_delay=0.3) as server:
        client = HttpClient(
            host_policies={HOST: HostPolicy(min_interval=INTERVALO, max_concurrency=2)}
        )

        def job(i: int) -> object:
            if i % 2:
                return client.get(server.url(f"/g{i}")).status_code
            return consume(client, server.url(f"/s{i}"))

        run_parallel(job, 6)
    assert len(server.arrivals) == 6
    assert server.max_active <= 2
    assert min(gaps(server.arrivals)) >= TOLERANCIA * INTERVALO


def test_429_com_retry_after_pausa_todas_as_threads():
    with FakeServer() as server:
        t429: list[float] = []

        def responder(handler: BaseHTTPRequestHandler, path: str) -> bool:
            if path != "/primeiro":
                return False
            t429.append(time.monotonic())
            handler.send_response(429)
            handler.send_header("Retry-After", "1")
            handler.send_header("Content-Length", "0")
            handler.end_headers()
            return True

        server.responder = responder
        client = HttpClient(
            attempts=1,
            host_policies={
                HOST: HostPolicy(min_interval=0.0, max_concurrency=4, backoff_base=0.5)
            },
        )
        with pytest.raises(RetryableHttpStatusError):
            consume(client, server.url("/primeiro"))
        # O 429 já aconteceu: as demais threads esperam a pausa do host.
        t0 = time.monotonic()
        run_parallel(lambda i: consume(client, server.url(f"/outro{i}")), 4)
    chegadas = [t for t, p in server.arrivals if p.startswith("/outro")]
    assert len(chegadas) == 4
    assert min(chegadas) - t429[0] >= 0.9
    assert time.monotonic() - t0 >= 0.9


def test_vaga_liberada_se_o_corpo_levanta_excecao():
    with FakeServer(body_delay=0.0) as server:
        client = HttpClient(
            host_policies={HOST: HostPolicy(min_interval=0.0, max_concurrency=1)}
        )
        for _ in range(3):
            with pytest.raises(RuntimeError, match="boom"):
                with client.stream("GET", server.url()):
                    raise RuntimeError("boom")
        assert consume(client, server.url()) == len(BODY)


def test_vaga_liberada_se_o_status_e_erro():
    with FakeServer(body_delay=0.0) as server:

        def responder(handler: BaseHTTPRequestHandler, path: str) -> bool:
            handler.send_response(404)
            handler.send_header("Content-Length", "0")
            handler.end_headers()
            return True

        server.responder = responder
        client = HttpClient(
            attempts=1,
            host_policies={HOST: HostPolicy(min_interval=0.0, max_concurrency=1)},
        )
        for _ in range(3):
            with pytest.raises(HttpStatusError):
                consume(client, server.url())
        server.responder = None
        assert consume(client, server.url()) == len(BODY)


def test_vaga_liberada_se_o_consumidor_abandona_o_gerenciador():
    """Um ``stream()`` aberto e descartado sem ``__exit__`` devolve a vaga."""
    import gc

    with FakeServer(body_delay=0.0) as server:
        client = HttpClient(
            host_policies={HOST: HostPolicy(min_interval=0.0, max_concurrency=1)}
        )
        ctx = client.stream("GET", server.url())
        ctx.__enter__()
        del ctx
        gc.collect()
        resultado: list[int] = []
        t = threading.Thread(
            target=lambda: resultado.append(consume(client, server.url())),
            daemon=True,
        )
        t.start()
        t.join(timeout=5)
        assert resultado == [len(BODY)]


def test_chamada_aninhada_no_mesmo_host_nao_trava():
    """``get()`` dentro do corpo de ``stream()`` com ``max_concurrency=1``."""
    with FakeServer(body_delay=0.0) as server:
        client = HttpClient(
            host_policies={HOST: HostPolicy(min_interval=0.0, max_concurrency=1)}
        )
        resultado: list[int] = []

        def aninhado() -> None:
            with client.stream("GET", server.url("/a")) as response:
                resultado.append(client.get(server.url("/b")).status_code)
                resultado.append(len(b"".join(response.iter_bytes())))

        t = threading.Thread(target=aninhado, daemon=True)
        t.start()
        t.join(timeout=5)
        assert resultado == [200, len(BODY)]


def test_clientes_com_estado_compartilhado_dividem_o_limite():
    with FakeServer(body_delay=0.5) as server:
        base = HttpClient(
            host_policies={HOST: HostPolicy(min_interval=INTERVALO, max_concurrency=2)}
        )
        clientes = [HttpClient().share_host_state(base) for _ in range(6)]
        run_parallel(lambda i: consume(clientes[i], server.url(f"/f{i}")), 6)
    assert server.max_active == 2
    assert min(gaps(server.arrivals)) >= TOLERANCIA * INTERVALO


def test_sem_politica_nada_muda():
    """Sem ``HostPolicy`` o ``stream`` segue sem teto nem semáforo."""
    with FakeServer(body_delay=0.3) as server:
        client = HttpClient()
        run_parallel(lambda i: consume(client, server.url(f"/f{i}")), 8)
        assert client._host_semaphores == {}
        assert client._host_guards == {}
    assert server.max_active > 2


def test_max_concurrency_zero_nao_limita():
    with FakeServer(body_delay=0.3) as server:
        client = HttpClient(
            host_policies={HOST: HostPolicy(min_interval=0.0, max_concurrency=0)}
        )
        run_parallel(lambda i: consume(client, server.url(f"/f{i}")), 8)
        assert client._host_semaphores == {}
    assert server.max_active > 2


def test_fechar_em_ordem_inversa_so_solta_a_vaga_no_ultimo():
    """A (dona da vaga) fecha antes de B (aninhado): a vaga segue presa até B."""
    with FakeServer(body_delay=0.0) as server:
        client = HttpClient(
            host_policies={HOST: HostPolicy(min_interval=0.0, max_concurrency=1)}
        )
        a = client.stream("GET", server.url("/a"))
        b = client.stream("GET", server.url("/b"))
        a.__enter__()
        b.__enter__()
        a.__exit__(None, None, None)  # fecha o dono antes do aninhado

        livre = threading.Event()

        def outra_thread() -> None:
            consume(client, server.url("/c"))
            livre.set()

        t = threading.Thread(target=outra_thread, daemon=True)
        t.start()
        assert not livre.wait(0.5)  # B ainda segura a vaga
        b.__exit__(None, None, None)
        assert livre.wait(5)  # agora a vaga voltou
        t.join(timeout=5)
        # contador zerado: a mesma thread volta a abrir e fechar sem travar
        assert consume(client, server.url("/d")) == len(BODY)
        assert client._host_slots_held.hosts == {}


def test_espera_longa_pela_vaga_avisa_uma_vez_e_continua(monkeypatch, caplog):
    import logging

    from quantilica.core import http as http_mod

    monkeypatch.setattr(http_mod, "HOST_SLOT_WARN_AFTER", 0.1)
    with FakeServer(body_delay=0.0) as server:
        client = HttpClient(
            host_policies={HOST: HostPolicy(min_interval=0.0, max_concurrency=1)}
        )
        segura = threading.Event()
        solta = threading.Event()

        def dona() -> None:
            with client.stream("GET", server.url("/a")):
                segura.set()
                solta.wait(5)

        t = threading.Thread(target=dona, daemon=True)
        t.start()
        assert segura.wait(5)
        resultado: list[int] = []
        t2 = threading.Thread(
            target=lambda: resultado.append(consume(client, server.url("/b"))),
            name="espera-vaga",
            daemon=True,
        )
        with caplog.at_level(logging.WARNING):
            t2.start()
            time.sleep(0.6)  # bem acima do limiar: ainda sem erro, ainda esperando
            assert resultado == []
            solta.set()
            t2.join(timeout=5)
            t.join(timeout=5)
    assert resultado == [len(BODY)]
    avisos = [r for r in caplog.records if "free slot" in r.getMessage()]
    assert len(avisos) == 1
    assert HOST in avisos[0].getMessage()
    assert "espera-vaga" in avisos[0].getMessage()
