# Changelog

Todas as mudanças notáveis deste projeto serão documentadas neste arquivo.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

## [0.9.0] - 2026-10-03

### Adicionado

- `files`: `decompress_archive(archive_path, output_dir=None, target_extensions=None) -> Path` — extração canônica de arquivos compactados para os fetchers. ZIP via `zipfile` com proteção *Zip Slip* (todos os membros validados contra o diretório de destino); TAR/TGZ/TBZ/TXZ via `tarfile` com `filter='data'` (Python >= 3.12); fallback transparente para o binário externo `7z` (`.7z`, `.rar` e formatos não suportados nativamente) com `StorageError` claro quando o binário está ausente ou falha. Com `target_extensions` (ex.: `('.csv', '.xls')`), retorna o primeiro arquivo de dados compatível; sem filtro, retorna arquivo único, o primeiro arquivo de dados comum, ou o diretório de destino quando há múltiplos arquivos. Exportado também via `quantilica.core`.
- `ftp`: `parse_ftp_list_line(line) -> tuple[str, int, datetime] | None` — parser canônico de listagens de diretório FTP legadas nos formatos IIS/Windows (`MM-DD-YY  HH:MMAM|PM  SIZE NAME`, com `<DIR>` para diretórios) e UNIX `ls -l` (permissões, tamanho, mês, dia, ano-ou-hora; ano retroativo quando a hora cai no futuro). Retorna `(filename, size_bytes, modified_dt)` apenas para arquivos regulares; retorna `None` para diretórios, links/dispositivos, `total` e linhas vazias/inválidas. Exportado também via `quantilica.core`.

### Alterado

- `http`: `_RateLimiter` promovida à API pública como `RateLimiter` (elimina importação de símbolo privado nos fetchers, ex. `sidra-fetcher`). Alias retrocompatível `_RateLimiter = RateLimiter` mantido. `RateLimiter` e `HttpClient` (que continua consumindo o limiter via `min_interval`) exportados em `http.__all__` e via `quantilica.core`.

## [0.8.1] - 2026-10-02

### Adicionado

- `sync`: `IncrementalSyncStrategy(policy, force)` — wrapper canônico de política incremental sobre `should_skip` (método `strategy.should_skip(target_path, remote_stat=...)`), para pipelines delegarem a decisão de skip a um único objeto de política. Exportado também via `quantilica.core`.

## [0.8.0] - 2026-10-02

### Adicionado

- `sync`: novo módulo de sincronização com `RemoteStat` (metadata remota normalizada: `size`, `last_modified`, `etag`), protocolo `FreshnessProbe`, probes concretos `HttpFreshnessProbe` (HEAD com fallback GET; captura `Content-Length`/`Last-Modified`/`ETag`) e `FtpFreshnessProbe` (`SIZE`/`MDTM` sobre `FtpClient`, MDTM interpretado como UTC), `should_skip(target, stat, policy, force)` com políticas `freshness` (padrão), `strict_manifest`, `exists` e `never`, e `is_manifest_valid(manifest_path)` (valida `sha256` + `size_bytes` do sidecar contra o artefato em disco). Símbolos exportados também via `quantilica.core`.
- `manifests`: `write_manifest_sidecar(target, manifest)` pública, que grava o sidecar no formato `<target>.manifest.json` (escrita atômica) e retorna o `Path`; `manifest_sidecar_path` e constante `MANIFEST_SIDECAR_SUFFIX` auxiliares; alias `ExecutionManifest = RunManifest` para compatibilidade (`http.py` e `ftp.py` reutilizam o helper).
- `HttpClient`: parâmetro `min_interval: float = 0.0` (rate-limiting thread-safe por requisição) com `_RateLimiter` nativo — o lock rápido só reserva o próximo slot e o `sleep` acontece fora do lock; rate-limit aplicado em `request`, `stream` e no fallback GET de `head_or_get`.

## [0.7.1] - 2026-09-28

### Corrigido

- `FtpClient.download_with_manifest` passa a emitir o contrato canônico de progresso `progress(downloaded, total)` (`ProgressCallback`, igual ao HTTP) em vez de `progress(n)` — corrige `TypeError` que abortava todo download FTP com barra de progresso (datasus, pdet) no primeiro chunk.
- Conexão FTP de saída (`_connected`) suprime falha de `quit()` pós-RETR (ex.: IIS com `550 network name no longer available`): download íntegro não vira mais `FetchError`; exceções do corpo propagam intactas.

## [0.7.0] - 2026-09-02

### Adicionado

- `HttpClient` e `AsyncHttpClient` com pooling keep-alive (`httpx2.Limits` 50/20/30s), lifecycle `__enter__/__exit__/close` e `__aenter__/__aexit__/aclose`, `_build_client`/`_get_client`; `with HttpClient() as client:` reutiliza a mesma `httpx2.Client` (elimina 50-100 handshakes em lotes SIDRA/BCB), fora do `with` mantém modo efêmero 100% retrocompatível.
- `emulate_browser: bool = False` — quando `True`, injeta `BROWSER_HEADERS` (Chrome 131, `Accept-Language: pt-BR`) com `Accept-Encoding` travado em `gzip, deflate` (previne `HTTP 406` do BCB).

### Alterado

- `stream()` e `head_or_get` com branch persistente quando em sessão.

## [0.6.0] - 2026-08-30

### Alterado

- **Quebra de compatibilidade (dependência):** migração de `httpx` para `httpx2` (fork mantido pelo Pydantic, API idêntica ao httpx 0.28.1). `HttpClient`/`AsyncHttpClient` agora aceitam e retornam tipos `httpx2` (`httpx2.Response`, `httpx2.Cookies`, transportes `httpx2.BaseTransport`/`httpx2.AsyncBaseTransport`); códigos que injetavam objetos `httpx` devem trocar a importação. Visível também: verificação TLS passa a usar o trust store do sistema operacional (em vez do bundle `certifi`), o logger interno é `httpx2` e o User-Agent padrão de fábrica passa a `python-httpx2/...` (o core já sobrescreve com `quantilica-core`/`BROWSER_HEADERS`).

## [0.5.0] - 2026-08-10

### Adicionado
- Extensão de suporte para cliente de FTP via `FtpClient`.
- Inclusão do parâmetro `data` no método `request` do `HttpClient` e expansão geral das assinaturas.

## [0.4.0] - 2026-08-07
*(Release de remoção do catálogo/CLI do core)*

## [0.3.2] - 2026-07-30

### Corrigido

- Fallback para requisição GET quando o servidor retorna HTTP 403 Forbidden em requisições HEAD (comum em portais do governo como gov.br / ANP).
- A busca da data de última modificação (`Last-Modified`) agora tenta uma requisição GET em caso de falha no HEAD, garantindo nomes de arquivos com timestamp e cacheamento correto.

## [0.3.1] - 2026-07-16

### Corrigido

- Exemplos de import no README (namespace package `quantilica.core.*`, não `quantilica_core.*`)
- Instrução de instalação no README (`pip install quantilica-core`, em vez de git+https)

### Adicionado

- Primeiro release público no PyPI
