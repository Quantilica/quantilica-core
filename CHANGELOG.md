# Changelog

Todas as mudanças notáveis deste projeto serão documentadas neste arquivo.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

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
