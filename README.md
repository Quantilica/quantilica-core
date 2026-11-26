# quantilica-core: Fundação de infraestrutura para projetos Quantilica

![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square) ![Python](https://img.shields.io/badge/python-3.12+-blue.svg?style=flat-square)

Biblioteca de utilitários domain-neutral que serve como base para todos os coletores e pipelines Quantilica. Centraliza rede resiliente, armazenamento atômico, proveniência de dados e logging estruturado — permitindo que cada pacote de domínio foque exclusivamente na lógica de sua fonte de dados.

Para a documentação completa, consulte [https://docs.quantilica.com](https://docs.quantilica.com).

## Instalação

O `quantilica-core` é publicado no PyPI:

```bash
uv add quantilica-core
```

## Uso Rápido

```python
from quantilica.core.http import HttpClient
from quantilica.core.storage import LocalStorage
from quantilica.core.manifests import DownloadManifest, write_manifest_sidecar

# Cliente HTTP com retry automático
client = HttpClient(attempts=3)
response = client.get("https://api.ibge.gov.br/...")

# Escrita atômica em disco
storage = LocalStorage("dados/raw")
stat = storage.write_bytes("sidra/tabela.csv", response.content)

# Registro de proveniência (SHA-256)
manifest = DownloadManifest.from_file(
    source_id="ibge",
    dataset_id="sidra-1234",
    url="https://...",
    file_path=stat.path,
    producer="ibge-sidra-fetcher",
)
write_manifest_sidecar(stat.path, manifest)  # grava <arquivo>.manifest.json ao lado
```

## Módulos

| Módulo | Descrição |
| :--- | :--- |
| `http` | `HttpClient` e `AsyncHttpClient` com backoff exponencial, jitter, retomada de download e política por host (`HostPolicy`) |
| `ratelimit` | `TokenBucket`, `CircuitBreaker` (`CircuitOpenError`) e `HostGuard` para respeitar as fontes (o `RateLimiter` simples fica em `http`) |
| `retry` | `retry_call`, `with_retry` e `exponential_delay` para falhas de rede e erros transientes |
| `ftp` | `FtpClient`, `MonitoredFTP` e `parse_ftp_list_line` (listagens UNIX e IIS) |
| `ckan`, `odata` | Clientes de portais CKAN e paginação OData |
| `storage` | `LocalStorage` (escrita atômica por chave) e `BaseDataRepository` e `StampedDataRepository` (layout `{dataset_id}/` e nomes com carimbo `@AAAAMMDD`) |
| `files` | Escrita atômica, `sha256`, `decompress_archive` e `extracted_archive` |
| `manifests` | `DownloadManifest`, `ExecutionManifest`, `DatasetManifest`, `RunManifest` e o sidecar `<arquivo>.manifest.json` |
| `sync` | Sincronização incremental: `FreshnessPolicy`, sondas HTTP/FTP e `should_skip` |
| `regeneration` | Detecção de arquivo regenerado pela fonte sem mudança de conteúdo |
| `lake` | Contrato do Data Lake em Parquet: `DatasetDescriptor`, `LakeConverter`, `materialize_view` |
| `registry` | Registro público de fetchers (`FetcherInfo`) |
| `dates`, `config`, `cache` | `utc_now`/`isoformat_utc`, `EnvSettings`/`load_dotenv` e `FileCache` |
| `logging` | Logging estruturado via `get_logger` e `log_step` |
| `exceptions` | Hierarquia padrão: `QuantilicaError`, `FetchError`, `ParseError`, `StorageError` |

## Princípios de Design

1. **Neutralidade de domínio** — o core nunca sabe o que é IBGE, DATASUS ou INMET.
2. **Leveza** — dependências mínimas no núcleo; integrações pesadas são extras opcionais.
3. **Estabilidade** — alta cobertura de testes em todos os componentes de infraestrutura.
4. **DX** — APIs tipadas e tratamento de erros consistente em toda a organização.

## Desenvolvimento

```bash
git clone https://github.com/Quantilica/quantilica-core.git
cd quantilica-core
uv sync --dev
uv run pytest
```

## Licença

MIT — veja [LICENSE](LICENSE).
