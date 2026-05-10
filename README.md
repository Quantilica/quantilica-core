# quantilica-core: Fundação de infraestrutura para projetos Quantilica

![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square) ![Python](https://img.shields.io/badge/python-3.12+-blue.svg?style=flat-square)

Biblioteca de utilitários domain-neutral que serve como base para todos os coletores e pipelines Quantilica. Centraliza rede resiliente, armazenamento atômico, proveniência de dados e logging estruturado — permitindo que cada pacote de domínio foque exclusivamente na lógica de sua fonte de dados.

## Instalação

```bash
pip install "quantilica-core @ git+https://github.com/Quantilica/quantilica-core.git"
```

Com uv:

```bash
uv add "quantilica-core @ git+https://github.com/Quantilica/quantilica-core.git"
```

## Uso Rápido

```python
from quantilica_core.http import HttpClient
from quantilica_core.storage import LocalStorage
from quantilica_core.manifests import DownloadManifest

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
    producer="sidra-fetcher",
)
manifest.write_json(stat.path.with_suffix(".manifest.json"))
```

## Módulos

| Módulo | Descrição |
| :--- | :--- |
| `http` | `HttpClient` e `AsyncHttpClient` com backoff exponencial e jitter |
| `retry` | Lógica de retry configurável para falhas de rede e erros transientes |
| `storage` | `LocalStorage` para gerenciar artefatos brutos e processados atomicamente |
| `manifests` | `DownloadManifest` e `ExecutionManifest` para rastreabilidade completa |
| `metadata` | Modelos `MetadataCatalog`, `Source`, `Dataset` para interoperabilidade |
| `logging` | Logging estruturado via `get_logger` e `log_step` |
| `exceptions` | Hierarquia padrão: `FetchError`, `ParseError`, `StorageError` |

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
