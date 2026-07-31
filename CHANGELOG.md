# Changelog

Todas as mudanças notáveis deste projeto serão documentadas neste arquivo.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

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
