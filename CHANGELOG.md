# Changelog

Todas as mudanças notáveis deste projeto serão documentadas neste arquivo.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

## [Não lançado]

### Adicionado

- `parse_ftp_list_line(line, *, now=None)`: relógio injetável para resolver o ano das linhas `ls -l` que trazem hora em vez de ano. Sem `now` o comportamento é o de antes (relógio local).

### Corrigido

- `parse_ftp_list_line`: o instante resolvido de uma linha `ls -l` com hora deixa de herdar os segundos e microssegundos do relógio (agora é `HH:MM:00`), e `Feb 29` num ano não bissexto deixa de levantar `ValueError` (recua para o ano bissexto mais recente em que a data existe).
- `tests/test_ftp.py`: os testes de "hora recente" não dependem mais do relógio real. Antes `test_parse_ftp_list_line_unix_regular_file_recent_time` falhava de 01/01 a 21/04 de cada ano; agora os dois lados da virada usam relógio fixo.

### Alterado

- `lake_root()` deixa de usar `~/data/lake` como padrão e passa a usar `<default_data_dir()>/lake`. Novo `default_data_dir()`: `$XDG_DATA_HOME/quantilica` ou, sem ela, `~/.local/share/quantilica`. `QUANTILICA_LAKE_DIR` e `QUANTILICA_DATA_DIR` continuam tendo precedência; quem já as define não muda de caminho.

## [0.27.1] - 2026-11-19

### Corrigido

- `decompress_archive` e `extracted_archive` deixam de gravar os membros-lixo no destino: temporários do Office (`~$*`), checksums `md5_*`, `Thumbs.db`, `.DS_Store` e entradas sob `__MACOSX/` (nomes comparados sem diferenciar maiúsculas). Antes só deixavam de ser escolhidos como resultado, mas continuavam na pasta de extração, e quem lista a pasta (`rglob`, `glob("*")`) os lia como dados. ZIP e TAR não os extraem (o ZIP continua checando todos os membros contra path traversal); no `.7z`/`.rar` o `7z` escreve tudo, então os arquivos-lixo que a própria extração criou são removidos em seguida (lixo que já estava em `output_dir` fica, pois é do chamador). Arquivo só com lixo extrai nada e devolve o destino (ou levanta `StorageError` se `target_extensions` foi pedido); arquivo vazio continua levantando `Archive produced no files`.

## [0.27.0] - 2026-11-19
### Alterado
- `profile_from_lake`: um Parquet agrupado em `ano=AAAA/` (sem `mes=` no caminho) cujos membros do rodapé `quantilica.members` têm todos a coluna de membro `mes` passa a ter perfil `mensal`, com períodos `AAAA-MM`, em vez de um período anual que somava os retratos de todos os meses. Tudo é exato e vem só do rodapé: `linhas` do mês é a soma de `rows` dos membros dele; os nulos de cada coluna são a soma de `null_counts[coluna]` dos membros do mês que a trazem (estatística gravada por `LakeWriter.write_group(member_stats=True)`, do `quantilica-analytics`), mais as `rows` dos membros do mês que não a trazem; coluna que nenhum membro do mês traz fica `None` naquele mês (a coluna não existia ali), de modo que `derivar_eras` enxerga as colunas que entram no meio do ano. Meses com soma de `rows` 0 são descartados. `null_counts` de membro maior que as `rows` dele, ou citando coluna que não existe no esquema do arquivo, levanta `ValueError` (nada é aparado em silêncio). `LakeProfile.colunas` sai na ordem do esquema do arquivo. Membro sem `null_counts` nesse caminho levanta `ValueError` (regrave o arquivo com a estatística); não há aproximação. O formato do documento do perfil não muda (`versao` continua 1, sem chave nova). Membros sem `mes`, ou arquivo sem membros, continuam `anual`; `mes` só em parte dos membros, fora de 1-12 ou `rows` que não somam o `num_rows` do arquivo levantam `ValueError`; misturar esse tipo de arquivo com `ano=/mes=` ou com arquivos anuais é granularidade mista (`ValueError`).

## [0.26.0] - 2026-11-18
### Adicionado
- `trusted_sha256(path)` (em `quantilica.core.manifests`, exportada de `quantilica.core`): devolve o sha256 gravado no sidecar `<arquivo>.manifest.json` sem ler o arquivo, quando o sidecar é confiável (JSON válido, `sha256` de 64 hex, `size_bytes` igual ao tamanho atual e arquivo não mais novo que o sidecar, com tolerância de 2 s); caso contrário calcula lendo o arquivo (`sha256_file`). O sidecar é procurado ao lado do caminho informado, e não do alvo de um link simbólico (o tamanho e o `mtime` são os do alvo). `QUANTILICA_LAKE_REHASH=1` força sempre a leitura. Evita baixar o bruto inteiro a cada build quando ele está numa montagem de rede. **Limite da garantia:** na prática o que protege é o tamanho. O `mtime` do bruto baixado costuma ser o `Last-Modified` do servidor, quase sempre mais velho que o sidecar, então a regra do `mtime` só pega arquivo regravado depois do sidecar na mesma máquina; um arquivo trocado por outro conteúdo do mesmo tamanho não é percebido. Para detectar isso, use `QUANTILICA_LAKE_REHASH=1` ou uma auditoria que leia os arquivos.

### Corrigido
- `check_fetcher_docs`/`parse_variables_table`: nome de coluna com espaço nas pontas (o nome real da coluna no Parquet, como em alguns datasets de anac, inep, anp e datasus) passa a ser comparado exato. Na tabela de variáveis o nome vai entre crases e o texto entre elas vale literalmente (`` ` NOME ` `` é `" NOME "`); os espaços de fora das crases continuam sendo só o enchimento da tabela. Antes a célula era aparada e o check acusava divergência que não existia. Nome escrito na página que só difere do da coluna no esquema nos espaços das pontas (por exemplo, a página antiga que o escreve aparado) deixa de gerar erro e passa a gerar um **aviso** com o nome exato esperado.

### Alterado
- Verificação de tipos com `pyright` (`[tool.pyright]`, modo `basic`) zerada em `src`; `parse_ftp_list_line` devolve `None` para mês desconhecido em vez de `TypeError`.


## [0.25.0] - 2026-11-16
### Adicionado

- `HttpClient.share_host_state(outro)`: o cliente passa a usar as políticas por host, o limitador de intervalo mínimo, os guardas (pausa de `429`, disjuntor) e os semáforos de `max_concurrency` de `outro`. Serve aos clientes por thread de um download paralelo, que mantêm cada um o seu pool de conexões mas obedecem a um só ritmo por host (antes, N clientes mandavam N vezes o ritmo configurado).

### Corrigido

- `HostPolicy.max_concurrency` vale também em `HttpClient.stream()` (e, portanto, em `download_with_manifest`, `get_bytes` e no `GET` de `head_or_get`): a vaga fica presa até o fim do bloco `with`, é devolvida em qualquer saída (exceção, status de erro, gerenciador abandonado) e não trava uma chamada aninhada da mesma thread ao mesmo host. Antes só `request()` aplicava o teto. **Atenção:** o padrão `max_concurrency=4` passa a valer também nos downloads: quem tem política só com `min_interval` fica com no máximo 4 downloads simultâneos por host. O teto existe só no `HttpClient` síncrono (o `AsyncHttpClient` não tem). O corpo de um `stream()` não deve chamar outro host que tenha política (duas threads, cada uma com a vaga de um host, esperando a do outro, travam com `max_concurrency=1`); uma espera por vaga acima de 60 s (`HOST_SLOT_WARN_AFTER`) gera um aviso no log com o host e a thread, sem erro. Um `stream()` aberto e nunca fechado devolve a vaga quando o gerador é finalizado (na hora no CPython; em ciclo de referências, só quando o coletor roda).
- Com política por host, a vaga de `max_concurrency` passa a ser tomada **antes** da ficha do ritmo (em `request()` era depois): o intervalo mínimo é contado a partir do momento em que a requisição pode de fato sair, e requisições que esperavam a vaga já não saem em rajada. Sem política, nada muda.


## [0.24.0] - 2026-11-15
### Adicionado

- `lake_profile.period_from_path` e `profile_from_lake` aceitam cinco granularidades (`GRANULARIDADES`): `anual` (`AAAA`), `semestral` (`ano=` + `semestre=1-2`, `AAAA-S1`), `trimestral` (`ano=` + `trimestre=1-4`, `AAAA-T1`), `mensal` (`AAAA-MM`) e `diaria` (`ano=` + `mes=` + `dia=`, `AAAA-MM-DD`). Os valores das chaves são só dígitos ASCII (`+3`, `1_1`, espaços e dígitos não ASCII levantam `ValueError`). Valores fora da faixa, dia inexistente, `dia=` sem `mes=` e combinações ambíguas (`semestre=` com `trimestre=`, por exemplo) levantam `ValueError`; o dataset segue com uma granularidade só (mista é erro). `derivar_eras` não supõe granularidade. **Muda o resultado de alguns datasets** (os perfis são regerados no próximo `catalog sync`): pastas `ano=/mes=/dia=` (como o b3) passam de `mensal` a `diaria`; `ano=/trimestre=` (como o pgfn) passa de `anual` a `trimestral`; `ano=/dia=` sem `mes=`, `ano=/trimestre=/mes=` e `competencia=` junto com `semestre=`/`trimestre=`/`dia=` passam a levantar `ValueError` (o dataset fica sem perfil); e `schema_from_lake` passa a gravar `periodo_inicio`/`periodo_fim` desses datasets no formato da nova granularidade (`AAAA-T1`, `AAAA-MM-DD`). Datasets só com `ano=` ou `ano=/mes=` (ou `competencia=` sozinho) seguem iguais.
- `lake_profile.period_from_path` aceita `competencia=AAAAMM` (sem `ano=`) como período mensal (`AAAA-MM`); com `ano=` e `competencia=` no mesmo caminho vale o `ano=`. `profile_from_lake` passa a perfilar datasets particionados assim, com granularidade `mensal`.
- `lake_profile.DatasetSemPeriodoError` (subclasse de `ValueError`).

### Alterado

- `lake_profile.profile_from_lake`: as colunas que os rodapés declaram em `quantilica.member_columns` saem do perfil, além das de `EXCLUDED_COLUMNS` (`uf` deixa de ser a única).
- `lake_profile.profile_from_lake`: dataset em que nenhum arquivo tem `ano=` (nem `competencia=`) levanta um único `DatasetSemPeriodoError` com a contagem de arquivos, em vez de um `ValueError` pelo primeiro arquivo. Como é um `ValueError`, quem já o capturava segue funcionando. Dataset com só parte dos arquivos sem período levanta `ValueError` com a contagem.

## [0.23.0] - 2026-11-15

### Adicionado

- `HostPolicy` ganha token bucket, backoff de `429` por host e disjuntor.
  Sem configurar os campos novos o comportamento é o de antes:
  - `burst` (capacidade do balde; `1`, o padrão, é o espaçamento fixo de
    sempre).
  - `429`: por padrão (`backoff_base=None`) só a requisição recusada espera,
    pelo `Retry-After` do servidor (nunca cortado) ou, sem ele, pelo
    `retry_base_delay` do cliente dobrando a cada tentativa. É opt-in definir
    `backoff_base`: aí o host inteiro pausa (todas as threads, não só a
    recusada) por `Retry-After` quando houver, ou por `backoff_base` dobrando a
    cada `429` seguido (também com corpo HTML), e zera na primeira resposta
    boa. Respostas `429` da mesma rajada contam uma vez só: a que chega
    enquanto a pausa vigente corre devolve o tempo que falta, sem escalar. O
    backoff dobrado para em 300 s; só um `backoff_max` explícito também limita
    o `Retry-After` do servidor.
  - Disjuntor (opt-in, `breaker_threshold`/`breaker_cooldown`/
    `breaker_statuses`): um status de `breaker_statuses` (`405`) fora de `HEAD`
    ou uma página HTML de manutenção ou bloqueio abre o disjuntor; aberto, o
    host recusa na hora com `CircuitOpenError`, sem enviar nada nem tentar de
    novo; passado o cooldown uma requisição de teste decide, e qualquer
    resultado dela que não seja sucesso (5xx, `429`, erro de rede) reabre o
    disjuntor em vez de travá-lo. `HEAD` respondido com status do disjuntor ou
    `403` é neutro (não zera a contagem nem fecha o meio-aberto), e o `GET` de
    reserva do `head_or_get` passa pelo disjuntor.
  - Página HTML de manutenção (`looks_like_maintenance_html`): só vale corpo
    HTML curto (cabe nos 8 KiB lidos e tem pouco texto visível) com o marcador
    no texto visível (título conta; script, estilo, comentário e elemento
    oculto não) e resposta suspeita: status de erro (`>= 400`) ou HTML numa URL
    que não é de página (`.html`, `.php`, terminada em `/`...).
    `detect_maintenance_html=False` desliga a heurística e mantém o disjuntor
    por status (`405`). Só as chamadas que leem o corpo inteiro (`request`,
    `get`, `get_json`...) o inspecionam: `stream()` não inspeciona o corpo,
    só o status.
  Vale em `request`, `stream` e no `GET` de reserva do `head_or_get` do
  `HttpClient`; o `AsyncHttpClient` segue sem política de host. Novos:
  `quantilica.core.ratelimit` (`TokenBucket`, `CircuitBreaker`, `HostGuard`,
  `CircuitOpenError`, `looks_like_maintenance_html`) e `MaintenancePageError`.
- `quantilica.core.regeneration`: detector de regeração. `RegenerationRule`
  (colunas de carimbo a ignorar, delimitador, codificação, ordem opcional) e
  `normalized_content_sha256` hasheiam o conteúdo sem o carimbo, sem BOM, sem
  diferença de quebra de linha e dentro de gzip. `detect_regeneration` compara
  com o manifest anterior (`content_sha256` só vale com o mesmo `normalizer`) e
  devolve `skipped` quando só o carimbo ou o ETag mudou, `changed` quando o dado
  mudou ou não há hash comparável e `new` sem versão anterior.
  `with_content_hash` preenche `content_sha256` e `normalizer` no manifest.
  Campos além do cabeçalho entram no hash (marcados à parte, para nunca
  passarem por coluna comum), então uma linha com campo a mais não vira
  `skipped` por engano; o `GzipFile` e o arquivo sob ele são fechados.
- `quantilica.core.odata`: paginação OData por intervalo de chave, sem
  `$skip`. `iter_odata_key_pages`/`iter_odata_rows` pedem `chave gt última`
  ordenado pela chave com `$top` (`start`/`stop` inclusivos, `base_filter`,
  `select`, chave repetida com `unique_key=False` e `tiebreak`), terminam na
  página vazia (a página curta só encerra com `trust_short_page=True`, porque
  o servidor pode limitar o `$top`) e levantam `ODataPaginationError` quando a
  fonte ignora o `$filter` (caso do Ipeadata) ou manda chave nula em vez de
  repetir para sempre. O `base_filter` entra sempre entre parênteses. Chaves de
  texto são comparadas em Python e o servidor ordena pela própria colação: as
  duas ordens podem divergir (maiúsculas, acentos), então prefira chave
  numérica ou data ISO. `http_odata_fetcher` liga o helper ao `HttpClient` e
  levanta `ODataPaginationError` quando um `200` vem sem lista em `value_key`
  (lista vazia é página vazia válida), em vez de truncar em silêncio;
  `odata_literal` formata os literais.
- `quantilica.core.lake_profile` (extra `docs`, `pyarrow`): `profile_from_lake(lake_dir, source_id, dataset_id)` lê só os rodapés Parquet e devolve um `LakeProfile` com o documento `perfil` (versão 1: `granularidade`, `periodos`, `linhas` e `colunas.{nome}.nulos`, com `null` para coluna inexistente no período), a lista de colunas da união (`nome`, `dtype_arrow`, `periodo_inicio`, `periodo_fim`) e `perfil_sha256` do JSON canônico. O período vem do caminho Hive (`ano=`/`mes=`); `null_count` e `num_rows` são somados por grupo de linhas e por arquivo do período; as colunas `ano`, `mes`, `uf`, `_origem` e as chaves de partição ficam fora do documento. `derivar_eras(perfil)` agrupa períodos consecutivos com o mesmo conjunto de colunas em `{inicio, fim, n_colunas, entrou, saiu}`. Também `canonical_json`, `perfil_sha256` e `period_from_path`.

### Alterado
- `schema_from_lake` (e o `<dataset>.schema.json`) passa a trazer `periodo_inicio` e `periodo_fim` por coluna quando o dataset tem partição `ano`; as chaves são opcionais e `check_fetcher_docs` não as usa.

### Corrigido
- CI: o `uv sync` do workflow de testes deixa de passar `--index` e `--index-strategy` do índice Quantilica. O pacote não depende de nada que só exista no índice, e o GitHub Pages responde com 503 intermitente quando o caminho não existe, o que derrubava o job.

## [0.22.0] - 2026-11-14

### Adicionado

- `quantilica.core.files.extracted_archive(archive_path, *, target_extensions=None, dir=None)`
  (também em `quantilica.core`): gerenciador de contexto que extrai para
  `mkdtemp(prefix="quantilica_extract_")` (em `dir` ou `TMPDIR`), devolve o
  mesmo caminho que `decompress_archive` e **sempre** remove a raiz da extração
  (sucesso, exceção ou interrupção), subpastas incluídas. A remoção é robusta:
  entrada somente leitura é liberada e tentada de novo, e o que não sair vira
  `RuntimeWarning`, nunca uma exceção que mascara a original.

- `quantilica.core.files.remove_stale_atomic_temps(alvo, *, max_age_hours=None)`:
  apaga `.<nome>.<8 aleatórios>.tmp` **do mesmo alvo** com `mtime` acima de N
  horas (padrão 48 h; `QUANTILICA_TMP_MAX_AGE_HOURS` configura). É chamada antes
  de criar o `.tmp` só nos downloads em streaming: `write_stream_atomic` e os
  downloads de `HttpClient`/`AsyncHttpClient`. `write_bytes_atomic` (e com ele
  `write_text_atomic` e os manifests) **não** varre a pasta: os `.tmp` dele
  vivem milissegundos e um `scandir` por gravação custaria 100 a 200 s numa
  pasta de 29 mil entradas. A idade precisa ser finita e de pelo menos 6 h:
  `nan`, `inf` e valor abaixo do piso voltam ao padrão de 48 h com aviso (nunca
  viram "apague tudo"); `<= 0` desliga a limpeza. Um `.tmp` recente (outro
  processo baixando o mesmo arquivo), de outro alvo ou symlink nunca é tocado;
  falha ao remover vira aviso no log, nunca erro do download. Cobre o resíduo de
  `timeout`/`SIGTERM` que mata o download antes do `finally`.

### Alterado

- `decompress_archive(output_dir=None)` passa a emitir `DeprecationWarning`
  quando ela mesma cria a pasta temporária (o chamador é quem a remove, e a raiz
  não é `resultado.parent` quando o arquivo tem subpastas: `rmtree(inner.parent)`
  deixava `quantilica_extract_*` para trás). Se a extração falha nesse caminho,
  a pasta criada é removida antes de o erro subir. Com `output_dir` explícito
  nada muda: a pasta continua do chamador e nunca é removida.

## [0.21.0] - 2026-11-14

### Adicionado

- `quantilica.core.registry`: registro v2, com três camadas.
  `IdentidadePacote` (distribuição, módulo, repositório, entry points,
  `source_id`, versão: derivada dos metadados da distribuição, nunca
  declarada); `RegistroEcossistema` e `Renome` (fatos do ecossistema:
  `docs_pasta`, `vitrine`, `ci_integracao`, `json_leve`, `mascote`,
  `health_url` e `historico` de renomes); `carrega_registro_ecossistema(path)`
  valida o `registro.toml` (vocabulário `DOCS_PASTAS`, URL, data ISO, id fora do
  export é erro); `check_identidade` aponta distribuição fora da regra
  `<id>-fetcher` que o `historico` não explica; `registry_sha256` e o campo
  `sha256` do registro.
- `build_registry(infos, identidades=None, ecossistema=None, extras=None)` junta
  as camadas, ordena por id e calcula o `sha256` do conteúdo. `FetcherInfo` não
  muda.

### Alterado

- `REGISTRY_VERSION` sobe para 2. Os campos da versão 1 mantêm nome e formato; a
  versão 2 só acrescenta (campos novos e `sha256`). Leitor que exige
  `version == 1` precisa aceitar 2.

## [0.20.0] - 2026-11-10

### Adicionado

- `FetcherInfo.redistribuicao` (`"aberta"` | `"restrita"` | `None` = não
  avaliada, vocabulário `REDISTRIBUICOES`) e `restricao_nota` (motivo curto).
  `to_dict()`/`from_dict()` os incluem; dicionário antigo, sem os campos, segue
  válido. `REGISTRY_VERSION` continua 1: campo opcional novo é aditivo.
- `is_redistributable(info)`: `False` só para fonte marcada como `restrita`.

## [0.19.0] - 2026-11-10

### Adicionado

- `quantilica.core.registry`: modelo do registro de fetchers. `FetcherInfo`
  (id canônico, apelidos, nome, órgão, licença, página, cadência, domínio) com
  validação e `to_dict()`/`from_dict()`, as dataclasses de catálogo
  `DatasetEntry` e `GroupInfo` (união dos campos das cópias nos pacotes) e as
  funções puras `resolve_alias`, `check_unique` e `build_registry`
  (`{"version": 1, "fetchers": [...]}`, ordenado por id).

## [0.18.0] - 2026-11-08

### Adicionado

- `quantilica.core.docs`: verificação da norma de documentação dos dados dos fetchers. `check_fetcher_docs(docs_dir, datasets, source_id)` devolve os problemas (erros e avisos) da pasta `docs/` de um fetcher e `assert_fetcher_docs(...)` serve de teste-padrão no pytest: página por dataset e vice-versa, cabeçalho YAML no esquema da norma, as nove seções na ordem, esquema de referência (`<id>.schema.json`), partições coerentes e dicionário de variáveis que cobre exatamente as colunas do esquema. `parse_dataset_page`, `render_ficha` e `update_ficha` leem o cabeçalho e regeneram a ficha técnica; `schema_from_lake` e `write_schema` geram o esquema de referência lendo só os rodapés dos Parquet do lake.
- Extra opcional `docs` (`pip install "quantilica-core[docs]"`) com `pyyaml` e `pyarrow`, necessários só para `quantilica.core.docs`.

## [0.17.0] - 2026-11-06

### Corrigido

- `download_with_manifest` (sync e async) não aceita mais como completo um download truncado quando o servidor responde sem `Content-Length` (`Transfer-Encoding: chunked`). O tamanho esperado passa a vir do total do `Content-Range` (qualquer diferença é erro) ou, na falta dele, do sufixo de um `ETag` no formato `"<mtime>-<tamanho>"` (heurístico: só um download menor conta como truncado). A transferência incompleta é retentada com retomada por `Range`; esgotadas as tentativas, levanta o novo `IncompleteDownloadError` (subclasse de `StorageError`) sem promover o arquivo nem gravar o manifest. Respostas com `Content-Encoding` diferente de `identity` não são verificadas.

- `check_payload(..., require_final_newline=True)` rejeita `.csv`, `.tsv` e `.txt` que não terminam em quebra de linha (`CorruptPayloadError`). O `download_with_manifest` ativa a regra só quando não foi possível verificar o tamanho do download (sem `Content-Length`, `Content-Range` nem `ETag` com tamanho); o padrão de `check_payload` não muda. `.tsv` passa a ser tratado como arquivo de dados (rejeita página HTML).

## [0.16.1] - 2026-11-06

### Corrigido

- `decompress_archive` não escolhe mais, entre vários arquivos extraídos, lock files do Office (`~$*`), checksums (`md5_*`), `Thumbs.db`, `.DS_Store` nem entradas de `__MACOSX/`. A escolha nos demais casos não muda.

- `download_with_manifest(deduplicate_by_sha256=True)` também deduplica contra arquivos legados sem o carimbo `@AAAAMMDD` no nome (mesma chave lógica e extensão, por exemplo `X.zip` contra `X@20261104.zip`). O SHA-256 do legado vem do manifesto ou, se não houver, é calculado do arquivo. Antes, o sync criava uma versão `@<hoje>` com conteúdo idêntico (caso dos ZIPs do sigtap). `file_key()` e `composite_file_key()` não mudam.

## [0.16.0] - 2026-11-03

### Adicionado

- `lake.partition_types_for()`, `lake.composite_file_key()` e `lake.CANONICAL_PARTITION_TYPES`: tipos canônicos das chaves de partição do lake e chave de arquivo composta (arquivo mais membro, como a aba de uma planilha). `storage.format_partition_token()` e `BaseDataRepository.path_for_entry()` montam o token de partição ISO 8601 (ano, semestre, trimestre, mês, geografia) e o caminho de destino de uma entrada, para os repositórios de dados dos coletores concordarem entre si.
- `validate_payload_signature()`, `expected_format_for_path()`, `ExpectedFormat`, `InvalidPayloadSignatureError` e `EmptyPayloadError`: conferem a assinatura do conteúdo baixado contra o formato esperado, rejeitando página de erro ou bloqueio de firewall servida com HTTP 200, arquivo vazio e ZIP/XLSX corrompido. `download_with_manifest()` (síncrono e assíncrono) chama a conferência antes de gravar o arquivo (parâmetros `expected_format`, `expect_header`, `max_bytes` e `allow_header_only`, este último ligado por padrão).
- `download_with_manifest(deduplicate_by_sha256=True)`: depois do download, compara o SHA-256 com os arquivos já gravados no mesmo diretório com o mesmo nome lógico (só a data `@AAAAMMDD` difere) e devolve o existente, sem criar uma versão nova do mesmo conteúdo.
- `sync.FreshnessPolicy` (`head_or_manifest`, `head_fallback_get`, `remote_only`) e o parâmetro `policy` de `HttpFreshnessProbe`. Com `remote_only` a frescura é decidida pelo conteúdo (SHA-256) e a sonda não faz nenhuma requisição.
- `HttpClient` e `AsyncHttpClient` aceitam `host_policies` (`http.HostPolicy`: intervalo mínimo, concorrência máxima, prazo total, limite de redirecionamentos e nova tentativa em fim de conexão TLS por host). `HttpClient` ganha `http2` (desligado por padrão, para não travar em servidores com HTTP/2 mal implementado) e `jitter`; `AsyncHttpClient` ganha `min_interval`. `exceptions.TooManyRedirects` sinaliza o excesso de redirecionamentos.
- `retry`: o cabeçalho `Retry-After` das respostas com status de nova tentativa (408, 429, 5xx) passa a ser respeitado como espera entre tentativas, e `RetryableHttpStatusError` ganha `retry_after`.
- `ckan.CkanClient`, `ckan.pick_resources()` e `exceptions.CkanError`: cliente da API v3 de portais CKAN (`status_show`, `package_show`, `package_search` paginado).
- `http.remote_zip_members()` e `http.ZipMember`: lista os membros de um ZIP remoto por requisições `Range`, sem baixar o arquivo inteiro.
- `DownloadManifest` ganha os campos opcionais `content_sha256` e `normalizer`.

### Corrigido

- O teste do limite de taxa tolera a resolução do relógio no Windows.

## [0.15.0] - 2026-10-30

### Adicionado

- `lake.select_current()`: escolhe a versão vigente de cada arquivo bruto. A chave é o diretório mais `file_key()`, e a versão é a data `@AAAAMMDD` ou `@AAAAMMDDTHHMMSS` do nome; o arquivo sem data é a versão mais antiga possível, e arquivos de formatos diferentes (gêmeos) da mesma versão ficam juntos. O manifesto (`.manifest.json`) acompanha o arquivo escolhido. `exclude` aceita `sha256` (lido do manifesto, e só calculado se não houver) e caminhos relativos, e é aplicado depois da escolha: excluir a versão mais nova não traz de volta uma anterior.
- `lake.materialize_view()` e `lake.ViewSummary`: criam uma árvore com o mesmo layout do bruto, só com a versão vigente de cada arquivo e o manifesto dela, por `hardlink`, `symlink` ou `copy`. Quando o modo pedido falha (outro volume, Windows sem permissão para symlink), recua de hardlink para symlink e para cópia. Pode ser repetida sem duplicar nada e remove da visão o que deixou de ser vigente.
- `lake.LakeConverter` (`typing.Protocol`, verificável com `isinstance`) formaliza o contrato do conversor registrado no entry point `quantilica.lake`, sem exigir mudança dos conversores existentes. Ganha o método opcional `inventory(input_dir)`, que devolve uma lista de `lake.Planned` (arquivo, dataset de destino e motivo de ignorar: `gemeo`, `nao_tabular` ou `superado`). `lake.converter_inventory()` chama o método e devolve `None` para o conversor que não o implementa.

## [0.14.1] - 2026-10-30

### Corrigido
- `lake.file_key` reconhece a extensão `.dbc` (DBF comprimido do DATASUS) e tira dela a
  data de publicação, como nas demais: `sih-rd_202001-sp@20200301.dbc` vira
  `sih-rd_202001-sp`. Antes, cada republicação de um DBC ganhava uma chave nova, e a
  versão anterior continuava no lake ao lado da nova.

## [0.14.0] - 2026-10-29

### Adicionado

- `files.check_payload()` e `exceptions.CorruptPayloadError`: rejeitam o download cujo conteúdo contradiz o nome (página HTML salva como `.csv`/`.zip`, ZIP ou XLSX com CRC inválido, 7z que falha no `7z t`). Os downloads HTTP e FTP chamam a checagem antes de substituir o arquivo e de gravar o sidecar, então um arquivo quebrado não chega a ser registrado; no FTP o erro é transitório e o download é refeito. `write_stream_atomic` ganha o parâmetro `validate`.

### Alterado

- `lake.dataset_lock()` passa a dar exclusão mútua também no Windows, com `msvcrt.locking` no primeiro byte de `<dataset>/.lock`; antes era um no-op sem `fcntl`, e dois conversores no mesmo dataset podiam gravar `_dataset.json` ao mesmo tempo.

## [0.13.0] - 2026-10-27

### Adicionado

- `lake`: novo módulo `quantilica.core.lake`, sem dependências novas, com o contrato de layout do lake Parquet particionado no estilo Hive: `lake_root()` (`QUANTILICA_LAKE_DIR`, com fallback para `QUANTILICA_DATA_DIR/lake` e `~/data/lake`), `file_key()`, `encode_partition_value()`, `partition_dir()` e `parse_partition_path()`, `DatasetDescriptor` (leitura e escrita atômica de `_dataset.json`), `dataset_lock()` (`flock` em `<dataset>/.lock`, sem efeito onde não há `fcntl`) e `load_lake_converter()` (entry point `quantilica.lake`). Tudo exportado em `quantilica.core`.

## [0.12.1] - 2026-10-19

### Corrigido

- `files`: `check_free_space` não cria mais diretórios. Antes, um caminho sem extensão era tratado como diretório e recebia `mkdir`. Num alvo de download sem extensão (por exemplo, um arquivo nomeado pelo texto de um link, como "API pública"), a pasta era criada no lugar do arquivo e o download falhava com `IsADirectoryError` no `os.replace` final. Agora o espaço livre é medido no ancestral existente mais próximo.

## [0.12.0] - 2026-10-18

### Adicionado

- `manifests`: `read_manifest_sidecar(target)` para leitura resiliente do sidecar `<target>.manifest.json`, com retorno `None` quando o sidecar não existe ou está corrompido (com warning) e preenchimento automático de `size_bytes` e `path` a partir do arquivo de origem quando omitidos.

## [0.11.0] - 2026-10-14

### Adicionado

- `http`: `max_resumes` (keyword-only, padrão `DEFAULT_MAX_RESUMES = 1000`) em `HttpClient` e `AsyncHttpClient`. O retry de `download_with_manifest` passou a considerar progresso: uma tentativa que falha depois de avançar o arquivo parcial, com o servidor honrando `Range`, não consome `attempts`; fica limitada por `max_resumes` (excedido, levanta `RetryError`) e espera só `retry_base_delay` entre as retomadas. Reinício do zero (servidor ignora `Range`), parcial divergente e `416` continuam consumindo tentativas. Resolve servidores que derrubam a conexão a cada poucos MB (por exemplo `balanca.economia.gov.br`, reset a cada ~3,8 MB).
- `http`: `DEFAULT_MAX_RESUMES` exportado em `__all__`.

## [0.10.1] - 2026-10-08

### Corrigido

- `http`: `is_remote_more_recent`, `_head_fresh` e `download_with_manifest`
  verificam o sidecar `<target>.manifest.json` quando o arquivo local de dados
  não existe (sem retenção local), evitando re-download redundante quando o
  remoto permanece inalterado (`manifest-fresh`).
- `http`: `download_with_manifest` inicializa `etag` e `last_modified` a partir
  dos headers da resposta GET de streaming caso o HEAD retorne 404, prevenindo
  `UnboundLocalError`.
- `manifests`: `DownloadManifest.read_json` valida que o conteúdo parseado é
  um dicionário antes de instanciar a classe.

## [0.10.0] - 2026-10-07

### Adicionado

- `http`: `VerifyOption = bool | str | ssl.SSLContext` — `HttpClient` e
  `AsyncHttpClient` aceitam caminho de CA bundle ou `SSLContext` além de
  booleano no parâmetro `verify`.
- `http`: `resolve_verify_from_env() -> bool | str` — resolve verificação TLS
  a partir do ambiente (`QUANTILICA_CA_BUNDLE` tem precedência;
  `QUANTILICA_SSL_VERIFY` com `0/false/no/off` desliga, caminho usa como
  bundle; padrão `True`).
- `http`: resume com `Range` em `download_with_manifest` (sync e async) — o
  parcial sobrevive entre tentativas; servidor que ignora `Range` volta a
  restart-from-zero; 416 reinicia uma vez.
- `http`: `is_remote_more_recent(response, local_path, check_size=True)` —
  predicado público de freshness compartilhado entre download e verificação
  (alias `_is_remote_more_recent` mantido).

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
