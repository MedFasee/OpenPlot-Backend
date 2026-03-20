# Endpoints de Runs - Documentacao Tecnica

## Visao Geral

O arquivo `RunsEndpoints.cs` concentra rotas de:

- listagem de buscas/runs;
- detalhamento de terminais/PMUs;
- consulta de series temporais (`/series/*`) via **POST**.

Os endpoints de serie exigem autenticacao e seguem um padrao de resposta consistente, com:

- `run_id`
- `window` (`from`, `to`)
- `meta` (titulo e eixos)
- `series`
- `modes` (quando aplicavel)
- `cache_id` (quando ha persistencia de cache)
- campos especificos por tipo (`unit`, `kind`, `tri`, `phase`, etc.)

---

## Arquitetura Comum

### Servicos compartilhados

- **`IRunContextRepository`**: resolve contexto do run (janela, PMUs, PDC).
- **`IPlotMetaBuilder`**: gera `title`, `xLabel`, `yLabel`.
- **`ILabelService`**: monta o rotulo canonico da busca (`nome_busca`).
- **`IPmuQueryHelper`**: normalizacao/filtro de PMUs para endpoint/SQL.
- **`IPhasorRequestService`**: normalizacao de entrada de fasores (`tri/phase/pmu`).
- **`ISeriesAssemblyService`**: centraliza downsampling e montagem de `RowsCacheV2`.

### Padrao de implementacao

1. Validar entrada.
2. Resolver contexto do run.
3. Buscar dados (repositorio ou SQL direto).
4. Processar serie (calculo, normalizacao de unidade, etc.).
5. Montar e salvar cache (`RowsCacheV2`) quando aplicavel.
6. Montar resposta com metadados e campos especificos.

---

## Visao rapida (tabela)

| Endpoint | Metodo | Request Body | Handler | Cache | Filtro PMU |
|---|---|---|---|---|---|
| `/runs` | GET | - | (inline) | Nao | Nao |
| `/terminals/{nomeBusca}` | GET | - | (inline) | Nao | Nao |
| `/series/voltage/by-run` | POST | `SeriesByRunRequest` | `VoltageSeriesHandler` | Sim | Sim |
| `/series/current/by-run` | POST | `SeriesByRunRequest` | `CurrentSeriesHandler` | Sim | Sim |
| `/series/seq/by-run` | POST | `SeqSeriesByRunRequest` | `SeqSeriesHandler` | Sim | Sim |
| `/series/unbalance/by-run` | POST | `UnbalanceSeriesByRunRequest` | `UnbalanceSeriesHandler` | Sim | Sim |
| `/series/frequency/by-run` | POST | `SeriesByRunRequest` | `SimpleSeriesHandler` | Sim | Sim |
| `/series/dfreq/by-run` | POST | `SeriesByRunRequest` | `SimpleSeriesHandler` | Sim | Sim |
| `/series/digital/by-run` | POST | `SeriesByRunRequest` | `SimpleSeriesHandler` | Sim | Sim |
| `/series/thd/by-run` | POST | `SeriesByRunRequest` | `ThdSeriesHandler` | Sim | Sim |
| `/series/power/by-run` | POST | `PowerSeriesByRunRequest` | `PowerSeriesHandler` | Sim | Sim |
| `/series/angle-diff/by-run` | POST | `AngleDiffSeriesByRunRequest` | `AngleDiffSeriesHandler` | Sim | Sim |
| `/series/angle-diff/teste` | GET | - | (inline) | Nao | Nao |

---

## Esquema visual (raiz de arquivos)

```
openplot.api/
??? OpenPlot.Api/
?   ??? Features/
?   ?   ??? Runs/
?   ?   ?   ??? RunsEndpoints.cs
?   ?   ?   ??? Contracts/
?   ?   ?   ?   ??? Dtos.cs (RowsCacheV2, RowsCacheSeries, RowsCachePoint)
?   ?   ?   ?   ??? Queries.cs (WindowQuery, SimpleSeriesQuery, SeqRunQuery, etc.)
?   ?   ?   ??? Repositories/
?   ?   ?   ?   ??? RunContextRepository.cs
?   ?   ?   ?   ??? MeasurementsRepository.cs
?   ?   ?   ??? Handlers/
?   ?   ?   ?   ??? VoltageSeriesHandler.cs
?   ?   ?   ?   ??? CurrentSeriesHandler.cs
?   ?   ?   ?   ??? SeqSeriesHandler.cs
?   ?   ?   ?   ??? UnbalanceSeriesHandler.cs
?   ?   ?   ?   ??? SimpleSeriesHandler.cs
?   ?   ?   ?   ??? ThdSeriesHandler.cs
?   ?   ?   ?   ??? PowerSeriesHandler.cs
?   ?   ?   ?   ??? AngleDiffSeriesHandler.cs
?   ?   ?   ?   ??? PmuQueryHelper.cs (IPmuQueryHelper)
?   ?   ?   ?   ??? PhasorRequestService.cs (IPhasorRequestService)
?   ?   ?   ?   ??? SeriesAssemblyService.cs (ISeriesAssemblyService)
?   ?   ?   ?   ??? Responses/
?   ?   ?   ?   ?   ??? SeriesResponseBuilderFluent.cs
?   ?   ?   ?   ??? Base/
?   ?   ?   ?       ??? BaseSeriesHandler.cs
?   ?   ?   ??? ...
?   ?   ??? Auth/
?   ?   ?   ??? AuthEndpoints.cs
?   ?   ??? ...
?   ??? Services/
?   ?   ??? LabelService.cs
?   ?   ??? PlotMetaBuilder.cs
?   ?   ??? ...
?   ??? Program.cs
?   ??? appsettings.json
??? docs/
    ??? Features/
        ??? runsEndpoints.md
```

---

## Dependencias por handler (visao rapida)

| Handler | Dependencias principais | Responsabilidade central |
|---|---|---|
| `VoltageSeriesHandler` | `IRunContextRepository`, `IMeasurementsRepository`, `IPlotMetaBuilder`, `IPhasorRequestService`, `ISeriesAssemblyService`, `IAnalysisCacheRepository` | Serie de tensao (MAG), unidade raw/pu, cache e metadados |
| `CurrentSeriesHandler` | `IRunContextRepository`, `IMeasurementsRepository`, `IPlotMetaBuilder`, `IPhasorRequestService`, `ISeriesAssemblyService`, `IAnalysisCacheRepository` | Serie de corrente (MAG), cache e metadados |
| `SeqSeriesHandler` | `IRunContextRepository`, `IMeasurementsRepository`, `IPlotMetaBuilder`, `ISeriesAssemblyService`, `IAnalysisCacheRepository` | Calculo de sequencias (pos/neg/zero) e resposta |
| `UnbalanceSeriesHandler` | `IRunContextRepository`, `IMeasurementsRepository`, `IPlotMetaBuilder`, `ISeriesAssemblyService`, `IAnalysisCacheRepository` | Calculo de desequilibrio (seqNeg/seqPos) |
| `SimpleSeriesHandler` | `IRunContextRepository`, `IMeasurementsRepository`, `ITimeSeriesDownsampler`, `IPlotMetaBuilder`, `ISeriesAssemblyService`, `IAnalysisCacheRepository` | Series simples (frequency, dfreq, digital) |
| `ThdSeriesHandler` | `IRunContextRepository`, `IDbConnectionFactory`, `ITimeSeriesDownsampler`, `IPlotMetaBuilder`, `IPmuQueryHelper`, `ISeriesAssemblyService`, `IAnalysisCacheRepository` | Serie THD por SQL direto |
| `PowerSeriesHandler` | `IRunContextRepository`, `IDbConnectionFactory`, `IPlotMetaBuilder`, `IPmuQueryHelper`, `ISeriesAssemblyService`, `IAnalysisCacheRepository` | Potencia ativa/reativa por composicao fasorial |
| `AngleDiffSeriesHandler` | `IRunContextRepository`, `IAnalysisCacheRepository`, `IDbConnectionFactory`, `ITimeSeriesDownsampler`, `IPmuQueryHelper`, `ISeriesAssemblyService` | Diferenca angular por fase ou sequencia, com persistencia de cache |

---

## Endpoints

### `GET /runs`

Lista runs agrupados por calendario (ano -> mes -> dia -> itens) para o usuario autenticado.

#### Entrada
- Query opcional: `status`

#### Fluxo tecnico
1. Obtem `username` dos claims.
2. Executa `SearchSql.ListRuns`.
3. Gera `label` via `ILabelService.BuildLabel(...)`.
4. Agrupa por data UTC.

#### Retorno
- `200` com `{ status, data }` (`data` em estrutura de calendario)

---

### `GET /terminals/{nomeBusca}`

Retorna metadados da busca e hierarquia de terminais/PMUs.

#### Entrada
- Rota: `nomeBusca`
- Query opcional: `id`

#### Fluxo tecnico
1. Resolve run por `id` ou por `nomeBusca` (label).
2. Calcula `resolutionSearch` a partir de `select_rate`.
3. Executa SQL de resolucao de PMUs/sinais via JSON do run.
4. Monta hierarquia com `IPmuHierarchyService`.

#### Retorno
- `200` com `xml_file`, periodo, total de terminais, `nome_busca` e arvore de terminais.

---

## Endpoints de Serie (POST)

A partir de marco de 2026, todos os endpoints de serie foram convertidos para **POST** para suportar listas grandes de PMUs sem limites de tamanho de URL.

### Request Bodies Padrao

#### `SeriesByRunRequest` (base)
```json
{
  "runId": "550e8400-e29b-41d4-a716-446655440000",
  "maxPoints": "5000",
  "unit": "raw",
  "tri": false,
  "phase": "A",
  "pmu": ["PMU1", "PMU2"],
  "from": "2026-01-24T21:46:14.000Z",
  "to": "2026-01-24T21:46:37.990Z"
}
```

Usada por: `voltage`, `current`, `frequency`, `dfreq`, `digital`, `thd`.

#### `SeqSeriesByRunRequest`
```json
{
  "runId": "550e8400-e29b-41d4-a716-446655440000",
  "maxPoints": "5000",
  "unit": "raw",
  "voltLevel": 138,
  "kind": "voltage",
  "seq": "pos",
  "pmu": ["PMU1"],
  "from": "2026-01-24T21:46:14.000Z",
  "to": "2026-01-24T21:46:37.990Z"
}
```

Usada por: `seq/by-run`.

#### `UnbalanceSeriesByRunRequest`
```json
{
  "runId": "550e8400-e29b-41d4-a716-446655440000",
  "maxPoints": "5000",
  "voltLevel": 138,
  "kind": "voltage",
  "pmu": ["PMU1"],
  "from": "2026-01-24T21:46:14.000Z",
  "to": "2026-01-24T21:46:37.990Z"
}
```

Usada por: `unbalance/by-run`.

#### `PowerSeriesByRunRequest`
```json
{
  "runId": "550e8400-e29b-41d4-a716-446655440000",
  "maxPoints": "5000",
  "which": "active",
  "unit": "raw",
  "tri": false,
  "total": false,
  "phase": "A",
  "pmu": ["PMU1"],
  "from": "2026-01-24T21:46:14.000Z",
  "to": "2026-01-24T21:46:37.990Z"
}
```

Usada por: `power/by-run`.

#### `AngleDiffSeriesByRunRequest`
```json
{
  "runId": "550e8400-e29b-41d4-a716-446655440000",
  "maxPoints": "5000",
  "kind": "voltage",
  "reference": "PMU_REF",
  "phase": "A",
  "sequence": "pos",
  "pmu": ["PMU1", "PMU2"],
  "from": "2026-01-24T21:46:14.000Z",
  "to": "2026-01-24T21:46:37.990Z"
}
```

Usada por: `angle-diff/by-run`.

---

### `POST /series/voltage/by-run`

Consulta serie de tensao (MAG), com modo monofasico/trifasico e unidade raw/pu.

#### Entrada
- Body: `SeriesByRunRequest`

#### Handler
- `VoltageSeriesHandler`

#### Observacoes
- Usa `IPhasorRequestService` para validacao/normalizacao de `tri/phase/pmu`.
- Usa `ISeriesAssemblyService` para downsampling e construcao de cache.
- Persiste `cache_id`.

---

### `POST /series/current/by-run`

Consulta serie de corrente (MAG), com modo monofasico/trifasico.

#### Entrada
- Body: `SeriesByRunRequest`

#### Handler
- `CurrentSeriesHandler`

#### Observacoes
- Mesmo padrao estrutural de `voltage`.
- Usa `ISeriesAssemblyService` para blocos comuns (pontos + cache).

---

### `POST /series/seq/by-run`

Retorna series de sequencias (positiva, negativa, zero) com calculo automatico.

#### Entrada
- Body: `SeqSeriesByRunRequest`
- Parametos esperados: `kind` (voltage|current), `seq` (pos|neg|zero)

#### Handler
- `SeqSeriesHandler`

#### Observacoes
- Calcula sequencias a partir de tres fasores de entrada (A, B, C).
- Requer `voltLevel` para normalizacao em pu se necessario.

---

### `POST /series/unbalance/by-run`

Calcula e retorna desequilibrio (KU = seqNeg/seqPos).

#### Entrada
- Body: `UnbalanceSeriesByRunRequest`

#### Handler
- `UnbalanceSeriesHandler`

#### Observacoes
- Depende de calculo interno de sequencias.
- Requer `voltLevel` para normalizacao se necessario.

---

### `POST /series/frequency/by-run`

Retorna serie de frequencia (Hz) em tempo real.

#### Entrada
- Body: `SeriesByRunRequest`

#### Handler
- `SimpleSeriesHandler`

#### Observacoes
- Filtra por `quantity='frequency'` e `component='freq'`.
- Suporta downsampling e cache.

---

### `POST /series/dfreq/by-run`

Retorna serie de variacao de frequencia (df/dt em Hz/s).

#### Entrada
- Body: `SeriesByRunRequest`

#### Handler
- `SimpleSeriesHandler`

#### Observacoes
- Filtra por `quantity='frequency'` e `component='dfreq'`.
- Escala temporal diferente de `frequency`.

---

### `POST /series/digital/by-run`

Retorna dados digitais (alarmes, status bits, etc.).

#### Entrada
- Body: `SeriesByRunRequest`

#### Handler
- `SimpleSeriesHandler`

#### Observacoes
- Filtra por `quantity='digital'`.
- Nao possui dimensao de fase.

---

### `POST /series/thd/by-run`

Retorna series de THD (Total Harmonic Distortion) de tensao ou corrente.

#### Entrada
- Body: `SeriesByRunRequest`
- Campo `unit` indica o tipo: "voltage" ou "current"

#### Handler
- `ThdSeriesHandler`

#### Observacoes
- Consulta SQL direto para eficiencia.
- Suporta filtro por fase (A, B, C) e trifasico.

---

### `POST /series/power/by-run`

Retorna series de potencia (ativa ou reativa) calculada a partir de V e I.

#### Entrada
- Body: `PowerSeriesByRunRequest`
- Campo `which`: "active" (P) ou "reactive" (Q)

#### Handler
- `PowerSeriesHandler`

#### Observacoes
- Calcula composicao fasorial de voltage e current.
- Suporta unidade raw ou mw.
- Pode retornar por fase ou valor total.

---

### `POST /series/angle-diff/by-run`

Calcula diferenca angular entre PMU de referencia e PMUs de medicao.

#### Entrada
- Body: `AngleDiffSeriesByRunRequest`
- Campo `reference`: PMU de referencia (obrigatorio)
- Campo `kind`: "voltage" ou "current"
- Campo `phase` ou `sequence`: fase (A|B|C) ou sequencia (pos|neg|zero)

#### Handler
- `AngleDiffSeriesHandler`

#### Fluxo tecnico
1. Resolve contexto do run.
2. Carrega dados da PMU de referencia e PMUs de medicao.
3. Calcula diferenca de angulo (em graus).
4. Persiste cache com `referenceTerminal`.

#### Observacoes
- Cache inclui campo `ReferenceTerminal` para rastreamento.
- Suporta modo monofasico (por fase) ou trifasico (por sequencia).

---

## Tratamento de Janelas Temporais

Todos os endpoints de serie POST suportam `from` e `to` para filtrar dados dentro de uma janela temporal:

```json
{
  "runId": "...",
  "from": "2026-01-24T21:46:14.000Z",
  "to": "2026-01-24T21:46:37.990Z"
}
```

Se omitidos, usa-se o intervalo completo do run.

---

## Exemplo de Chamada (cURL)

```bash
curl -X POST http://localhost:5000/api/v1/series/voltage/by-run \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{
    "runId": "550e8400-e29b-41d4-a716-446655440000",
    "maxPoints": "5000",
    "unit": "raw",
    "tri": true,
    "pmu": ["PMU_A", "PMU_B", "PMU_C"],
    "from": "2026-01-24T21:46:14.000Z",
    "to": "2026-01-24T21:46:37.990Z"
  }'
```

---

## Mudanca de GET para POST (Marco 2026)

**Motivo**: Listas grandes de PMUs (dezenas ou centenas) podem exceder limites de tamanho de URL (tipicamente 2KB a 8KB). Ao mover para POST com request body JSON, eliminamos essa restricao.

**Compatibilidade**: Clients devem adaptar suas chamadas de GET + `?pmu=PMU1&pmu=PMU2&...` para POST + body JSON.

**Beneficios**:
- Sem limites praticos de tamanho de lista de PMU.
- Estrutura mais clara e tipada via DTOs.
- Facilita adicionar novos parametros no futuro.
