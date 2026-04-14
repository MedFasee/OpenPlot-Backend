# openPlot – Backend

Este repositório contém o backend do **openPlot**, orientado à integração com o ecossistema **MedPlot**.
O objetivo é disponibilizar uma API HTTP para consulta/visualização de séries temporais (tensão, corrente, sequência, THD, potência etc.), além de ferramentas de ingestão e importação de metadados.

---

## 1. Visão geral da solução

A solution `openplot.sln` é composta pelos seguintes projetos:

### `OpenPlot.Api`
API HTTP (Minimal API) responsável por:

- autenticação e sessão;
- cadastro e consulta de *search runs*;
- recuperação de séries temporais para plotagem (tensão, corrente, sequência, desequilíbrio, frequência, THD, potência, diferença angular etc.);
- geração de metadados de gráficos (`title`, `xLabel`, `yLabel`) e envelopes consistentes;
- pós-processamento baseado em `cache_id` (ex.: DFT).

### `OpenPlot.Ingestor.Gsf`
Aplicação de ingestão responsável por:

- conectar-se ao stack GSF/openHistorian (SNAPDB);
- ler medidas brutas do historizador;
- normalizar dados para o modelo do openPlot;
- persistir medições e metadados nas tabelas `openplot.*` no PostgreSQL.

### `OpenPlot.XmlImporter`
Ferramenta de importação destinada a:

- importar arquivos XML do legado MedPlot;
- interpretar PDCs, PMUs, sinais/canais e configurações;
- persistir/atualizar o inventário no banco (`pdc`, `pmu`, `signal`, `pdc_pmu`, etc.).

### `OpenPlot.ExportWorker`
Worker Service responsável por exportação assíncrona de runs para **COMTRADE**.

Responsabilidades principais:

- consumir jobs de exportação (fila de `run_id`) e controlar status (`queued`, `running`, `done`, `failed`);
- carregar o contexto do run em `openplot.search_runs` (PDC, janela e filtros/PMUs);
- consultar medições no PostgreSQL e alinhar séries para o formato COMTRADE;
- gerar arquivos COMTRADE (incluindo compactação em `.zip` e escrita atômica `.tmp -> rename`);
- persistir metadados do artefato gerado (path, nome, tamanho, hash) e progresso do job.

Arquivos relevantes:

- `OpenPlot.ExportWorker/worker.cs`: loop do `BackgroundService` que processa a fila.
- `OpenPlot.ExportWorker/Build/ComtradeBuildService.cs`: montagem das séries/canais.
- `OpenPlot.ExportWorker/Comtrade/*`: naming e writer do padrão COMTRADE.
- `OpenPlot.ExportWorker/Storage/DiskExportStore.cs`: escrita atômica e organização em disco.

### `tests/OpenPlot.UnitTests`
Projeto de testes unitários para regras puras e componentes sem dependência externa.

Cobertura atual:

- `Dft`;
- `PlotMetaBuilder`;
- `DftMetaBuilder`;
- `DiskExportStore`.

### `tests/OpenPlot.Api.IntegrationTests`
Projeto de testes de integração HTTP baseado em `WebApplicationFactory<Program>`.

Cobertura atual:

- `POST /api/v1/auth/login`;
- `POST /api/v1/auth/logout`;
- `GET /api/v1/dft`.

---

## 2. Arquitetura lógica (alto nível)

1. **Catálogo / Metadados**: carregados via `OpenPlot.XmlImporter`.
2. **Ingestão**: medições brutas chegam via `OpenPlot.Ingestor.Gsf` e são persistidas em `openplot.measurements`.
3. **Consulta/Visualização**: o frontend consome a `OpenPlot.Api` para listar runs e obter séries temporais.
4. **Cache analítico**: alguns endpoints de séries persistem payloads (`RowsCacheV2`) e retornam `cache_id`.
5. **Pós-processamento**: endpoints como `/dft` operam em cima do `cache_id`.
6. **Exportação**: `OpenPlot.ExportWorker` processa jobs e gera COMTRADE.

---

## 3. Documentação técnica

- Documentação de features: `docs/Features/README.md`
- Endpoints de runs/séries: `docs/runsEndpoints.md`
- Testes automatizados: `docs/testing.md`

---

## 4. Execução (desenvolvimento)

> Observação: PENDING DOCKER

- API: projeto `OpenPlot.Api`
- Worker de exportação: projeto `OpenPlot.ExportWorker`
- Ingestor: projeto `OpenPlot.Ingestor.Gsf`
- Importador: projeto `OpenPlot.XmlImporter`

## 5. Execução dos testes

```powershell
dotnet test tests/OpenPlot.UnitTests/OpenPlot.UnitTests.csproj
dotnet test tests/OpenPlot.Api.IntegrationTests/OpenPlot.Api.IntegrationTests.csproj
