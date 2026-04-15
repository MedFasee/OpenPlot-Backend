# Testes automatizados

## Visão geral

A suíte de testes está centralizada no diretório `tests/` e atualmente possui dois projetos focados nos pontos mais sensíveis do backend:

- `tests/OpenPlot.UnitTests`
- `tests/OpenPlot.Api.IntegrationTests`

O objetivo é cobrir regras puras, composição de metadados, escrita de artefatos, contratos de resposta e fluxos HTTP principais sem depender de infraestrutura externa desnecessária.

## Escopo atual

### `tests/OpenPlot.UnitTests`

Cobertura unitária para:

- `OpenPlot.Features.PostProcessing.Handlers.Dft`
  - cálculo de `ZoomBounds`;
  - reamostragem `hold-last`;
  - FFT single-sided;
  - montagem do resultado de `Compute`.
- `OpenPlot.Features.Runs.Contracts.PlotMetaBuilder`
  - `title`, `xLabel` e `yLabel` para frequência, `dfreq`, THD e diferença angular.
- `OpenPlot.Features.PostProcessing.Handlers.DftMetaBuilder`
  - fallback sem séries;
  - composição de metadados para sequência positiva.
- `OpenPlot.ExportWorker.Storage.DiskExportStore`
  - resolução do diretório diário em `comtrade/yyyy-MM-dd`;
  - sanitização do nome final do `.zip`;
  - escrita atômica do arquivo;
  - cálculo de `sha256`.
- `RunsEndpoints`
  - propagação e valor padrão de `conv_comtrade`;
  - projeção SQL de `conv_comtrade` em `SearchSql.ListRuns`.
- handlers e utilitários de séries
  - validação base de `BaseSeriesHandler`;
  - fluxo de downsampling e cache em `SimpleSeriesHandler`;
  - normalização de PMUs em `PmuQueryHelper`;
  - composição de payload em `SeriesResponseBuilder`;
  - contratos de `ISeriesQuery`, `AngleDiffQuery`, `ByRunQuery` e `PowerPlotQuery`.

### `tests/OpenPlot.Api.IntegrationTests`

Cobertura de integração HTTP com `WebApplicationFactory<Program>` para:

- `POST /api/v1/auth/login`
- `POST /api/v1/auth/logout`
- `GET /api/v1/dft`

Nesses testes, a aplicação sobe com pipeline real de Minimal API, sessão, autenticação e middleware, mas com dependências externas substituídas por doubles de teste para manter execução rápida e determinística.

## Infra de teste

Os testes unitários usam `xUnit`, `Moq` e helpers compartilhados em `tests/OpenPlot.UnitTests/Infrastructure` para padronizar mocks e asserções de resultados HTTP.

Os testes de integração usam:

- `WebApplicationFactory<Program>`;
- esquema de autenticação de teste;
- repositório de cache analítico em memória;
- repositório de log de requests no-op;
- serviço de autenticação fake.

Para suportar esse cenário, `OpenPlot.Api/Program.cs` expõe `public partial class Program`.

## Como executar

Na raiz do repositório:

```powershell
# unitários
dotnet test tests/OpenPlot.UnitTests/OpenPlot.UnitTests.csproj

# integração HTTP
dotnet test tests/OpenPlot.Api.IntegrationTests/OpenPlot.Api.IntegrationTests.csproj

# todos os testes
dotnet test tests/OpenPlot.UnitTests/OpenPlot.UnitTests.csproj && dotnet test tests/OpenPlot.Api.IntegrationTests/OpenPlot.Api.IntegrationTests.csproj
```

## Observações

- Os testes atuais não dependem de PostgreSQL real.
- Os projetos de teste podem ser executados diretamente por `dotnet test`, mesmo quando não estiverem carregados na `openplot.sln` principal.
- Uma próxima etapa natural é criar um projeto de integração com banco para `RunContextRepository`, `MeasurementsRepository` e `AnalysisCacheRepository`.
