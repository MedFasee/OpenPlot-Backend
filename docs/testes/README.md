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
- `OpenPlot.Features.PostProcessing.Handlers.Prony`
  - montagem do resultado de `Compute` com séries válidas;
  - preenchimento de `Specs`, `ModeShapeCandidatesHz`, `OriginalPoints` e `EstimatedPoints`;
  - erro para ordem não positiva;
  - erro para janela inválida;
  - erro para ordem maior ou igual ao número de amostras;
  - erro para janela com poucas amostras para a ordem solicitada.
- `OpenPlot.Features.Runs.Contracts.PlotMetaBuilder`
  - `title`, `xLabel` e `yLabel` para frequência, `dfreq`, THD e diferença angular.
- `OpenPlot.Features.PostProcessing.Handlers.DftMetaBuilder`
  - fallback sem séries;
  - composição de metadados para sequência positiva.
- `OpenPlot.Features.PostProcessing.Handlers.PronyMetaBuilder`
  - fallback sem séries;
  - composição de metadados para sequência positiva.
- `OpenPlot.ExportWorker.Storage.DiskExportStore`
  - resolução do diretório diário em `comtrade/yyyy-MM-dd`;
  - sanitização do nome final do `.zip`;
  - escrita atômica do arquivo;
  - cálculo de `sha256`.
- `ExportEndpoints`
  - decisão de conversão por status (`CanConvertSearchRun`);
  - payload de erro para consulta incompleta (`BuildIncompleteRunError`);
  - expiração por data (`IsExpiredExport`);
  - remoção de arquivo expirado e limpeza de diretório (`DeleteExpiredExportFile`).
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

- `POST /api/v1/auth/login`;
- `POST /api/v1/auth/logout`;
- `GET /api/v1/dft`;
- `GET /api/v1/prony` com sucesso;
- `GET /api/v1/prony` com `404` para `cache_id` inexistente;
- `GET /api/v1/prony` com `400` para ordem inválida ou indisponível na janela.

Nesses testes, a aplicação sobe com pipeline real de Minimal API, sessão, autenticação e middleware, mas com dependências externas substituídas por doubles de teste para manter a execução rápida e determinística.

## Relatório consolidado por feature e projeto

### `OpenPlot.Api`

Cobertura observada por feature:

- `Auth`
  - integração para login e logout.
- `Export`
  - testes unitários para helpers/validações de `ExportEndpoints`;
  - não há, no estado atual, cobertura de integração HTTP para o fluxo completo de exportação.
- `PostProcessing`
  - unitários para `Dft`, `Prony`, `DftMetaBuilder`, `PronyMetaBuilder` e `PlotMetaBuilder`;
  - integração para `GET /api/v1/dft` e `GET /api/v1/prony`.
- `Runs`
  - unitários para `RunsEndpoints`, `BaseSeriesHandler`, `SimpleSeriesHandler`, `PmuQueryHelper`, `SeriesResponseBuilder` e contratos de query.

Lacunas ainda visíveis em `OpenPlot.Api`:

- ausência de testes de integração para `Search`, `Import`, `Catalog/Config` e parte do fluxo de `Runs`;
- ausência de cobertura automatizada específica para repositórios com acesso real a banco (`RunContextRepository`, `MeasurementsRepository`, `AnalysisCacheRepository`);
- não existe hoje cenário testável de entrada para `modeshape` em Prony, porque a API atual apenas retorna `modeShapeCandidatesHz` e não recebe esse valor como parâmetro.

### `OpenPlot.ExportWorker`

Cobertura observada:

- testes unitários para `DiskExportStore`.

Lacunas atuais:

- sem cobertura automatizada observada para `worker.cs`, `ComtradeBuildService`, writers COMTRADE e orquestração completa do job de exportação.

### `OpenPlot.XmlImporter`

Cobertura observada:

- não foi identificado projeto de teste específico nem testes automatizados direcionados ao importador XML no workspace atual.

### `OpenPlot.Ingestor.Gsf`

Cobertura observada:

- não foi identificado projeto de teste específico nem testes automatizados direcionados ao ingestor GSF no workspace atual.

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
dotnet test tests/OpenPlot.UnitTests/OpenPlot.UnitTests.csproj
dotnet test tests/OpenPlot.Api.IntegrationTests/OpenPlot.Api.IntegrationTests.csproj
```

## Observações

- Os testes atuais não dependem de PostgreSQL real.
- Os projetos de teste podem ser executados diretamente por `dotnet test`, mesmo quando não estiverem carregados na `openplot.sln` principal.
- Há avisos de dependências/pacotes durante o build de teste, mas a suíte validada nesta tarefa passou integralmente (`69/69` unitários e `18/18` integração).
- Uma próxima etapa natural é criar um projeto de integração com banco para `RunContextRepository`, `MeasurementsRepository` e `AnalysisCacheRepository`.

