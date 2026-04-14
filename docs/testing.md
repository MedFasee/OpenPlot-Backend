# Testes automatizados

## Visão geral

A solução agora possui dois projetos de teste focados nos pontos mais sensíveis do backend:

- `tests/OpenPlot.UnitTests`
- `tests/OpenPlot.Api.IntegrationTests`

O objetivo é cobrir regras puras, composição de metadados, escrita de artefatos e fluxos HTTP principais sem depender de infraestrutura externa desnecessária.

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
  - resolução de diretório/nome final;
  - escrita atômica do arquivo;
  - cálculo de `sha256`.

### `tests/OpenPlot.Api.IntegrationTests`

Cobertura de integração HTTP com `WebApplicationFactory<Program>` para:

- `POST /api/v1/auth/login`
- `POST /api/v1/auth/logout`
- `GET /api/v1/dft`

Nesses testes, a aplicação sobe com pipeline real de Minimal API, sessão, autenticação e middleware, mas com dependências externas substituídas por doubles de teste para manter execução rápida e determinística.

## Infra de teste

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

- Os testes adicionados não dependem de PostgreSQL real.
- A próxima etapa natural é criar um terceiro projeto de integração com banco para `RunContextRepository`, `MeasurementsRepository` e `AnalysisCacheRepository`.
- A suíte atual foi desenhada para validar os blocos com maior retorno imediato: cálculo, metadados, artefatos e endpoints críticos.
