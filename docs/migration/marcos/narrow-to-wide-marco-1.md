# Marco 1 — Migração Narrow -> Wide (base de leitura/escrita)

## Escopo aplicado
- Criação de mapeamento semântico central na API para tradução segura `signal -> coluna wide`.
- Migração de leitura da API (`MeasurementsRepository`) para `openplot.measurements_wide`.
- Migração de SQL interno dos handlers `PowerSeriesHandler` e `AngleDiffSeriesHandler` para `openplot.measurements_wide`.
- Migração de escrita do ingestor para frame wide com `UPSERT` por `(pdc_pmu_id, ts)`.
- Atualização do bootstrap de schema do ingestor para `openplot.measurements_wide`.
- Migração da leitura do ExportWorker para `openplot.measurements_wide` mantendo shape legado por `SignalId`.

## Arquivos alterados
- `OpenPlot.Api/Features/Runs/Repositories/WideSignalColumnMap.cs`
- `OpenPlot.Api/Features/Runs/Repositories/MeasurementsRepository.cs`
- `OpenPlot.Api/Features/Runs/Handlers/PowerSeriesHandler.cs`
- `OpenPlot.Api/Features/Runs/Handlers/AngleDiffSeriesHandler.cs`
- `OpenPlot.Ingestor.Gsf/Hosting/IngestorChunkPipeline.cs`
- `OpenPlot.Ingestor.Gsf/DbOps.cs`
- `OpenPlot.ExportWorker/Data/MeasurementsRepo.cs`

## Decisões técnicas
- Tradução de valor por `CASE` SQL com whitelist de colunas permitidas.
- Nenhuma interpolação de nome de coluna fornecido por usuário.
- Preservação do contrato atual de saída por `signal_id` no backend/API.
- Escrita wide com merge de colunas (`COALESCE`) para suportar gravação parcial/repetida por timestamp.

## Validação
- Build da solução executado com sucesso após aplicação das mudanças.

## Próximos marcos
- Ajustes de testes automatizados (unit/integration) para cenários Wide.
- Revisão de performance das consultas com projeção `CASE`.
- Evolução de política de exclusão parcial de grandezas (SET NULL + limpeza de frame vazio).
