# Marco 06 - fatiamento do ingestor e coordenação distribuída

## Objetivo da etapa

Reduzir o tamanho e o acoplamento do fluxo principal do ingestor, separando a coleta de jobs, o processamento do job e o pipeline pesado de chunks em serviços menores, além de diminuir o custo da coordenação distribuída de slots de chunk.

## Arquivos alterados

- `OpenPlot.Ingestor.Gsf/Program.cs`
- `OpenPlot.Ingestor.Gsf/Hosting/IngestorJobService.cs`
- `OpenPlot.Ingestor.Gsf/Hosting/QueuedJobPicker.cs`
- `OpenPlot.Ingestor.Gsf/Hosting/IngestorJobProcessor.cs`
- `OpenPlot.Ingestor.Gsf/Hosting/IngestorChunkPipeline.cs`
- `OpenPlot.Ingestor.Gsf/Hosting/IngestorProgressReporter.cs`
- `OpenPlot.Ingestor.Gsf/Hosting/SearchRunJob.cs`
- `OpenPlot.Ingestor.Gsf/Hosting/IngestorJobExceptions.cs`
- `OpenPlot.Ingestor.Gsf/Hosting/DistributedChunkCoordinator.cs`

## Decisões de projeto

1. `IngestorJobService` passou a atuar apenas como laço de worker, delegando a retirada da fila e o processamento completo para serviços dedicados.
2. `QueuedJobPicker` centraliza a seleção transacional de jobs em `queued`, mantendo `FOR UPDATE SKIP LOCKED` fora do orquestrador principal.
3. `IngestorJobProcessor` ficou responsável pela orquestração do job, status e tratamento de erro, enquanto `IngestorChunkPipeline` concentra carregamento de canais, leitura dos intervalos e persistência de medições.
4. A coordenação distribuída por advisory lock passou a reutilizar uma única conexão por tentativa completa de aquisição, reduzindo abertura e descarte de conexões quando todos os slots estão ocupados.
5. O ponto de entrada registrou explicitamente as novas dependências para manter o bootstrap enxuto e previsível.

## Riscos controlados

- Preservação do comportamento de cancelamento por job durante o processamento paralelo dos chunks.
- Manutenção do fluxo existente de marcação de status (`running`, `done`, `no_data`, `failed`, `canceled`, `bad_connection`).
- Continuidade da coordenação distribuída global sem voltar para controle em memória local.

## Validação executada

- Verificação de erros nos arquivos novos e alterados do ingestor após a extração dos serviços.
- Verificação de erros no coordenador distribuído após a otimização do ciclo de aquisição dos locks.

## Impacto esperado na qualidade

- Menor complexidade por classe no ingestor.
- Maior coesão entre responsabilidades operacionais.
- Menor custo de manutenção para evoluir fila, processamento e pipeline de chunks de forma independente.
- Menor churn de conexão na disputa por slots globais, ajudando a estabilidade em cenários com múltiplas instâncias.
