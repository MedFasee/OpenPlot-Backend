# Marco de Refatoração: Extração do núcleo operacional do Ingestor

## Objetivo
Reduzir a concentração de responsabilidades no `Program.cs` do `OpenPlot.Ingestor.Gsf`, remover o acoplamento do worker com métodos estáticos do ponto de entrada e substituir a coordenação global em memória por uma estratégia distribuída baseada em banco.

## O que mudou

### 1. Extração do processamento de jobs para serviço dedicado
A lógica principal de loop operacional, dequeue de jobs, processamento, progresso, verificação de cancelamento e pipeline de chunks foi movida para `OpenPlot.Ingestor.Gsf/Hosting/IngestorJobService.cs`.

Motivo:
- retirar responsabilidade operacional do `Program.cs`;
- melhorar coesão e testabilidade;
- criar uma fronteira mais clara entre bootstrap e execução de domínio operacional.

### 2. Desacoplamento do worker em relação ao Program
O `IngestorWorker` deixou de chamar métodos estáticos do `Program` e passou a depender de `IIngestorJobService`.

Arquivo ajustado:
- `OpenPlot.Ingestor.Gsf/Hosting/IngestorWorker.cs`

Motivo:
- reduzir acoplamento estrutural;
- alinhar o worker ao uso de serviços explícitos via DI.

### 3. Substituição da coordenação local por coordenação distribuída
Foi criada a abstração `IChunkExecutionCoordinator` com implementação `PostgresAdvisoryLockChunkExecutionCoordinator` em `OpenPlot.Ingestor.Gsf/Hosting/DistributedChunkCoordinator.cs`.

Motivo:
- remover dependência de limitação global em memória do processo;
- permitir controle de concorrência global entre múltiplas instâncias usando advisory locks do PostgreSQL.

### 4. Simplificação do runtime context
`IngestorRuntimeContext` deixou de carregar `SemaphoreSlim` local e passou a manter apenas opções operacionais compartilhadas.

Arquivo ajustado:
- `OpenPlot.Ingestor.Gsf/Hosting/IngestorRuntimeContext.cs`

Motivo:
- remover estado local desnecessário;
- refletir o novo modelo de coordenação distribuída.

### 5. Redução do Program.cs a bootstrap
O `Program.cs` agora contém apenas bootstrap: inicialização de dependências base, carregamento de configuração, garantia de schema e composição do host.

Arquivo ajustado:
- `OpenPlot.Ingestor.Gsf/Program.cs`

Motivo:
- transformar o ponto de entrada em composição pura;
- reduzir o principal hotspot de risco arquitetural do projeto.

## Impacto esperado
- Melhor manutenabilidade do ingestor.
- Melhor separação entre hosting e processamento.
- Menor acoplamento com métodos estáticos globais.
- Melhor preparação para cenários com múltiplas instâncias do ingestor.

## Pendências futuras recomendadas
- Extrair o pipeline de acesso a banco e mapeamento de canais para repositórios/serviços menores.
- Introduzir logging estruturado no `IngestorJobService`, reduzindo dependência de `Console.WriteLine`.
- Evoluir a coordenação distribuída para telemetria e diagnósticos de contenção.
