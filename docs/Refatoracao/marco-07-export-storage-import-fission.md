# Marco 07 - estabilização do ingestor, afinação do export e fatiamento do import XML

## Objetivo da etapa

Concluir a estabilização da refatoração recente do ingestor, reduzir o acoplamento do fluxo de export com service locator e filesystem, e iniciar a quebra do importador XML monolítico em componentes menores e mais coesos.

## Arquivos alterados

- `OpenPlot.Ingestor.Gsf/Hosting/IngestorJobProcessor.cs`
- `OpenPlot.Ingestor.Gsf/Hosting/IngestorChunkPipeline.cs`
- `OpenPlot.ExportWorker/worker.cs`
- `OpenPlot.ExportWorker/ExportRunProcessor.cs`
- `OpenPlot.ExportWorker/Program.cs`
- `OpenPlot.ExportWorker/Storage/DiskExportStore.cs`
- `OpenPlot.Api/Features/Export/ExportArtifactStore.cs`
- `OpenPlot.Api/Features/Export/ExportFileService.cs`
- `OpenPlot.Api/Features/Export/ExportEndpoints.cs`
- `OpenPlot.Api/Configuration/OpenPlotApiServiceCollectionExtensions.cs`
- `OpenPlot.Api/Features/Import/XmlCatalogImporter.cs`
- `OpenPlot.Api/Features/Import/XmlCatalogModels.cs`
- `OpenPlot.Api/Features/Import/XmlCatalogParser.cs`
- `OpenPlot.Api/Features/Import/XmlCatalogPersistence.cs`

## Decisões de projeto

1. A regressão do ingestor foi corrigida primeiro para restaurar a capacidade de build antes de seguir com outras refatorações.
2. O `Worker` de export foi reduzido a um `BackgroundService` fino, delegando a lógica de negócio para `IExportRunProcessor`.
3. O storage de artefatos de export passou a ser acessado por abstrações explícitas, mantendo a implementação em disco local como adaptação inicial.
4. Os endpoints da API deixaram de conhecer diretamente paths e filesystem para download de export, centralizando essa decisão no serviço especializado.
5. O importador XML foi reestruturado em parser, modelos intermediários e camada de persistência para reduzir tamanho de classe e facilitar novos cortes futuros.

## Riscos controlados

- Preservação do comportamento existente de export COMTRADE enquanto o worker foi desacoplado do `IServiceProvider` no nível do loop.
- Compatibilidade total com armazenamento em disco local apesar da introdução das abstrações de artefato.
- Manutenção do fluxo atual de importação XML com tratamento de notas e erros por arquivo.

## Validação executada

- Build completo da solução após a correção do ingestor.
- Build completo da solução após a extração do processador de export.
- Build completo da solução após introdução das abstrações de storage e o fatiamento do importador XML.
- Verificação no Test Explorer de que não há testes cadastrados para `OpenPlot.Ingestor.Gsf`, limitando a validação desse projeto à compilação.

## Impacto esperado na qualidade

- Redução do acoplamento estrutural no worker de export.
- Melhor preparação para suportar storage remoto de export em evolução futura.
- Endpoints HTTP mais finos e aderentes ao papel de adaptadores.
- Menor complexidade no importador XML, com responsabilidades mais coesas e mais simples de evoluir.
- Maior estabilidade da solução após consolidação da refatoração do ingestor.
