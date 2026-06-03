# Marco de Refatoração: Desacoplamento da API e reorganização do Ingestor

## Objetivo
Reduzir o acoplamento arquitetural entre a API e projetos de infraestrutura/processamento, além de iniciar a reorganização estrutural do ingestor para um modelo mais aderente a hosting moderno, shutdown cooperativo e evolução horizontal.

## O que mudou

### 1. Remoção do acoplamento direto da API com projetos externos
A API deixou de depender diretamente de `OpenPlot.XmlImporter` e `OpenPlot.Ingestor.Gsf` no arquivo de projeto.

Arquivo ajustado:
- `OpenPlot.Api/OpenPlot.csproj`

Motivo:
- remover dependência de camada inadequada;
- permitir evolução mais independente da API;
- reduzir propagação de responsabilidades de infraestrutura para o projeto web.

### 2. Internalização do importador XML necessário à API
A lógica necessária para importação XML foi internalizada em `OpenPlot.Api/Features/Import/XmlCatalogImporter.cs`, e o serviço `XmlImportService` passou a depender apenas dessa implementação local.

Arquivos ajustados:
- `OpenPlot.Api/Features/Import/XmlCatalogImporter.cs`
- `OpenPlot.Api/Features/Import/XmlImportService.cs`

Motivo:
- eliminar o uso direto do projeto `OpenPlot.XmlImporter` pela API;
- manter o caso de uso disponível sem dependência cruzada de projeto.

### 3. Extração da configuração operacional do ingestor
A configuração do ingestor foi movida para tipos dedicados em `OpenPlot.Ingestor.Gsf/Hosting`.

Arquivos criados:
- `OpenPlot.Ingestor.Gsf/Hosting/IngestorOptions.cs`
- `OpenPlot.Ingestor.Gsf/Hosting/IngestorRuntimeContext.cs`
- `OpenPlot.Ingestor.Gsf/Hosting/IngestorConfigurationLoader.cs`

Arquivo ajustado:
- `OpenPlot.Ingestor.Gsf/Program.cs`

Motivo:
- reduzir estado global espalhado;
- centralizar opções operacionais e recursos compartilhados;
- preparar o processo para um modelo mais testável e observável.

### 4. Introdução de BackgroundService no ingestor
Foi criado o `IngestorWorker` como `BackgroundService`, passando a executar o loop operacional com `CancellationToken` e `Task.Delay` cooperativo.

Arquivos criados/ajustados:
- `OpenPlot.Ingestor.Gsf/Hosting/IngestorWorker.cs`
- `OpenPlot.Ingestor.Gsf/Program.cs`
- `OpenPlot.Ingestor.Gsf/OpenPlot.Ingestor.Gsf.csproj`

Motivo:
- substituir o modelo com `while(true)` e `Thread.Sleep` no fluxo principal;
- melhorar shutdown gracioso;
- alinhar o projeto ao padrão moderno de worker service.

### 5. Simplificação do bootstrap do ingestor
O `Program.cs` passou a atuar mais como composição e bootstrap: carrega runtime, garante schema e inicializa o host.

Motivo:
- reduzir a responsabilidade do ponto de entrada;
- explicitar a separação entre bootstrap e execução do trabalho em background.

## Impacto esperado
- Menor acoplamento arquitetural da API.
- Melhor base para modularização futura do ingestor.
- Melhor comportamento operacional em cenários de parada e evolução para múltiplas instâncias.
- Avanço incremental em clean architecture e manutenabilidade.

## Pendências futuras recomendadas
- Continuar a fatiar `OpenPlot.Ingestor.Gsf/Program.cs`, extraindo regras de negócio e acesso a dados para serviços dedicados.
- Remover o restante de APIs estáticas do `Program` no ingestor.
- Evoluir exportação para storage compartilhado, removendo dependência de disco local para escala horizontal.
- Revisar a API para retirar outros acoplamentos de infraestrutura ainda presentes em endpoints e serviços.
