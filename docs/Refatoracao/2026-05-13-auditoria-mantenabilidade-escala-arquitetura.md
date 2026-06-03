# Marco de Refatoração: Auditoria de mantenabilidade, escala horizontal e arquitetura

## Objetivo
Aplicar uma primeira rodada de refatorações estruturais derivadas da auditoria técnica da solução, reduzindo acoplamento, removendo duplicações transversais e preparando a API para evolução com fronteiras mais claras.

## O que mudou

### 1. Centralização do contexto do usuário
Foi criado o serviço `IUserContextAccessor` em `OpenPlot.Api/Services/Security/UserContextAccessor.cs` para resolver usuário e identificador a partir de claims e fallback de sessão.

Arquivos ajustados:
- `OpenPlot.Api/Services/Logging/RequestLoggingMiddleware.cs`
- `OpenPlot.Api/Features/Search/SearchEndpoints.cs`
- `OpenPlot.Api/Features/Runs/RunsListingEndpoints.cs`
- `OpenPlot.Api/Features/Export/ExportEndpoints.cs`
- `OpenPlot.Api/Configuration/OpenPlotApiServiceCollectionExtensions.cs`

Motivo:
- eliminar duplicação de lógica de identidade;
- reduzir inconsistências entre endpoints e middleware;
- facilitar evolução de autenticação/autorização.

### 2. Extração do fluxo de importação
Foi criado o serviço `IXmlImportService` em `OpenPlot.Api/Features/Import/XmlImportService.cs` para encapsular a orquestração do importador XML.

Arquivo ajustado:
- `OpenPlot.Api/Features/Import/ImportEndpoints.cs`

Motivo:
- remover conhecimento de configuração e instanciação do importador de dentro do endpoint;
- deixar o endpoint mais fino e focado em HTTP.

### 3. Extração das operações de arquivo de exportação
Foi criado o serviço `IExportFileService` em `OpenPlot.Api/Features/Export/ExportFileService.cs` para centralizar purge de exportações expiradas e resolução do arquivo físico.

Arquivo ajustado:
- `OpenPlot.Api/Features/Export/ExportEndpoints.cs`

Motivo:
- reduzir mistura entre camada HTTP, regra de fluxo e acesso a filesystem;
- preparar o caminho para futura substituição de disco local por storage compartilhado.

### 4. Correção de boundary leakage em TimeSeries
Os tipos `ITimeSeries` e `TimeSeries` da API deixaram de usar o namespace do ingestor e passaram a pertencer à própria API.

Arquivos ajustados:
- `OpenPlot.Api/Data/ITimeSeries.cs`
- `OpenPlot.Api/Data/TimeSeries.cs`

Motivo:
- remover vazamento de domínio entre projetos;
- explicitar melhor a fronteira entre API e ingestor.

## Impacto esperado
- Melhor legibilidade e coesão em fluxos da API.
- Menor duplicação de regras transversais.
- Melhor base para evoluções futuras ligadas a clean architecture.
- Preparação incremental para cenários de expansão horizontal, especialmente no fluxo de exportação.

## Pendências futuras recomendadas
- Extrair serviços de aplicação para os fluxos de search/export em maior profundidade.
- Remover acoplamentos restantes da API com projetos de infraestrutura.
- Atacar a refatoração estrutural do `OpenPlot.Ingestor.Gsf/Program.cs`, hoje ainda concentrando responsabilidades demais.
