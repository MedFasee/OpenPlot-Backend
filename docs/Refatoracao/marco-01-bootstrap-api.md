# Marco 01 - Modularização do bootstrap da API

## Objetivo

Reduzir o acoplamento e a complexidade do `OpenPlot.Api/Program.cs`, transformando o ponto de entrada da API em um orquestrador enxuto.

## Alterações aplicadas

- criação de `OpenPlot.Api/Configuration/OpenPlotApiServiceCollectionExtensions.cs`;
- criação de `OpenPlot.Api/Configuration/OpenPlotApiApplicationBuilderExtensions.cs`;
- extração da configuração de serviços da API para métodos coesos;
- extração da configuração do pipeline HTTP para composição dedicada;
- simplificação do `Program.cs`.

## Arquivos alterados

- `OpenPlot.Api/Program.cs`
- `OpenPlot.Api/Configuration/OpenPlotApiServiceCollectionExtensions.cs`
- `OpenPlot.Api/Configuration/OpenPlotApiApplicationBuilderExtensions.cs`

## Ganhos obtidos

- redução significativa do tamanho e da complexidade do `Program.cs`;
- remoção de duplicações evidentes de DI;
- separação explícita entre composição de serviços e pipeline;
- base mais segura para refatorações futuras.

## Validação executada

- compilação da solução com sucesso;
- testes de integração da API executados com sucesso no marco;
- falha unitária identificada como pré-existente por problema de encoding em strings de exportação.

## Impacto esperado na qualidade

- melhora direta em legibilidade e modularização;
- melhora indireta na manutenção do bootstrap;
- redução do risco de evolução no ponto de entrada da API.
