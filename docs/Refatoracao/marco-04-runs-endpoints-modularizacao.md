# Marco 04 - Modularização física dos endpoints de Runs

## Objetivo

Continuar a refatoração de `RunsEndpoints.cs`, saindo de uma melhoria apenas interna de helpers para uma separação física por subáreas funcionais, com menor acoplamento e maior coesão entre rotas relacionadas.

## Alterações aplicadas

- extração da listagem de buscas para `RunsListingEndpoints.cs`;
- extração da consulta de terminais para `RunsTerminalEndpoints.cs`;
- extração das séries simples (`frequency`, `dfreq` e `digital`) para `RunsSimpleSeriesEndpoints.cs`;
- delegação explícita no agregador principal por meio de `MapRunsListing()`, `MapRunsTerminals()` e `MapRunsSimpleSeries()`;
- remoção dos handlers inline correspondentes em `RunsEndpoints.cs`;
- reaproveitamento controlado de helpers mínimos do novo módulo de séries simples para manter a transição incremental.

## Arquivos alterados

- `OpenPlot.Api/Features/Runs/RunsEndpoints.cs`
- `OpenPlot.Api/Features/Runs/RunsListingEndpoints.cs`
- `OpenPlot.Api/Features/Runs/RunsTerminalEndpoints.cs`
- `OpenPlot.Api/Features/Runs/RunsSimpleSeriesEndpoints.cs`
- `docs/Refatoracao/marco-04-runs-endpoints-modularizacao.md`

## Decisões de projeto

- priorizar extrações pequenas e reversíveis, preservando contratos HTTP e handlers já existentes;
- usar `RunsEndpoints.cs` como agregador fino, delegando responsabilidades para módulos especializados;
- evitar reescrever lógica de domínio nesta etapa, concentrando o esforço na organização estrutural;
- manter a documentação por marco para registrar impacto, limites e próximos passos.

## Riscos controlados

- risco de quebra contratual reduzido pela preservação das mesmas rotas e handlers;
- risco de regressão funcional minimizado ao mover blocos coesos sem alterar regras centrais;
- risco de fragmentação excessiva controlado pela separação por subárea funcional, e não por endpoint isolado.

## Validação executada

- verificação de erros nos novos arquivos extraídos;
- verificação de erros em `RunsEndpoints.cs` após a delegação das rotas.

## Impacto esperado na qualidade

- redução do tamanho e da carga cognitiva de `RunsEndpoints.cs`;
- melhoria de legibilidade e navegabilidade da feature de Runs;
- aumento de coesão por arquivo, com responsabilidades mais claras;
- melhor base para a próxima etapa de extração das rotas analíticas e fasoriais restantes.

## Próximo passo natural

Extrair os endpoints analíticos e fasoriais remanescentes de `RunsEndpoints.cs` em novos módulos pequenos, mantendo a mesma estratégia incremental e a validação por build e testes unitários.
