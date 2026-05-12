# Marco 03 - Redução de duplicação nos endpoints de séries de Runs

## Objetivo

Continuar a refatoração de `RunsEndpoints.cs`, reduzindo duplicação nos endpoints de séries e deixando a composição de requests mais explícita, previsível e coesa.

## Alterações aplicadas

- extração de helpers internos para composição de `WindowQuery`;
- extração de helpers para montagem de `ByRunQuery`, `SimpleSeriesQuery`, `SeqRunQuery`, `UnbalanceRunQuery`, `PowerPlotQuery` e `AngleDiffQuery`;
- extração de helpers para `SeqRequest` e `UnbalanceRequest`;
- extração de helper para normalização de PMUs;
- extração de helper para criação de `MeasurementsQuery` simples;
- extração de helpers para modos de UI (`oscillations` e `oscillations + events`);
- simplificação dos endpoints `voltage`, `current`, `seq`, `unbalance`, `frequency`, `dfreq`, `thd`, `digital`, `power` e `angle-diff`.

## Arquivos alterados

- `OpenPlot.Api/Features/Runs/RunsEndpoints.cs`
- `docs/Refatoracao/marco-03-runs-series-helpers.md`

## Decisões de projeto

- manter os helpers dentro do próprio arquivo nesta etapa para evitar fragmentação excessiva;
- centralizar composição repetida antes de separar fisicamente os endpoints por arquivo;
- preservar contratos HTTP e handlers existentes.

## Ganhos obtidos

- menor repetição estrutural nos endpoints de séries;
- melhor legibilidade do fluxo principal de `MapRuns`;
- decisões comuns agora concentradas em métodos nomeados;
- redução do risco de inconsistência entre endpoints equivalentes.

## Validação prevista

- compilação da solução;
- testes unitários do projeto `OpenPlot.UnitTests`.

## Próximo passo natural

Separar `RunsEndpoints.cs` em componentes por subárea, por exemplo:

- listagem e terminais;
- séries simples;
- séries fasoriais;
- séries analíticas.
