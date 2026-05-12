# Marco 05 - Consolidação de `RunsEndpoints` como agregador fino

## Objetivo

Concluir a próxima etapa da refatoração de `RunsEndpoints.cs`, reduzindo o arquivo principal a um papel de composição de rotas e deslocando os blocos restantes para módulos menores e mais coesos.

## O que mudou

- extração dos endpoints fasoriais básicos (`voltage` e `current`) para `RunsPhasorSeriesEndpoints.cs`;
- extração dos endpoints analíticos de sequência e desequilíbrio (`seq` e `unbalance`) para `RunsAnalyticalSeriesEndpoints.cs`;
- extração dos endpoints avançados (`thd`, `power` e `angle-diff`) para `RunsAdvancedSeriesEndpoints.cs`;
- simplificação de `RunsEndpoints.cs`, que passou a concentrar principalmente:
  - helpers compartilhados de composição;
  - criação do grupo principal de rotas;
  - delegação explícita para módulos especializados;
- limpeza de imports obsoletos e remoção de injeção sem uso em endpoint avançado.

## Por que as mudanças foram feitas

- reduzir a carga cognitiva do arquivo principal da feature de Runs;
- aumentar a coesão por subárea funcional, agrupando rotas correlatas no mesmo arquivo;
- facilitar manutenção, navegação e futuras alterações sem expandir novamente `RunsEndpoints.cs`;
- preservar contratos HTTP e handlers existentes, mantendo a refatoração com baixo risco;
- preparar a feature para próximas melhorias estruturais sem depender de uma reescrita ampla.

## Arquivos alterados

- `OpenPlot.Api/Features/Runs/RunsEndpoints.cs`
- `OpenPlot.Api/Features/Runs/RunsPhasorSeriesEndpoints.cs`
- `OpenPlot.Api/Features/Runs/RunsAnalyticalSeriesEndpoints.cs`
- `OpenPlot.Api/Features/Runs/RunsAdvancedSeriesEndpoints.cs`
- `docs/Refatoracao/marco-05-runs-endpoints-agregador-fino.md`

## Decisões de projeto

- manter os helpers de composição em `RunsEndpoints.cs` nesta etapa para evitar espalhamento prematuro de lógica transversal;
- separar os endpoints por afinidade funcional, e não por quantidade fixa de métodos;
- continuar usando extrações pequenas e reversíveis para preservar comportamento e reduzir risco.

## Riscos controlados

- risco de quebra contratual controlado pela manutenção das mesmas rotas e handlers;
- risco de regressão reduzido por validação incremental após as extrações;
- risco de fragmentação excessiva mitigado pela divisão por grupos coesos de responsabilidade.

## Validação executada

- compilação da solução após a extração dos módulos;
- compilação adicional após a simplificação residual de `RunsEndpoints.cs`.

## Impacto esperado na qualidade

- `RunsEndpoints.cs` passa a atuar de forma mais clara como agregador;
- menor acoplamento estrutural dentro da feature de Runs;
- melhor legibilidade e manutenção dos endpoints restantes;
- base mais sólida para avançar em outros hotspots da solução, como `OpenPlot.Ingestor.Gsf` e `OpenPlot.XmlImporter`.
