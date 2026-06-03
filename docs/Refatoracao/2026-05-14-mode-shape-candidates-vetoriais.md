# Enriquecimento de modeShapeCandidatesHz no Prony

## Contexto
O retorno de `modeShapeCandidatesHz` no endpoint de Prony foi evoluído para já transportar o vetor mínimo necessário ao traçado de mode shapes, seguindo o comportamento funcional esperado a partir do MedPlot.

## O que mudou
- `modeShapeCandidatesHz` deixou de ser uma lista simples de frequências;
- cada candidato agora contém:
  - `index` do modo;
  - `frequencyHz`;
  - `vector` com os pontos por série válidos;
- cada item de `vector` inclui:
  - identificação da série;
  - `pmu`, `phase`, `component`, `quantity`, `unit`;
  - `amplitude` e `phaseRad`.

## Regras mantidas
- candidatos de mode shape continuam existindo apenas quando há pelo menos `2` séries válidas;
- a frequência candidata continua obedecendo ao filtro legado do MedPlot: `1e-6 < f < 10 Hz`.

## Motivação
Essa mudança reduz trabalho na camada consumidora do endpoint, porque a resposta do Prony já entrega o conjunto mínimo necessário para selecionar um modo e traçar sua mode shape sem recompor o vetor por junções adicionais no cliente.
