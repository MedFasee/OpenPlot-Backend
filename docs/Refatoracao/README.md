# Refatoração da solução OpenPlot

## Objetivo

Este diretório registra os marcos de refatoração aplicados na solução com foco em:

- clean code;
- legibilidade;
- redução de duplicação;
- maior coesão;
- melhor modularização;
- evolução gradual da nota técnica da solução.

## Como usar esta documentação

Cada marco deve registrar:

1. objetivo da etapa;
2. arquivos alterados;
3. decisões de projeto;
4. riscos controlados;
5. validação executada;
6. impacto esperado na qualidade.

## Marcos

- `marco-01-bootstrap-api.md` - modularização inicial do bootstrap da API.
- `marco-02-encoding-runs.md` - correções de encoding, estabilização de testes unitários e primeira quebra de responsabilidade em `RunsEndpoints`.
- `marco-03-runs-series-helpers.md` - extração de helpers para reduzir duplicação nos endpoints de séries de `RunsEndpoints`.
- `marco-04-runs-endpoints-modularizacao.md` - separação física de `RunsEndpoints` em módulos menores para listagem, terminais e séries simples.
- `marco-05-runs-endpoints-agregador-fino.md` - consolidação de `RunsEndpoints` como agregador fino com extração dos blocos fasoriais, analíticos e avançados restantes.

## Critério de evolução

A solução será considerada em avanço consistente quando cada marco:

- reduzir acoplamento estrutural;
- preservar comportamento existente;
- melhorar legibilidade do código;
- manter ou ampliar a validação automatizada disponível.
