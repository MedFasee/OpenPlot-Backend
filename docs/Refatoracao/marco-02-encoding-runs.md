# Marco 02 - Encoding e primeira extração de responsabilidades em Runs

## Objetivo

Corrigir problemas visíveis de encoding que afetam testes e legibilidade, além de iniciar a modularização incremental de `RunsEndpoints.cs` nas áreas mais estáveis.

## Escopo desta etapa

- corrigir mensagens quebradas no fluxo de exportação;
- estabilizar a suíte unitária afetada por encoding;
- extrair helpers de `RunsEndpoints.cs` para reduzir complexidade nas rotas de listagem e terminais;
- manter contratos HTTP e comportamento funcional.

## Estratégia

1. corrigir primeiro o problema textual que já quebra teste automatizado;
2. criar helpers internos para separar montagem de calendário e resolução de metadados de terminais;
3. validar por build e testes unitários;
4. registrar o avanço neste documento.

## Critérios de pronto

- mensagens corrigidas e legíveis;
- teste unitário de exportação estabilizado;
- `RunsEndpoints.cs` menor e com responsabilidades mais explícitas;
- build da solução permanecendo íntegro.

## Registro de execução

### Alterações aplicadas

- correção de textos com encoding quebrado em `OpenPlot.Api/Features/Export/ExportEndpoints.cs`;
- centralização de mensagens do fluxo de exportação em constantes nomeadas;
- extração de helpers internos em `OpenPlot.Api/Features/Runs/RunsEndpoints.cs` para:
  - montagem do calendário de runs;
  - resolução de `SearchRun` por id ou label;
  - carregamento de PMUs do run;
  - montagem da resposta de terminais.

### Arquivos alterados

- `OpenPlot.Api/Features/Export/ExportEndpoints.cs`
- `OpenPlot.Api/Features/Runs/RunsEndpoints.cs`
- `docs/Refatoracao/README.md`
- `docs/Refatoracao/marco-01-bootstrap-api.md`
- `docs/Refatoracao/marco-02-encoding-runs.md`

### Decisões de projeto

- manter os helpers dentro de `RunsEndpoints.cs` nesta iteração para reduzir risco e evitar fragmentação prematura;
- corrigir primeiro os textos da exportação porque havia uma falha unitária explícita associada ao problema;
- preservar integralmente os contratos HTTP e o formato das respostas.

### Validação prevista para o marco

- build da solução;
- testes unitários do projeto `OpenPlot.UnitTests`.

### Impacto esperado

- remoção de uma falha de qualidade perceptível ligada a encoding;
- melhora de legibilidade no fluxo de exportação;
- redução da complexidade local de `RunsEndpoints.cs` nas rotas mais estáveis;
- melhor base para futuras extrações por subfeature.
