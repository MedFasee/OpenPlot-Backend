# Marco de Refatoração - Payload de modos com `enabled` para Prony e DFT

## Contexto
Após ajustar a lógica dinâmica do Prony no `by-run`, foi identificado que a migração ainda não respeitava o formato de resposta esperado pelo front: Prony não deve desaparecer do payload quando indisponível, e DFT também deve expor sinalização explícita de habilitação.

## O que mudou
- `DFT` passou a ser retornado como objeto com campo `enabled`.
- `Prony` passou a ser retornado sempre como objeto com:
  - `enabled`
  - `Ordem`
- O cálculo da ordem padrão do Prony foi separado da lógica de habilitação.

## Regras aplicadas
- A ordem padrão do Prony continua sendo derivada da janela efetiva, seguindo a lógica herdada do MedPlot para o valor inicial.
- A habilitação é calculada separadamente com base na viabilidade do cálculo para a janela/resolução.
- Mesmo quando desabilitado, o item permanece no payload com `enabled = false`.

## Motivo do ajuste
A migração anterior capturou parte da lógica funcional, mas não preservou corretamente o contrato implícito esperado pelo front. Este ajuste corrige essa diferença de integração sem remover a lógica dinâmica recém-introduzida.

## Impacto esperado
- O front passa a receber um payload estável para `DFT` e `Prony`.
- A indisponibilidade do Prony deixa de ser inferida por ausência do item e passa a ser explicitada por `enabled = false`.