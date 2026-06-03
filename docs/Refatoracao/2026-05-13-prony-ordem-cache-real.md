# Marco de Refatoração - Ordem dinâmica do Prony baseada no cache real

## Contexto
A lógica dinâmica da ordem do Prony no `by-run` ainda usava uma aproximação baseada apenas em janela temporal e taxa de amostragem. Para maior paridade com o processamento real do pós-processamento, o cálculo passou a considerar o payload de cache efetivamente gerado para o Prony.

## O que foi analisado
- O `by-run` monta um `RowsCacheV2` antes do downsampling de visualização.
- O cache contém:
  - `From` / `To`
  - `SelectRate`
  - todas as séries selecionadas
  - todos os pontos brutos por série
- O `Prony.Compute` usa exatamente esse payload para:
  - determinar o grid uniforme comum (`n`)
  - identificar séries válidas
  - validar restrições da ordem

## O que mudou
- O cálculo de `Ordem` e `enabled` passou a usar `UiMenuContext.FromCache(cachePayload)`.
- A contagem efetiva de pontos agora é derivada do mesmo grid uniforme implícito no cache.
- A habilitação do Prony considera também:
  - total de séries no cache
  - séries válidas com pelo menos 2 pontos
  - a desigualdade de viabilidade usada no próprio `Prony.Compute`

## Impacto esperado
- Melhor aderência entre o valor sugerido no `by-run` e o que o pós-processamento realmente consegue executar.
- Menor dependência de inferência puramente temporal quando o cache já contém a fonte real de verdade.
- Maior paridade funcional entre preparação de dados e execução do Prony.

## Observações
O `DFT` e o `Prony` continuam presentes no payload `modes` com campo `enabled`. Para o Prony, a ordem continua sendo enviada mesmo quando o cálculo estiver desabilitado.