# Marco de Refatoração - Paridade do núcleo de Prony

## Contexto
A migração do Prony do MedPlot para o OpenPlot já preservava a estrutura geral do algoritmo, mas ainda havia diferenças com potencial de alterar os resultados numéricos do núcleo de processamento.

## O que mudou
- O processamento passou a priorizar a projeção exata das amostras na grade temporal uniforme da janela antes de recorrer ao `hold-last`.
- A pseudoinversa usada na etapa dos resíduos passou a seguir a mesma formulação algébrica do MedPlot, reconstruindo a inversa complexa a partir do bloco real equivalente de `Z^T Z`.

## Motivo das mudanças
O objetivo deste marco é reduzir divergências entre o backend e o comportamento legado do MedPlot, preservando a semântica do núcleo do método de Prony e aproximando os resultados produzidos nos dois ambientes.

## Impacto esperado
- Maior paridade quando as séries já estiverem alinhadas na grade esperada pela taxa de amostragem.
- Menor diferença numérica na etapa de cálculo dos resíduos e, por consequência, em amplitude, fase, energia e reconstrução dos sinais estimados.

## Observações
Ainda podem existir pequenas diferenças residuais por conta da biblioteca numérica utilizada no OpenPlot em relação ao ambiente legado do MedPlot.