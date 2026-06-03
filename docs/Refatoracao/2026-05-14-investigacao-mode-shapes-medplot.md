# Investigação do bloqueio de Mode Shapes no Prony

## Contexto
Foi investigada a causa de o recurso de Mode Shapes aparecer desabilitado no comportamento legado do MedPlot.

## Resultado da investigação
No MedPlot, o bloqueio não acontece por causa de modos com baixa energia. Em `GraficoProny.cs`, o botão de `Mode Shapes` é desabilitado apenas quando:
- não existe nenhuma frequência candidata com `0 < f < 10 Hz`; ou
- o número efetivo de sinais usados no cálculo é menor que `2`.

Isso significa que modos "fantasmas" não desabilitam o recurso. Pelo contrário: como o combo legado usa apenas o filtro de frequência positiva abaixo de `10 Hz`, esses modos ainda podem entrar na lista de candidatos.

## Ajuste aplicado no OpenPlot
A migração do OpenPlot ainda publicava `ModeShapeCandidatesHz` mesmo quando havia apenas uma série válida. Isso divergia do MedPlot, que não habilita Mode Shapes nesse cenário.

Ajuste realizado:
- `ModeShapeCandidatesHz` agora só é preenchido quando existem pelo menos duas séries válidas para compor a mode shape;
- os testes foram atualizados para cobrir os cenários com uma série e com múltiplas séries.

## Motivação
Esse alinhamento reduz divergência funcional entre o MedPlot e a API do OpenPlot, evitando que a camada consumidora interprete incorretamente a existência de candidates para Mode Shapes em casos onde o legado manteria o recurso desabilitado.
