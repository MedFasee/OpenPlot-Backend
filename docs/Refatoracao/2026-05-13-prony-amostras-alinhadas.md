# Marco de Refatoração - Correção da aplicação do Prony com amostras alinhadas

## Contexto
A migração do núcleo de Prony preservava a estrutura de saída, mas ainda aplicava o método de forma diferente do MedPlot ao preparar os sinais de entrada. Isso podia gerar um primeiro valor incorreto por reconstrução artificial da janela e propagar erro para todo o ajuste.

## Problema identificado
No MedPlot, o Prony é aplicado sobre amostras reais já indexadas e alinhadas no gráfico. No OpenPlot, o algoritmo ainda podia:
- reconstruir uma grade temporal uniforme antes de usar os pontos reais;
- preencher o início da janela com `hold-last` quando uma série não cobria exatamente o primeiro instante.

Esse comportamento podia inventar o primeiro ponto efetivo do sinal usado no ajuste.

## O que mudou
- O `Prony.cs` passou a priorizar o uso de amostras reais alinhadas do cache.
- Quando existe um conjunto de timestamps alinhado entre as séries, o método usa esses pontos diretamente, como no MedPlot.
- O fallback por grade uniforme só mantém séries que realmente cobrem a janela solicitada, evitando preenchimento artificial no início.

## Impacto esperado
- Maior paridade entre o OpenPlot e o MedPlot no núcleo do ajuste.
- Eliminação do erro causado por um primeiro ponto sintético no sinal de entrada.
- Menor propagação de erro para polos, resíduos, energia e reconstrução estimada.

## Observações
A estrutura pública de saída do Prony foi mantida. A correção atuou apenas na preparação e aplicação do método sobre os sinais do cache.