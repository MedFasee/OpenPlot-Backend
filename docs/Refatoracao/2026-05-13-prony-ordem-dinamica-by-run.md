# Marco de Refatoração - Prony dinâmico no by-run

## Contexto
As respostas `by-run` do OpenPlot estavam enviando a configuração do Prony com ordem fixa em `300`, sem considerar a janela efetiva do run nem a resolução disponível. No MedPlot, a ordem inicial do Prony é dinâmica e depende da janela visível, e o recurso só fica disponível quando a janela atende às restrições do cálculo.

## O que mudou
- A montagem de `modes` passou a recalcular a configuração do Prony com base na janela efetiva retornada ao front e no `select_rate` do run.
- A ordem inicial enviada ao front agora segue a lógica herdada do MedPlot:
  - usa o span de pontos da janela;
  - adota aproximadamente um quarto desse span como ordem inicial;
  - limita o valor máximo em `300`.
- O payload deixa de expor `Prony` quando a janela/resolução não suportam cálculo válido.

## Regras aplicadas
- janela máxima de 60 segundos;
- pelo menos 4 pontos na janela;
- `select_rate` válido;
- ordem maior que zero e menor que o número de pontos;
- teto de 300 para a ordem enviada ao front.

## Impacto esperado
- O front recebe uma ordem inicial de Prony coerente com a janela do `by-run`, em vez de um valor estático.
- O menu de Prony deixa de aparecer habilitado quando a própria janela do run inviabiliza o cálculo.
- O comportamento fica alinhado ao fluxo legado observado no MedPlot para o repasse inicial da ordem do Prony.

## Observações
A lógica do backend representa a janela efetiva devolvida pelo `by-run`. Reajustes posteriores por zoom continuam sendo responsabilidade da interface cliente.