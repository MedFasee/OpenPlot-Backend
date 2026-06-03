# Revisão da migração do Prony

## Contexto
A implementação do `Prony` no OpenPlot foi revisada para alinhar a tabela de modos e as possibilidades de mode shapes ao comportamento original do MedPlot, mantendo o mesmo contrato de entrada e saída já exposto pelo backend.

## O que mudou
- a montagem de `ModeShapeCandidatesHz` passou a seguir o critério legado do MedPlot: frequência positiva abaixo de 10 Hz, preservando repetições e ordenando em ordem crescente;
- a lógica da tabela de modos foi deixada explícita com os mesmos filtros do MedPlot para modos visíveis: frequência positiva abaixo de 10 Hz e energia acima de `1e-3`, ordenada por energia decrescente;
- os testes de unidade e integração do pós-processamento foram atualizados para validar essas regras;
- as séries originais e estimadas permaneceram inalteradas.

## Motivação
O MedPlot é a referência funcional dessa migração. A revisão reduz divergências entre a versão desktop e a API do OpenPlot sem quebrar o payload atual consumido pelo restante da aplicação.
