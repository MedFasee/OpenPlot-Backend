# Validação da ordem do Prony por N/4

## Contexto
A validação da ordem solicitada ao Prony foi ajustada para seguir a regra de negócio baseada no número de amostras efetivas da janela analisada.

## O que mudou
- a ordem máxima permitida no cálculo do Prony passou a ser `número de amostras / 4` com arredondamento para inteiro;
- a validação ocorre antes da execução do ajuste do Prony, rejeitando requisições com ordem acima do limite calculado para a janela atual;
- o cálculo de ordem padrão e a habilitação do menu de Prony também foram alinhados à mesma regra na camada de UI metadata;
- testes unitários e de integração passaram a cobrir explicitamente esse caso.

## Motivação
Isso evita pedidos inviáveis como ordem muito acima do limite aceito para a quantidade real de amostras da janela, reduzindo falhas tardias e alinhando a validação ao comportamento esperado pelo usuário.
