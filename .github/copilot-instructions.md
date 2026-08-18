# Copilot Instructions

## Diretrizes de projeto
- Seguir sempre a estrutura proposta no arquivo Tech_Assessment_MedPlot_SSO para a implementação do fluxo SSO, usando por hora usuário técnico fixo por cliente SSO.
- No projeto OpenPlot, padronizar a nomenclatura do método de pós-processamento usando apenas 'CCA', abolindo o uso de 'CVA' em nomes públicos e internos relacionados a esse recurso.

## Diretrizes de arquitetura
- Usar um único motor de busca de dados baseado em PMU, com seleção de coluna Wide dependente da chamada da API; no exporter, buscar todos os dados (todas as colunas) sem distinção.