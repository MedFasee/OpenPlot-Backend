# PostProcessing Endpoints - Documentação Técnica

## Visão Geral

A feature `PostProcessing` executa análises derivadas a partir de um `cache_id` previamente persistido pelos endpoints de séries.

## Responsabilidade da Feature

A feature concentra:

- recuperação de payload analítico a partir de `cache_id`;
- execução de transformações no domínio da frequência;
- retorno de séries derivadas para visualização analítica.

## Componentes Principais

- **`PostProcessingEndpoints`**: rotas HTTP da feature.
- **`IAnalysisCacheRepository`**: recuperação do payload base (`RowsCacheV2`).
- **`Dft`**: cálculo da transformada discreta de Fourier sobre o cache.

---

## Endpoint

## `GET /api/v1/dft`

Executa análise DFT sobre um conjunto de séries previamente armazenado em cache.

### Entrada
- Query: `cache_id`

### Fluxo técnico
1. Recupera `RowsCacheV2` do repositório de cache.
2. Executa `Dft.Compute(payload)`.
3. Projeta as especificações em séries de frequência.
4. Retorna metadados, janela original e séries espectrais.

### Retorno
- `200` com espectro calculado.
- `404` se o `cache_id` não existir.

### Observações
- Esta feature depende diretamente do contrato de cache produzido pelas features de séries.
- O endpoint opera sobre dados já consolidados, não sobre medições brutas.

---

## Considerações de Arquitetura

- A feature `PostProcessing` desacopla o processamento avançado da fase de consulta inicial.
- O uso de `cache_id` evita reconsulta ao banco de medições para análises subsequentes.
- O modelo favorece composição de pipelines analíticos em múltiplas etapas.
