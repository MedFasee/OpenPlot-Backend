# Config Endpoints - Documentação Técnica

## Visão Geral

A feature `Config` expõe o catálogo de PDCs e a árvore de terminais/PMUs disponíveis para seleção nas buscas.

> Observação: embora o arquivo esteja na pasta `Features/Catalog`, a extensão pública registrada é `MapConfig`.

## Responsabilidade da Feature

A feature concentra:

- listagem de arquivos/fontes configuradas (`configs`);
- listagem de terminais por PDC;
- organização hierárquica dos terminais por área, estado, tensão e estação.

## Componentes Principais

- **`ConfigEndpoints`**: rotas HTTP da feature.
- **`IDbConnectionFactory`**: acesso ao banco.
- **`PdcSql`**: SQL para listagem de PDCs.

---

## Endpoints

## `GET /api/v1/configs`

Lista os PDCs/fontes configurados para consulta.

### Entrada
- Sem parâmetros obrigatórios.

### Fluxo técnico
1. Executa `PdcSql.ListPdcNames`.
2. Projeta o resultado em `arquivos` com nome e resolução máxima.

### Retorno
- `200` com `{ status, data: { arquivos } }`.

---

## `GET /api/v1/configs/{pdcName}/terminals`

Retorna a árvore de terminais/PMUs de um PDC específico.

### Entrada
- Rota: `pdcName`

### Fluxo técnico
1. Busca PMUs associadas ao PDC.
2. Ordena por área, estado, tensão, estação e identificador.
3. Monta estrutura hierárquica:
   - área
   - estado
   - tensão
   - estação
   - terminais

### Retorno
- `200` com estrutura hierárquica de terminais.
- `404` se não houver PMUs para o PDC informado.

---

## Considerações de Arquitetura

- A feature `Config` funciona como catálogo operacional para alimentar filtros e seleções do front-end.
- Não executa análises; apenas expõe metadados estruturados do inventário de PDCs/PMUs.
- É uma dependência funcional importante para a criação de buscas na feature `Search`.
