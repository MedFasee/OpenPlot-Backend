# Banco de dados externo

O OpenPlot não provisiona PostgreSQL/TimescaleDB.

## Regra

A API, o ExportWorker e o Ingestor são consumidores de um banco existente e externo. A connection string deve ser fornecida por configuração/secret (`OPENPLOT_DB_CONNECTION`).

O `docker-compose.yml` sobe somente componentes da aplicação. Não há serviço `postgres`, volume `pgdata` ou `depends_on` de banco.

## Schema

Nenhum componente executa `CREATE DATABASE`, `CREATE SCHEMA` ou `CREATE TABLE` no runtime.

O Ingestor realiza somente uma validação de pré-requisitos no startup usando `to_regclass`. Caso uma tabela esperada não exista, ele falha de forma explícita em vez de tentar criá-la.


## Deploy

A infraestrutura que executar os containers precisa garantir resolução DNS, rota e política de rede até o host externo do PostgreSQL/TimescaleDB, além de injetar a connection string por secret store.
