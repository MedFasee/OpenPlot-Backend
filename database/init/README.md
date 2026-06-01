# Bootstrap do banco OpenPlot

## Estado atual
- O bootstrap do banco no Docker Compose não usa mais restore de dump.
- O serviço `postgres` monta diretamente `database/dumps/openplot_create_timescaledb.sql` em `/docker-entrypoint-initdb.d`.
- O banco sobe em uma imagem `timescale/timescaledb` já preparada para habilitar a extensão TimescaleDB.
- O script principal também concentra as tabelas auxiliares do sistema, incluindo logs da API e SSO.

## Impacto operacional
- Em ambiente novo, o schema é criado do zero a partir do script SQL principal.
- O volume do PostgreSQL precisa estar vazio na primeira subida para que o `docker-entrypoint-initdb.d` execute o bootstrap.
- Os serviços da aplicação continuam se conectando ao database `postgres`, usando o schema `openplot`.

## Observação
- Se o volume `postgres-data` já existir com dados antigos, remova-o antes de recriar o ambiente para reaplicar o bootstrap inicial.
