# Bootstrap do banco OpenPlot

## Estado atual
- O bootstrap do banco no Docker Compose aplica o schema por `database/dumps/openplot_create_timescaledb.sql`.
- Quando existir `database/dumps/openplot.dump`, o restore e feito automaticamente no primeiro startup do volume.
- O restore automatico pode ser desabilitado com `OPENPLOT_POSTGRES_RESTORE_DUMP=false`.
- O banco sobe em uma imagem `timescale/timescaledb` já preparada para habilitar a extensão TimescaleDB.
- O script principal também concentra as tabelas auxiliares do sistema, incluindo logs da API e SSO.

## Impacto operacional
- Em ambiente novo, o schema é criado do zero a partir do script SQL principal.
- Em ambiente novo com dump disponivel, os dados do dump sao restaurados automaticamente e o ambiente fica com o inventario inicial de PDCs/PMUs pronto para uso.
- O volume do PostgreSQL precisa estar vazio na primeira subida para que o `docker-entrypoint-initdb.d` execute o bootstrap.
- Os serviços da aplicação continuam se conectando ao database `postgres`, usando o schema `openplot`.

## Observação
- Se o volume `postgres-data` já existir com dados antigos, remova-o antes de recriar o ambiente para reaplicar o bootstrap inicial.
