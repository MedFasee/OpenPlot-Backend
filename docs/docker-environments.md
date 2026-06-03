# Ambientes Docker

## Desenvolvimento
Use o compose base com o override de desenvolvimento e o arquivo `.env.dev`.

`docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml up -d --build`

## Produção
Use o compose base com o override de produção e o arquivo `.env.prod`.

`docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml up -d`

## Observações
- Os ambientes usam `COMPOSE_PROJECT_NAME` diferente para evitar conflito de rede, containers e volume nomeado.
- Em desenvolvimento, as imagens são construídas localmente.
- Em produção, as imagens são resolvidas pelas tags definidas em `.env.prod`.
- Os diretórios bindados usam roots distintos por ambiente.
