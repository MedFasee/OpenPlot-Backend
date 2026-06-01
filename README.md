# openPlot – Backend

Este repositório contém o backend do **openPlot**, orientado à integração com o ecossistema **MedPlot**.
O objetivo é disponibilizar uma API HTTP para consulta/visualização de séries temporais (tensão, corrente, sequência, THD, potência etc.), além de ferramentas de ingestão e importação de metadados.

---

## 1. Visão geral da solução

A solution `openplot.sln` é composta pelos seguintes projetos:

### `OpenPlot.Api`
API HTTP (Minimal API) responsavel por:

- autenticação e sessão;
- cadastro e consulta de *search runs*;
- recuperação de séries temporais para plotagem (tensão, corrente, sequência, desequilíbrio, frequência, THD, potência, diferença angular etc.);
- geração de metadados de gráficos (`title`, `xLabel`, `yLabel`) e envelopes consistentes;
- pos-processamento baseado em `cache_id` (ex.: DFT e Prony).

### `OpenPlot.Ingestor.Gsf`
Aplicação de ingestão responsável por:

- conectar-se ao stack GSF/openHistorian (SNAPDB);
- ler medidas brutas do historizador;
- normalizar dados para o modelo do openPlot;
- persistir medições e metadados nas tabelas `openplot.*` no PostgreSQL.

### `OpenPlot.XmlImporter`
Ferramenta de importação destinada a:

- importar arquivos XML do legado MedPlot;
- interpretar PDCs, PMUs, sinais/canais e configurações;
- persistir/atualizar o inventário no banco (`pdc`, `pmu`, `signal`, `pdc_pmu`, etc.).

### `OpenPlot.ExportWorker`
Worker Service responsável por exportação assíncrona de runs para **COMTRADE**.

Responsabilidades principais:

- consumir jobs de exportação (fila de `run_id`) e controlar status (`queued`, `running`, `done`, `failed`);
- carregar o contexto do run em `openplot.search_runs` (PDC, janela e filtros/PMUs);
- consultar medições no PostgreSQL e alinhar séries para o formato COMTRADE;
- gerar arquivos COMTRADE (incluindo compactação em `.zip` e escrita atômica `.tmp -> rename`);
- persistir metadados do artefato gerado (path, nome, tamanho, hash) e progresso do job.

Arquivos relevantes:

- `OpenPlot.ExportWorker/worker.cs`: loop do `BackgroundService` que processa a fila.
- `OpenPlot.ExportWorker/Build/ComtradeBuildService.cs`: montagem das séries/canais.
- `OpenPlot.ExportWorker/Comtrade/*`: naming e writer do padrão COMTRADE.
- `OpenPlot.ExportWorker/Storage/DiskExportStore.cs`: escrita atômica e organização em disco.

## 2. Projetos de teste

Todos os testes automatizados ficam centralizados no diretório `tests/`.

### `tests/OpenPlot.UnitTests`
Projeto de testes unitários para regras puras, contratos de resposta e handlers sem dependência externa.

Cobertura atual:

- `Dft`;
- `PlotMetaBuilder`;
- `DftMetaBuilder`;
- `DiskExportStore`;
- `RunsEndpoints`;
- `BaseSeriesHandler`;
- `SimpleSeriesHandler`;
- `PmuQueryHelper`;
- `SeriesResponseBuilder`;
- contratos de queries de séries.

No caso de `DiskExportStore`, a suíte valida a resolução do diretório diário, a sanitização do nome final do `.zip`, a escrita atômica e o cálculo de `sha256`.

### `tests/OpenPlot.Api.IntegrationTests`
Projeto de testes de integração HTTP baseado em `WebApplicationFactory<Program>`.

Cobertura atual:

- `POST /api/v1/auth/login`;
- `POST /api/v1/auth/logout`;
- `GET /api/v1/dft`.

No estado atual do workspace, a cobertura automatizada de post-processing esta concentrada em DFT; a feature Prony ja esta implementada na API, mas ainda nao aparece listada aqui com cobertura equivalente.

A documentacao detalhada de testes esta em `docs/Testes/README.md`.

## 2.1 Documentacao tecnica por feature

Os documentos tecnicos da API ficam em `docs/Features/`.

Referencias principais:

- `docs/Features/runsEndpoints.md` - endpoints de runs, terminais e series.
- `docs/Features/postProcessingEndpoints.md` - arquitetura e contratos de DFT e Prony sobre `cache_id`.

## 3. Execucao com Docker Compose

O material base desta secao foi consolidado a partir de `docs/docker_compose/doc.pdf`.

O compose do backend foi ajustado para subir o banco em Linux com TimescaleDB.

### Ambiente recomendado

- Em servidor Windows 2022 ou superior, a topologia recomendada e:
  - Windows Server 2022;
  - Hyper-V;
  - uma VM Ubuntu Server ou Debian;
  - Docker Engine executando containers Linux.
- Em ambiente local, foi utilizado `Docker Desktop 4.75.0`.
- Antes de subir os containers, garantir que o Docker esteja em execucao.

### Ajustes previos

#### Backend

- Manter `ASPNETCORE_ENVIRONMENT` como `Development` no `docker-compose.yml`.
- Ajustar o diretorio raiz dos arquivos externos por meio da variavel `OPENPLOT_DATA_ROOT` no arquivo `.env`.
- O servico `postgres` utiliza a imagem `timescale/timescaledb:latest-pg17`.
- O banco inicializado pelo compose continua sendo `postgres`.
- O bootstrap completo do schema acontece a partir de `database/dumps/openplot_create_timescaledb.sql`.
- Esse script concentra a criacao das estruturas do OpenPlot, incluindo TimescaleDB, hypertable `openplot.measurements_ht`, view de compatibilidade `openplot.measurements`, logs da API e tabelas de SSO.

Exemplo para ambiente Linux:

```env
OPENPLOT_DATA_ROOT=/srv/openplot
```

Com essa configuracao, o compose monta automaticamente:

- `${OPENPLOT_DATA_ROOT}/xml` em `/data/xml`;
- `${OPENPLOT_DATA_ROOT}/exports` em `/data/exports`;
- `${OPENPLOT_DATA_ROOT}/logs/...` para os logs dos servicos.

Observacoes sobre o banco:

- O container carrega a extensao TimescaleDB no startup do PostgreSQL.
- O bootstrap automatico so roda na primeira inicializacao do volume `postgres-data`.
- O schema da aplicacao continua sendo `openplot`, mesmo com o database nomeado como `postgres`.
- Para recriar o banco do zero com o novo schema, remova o volume antes de subir novamente.

Tambem e necessario copiar o conteudo de `Config` para `openplot-data/xml` antes da execucao quando esse diretório for a fonte dos XMLs consumidos pela aplicacao.

#### Frontend

- No projeto de frontend, alterar o campo `env_file` do `docker-compose` para a configuracao de producao, conforme documentado em `docs/docker_compose/doc.pdf`.

### Subida dos containers

Abrir um terminal na pasta do `docker-compose` de cada projeto envolvido:

- frontend;
- backend + persistencia.

Executar:

```powershell
docker compose up -d --build
```

O comando constroi as imagens e sobe os containers em background.

### Limpeza do ambiente

Para derrubar os containers e remover volumes e orfaos:

```powershell
docker compose down -v --remove-orphans
```

---

## 4. Execução dos testes

Na raiz do repositório:

```powershell
dotnet test tests/OpenPlot.UnitTests/OpenPlot.UnitTests.csproj
dotnet test tests/OpenPlot.Api.IntegrationTests/OpenPlot.Api.IntegrationTests.csproj
```
