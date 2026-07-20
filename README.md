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
- pos-processamento baseado em `cache_id` (ex.: DFT, Prony e CCA).

### `OpenPlot.Ingestor.Gsf`
Aplicação de ingestão responsável por:

- conectar-se ao stack GSF/openHistorian (SNAPDB);
- ler medidas brutas do historizador;
- normalizar dados para o modelo do openPlot;
- persistir medições e metadados nas tabelas `openplot.*` no PostgreSQL.

Observação operacional atual:

- em ambiente Docker, o ingestor roda internamente na API como `BackgroundService` (configuração `BackgroundWorkers:Ingestor`), sem necessidade de container dedicado.

### `OpenPlot.XmlImporter`
Ferramenta de importação destinada a:

- importar arquivos XML do legado MedPlot;
- interpretar PDCs, PMUs, sinais/canais e configurações;
- persistir/atualizar o inventário no banco (`pdc`, `pmu`, `signal`, `pdc_pmu`, etc.).

Observação operacional atual:

- em ambiente Docker, a importação XML é executada internamente pela API como `BackgroundService` (configuração `BackgroundWorkers:XmlImporter`), reutilizando a mesma lógica de importação da API.

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

Observação operacional atual:

- em ambiente Docker, o export worker roda internamente na API como `BackgroundService` (configuração `BackgroundWorkers:ExportWorker`), sem necessidade de container dedicado.

## 2. Projetos de teste

Todos os testes automatizados ficam centralizados no diretório `tests/`.

### `tests/OpenPlot.UnitTests`
Projeto de testes unitários para regras puras, contratos de resposta e handlers sem dependência externa.

Cobertura atual:

- `Dft`;
- `Prony`;
- `Cca`;
- `PlotMetaBuilder`;
- `DftMetaBuilder`;
- `PronyMetaBuilder`;
- `CcaMetaBuilder`;
- `UiMenuService`;
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
- `GET /api/v1/dft`;
- `GET /api/v1/prony`;
- `GET /api/v1/cca`.

No estado atual do workspace, os cenarios de post-processing com cobertura de integracao incluem DFT, Prony e CCA. A suite unitária completa ainda possui falhas preexistentes fora do escopo em testes antigos de `SimpleSeriesHandler` e `ExportEndpoints`, o que pode impedir a execucao integral do projeto `OpenPlot.UnitTests` mesmo com os novos testes do CCA presentes.

A documentacao detalhada de testes esta em `docs/Testes/README.md`.

## 2.1 Documentacao tecnica por feature

Os documentos tecnicos da API ficam em `docs/Features/`.

Referencias principais:

- `docs/Features/runsEndpoints.md` - endpoints de runs, terminais e series.
- `docs/Features/postProcessingEndpoints.md` - arquitetura e contratos de DFT, Prony e CCA sobre `cache_id`.

## 2.2 Subida dos ambientes dev e prod

### Dev

No ambiente de desenvolvimento, a API sobe com autenticação local em modo mock.

- `OPENPLOT_AUTH_PROVIDER=OpenPlot`
- `Auth:UseMock=true`
- `OPENPLOT_EMBED_INGESTOR_ENABLED=true`
- `OPENPLOT_EMBED_XMLIMPORTER_ENABLED=true`
- `OPENPLOT_EMBED_EXPORT_WORKER_ENABLED=true`

Subida recomendada:

```powershell
docker compose --env-file .env.dev -f docker-compose.yml -f docker-compose.dev.yml up -d --build
```

### Prod

No ambiente de producao do ONS, a API usa o SSO do ONS e nao o login local.

- `OPENPLOT_AUTH_PROVIDER=Ons`
- `Auth:UseMock=false`
- `OPENPLOT_EMBED_INGESTOR_ENABLED=true`
- `OPENPLOT_EMBED_XMLIMPORTER_ENABLED=true`
- `OPENPLOT_EMBED_EXPORT_WORKER_ENABLED=true`

Subida recomendada:

```powershell
docker compose --env-file .env.prod --env-file .env.prod.local -f docker-compose.prod.yml -f docker-compose.prod.secrets.yml up -d --build
```

Observacao:

- em dev e prod, o ingestor, o xml importer e o export worker rodam como servicos internos da API; nao ha container dedicado para esses processos.
- o bootstrap do banco executa automaticamente na primeira subida do volume PostgreSQL e restaura o dump base com os PDCs/PMUs iniciais do ambiente.

## 2.3 Comutacao de autenticacao no Docker (OpenPlot x ONS/SSO)

A API suporta dois fluxos de autenticacao:

- **OpenPlot**: login local (`/api/v1/auth/login`);
- **ONS/SSO**: fluxo SSO (`/api/v1/sso/*`).

A comutacao e feita por variavel de ambiente no container da API:

- `OPENPLOT_AUTH_PROVIDER=OpenPlot` -> habilita login local e desabilita endpoints SSO;
- `OPENPLOT_AUTH_PROVIDER=Ons` -> habilita SSO e desabilita login local.

No `docker-compose.yml`, essa variavel e injetada como:

- `AuthProvider__Provider: ${OPENPLOT_AUTH_PROVIDER:-OpenPlot}`

Valores padrao dos ambientes atuais:

- `.env.dev`: `OPENPLOT_AUTH_PROVIDER=OpenPlot`
- `.env.prod`: `OPENPLOT_AUTH_PROVIDER=Ons`

Observacao:

- segredos e configuracoes sensiveis (ex.: `ConfigITapiKey`) devem permanecer em `.env`/ambiente, sem uso de `launchSettings.json`.

### Subida segura em producao

Use dois arquivos de ambiente e um arquivo local de segredo no compose de producao:

- `.env.prod` (versionado): configuracoes nao sensiveis;
- `.env.prod.local` (nao versionado): segredos (`ConfigIT*`, endpoints privados, etc.).
- `secrets/configit.prod.json` (nao versionado): mapeamento de/para exigido pela lib do ConfigIT.

Passos:

1. copiar `.env.prod.local.example` para `.env.prod.local`;
2. preencher os valores reais de `ConfigITr`, `ConfigITapiKey`, `ConfigITamb`, `ConfigITpacote` e `ConfigITjsonFullPath`;
3. copiar `secrets/configit.prod.json.example` para `secrets/configit.prod.json` e preencher o de/para real (estrutura de dicionario; arquivo vazio `{}` e valido);
4. copiar `docker-compose.prod.secrets.example.yml` para `docker-compose.prod.secrets.yml`;
5. subir com:

```powershell
docker compose --env-file .env.prod --env-file .env.prod.local -f docker-compose.prod.yml -f docker-compose.prod.secrets.yml up -d --build
```

### Fluxo Docker atual (ambiente unico)

Com a nova estrutura, o `OpenPlot.Ingestor.Gsf`, o `OpenPlot.XmlImporter` e o `OpenPlot.ExportWorker` rodam internamente no container da API como `BackgroundService`.
Nao existem mais servicos dedicados para esses processos no compose.

Subida recomendada (prod):

```powershell
docker compose --env-file .env.prod --env-file .env.prod.local -f docker-compose.prod.yml -f docker-compose.prod.secrets.yml up -d --build postgres openplot-api
```

Subida apenas da API (quando quiser recriar somente o processo que agora inclui ingestor/xml importer):

```powershell
docker compose --env-file .env.prod --env-file .env.prod.local -f docker-compose.prod.yml -f docker-compose.prod.secrets.yml up -d --build --force-recreate openplot-api
```

Logs da API (inclui logs HTTP + workers embutidos):

```powershell
docker compose --env-file .env.prod --env-file .env.prod.local -f docker-compose.prod.yml -f docker-compose.prod.secrets.yml logs -f --tail 200 openplot-api
```

Controles de habilitacao:

- `OPENPLOT_EMBED_INGESTOR_ENABLED=true|false`
- `OPENPLOT_EMBED_XMLIMPORTER_ENABLED=true|false`

Essas variaveis ficam em `.env.dev` e `.env.prod`, com possibilidade de override em `.env.prod.local`.

---

## 3. Execução dos testes

Na raiz do repositório:

```powershell
dotnet test tests/OpenPlot.UnitTests/OpenPlot.UnitTests.csproj
dotnet test tests/OpenPlot.Api.IntegrationTests/OpenPlot.Api.IntegrationTests.csproj
```
