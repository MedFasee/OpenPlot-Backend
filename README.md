# openPlot – Backend

Este repositório contém o backend do **openPlot**, orientado à integração com o ecossistema **MedPlot**.
O objetivo é disponibilizar uma API HTTP para consulta/visualização de séries temporais (tensão, corrente, sequência, THD, potência etc.), além de ferramentas de ingestão e importação de metadados.

---

## 1. Visão geral da solução

A solution `openplot.sln` é composta pelos seguintes projetos:

### `OpenPlot.Api`
API HTTP responsavel por:

- autenticação e sessão;
- cadastro e consulta de *search runs*;
- recuperação de séries temporais para plotagem (tensão, corrente, sequência, desequilíbrio, frequência, THD, potência, diferença angular etc.);
- tratamento de frames faltantes e frames sem qualidade, considerando os códigos 2 e 29 ou o valor informado no próprio frame de dados, na montagem das séries e no cálculo de diferença angular;
- geração de metadados de gráficos (`title`, `xLabel`, `yLabel`) e envelopes consistentes;
- pos-processamento baseado em `cache_id` (ex.: DFT, Prony e CCA).

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

---

## 3. Execução dos testes

Na raiz do repositório:

```powershell
dotnet test tests/OpenPlot.UnitTests/OpenPlot.UnitTests.csproj
dotnet test tests/OpenPlot.Api.IntegrationTests/OpenPlot.Api.IntegrationTests.csproj
```

---

## 4. Deploy com Docker

O backend utiliza um único arquivo `docker-compose.yml`. A escolha entre desenvolvimento e produção é feita pelo arquivo de variáveis informado no comando:

- `.env.dev` para desenvolvimento;
- `.env.prod` para produção.

O PostgreSQL/TimescaleDB utilizado pelo openPlot é **externo**. O `docker-compose.yml` não cria, provisiona ou executa um banco de dados local.

### 4.1 Pré-requisitos

Para subir o backend com Docker:

- Docker Desktop ou Docker Engine com Docker Compose;
- acesso de rede ao PostgreSQL/TimescaleDB externo;
- `.env.dev` ou `.env.prod` preenchido com as credenciais e configurações do ambiente;
- acesso ao feed NuGet ONS quando o build de produção utilizar `NuGet.config`.

Todos os projetos do backend utilizam **.NET 10**.

### 4.2 Subir o ambiente de desenvolvimento

Na raiz do repositório:

```powershell
docker compose --env-file .env.dev up -d --build --force-recreate
```

Para acompanhar os containers:

```powershell
docker compose --env-file .env.dev ps
docker compose --env-file .env.dev logs -f
```

Para encerrar:

```powershell
docker compose --env-file .env.dev down
```

### 4.3 Subir o ambiente de produção

Na raiz do repositório:

```powershell
docker compose --env-file .env.prod up -d --build --force-recreate
```

Para acompanhar os containers:

```powershell
docker compose --env-file .env.prod ps
docker compose --env-file .env.prod logs -f
```

Para encerrar:

```powershell
docker compose --env-file .env.prod down
```

Não é necessário utilizar `docker-compose.dev.yml` ou `docker-compose.prod.yml`. O fluxo atual utiliza somente:

```text
docker-compose.yml
.env.dev
.env.prod
```

### 4.4 Serviços executados

O Compose sobe os principais processos do backend:

- `openplot-api`;
- `openplot-ingestor-gsf`;
- `openplot-export-worker`.

Os três acessam o mesmo banco PostgreSQL/TimescaleDB externo por configuração de ambiente.

O `OpenPlot.XmlImporter` permanece como ferramenta da solução, mas não precisa ser executado continuamente como serviço do Compose.

### 4.5 Armazenamento dos exports

O `OpenPlot.ExportWorker` grava os arquivos COMTRADE em `/data/exports`.

A API também precisa enxergar esse mesmo diretório para disponibilizar o download dos arquivos. Portanto, API e ExportWorker devem compartilhar o mesmo diretório do host:

```text
${OPENPLOT_DATA_ROOT}/exports
        │
        ├── OpenPlot.ExportWorker -> /data/exports
        └── OpenPlot.Api          -> /data/exports
```

Se o Worker gerar o arquivo, mas a API não possuir esse volume montado, o endpoint de download retornará `404 - arquivo de exportação não encontrado em disco`.

---

## 5. Arquivos que não devem ser commitados

Os arquivos de ambiente contêm connection strings, chaves JWT, credenciais de autenticação e outros segredos. Portanto, **`.env.dev` e `.env.prod` não devem ser versionados**.

O `.gitignore` deve manter, no mínimo:

```gitignore
# IDE / build
.vs/
**/bin/
**/obj/
*.user
*.suo

# Logs e temporários
logs/
*.log
*.tmp

# Segredos / configuração local
.env
.env.dev
.env.prod
.env.*.local
secrets/*.json

# Usuários locais utilizados pela autenticação
auth-local/
OpenPlot.Api/Auth/users.json
OpenPlot.Api/Auth/users.local.json

# Overrides contendo segredos
docker-compose.prod.secrets.yml
```

O arquivo `docker-compose.yml` deve ser versionado normalmente.

Credenciais reais nunca devem ser adicionadas a `appsettings.json`, `Dockerfile` ou ao próprio `docker-compose.yml`.

---

## 6. Diferenças principais entre DEV e PROD

| Configuração | DEV | PROD |
|---|---|---|
| Arquivo de variáveis | `.env.dev` | `.env.prod` |
| `ASPNETCORE_ENVIRONMENT` | `Development` | `Production` |
| Autenticação | OpenPlot/local | ONS/Keycloak |
| `Auth.UseMock` | `true` | `false` |
| Swagger | habilitado | desabilitado |
| CORS | qualquer origem | somente origens autorizadas |
| NuGet | `NuGet.dev.config` | `NuGet.config` |
| ConfigIT | desabilitado | habilitado |
| Porta padrão da API no host | `7011` | `17011` |
| Banco | externo | externo |
| Ingestor - `PollIntervalSeconds` | `2` | `4` |
| Ingestor - `ChunkMinutes` | `5` | `5` |
| Ingestor - `MaxParallelChunks` | `2` | `4` |
| Ingestor - `MaxParallelJobs` | `2` | `4` |
| Ingestor - `GlobalMaxParallelChunks` | `1` | `4` |

### 6.1 CORS

Em desenvolvimento, o backend pode aceitar chamadas de qualquer origem:

```text
Cors__AllowAnyOrigin=true
Cors__AllowCredentials=false
```

Isso **não desabilita a autenticação** dos endpoints protegidos.

Em produção, o CORS deve aceitar somente o frontend autorizado, por exemplo:

```text
Cors__AllowAnyOrigin=false
Cors__AllowCredentials=true
Cors__AllowedOrigins__0=http://localhost:5173
```

Quando o frontend possuir URL definitiva de produção, `Cors__AllowedOrigins__0` deve ser atualizado para essa origem.

### 6.2 Banco externo

A connection string é fornecida exclusivamente por variável de ambiente:

```text
ConnectionStrings__Db=Host=...;Port=5432;Database=...;Username=...;Password=...;Search Path=openplot;SSL Mode=Require
```

O backend deve somente **conectar e utilizar** o banco existente. A criação/provisionamento do PostgreSQL/TimescaleDB não faz parte do deploy do backend.

---

## 7. Verificações rápidas após o deploy

### API

```powershell
docker compose --env-file .env.prod logs -f openplot-api
```

### Ingestor

```powershell
docker compose --env-file .env.prod logs -f openplot-ingestor-gsf
```

Os parâmetros efetivamente recebidos podem ser conferidos com:

```powershell
docker compose --env-file .env.prod exec openplot-ingestor-gsf printenv |
    Select-String "OPENPLOT_INGESTOR"
```

### ExportWorker

```powershell
docker compose --env-file .env.prod logs -f openplot-export-worker
```

Em produção, recomenda-se limitar explicitamente o paralelismo do ExportWorker para evitar pressão excessiva sobre o banco:

```text
Exports__MaxParallelJobs=2
```

O valor pode ser aumentado posteriormente após validação de consumo de memória e carga no PostgreSQL/TimescaleDB.

