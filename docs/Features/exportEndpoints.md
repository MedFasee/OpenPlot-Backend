# Export Endpoints - Documentação Técnica

## Visão Geral

A feature `Export` expõe a fila consumida pelo projeto `OpenPlot.ExportWorker`. Ela registra solicitações de exportação em `openplot.comtrade_runs` e permite consultar o status do processamento.

## Responsabilidade da Feature

A feature concentra:

- validação do `run_id` solicitado pelo usuário autenticado;
- recebimento do `format` no corpo da requisição;
- enfileiramento idempotente na fila atualmente suportada;
- consulta do estado atual da exportação gerida pelo worker;
- entrega do arquivo gerado para download;
- roteamento de status e download por formato;
- preservação do vínculo com `openplot.search_runs`, que continua sendo a origem do contexto do run.

## Componentes Principais

- **`ExportEndpoints`**: rotas HTTP da feature.
- **`IDbConnectionFactory`**: acesso ao banco via Dapper.
- **`ExportSql`**: comandos SQL reutilizáveis da feature.
- **`OpenPlot.ExportWorker`**: consumidor assíncrono da fila `openplot.comtrade_runs`.

---

## Endpoints

## `POST /api/v1/export`

Solicita uma exportação para um `search_run` existente.

### Entrada
- Body: `QueueExportRequest`
  - `run_id`
  - `format`

### Fluxo técnico
1. Resolve o usuário autenticado.
2. Valida se o `run_id` pertence ao usuário em `openplot.search_runs`.
3. Valida o `format` solicitado.
4. Insere uma linha na fila atual apenas se ela ainda não existir.
5. Retorna o estado atual do job enfileirado e a rota de status correspondente ao formato.

### Retorno
- `202 Accepted` com `runId`, `format`, `status`, `progress` e `message`.
- `404` se o run não existir ou não pertencer ao usuário.

---

## `GET /api/v1/export/{format}/{id}`

Consulta o status atual da exportação, de forma direcionada ao formato solicitado.

### Entrada
- Rota: `format`
- Rota: `id`

### Fluxo técnico
1. Resolve o usuário autenticado.
2. Valida o `format` da rota.
3. Busca o registro de exportação vinculado ao `run_id`.
4. Garante o escopo do usuário pela associação com `openplot.search_runs`.
5. Garante coerência entre a rota consultada e o formato efetivamente suportado.

### Retorno
- `200` com `run_id`, `format`, `status`, `progress`, `message`, `error` e metadados do arquivo gerado.
- `404` se a exportação não existir ou não pertencer ao usuário.

---

## `GET /api/v1/export/{format}/{id}/file`

Fornece o arquivo exportado para download, também de forma direcionada ao formato.

### Entrada
- Rota: `format`
- Rota: `id`

### Fluxo técnico
1. Resolve o usuário autenticado.
2. Valida o `format` da rota.
3. Busca o status da exportação vinculada ao `run_id`.
4. Garante o escopo do usuário pela associação com `openplot.search_runs`.
5. Valida se o job já foi concluído.
6. Localiza o arquivo em disco a partir de `dir_path` e `file_name`.
7. Retorna o artefato como resposta de arquivo.

### Retorno
- `200` com o arquivo para download.
- `400` se a exportação ainda não tiver sido concluída ou o formato ainda não for suportado.
- `404` se a exportação ou o arquivo não existirem.

---

## Considerações de Arquitetura

- A API expõe um contrato genérico de exportação no nível HTTP.
- O formato solicitado entra pelo body no `POST` e também direciona `status` e `download` pela rota.
- Isso preserva a coesão para os formatos futuros sem quebrar a infraestrutura atual.
- Neste momento, a implementação continua usando a fila `openplot.comtrade_runs` já consumida pelo `OpenPlot.ExportWorker`.
- O processamento permanece centralizado no worker; a API apenas enfileira, consulta status e entrega o arquivo final.

## Nota Operacional - Carga COMTRADE por PMU

No fluxo de exportação COMTRADE, o `OpenPlot.ExportWorker` passou a particionar a carga das medições por PMU durante a leitura no PostgreSQL.

### Motivação

- alguns runs possuem janela de tempo ampla e muitas PMUs selecionadas;
- a leitura de todas as medições em uma única consulta pode gerar um `result set` muito grande;
- mesmo com processamento assíncrono, o `Npgsql` ainda precisa consumir o stream de resposta do banco dentro de um tempo aceitável;
- quando esse volume é excessivo, pode ocorrer `Timeout during reading attempt` durante a materialização dos dados.

### Estratégia adotada

- o worker resolve o contexto do `search_run` normalmente;
- quando há PMUs selecionadas no run, ele consulta as medições de uma PMU por vez;
- os resultados são acumulados em memória e seguem para a montagem final do COMTRADE;
- quando não há lista de PMUs disponível, o fluxo atual mantém a consulta agregada como fallback.

### Efeito prático

- reduz o volume retornado por consulta;
- reduz o tempo contínuo de leitura do stream pelo driver;
- diminui o risco de timeout no carregamento das medições;
- aumenta a quantidade de round-trips ao banco, mas com ganho de estabilidade para exports grandes.
