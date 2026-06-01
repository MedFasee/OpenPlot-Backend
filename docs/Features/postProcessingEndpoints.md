# PostProcessing Endpoints - Documentacao Tecnica

## Visao Geral

A feature `PostProcessing` concentra analises derivadas executadas sobre um `cache_id` previamente gerado pelos endpoints de series. Em vez de reler medicoes brutas no banco, a API reutiliza um payload consolidado (`RowsCacheV2`) e aplica algoritmos de pos-processamento orientados a visualizacao analitica.

No estado atual, a feature expoe dois endpoints autenticados:

- `GET /api/v1/dft`
- `GET /api/v1/prony`

Esses endpoints compartilham o mesmo modelo operacional:

1. carregar o cache analitico;
2. recortar a janela temporal solicitada;
3. executar o algoritmo especializado;
4. regenerar metadados de plot coerentes com a natureza da serie original;
5. projetar uma resposta HTTP pronta para consumo do frontend.

---

## Responsabilidade da Feature

A feature `PostProcessing` concentra:

- recuperacao de payload analitico a partir de `cache_id`;
- reaproveitamento do contrato de cache produzido pelas features de series;
- aplicacao de algoritmos de analise espectral e identificacao modal;
- montagem de metadados para visualizacao sem reconsultar medicoes brutas;
- isolamento do custo computacional da etapa de consulta inicial.

---

## Arquitetura Comum

### Componentes principais

- **`PostProcessingEndpoints`**: expoe as rotas HTTP e projeta as respostas JSON.
- **`IAnalysisCacheRepository` / `AnalysisCacheRepository`**: persistencia e recuperacao de `RowsCacheV2` em `openplot.analysis_cache`.
- **`Dft`**: calculo espectral por FFT single-sided sobre series reamostradas.
- **`Prony`**: ajuste modal multissinal para estimativa de polos, frequencias e amortecimentos.
- **`DftMetaBuilder`**: reconstroi `title`, `xLabel` e `yLabel` para o contexto da DFT.
- **`PronyMetaBuilder`**: reconstroi `title`, `xLabel` e `yLabel` para o contexto do Prony.
- **`IPlotMetaBuilder`**: servico compartilhado reutilizado pelos meta builders para manter consistencia com a nomenclatura das series originais.

### Contrato base consumido

Os endpoints de post-processing operam sobre `RowsCacheV2`, produzido pelos handlers de series em `Runs`:

- `from` / `to`: janela UTC consolidada do cache;
- `selectRate`: taxa de amostragem usada na montagem das series;
- `series[]`: conjunto de `RowsCacheSeries` com metadados e pontos;
- `series[].points[]`: pares temporais (`ts`, `value`) usados como entrada analitica.

Cada `RowsCacheSeries` transporta metadados suficientes para a regeneracao de contexto:

- `idName` (PMU);
- `pdcName`;
- `referenceTerminal`;
- `phase`;
- `component`;
- `quantity`;
- `unit`.

### Pipeline de cache

1. Um endpoint de series em `Runs` monta `RowsCacheV2`.
2. O payload e serializado em JSON e persistido por `AnalysisCacheRepository.SaveAsync(...)`.
3. O repositorio grava em `openplot.analysis_cache` com `cache_id`, `job_id`, `payload` e `last_accessed_at`.
4. Na leitura, `GetAsync<T>(...)` atualiza `last_accessed_at` e desserializa o payload.
5. Registros com mais de 24 horas sem acesso sao removidos no fluxo de escrita.

Esse desenho reduz acoplamento com a camada SQL de medicoes e favorece pipelines analiticos encadeados a partir do mesmo recorte temporal.

---

## Visao rapida

| Endpoint | Metodo | Entrada principal | Algoritmo | Resposta especializada |
|---|---|---|---|---|
| `/api/v1/dft` | GET | `cache_id`, `from`, `to` | FFT single-sided | espectro por serie + faixa inicial de zoom |
| `/api/v1/prony` | GET | `cache_id`, `order`, `from`, `to`, `include_points`, `include_all_modes` | Prony multissinal | modos dominantes, modos completos e reconstrucao temporal |

---

## Endpoint `GET /api/v1/dft`

Executa analise espectral sobre as series presentes em um cache analitico.

### Entrada

- Query obrigatoria:
  - `cache_id`: identificador do payload salvo em `analysis_cache`.
- Query opcionais:
  - `from`: limite inferior UTC para recorte da janela.
  - `to`: limite superior UTC para recorte da janela.

### Fluxo tecnico

1. Recupera `RowsCacheV2` via `IAnalysisCacheRepository.GetAsync<RowsCacheV2>(...)`.
2. Retorna `404` se o cache nao existir.
3. Normaliza `from` e `to` para UTC.
4. Chama `Dft.Compute(payload, fromUtc, toUtc)`.
5. Gera `meta` via `IDftMetaBuilder.Build(...)`.
6. Projeta cada especificacao em uma serie HTTP com metadados de PMU, grandeza, componente e fase.
7. Retorna a janela efetivamente usada no processamento e os limites iniciais de zoom frequencial.

### Regras internas relevantes

- O algoritmo usa `payload.SelectRate` como taxa de amostragem.
- A janela requisitada e truncada para nunca extrapolar `payload.From` e `payload.To`.
- Series com menos de 2 pontos validos no recorte sao descartadas.
- A reamostragem ocorre via estrategia `hold-last` em grade uniforme.
- A FFT e calculada no modo `single-sided` usando `MathNet.Numerics` com `FourierOptions.Matlab`.
- A componente DC e devolvida com magnitude zerada por compatibilidade com o comportamento esperado pelo cliente.
- O nome logico da serie e montado em formato `PMU|QUANTITY|COMPONENT|PHASE` para evitar colisao entre fases e componentes distintos.

### Resposta

- `200` com payload contendo:
  - `cache_id`;
  - `meta`;
  - `selectRate`;
  - `window` com `from` e `to` efetivos;
  - `zoom` com `fMin` e `fMax` iniciais;
  - `series[]` com pontos no formato `[hz, magnitude]`.
- `404` se `cache_id` nao existir.
- Erros de consistencia do cache podem resultar em falha de processamento se o payload estiver estruturalmente invalido.

### Shape resumido da resposta

```json
{
  "cache_id": "guid",
  "meta": {
    "title": "Espectro de Freq. ...",
    "xLabel": "Tempo (UTC)",
    "yLabel": "Frequencia (Hz)"
  },
  "selectRate": 60,
  "window": {
    "from": "2026-01-24T21:46:14Z",
    "to": "2026-01-24T21:46:37Z"
  },
  "zoom": {
    "fMin": 0.0,
    "fMax": 1.6
  },
  "series": [
    {
      "pmu": "PMU-1",
      "component": "mag",
      "quantity": "voltage",
      "phase": "A",
      "unit": "V",
      "meta": { "serie": "PMU-1|VOLTAGE|MAG|A" },
      "sr": 60.0,
      "n": 2048,
      "fMin": 0.05859375,
      "points": [[0.0, 0.0], [0.05859375, 12.34]]
    }
  ]
}
```

### Observacoes especificas

- `zoom.fMin` recebe `dft.Zoom.Position`.
- `zoom.fMax` recebe `dft.Zoom.Size`; semanticamente e o tamanho inicial da janela de zoom, nao o limite superior do espectro completo.
- O `meta.yLabel` atual e fixado em `Frequencia (Hz)` pelo `DftMetaBuilder`, mesmo que os pontos da serie representem magnitude espectral. A documentacao registra o comportamento implementado hoje.

---

## Endpoint `GET /api/v1/prony`

Executa identificacao modal via Prony sobre um conjunto de series previamente armazenado em cache.

### Entrada

- Query obrigatorias:
  - `cache_id`: identificador do payload salvo em `analysis_cache`.
  - `order`: ordem do modelo de Prony.
- Query opcionais:
  - `from`: limite inferior UTC para recorte da janela.
  - `to`: limite superior UTC para recorte da janela.
  - `include_points`: quando `true`, inclui pontos originais e estimados na resposta.
  - `include_all_modes`: quando `true`, inclui todos os modos calculados, sem o filtro legado de exibicao.

### Fluxo tecnico

1. Recupera `RowsCacheV2` via cache analitico.
2. Retorna `404` se o `cache_id` nao existir.
3. Normaliza a janela UTC e trunca o intervalo aos limites do cache.
4. Executa `Prony.Compute(payload, order, fromUtc, toUtc)`.
5. Gera `meta` via `IPronyMetaBuilder.Build(...)`.
6. Projeta, por serie, os modos dominantes, opcionalmente todos os modos e opcionalmente os sinais temporal original e reconstruido.
7. Retorna `modeShapeCandidatesHz` consolidados para apoio a analises posteriores no frontend.

### Regras internas relevantes

- `order` deve ser maior que zero.
- Todas as series validas sao reamostradas em uma mesma grade temporal uniforme para viabilizar o ajuste multissinal.
- O processamento exige pelo menos 2 pontos por serie no recorte.
- A ordem deve ser menor que o numero total de amostras reamostradas.
- Tambem e validada a relacao `valid.Count * (n - order) >= order` para evitar janela insuficiente.
- O ajuste usa matriz de minimos quadrados e, em caso de falha na resolucao QR, cai para a formulacao via equacoes normais.
- Os modos exibidos em `modes` passam por filtro legado: frequencia positiva abaixo de 10 Hz e energia acima de `1e-3`.
- `allModes` preserva todos os modos calculados quando explicitamente solicitado.
- `modeShapeCandidatesHz` agrega frequencias distintas abaixo de 10 Hz com arredondamento de 6 casas.

### Resposta

- `200` com payload contendo:
  - `cache_id`;
  - `meta`;
  - `selectRate`;
  - `window` com `from` e `to` efetivos;
  - `modeShapeCandidatesHz`;
  - `series[]` com modos e, opcionalmente, pontos temporais.
- `404` se `cache_id` nao existir.
- `400` para erros de entrada ou inviabilidade matematica do ajuste, incluindo:
  - ordem nao positiva;
  - ordem maior ou igual ao numero de amostras;
  - janela com poucas amostras para a ordem solicitada;
  - janela invalida.

### Shape resumido da resposta

```json
{
  "cache_id": "guid",
  "meta": {
    "title": "Prony da Frequencia ...",
    "xLabel": "Tempo (UTC)",
    "yLabel": "Hz"
  },
  "selectRate": 60,
  "window": {
    "from": "2026-01-24T21:46:14Z",
    "to": "2026-01-24T21:46:37Z"
  },
  "modeShapeCandidatesHz": [0.42, 1.15],
  "series": [
    {
      "pmu": "PMU-1",
      "component": "freq",
      "quantity": "frequency",
      "phase": null,
      "unit": "Hz",
      "meta": { "serie": "PMU-1|FREQUENCY|FREQ" },
      "sr": 60.0,
      "n": 1024,
      "order": 20,
      "modes": [
        {
          "index": 0,
          "energy": 10.5,
          "frequencyHz": 0.42,
          "dampingPercent": 3.2,
          "amplitude": 0.8,
          "phaseRad": 1.57,
          "real": -0.01,
          "imaginary": 2.63
        }
      ],
      "allModes": null,
      "originalPoints": null,
      "estimatedPoints": null
    }
  ]
}
```

### Observacoes especificas

- `include_points=false` reduz volume de payload e deve ser preferido quando o objetivo e apenas inspecao modal.
- `include_all_modes=true` e util para diagnostico, mas tende a ampliar ruido visual e trafego de resposta.
- O endpoint expoe tanto polos continuos (`real`, `imaginary`) quanto metricas diretamente consumiveis pelo frontend (`frequencyHz`, `dampingPercent`, `energy`).

---

## Metadados de plot

`DftMetaBuilder` e `PronyMetaBuilder` reaproveitam `IPlotMetaBuilder` para preservar a semantica da serie original, mas fazem adaptacoes locais:

- normalizam `quantity` (`active` -> `p_active`, `reactive` -> `p_reactive`);
- normalizam `component` (`seq` -> `mag`, `angle_diff_*` -> `angle`);
- inferem `PhaseMode` com base na fase, componente e distribuicao de PMUs e fases no cache;
- ajustam o titulo final com prefixos especificos (`Espectro de Freq.` e `Prony`);
- no caso da DFT, sobrescrevem o `yLabel` com `Frequencia (Hz)`.

Esse desenho evita que a feature de post-processing replique toda a logica de rotulagem usada pelos endpoints de series.

---

## Dependencias e registro na API

- O grupo de endpoints e registrado em `Program.cs` por `apiV1.MapPostProcessing()`.
- As rotas herdam `RequireAuthorization()` aplicado no grupo da feature.
- O consumo esperado e sempre autenticado, consistente com o restante da superficie analitica da API.

---

## Cobertura de testes

Estado atual observado no workspace:

- testes unitarios para `Dft`;
- testes unitarios para `DftMetaBuilder`;
- teste de integracao HTTP para `GET /api/v1/dft` com cache existente;
- teste de integracao HTTP para `GET /api/v1/dft` com `404` quando o cache nao existe.

Nao foi identificada cobertura equivalente, no estado atual, para:

- `Prony`;
- `PronyMetaBuilder`;
- `GET /api/v1/prony` em nivel de integracao.

Essa lacuna e relevante porque `Prony` possui mais validacoes numericas, flags opcionais de payload e maior variabilidade de saida.

---

## Consideracoes de arquitetura

- A feature desacopla consulta de dados e analise avancada, permitindo reuso do mesmo recorte temporal em multiplas transformacoes.
- O modelo baseado em `cache_id` reduz round-trips ao banco e encapsula o contexto analitico necessario para processamento posterior.
- O custo computacional migra da camada SQL para a camada de aplicacao, o que simplifica consulta de medicoes, mas exige cuidado com volume de payload e uso de memoria.
- A expiracao baseada em `last_accessed_at` e suficiente para cache temporario, mas nao substitui uma politica de governanca caso o volume de analises aumente significativamente.
- A manutencao da compatibilidade de nomenclatura com o frontend e parte do contrato da feature; por isso, diversos campos de resposta sao projetados explicitamente em `PostProcessingEndpoints` em vez de expor objetos internos diretamente.
