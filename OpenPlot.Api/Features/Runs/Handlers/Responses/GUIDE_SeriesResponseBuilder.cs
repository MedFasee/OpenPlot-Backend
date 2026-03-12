/// <summary>
/// GUIA DE PADRONIZAÇÃO DE RESPOSTAS - SeriesResponseBuilder
/// 
/// Este documento demonstra como usar o SeriesResponseBuilder para garantir
/// consistência em respostas de endpoints de séries temporais.
/// 
/// ============================================================================
/// PADRÃO DE RESPOSTA PADRONIZADO
/// ============================================================================
/// 
/// Todos os endpoints de séries devem retornar a seguinte estrutura:
/// 
/// {
///   "modes": { ... },                                    // opcional, UI modes
///   "run_id": "guid",                                    // obrigatório
///   "data": "dd/MM/yyyy",                                // obrigatório
///   [campos específicos do tipo: unit, tri, phase, kind, which, total, etc],
///   "cache_id": "string",                                // opcional
///   "resolved": {
///     "pdc": "PDC_NAME",
///     "pmu_count": 5
///   },
///   "window": { "from": "datetime", "to": "datetime" },
///   "meta": { ... },                                     // PlotMetaDto
///   "series": [ ... ]                                    // Array de séries
/// }
/// 
/// ============================================================================
/// EXEMPLO 1: VoltageSeriesHandler
/// ============================================================================
/// 
/// O VoltageSeriesHandler retorna:
/// - unit: "raw" ou "pu"
/// - tri: boolean
/// - phase: "ABC" ou "A"/"B"/"C"
/// 
/// Antes (inline na resposta):
///   return Results.Ok(new
///   {
///       modes,
///       run_id = q.RunId,
///       data,
///       unit,
///       tri,
///       phase = tri ? "ABC" : uphase,
///       cache_id = cacheId,
///       resolved = new { pdc = ctx.PdcName, pmu_count = ... },
///       window = new { from = windowFrom, to = windowTo2 },
///       meta = plotMeta,
///       series
///   });
/// 
/// Depois (com SeriesResponseBuilder):
///   var response = SeriesResponseBuilder
///       .For(q.RunId, windowFrom, windowTo2, series, rows, plotMeta)
///       .WithModes(modes)
///       .WithTypeSpecificField("unit", unit)
///       .WithTypeSpecificField("tri", tri)
///       .WithTypeSpecificField("phase", tri ? "ABC" : uphase)
///       .WithCacheId(cacheId)
///       .Build();
///   return Results.Ok(response);
/// 
/// ============================================================================
/// EXEMPLO 2: PowerSeriesHandler
/// ============================================================================
/// 
/// O PowerSeriesHandler retorna:
/// - which: "active" ou "reactive"
/// - unit: "raw" ou "mw"
/// - tri: boolean
/// - total: boolean
/// - phase: "A"/"B"/"C" ou null
/// 
/// Código:
///   var response = SeriesResponseBuilder
///       .For(query.RunId, windowFrom, windowTo, seriesOut, rows)
///       .WithTypeSpecificField("which", which)
///       .WithTypeSpecificField("unit", u)
///       .WithTypeSpecificField("tri", tri)
///       .WithTypeSpecificField("total", total)
///       .WithTypeSpecificField("phase", phase)
///       .Build();
///   return Results.Ok(response);
/// 
/// ============================================================================
/// EXEMPLO 3: ThdSeriesHandler
/// ============================================================================
/// 
/// O ThdSeriesHandler retorna:
/// - tri: boolean
/// - phase: "ABC" ou "A"/"B"/"C"
/// - kind: "voltage" ou "current"
/// - unit: "%"
/// 
/// Código:
///   var response = SeriesResponseBuilder
///       .For(query.RunId, windowFrom, windowTo, series, rows)
///       .WithTypeSpecificField("tri", tri)
///       .WithTypeSpecificField("phase", tri ? "ABC" : uphase)
///       .WithTypeSpecificField("kind", k)
///       .WithTypeSpecificField("unit", "%")
///       .WithCacheId(cacheId)
///       .Build();
///   return Results.Ok(response);
/// 
/// ============================================================================
/// API DO SeriesResponseBuilder
/// ============================================================================
/// 
/// Constructor:
///   SeriesResponseBuilder(runId, windowFrom, windowTo, series, rows, plotMeta?)
///   
/// Factory Method (recomendado):
///   SeriesResponseBuilder.For(runId, windowFrom, windowTo, series, rows, plotMeta?)
///   
/// Fluent Methods:
///   
///   .WithModes(Dictionary<string, object?>? modes)
///   Adiciona modos de UI à resposta. Aparece como primeiro campo.
///   
///   .WithCacheId(string? cacheId)
///   Adiciona ID de cache (opcional). Aparece entre dados e resolved.
///   
///   .WithTypeSpecificField(string key, object? value)
///   Adiciona um único campo específico do tipo (unit, tri, phase, etc).
///   Esses campos aparecem após 'data'.
///   
///   .WithTypeSpecificFields(Dictionary<string, object?> fields)
///   Adiciona múltiplos campos específicos do tipo de uma vez.
///   
///   .Build() -> object
///   Constrói o objeto de resposta final (Dictionary<string, object?>).
///   
/// ============================================================================
/// ORDEM DE CAMPOS NA RESPOSTA
/// ============================================================================
/// 
/// 1. modes (se presente)
/// 2. run_id
/// 3. data
/// 4. [campos específicos do tipo, em ordem alfabética: kind, phase, total, etc]
/// 5. cache_id (se presente)
/// 6. resolved
/// 7. window
/// 8. meta (se presente)
/// 9. series
/// 
/// ============================================================================
/// EXEMPLO COMPLETO: Novo Handler
/// ============================================================================
/// 
/// Para criar um novo handler que retorne séries, siga este padrão:
/// 
///   public async Task<IResult> HandleAsync(MyQuery q, WindowQuery w, CancellationToken ct)
///   {
///       // 1. Validação
///       var validation = ValidateInput(q);
///       if (!validation.isValid)
///           return SeriesErrorResponseBuilder.BadRequest(validation.errorMessage);
///       
///       // 2. Resolução de contexto
///       var ctx = await _runRepository.ResolveAsync(q.RunId, w.FromUtc, w.ToUtc, ct);
///       if (ctx is null)
///           return SeriesErrorResponseBuilder.NotFound("run_id não encontrado.");
///       
///       // 3. Query de dados
///       var rows = await _repository.QueryAsync(ctx, params, ct);
///       if (rows.Count == 0)
///           return SeriesErrorResponseBuilder.NotFound("Nenhuma série encontrada.");
///       
///       // 4. Transformação
///       var series = TransformRows(rows);
///       
///       // 5. Cache (se aplicável)
///       var cacheId = await _cacheRepository.SaveAsync(q.RunId, payload, ct);
///       
///       // 6. Metadados
///       var plotMeta = _metaBuilder.Build(w, ctx, measurements);
///       
///       // 7. Construção de resposta padronizada
///       var windowFrom = w.FromUtc ?? rows.Min(r => r.Ts);
///       var windowTo = w.ToUtc ?? rows.Max(r => r.Ts);
///       
///       var response = SeriesResponseBuilder
///           .For(q.RunId, windowFrom, windowTo, series, rows, plotMeta)
///           .WithModes(modes)  // se aplicável
///           .WithTypeSpecificField("meu_campo_1", valor1)
///           .WithTypeSpecificField("meu_campo_2", valor2)
///           .WithCacheId(cacheId)  // se aplicável
///           .Build();
///       
///       return Results.Ok(response);
///   }
/// 
/// ============================================================================
/// DICAS DE IMPLEMENTAÇÃO
/// ============================================================================
/// 
/// 1. Use factory method .For() para melhor legibilidade
/// 2. Ordene os .WithTypeSpecificField() por nome lógico (alfabético é opcional)
/// 3. plotMeta pode ser null se não aplicável (será omitido da resposta)
/// 4. modes é opcional, passe null ou omita WithModes() se não tiver
/// 5. Sempre use SeriesErrorResponseBuilder para erros consistentes
/// 6. A ordem alfabética dos campos específicos garante consistência
/// 
/// ============================================================================
/// BENEFÍCIOS
/// ============================================================================
/// 
/// ? Consistência: Todas as respostas seguem o mesmo padrão
/// ? Flexibilidade: Cada tipo de série pode adicionar seus campos específicos
/// ? Maintainability: Mudanças na estrutura são centralizadas
/// ? Type-safety: Compilação valida estrutura
/// ? Fluent API: Código limpo e legível
/// ? Omissão inteligente: Campos null/opcionais são omitidos automaticamente
/// 
/// ============================================================================
/// </summary>
internal static class SeriesResponseBuilderGuide
{
    // Este é apenas um arquivo de documentação - não contém código executável.
    // Use o SeriesResponseBuilder em seus handlers seguindo os exemplos acima.
}
