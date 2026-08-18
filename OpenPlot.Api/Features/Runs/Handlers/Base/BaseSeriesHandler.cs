using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.Logging;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Abstractions;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.UI;

namespace OpenPlot.Features.Runs.Handlers.Base;

/// <summary>
/// Handler base abstrato para séries temporais.
/// Encapsula fluxo comum: validação ? query ? transformação ? cache ? resposta.
/// Subclasses implementam apenas a lógica específica.
/// </summary>
/// <typeparam name="TQuery">Tipo de query específico do handler.</typeparam>
public abstract class BaseSeriesHandler<TQuery> : ISeriesHandler<TQuery>
    where TQuery : ISeriesQuery
{
    protected readonly IRunContextRepository _runRepository;
    protected readonly IPlotMetaBuilder _metaBuilder;
    protected readonly ISeriesCacheService _cacheService;
    protected readonly IUiMenuService _uiMenus;
    protected readonly ILogger _logger;

    protected BaseSeriesHandler(
        IRunContextRepository runRepository,
        IPlotMetaBuilder metaBuilder,
        ISeriesCacheService cacheService,
        IUiMenuService uiMenus,
        ILogger logger)
    {
        _runRepository = runRepository ?? throw new ArgumentNullException(nameof(runRepository));
        _metaBuilder = metaBuilder ?? throw new ArgumentNullException(nameof(metaBuilder));
        _cacheService = cacheService ?? throw new ArgumentNullException(nameof(cacheService));
        _uiMenus = uiMenus ?? throw new ArgumentNullException(nameof(uiMenus));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Template method que orquestra o fluxo completo de processamento.
    /// Implementações devem sobrescrever métodos específicos conforme necessário.
    /// </summary>
    public async Task<IResult> HandleAsync(
        TQuery query,
        WindowQuery window,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        // Passo 1: Validação de parâmetros de entrada
        var validationResult = ValidateInput(query, window);
        if (!validationResult.isValid)
        {
            return Results.BadRequest(validationResult.errorMessage);
        }

        try
        {
            // Passo 2: Resolver contexto do run (janela temporal, PDC, etc)
            var runContext = await _runRepository.ResolveAsync(
                query.RunId,
                window.FromUtc,
                window.ToUtc,
                ct);

            if (runContext is null)
            {
                return Results.NotFound("run_id não encontrado.");
            }

            // Passo 3: select rápido para resposta
            var noDownsample = query.MaxPointsIsAll;
            var maxPts = query.ResolveMaxPoints(@default: 5000);
            var frontWatch = Stopwatch.StartNew();
            _logger.LogInformation(
                "[BYRUN][{Handler}][FRONT][START] runId={RunId} maxPoints={MaxPoints}",
                typeof(TQuery).Name,
                query.RunId,
                noDownsample ? "all" : maxPts);

            var frontRows = await QueryDataAsync(query, runContext, window, ct, noDownsample ? null : maxPts);

            frontWatch.Stop();
            _logger.LogInformation(
                "[BYRUN][{Handler}][FRONT][END] runId={RunId} elapsedMs={ElapsedMs} rows={Rows}",
                typeof(TQuery).Name,
                query.RunId,
                frontWatch.ElapsedMilliseconds,
                frontRows.Count);

            if (frontRows.Count == 0)
            {
                return Results.NotFound(GetEmptyDataMessage());
            }

            // Passo 4: Resolver janela temporal definitiva
            var windowFrom = window.FromUtc ?? frontRows.Min(r => r.Ts);
            var windowTo = window.ToUtc ?? frontRows.Max(r => r.Ts);

            // Passo 5: preparar cache_id imediato e disparar persistência full em background
            object? cacheId = null;
            var cacheIdGuid = Guid.NewGuid();
            cacheId = cacheIdGuid;

            _ = Task.Run(async () =>
            {
                var bgWatch = Stopwatch.StartNew();
                var fullRowsCount = 0;
                var persisted = false;

                _logger.LogInformation(
                    "[BYRUN][{Handler}][CACHE-BG][START] runId={RunId} cacheId={CacheId}",
                    typeof(TQuery).Name,
                    query.RunId,
                    cacheIdGuid);

                try
                {
                    var fullRows = await QueryDataAsync(query, runContext, window, CancellationToken.None, null);
                    fullRowsCount = fullRows.Count;
                    if (fullRowsCount == 0)
                    {
                        return;
                    }

                    var cacheWindowFrom = window.FromUtc ?? fullRows.Min(r => r.Ts);
                    var cacheWindowTo = window.ToUtc ?? fullRows.Max(r => r.Ts);
                    var cachePayloadFull = BuildCachePayload(fullRows, cacheWindowFrom, cacheWindowTo, runContext);
                    if (cachePayloadFull is null)
                    {
                        return;
                    }

                    await _cacheService.SaveAsync(cacheIdGuid, query.RunId, cachePayloadFull, CancellationToken.None);
                    persisted = true;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Falha ao persistir cache assíncrono de by-run. runId={RunId}", query.RunId);
                }
                finally
                {
                    bgWatch.Stop();
                    _logger.LogInformation(
                        "[BYRUN][{Handler}][CACHE-BG][END] runId={RunId} cacheId={CacheId} elapsedMs={ElapsedMs} rows={Rows} persisted={Persisted}",
                        typeof(TQuery).Name,
                        query.RunId,
                        cacheIdGuid,
                        bgWatch.ElapsedMilliseconds,
                        fullRowsCount,
                        persisted);
                }
            });

            // Passo 6: Transformar dados para apresentação
            var series = TransformData(frontRows, maxPts, noDownsample);

            // Passo 7: Construir metadados
            var plotMeta = BuildPlotMeta(runContext, query, window);
            var resolvedModes = _uiMenus.RebuildForRun(
                modes,
                new UiMenuContext(windowFrom, windowTo, runContext.SelectRate));

            // Passo 8: Montar resposta final
            var response = SeriesResponseBuilderExtensions
                .BuildSeriesResponse(query.RunId, windowFrom, windowTo, series, plotMeta)
                .WithModes(resolvedModes)
                .WithCacheId(cacheId)
                .WithResolved(frontRows.First().PdcName, GetPmuCount(frontRows, series))
                .Build();

            return Results.Ok(response);
        }
        catch (OperationCanceledException)
        {
            return Results.StatusCode(StatusCodes.Status408RequestTimeout);
        }
        catch (Exception)
        {
            return Results.StatusCode(StatusCodes.Status500InternalServerError);
        }
    }

    /// <summary>
    /// Valida parâmetros de entrada comuns a todos os handlers.
    /// Subclasses podem sobrescrever para validações específicas.
    /// </summary>
    protected virtual (bool isValid, string? errorMessage) ValidateInput(
        TQuery query,
        WindowQuery window)
    {
        if (query.RunId == Guid.Empty)
        {
            return (false, "run_id é obrigatório.");
        }

        if (window.FromUtc.HasValue && window.ToUtc.HasValue && window.FromUtc >= window.ToUtc)
        {
            return (false, "from deve ser menor que to.");
        }

        return (true, null);
    }

    /// <summary>
    /// Executa a query específica para obter dados brutos.
    /// Deve ser implementado por subclasses.
    /// </summary>
    protected abstract Task<IReadOnlyList<MeasurementRow>> QueryDataAsync(
        TQuery query,
        RunContext runContext,
        WindowQuery window,
        CancellationToken ct,
        int? maxPoints);

    /// <summary>
    /// Transforma dados brutos em séries formatadas para resposta.
    /// Deve ser implementado por subclasses.
    /// </summary>
    protected abstract List<object> TransformData(
        IReadOnlyList<MeasurementRow> rows,
        int maxPoints,
        bool noDownsample);

    /// <summary>
    /// Constrói payload para cache (opcional).
    /// Retorna null se o handler não cacheia dados.
    /// </summary>
    protected virtual RowsCacheV2? BuildCachePayload(
        IReadOnlyList<MeasurementRow> rows,
        DateTime windowFrom,
        DateTime windowTo,
        RunContext runContext)
    {
        return null; // Default: não cachear
    }

    /// <summary>
    /// Constrói metadados do gráfico (título, labels, etc).
    /// Implementação padrão; subclasses podem customizar.
    /// </summary>
    protected virtual PlotMetaDto BuildPlotMeta(
        RunContext runContext,
        TQuery query,
        WindowQuery window)
    {
        return new PlotMetaDto(
            Title: "Série Temporal",
            XLabel: "Tempo",
            YLabel: "Valor"
        );
    }

    /// <summary>
    /// Retorna mensagem de erro quando nenhum dado é encontrado.
    /// Subclasses podem customizar a mensagem.
    /// </summary>
    protected virtual string GetEmptyDataMessage() =>
        "Nenhum dado encontrado para os filtros especificados.";

    /// <summary>
    /// Calcula contagem de PMUs a partir dos dados.
    /// Implementação padrão; subclasses podem customizar.
    /// </summary>
    protected virtual int GetPmuCount(
        IReadOnlyList<MeasurementRow> rows,
        List<object> series) =>
        rows.Select(r => r.IdName).Distinct().Count();
}
