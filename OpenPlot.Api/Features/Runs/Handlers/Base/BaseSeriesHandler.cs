using System.Globalization;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Abstractions;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.UI;

namespace OpenPlot.Features.Runs.Handlers.Base;

/// <summary>
/// Handler base abstrato para s�ries temporais.
/// Encapsula fluxo comum: valida��o ? query ? transforma��o ? cache ? resposta.
/// Subclasses implementam apenas a l�gica espec�fica.
/// </summary>
/// <typeparam name="TQuery">Tipo de query espec�fico do handler.</typeparam>
public abstract class BaseSeriesHandler<TQuery> : ISeriesHandler<TQuery>
    where TQuery : ISeriesQuery
{
    protected readonly IRunContextRepository _runRepository;
    protected readonly IPlotMetaBuilder _metaBuilder;
    protected readonly ISeriesCacheService _cacheService;
    protected readonly IUiMenuService _uiMenus;

    protected BaseSeriesHandler(
        IRunContextRepository runRepository,
        IPlotMetaBuilder metaBuilder,
        ISeriesCacheService cacheService,
        IUiMenuService uiMenus)
    {
        _runRepository = runRepository ?? throw new ArgumentNullException(nameof(runRepository));
        _metaBuilder = metaBuilder ?? throw new ArgumentNullException(nameof(metaBuilder));
        _cacheService = cacheService ?? throw new ArgumentNullException(nameof(cacheService));
        _uiMenus = uiMenus ?? throw new ArgumentNullException(nameof(uiMenus));
    }

    /// <summary>
    /// Template method que orquestra o fluxo completo de processamento.
    /// Implementa��es devem sobrescrever m�todos espec�ficos conforme necess�rio.
    /// </summary>
    public async Task<IResult> HandleAsync(
        TQuery query,
        WindowQuery window,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        // Passo 1: Valida��o de par�metros de entrada
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
                return Results.NotFound("run_id n�o encontrado.");
            }

            var requestedMaxPoints = query.ResolveMaxPoints(@default: 5000);
            var estimatedSeriesCount = EstimateSeriesCount(query, runContext);
            var maxPts = SeriesDownsamplingPlanner.ResolveTargetMaxPointsPerSeries(
                requestedMaxPoints,
                query.MaxPointsIsAll,
                estimatedSeriesCount,
                runContext.FromUtc,
                runContext.ToUtc,
                runContext.SelectRate);
            var noDownsample = false;

            // Passo 3: Executar query espec�fica do handler
            var rows = await QueryDataAsync(query, runContext, window, ct);

            if (rows.Count == 0)
            {
                return Results.NotFound(GetEmptyDataMessage());
            }

            // Passo 4: Resolver janela temporal definitiva
            var windowFrom = window.FromUtc ?? rows.Min(r => r.Ts);
            var windowTo = window.ToUtc ?? rows.Max(r => r.Ts);

            // Passo 5: Salvar em cache (se aplic�vel)
            var cachePayload = query is OpenPlot.Features.Runs.Contracts.SimpleSeriesQuery sq && sq.PreviewOnly
                ? null
                : BuildCachePayload(rows, windowFrom, windowTo, runContext);
            var cacheId = cachePayload is not null
                ? await _cacheService.SaveAsync(query.RunId, cachePayload, ct)
                : null;

            // Passo 6: Transformar dados para apresenta��o
            var series = TransformData(rows, maxPts, noDownsample);

            // Passo 7: Construir metadados
            var plotMeta = BuildPlotMeta(runContext, query, window);
            var resolvedModes = _uiMenus.RebuildForRun(
                modes,
                cachePayload is not null
                    ? UiMenuContext.FromCache(cachePayload)
                    : new UiMenuContext(windowFrom, windowTo, runContext.SelectRate));

            // Passo 8: Montar resposta final
            var response = SeriesResponseBuilderExtensions
                .BuildSeriesResponse(query.RunId, windowFrom, windowTo, series, plotMeta)
                .WithModes(resolvedModes)
                .WithCacheId(cacheId)
                .WithResolved(rows.First().PdcName, GetPmuCount(rows, series))
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
    /// Valida par�metros de entrada comuns a todos os handlers.
    /// Subclasses podem sobrescrever para valida��es espec�ficas.
    /// </summary>
    protected virtual (bool isValid, string? errorMessage) ValidateInput(
        TQuery query,
        WindowQuery window)
    {
        if (query.RunId == Guid.Empty)
        {
            return (false, "run_id � obrigat�rio.");
        }

        if (window.FromUtc.HasValue && window.ToUtc.HasValue && window.FromUtc >= window.ToUtc)
        {
            return (false, "from deve ser menor que to.");
        }

        return (true, null);
    }

    /// <summary>
    /// Executa a query espec�fica para obter dados brutos.
    /// Deve ser implementado por subclasses.
    /// </summary>
    protected abstract Task<IReadOnlyList<MeasurementRow>> QueryDataAsync(
        TQuery query,
        RunContext runContext,
        WindowQuery window,
        CancellationToken ct);

    /// <summary>
    /// Transforma dados brutos em s�ries formatadas para resposta.
    /// Deve ser implementado por subclasses.
    /// </summary>
    protected abstract List<object> TransformData(
        IReadOnlyList<MeasurementRow> rows,
        int maxPoints,
        bool noDownsample);

    /// <summary>
    /// Constr�i payload para cache (opcional).
    /// Retorna null se o handler n�o cacheia dados.
    /// </summary>
    protected virtual RowsCacheV2? BuildCachePayload(
        IReadOnlyList<MeasurementRow> rows,
        DateTime windowFrom,
        DateTime windowTo,
        RunContext runContext)
    {
        return null; // Default: n�o cachear
    }

    /// <summary>
    /// Constr�i metadados do gr�fico (t�tulo, labels, etc).
    /// Implementa��o padr�o; subclasses podem customizar.
    /// </summary>
    protected virtual PlotMetaDto BuildPlotMeta(
        RunContext runContext,
        TQuery query,
        WindowQuery window)
    {
        return new PlotMetaDto(
            Title: "S�rie Temporal",
            XLabel: "Tempo",
            YLabel: "Valor"
        );
    }

    /// <summary>
    /// Retorna mensagem de erro quando nenhum dado � encontrado.
    /// Subclasses podem customizar a mensagem.
    /// </summary>
    protected virtual string GetEmptyDataMessage() =>
        "Nenhum dado encontrado para os filtros especificados.";

    /// <summary>
    /// Calcula contagem de PMUs a partir dos dados.
    /// Implementa��o padr�o; subclasses podem customizar.
    /// </summary>
    protected virtual int GetPmuCount(
        IReadOnlyList<MeasurementRow> rows,
        List<object> series) =>
        rows.Select(r => r.IdName).Distinct().Count();

    protected virtual int EstimateSeriesCount(TQuery query, RunContext runContext)
    {
        return Math.Max(1, runContext.PmuNames.Count);
    }
}
