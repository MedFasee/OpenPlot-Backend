using System.Globalization;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Abstractions;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;

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

    protected BaseSeriesHandler(
        IRunContextRepository runRepository,
        IPlotMetaBuilder metaBuilder,
        ISeriesCacheService cacheService)
    {
        _runRepository = runRepository ?? throw new ArgumentNullException(nameof(runRepository));
        _metaBuilder = metaBuilder ?? throw new ArgumentNullException(nameof(metaBuilder));
        _cacheService = cacheService ?? throw new ArgumentNullException(nameof(cacheService));
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

            // Passo 3: Executar query específica do handler
            var rows = await QueryDataAsync(query, runContext, window, ct);

            if (rows.Count == 0)
            {
                return Results.NotFound(GetEmptyDataMessage());
            }

            // Passo 4: Resolver janela temporal definitiva
            var windowFrom = window.FromUtc ?? rows.Min(r => r.Ts);
            var windowTo = window.ToUtc ?? rows.Max(r => r.Ts);

            // Passo 5: Salvar em cache (se aplicável)
            var cachePayload = BuildCachePayload(rows, windowFrom, windowTo, runContext);
            var cacheId = cachePayload is not null
                ? await _cacheService.SaveAsync(query.RunId, cachePayload, ct)
                : null;

            // Passo 6: Transformar dados para apresentação
            var maxPts = query.ResolveMaxPoints(@default: 5000);
            var noDownsample = query.MaxPointsIsAll;
            var series = TransformData(rows, maxPts, noDownsample);

            // Passo 7: Construir metadados
            var plotMeta = BuildPlotMeta(runContext, query, window);

            // Passo 8: Montar resposta final
            var response = SeriesResponseBuilderExtensions
                .BuildSeriesResponse(query.RunId, windowFrom, windowTo, series, plotMeta)
                .WithModes(modes)
                .WithCacheId(cacheId)
                .WithResolved(rows.First().PdcName, GetPmuCount(rows, series))
                .Build();

            return Results.Ok(response);
        }
        catch (OperationCanceledException)
        {
            return Results.StatusCode(StatusCodes.Status408RequestTimeout);
        }
        catch (Exception ex)
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
        CancellationToken ct);

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
