using System.Globalization;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.Features.Runs.Handlers.Responses;

/// <summary>
/// Construtor de resposta padronizada para séries temporais.
/// Garante consistência de estrutura entre todos os endpoints.
/// 
/// Estrutura padrão:
/// {
///   modes?: { ... },                                    // UI modes (opcional)
///   run_id: guid,                                       // ID do run
///   data: "dd/MM/yyyy",                                 // Data da série
///   cache_id?: string,                                  // ID do cache (opcional)
///   [específicos por tipo]: unit, tri, phase, kind, which, total, etc.
///   resolved: { pdc: string, pmu_count: int },          // Metadados resolvidos
///   window: { from: DateTime, to: DateTime },           // Janela temporal
///   meta: PlotMetaDto,                                  // Metadados da série
///   series: object[]                                    // Array de séries
/// }
/// </summary>
public sealed class SeriesResponseBuilder
{
    private readonly Guid _runId;
    private readonly DateTime _windowFrom;
    private readonly DateTime _windowTo;
    private readonly List<object> _series;
    private readonly IReadOnlyList<MeasurementRow> _rows;
    private readonly PlotMetaDto? _plotMeta;

    private string? _cacheId;
    private Dictionary<string, object?>? _modes;

    // Campos específicos por tipo de série (adicionados conforme necessário)
    private readonly Dictionary<string, object?> _typeSpecificFields = new();

    public SeriesResponseBuilder(
        Guid runId,
        DateTime windowFrom,
        DateTime windowTo,
        List<object> series,
        IReadOnlyList<MeasurementRow> rows,
        PlotMetaDto? plotMeta = null)
    {
        _runId = runId;
        _windowFrom = windowFrom;
        _windowTo = windowTo;
        _series = series;
        _rows = rows;
        _plotMeta = plotMeta;
    }

    /// <summary>
    /// Adiciona ID de cache à resposta.
    /// </summary>
    public SeriesResponseBuilder WithCacheId(string? cacheId)
    {
        _cacheId = cacheId;
        return this;
    }

    /// <summary>
    /// Adiciona modos de UI à resposta.
    /// </summary>
    public SeriesResponseBuilder WithModes(Dictionary<string, object?>? modes)
    {
        _modes = modes;
        return this;
    }

    /// <summary>
    /// Adiciona campos específicos do tipo de série (unit, tri, phase, kind, etc.).
    /// Esses campos são incluídos na resposta após 'data' e antes de 'cache_id'.
    /// </summary>
    public SeriesResponseBuilder WithTypeSpecificFields(Dictionary<string, object?> fields)
    {
        if (fields != null)
        {
            foreach (var kvp in fields)
            {
                _typeSpecificFields[kvp.Key] = kvp.Value;
            }
        }
        return this;
    }

    /// <summary>
    /// Adiciona um único campo específico do tipo de série.
    /// </summary>
    public SeriesResponseBuilder WithTypeSpecificField(string key, object? value)
    {
        _typeSpecificFields[key] = value;
        return this;
    }

    /// <summary>
    /// Constrói objeto de resposta padronizado.
    /// 
    /// Ordem de campos na resposta (conforme padrão do VoltageSeriesHandler):
    /// 1. modes (se presente)
    /// 2. run_id
    /// 3. data
    /// 4. [campos específicos do tipo: unit, tri, phase, kind, which, total, etc]
    /// 5. cache_id (se presente)
    /// 6. resolved
    /// 7. window
    /// 8. meta
    /// 9. series
    /// </summary>
    public object Build()
    {
        var dataStr = _windowFrom.Date.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);
        var pmuCount = _rows.Select(r => r.IdName).Distinct().Count();

        // Constrói resposta de forma dinâmica preservando ordem
        var response = new Dictionary<string, object?>();

        // 1. modes
        if (_modes != null)
            response["modes"] = _modes;

        // 2. run_id
        response["run_id"] = _runId;

        // 3. data
        response["data"] = dataStr;

        // 4. campos específicos do tipo (unit, tri, phase, kind, which, total, etc)
        foreach (var kvp in _typeSpecificFields.OrderBy(x => x.Key))
        {
            response[kvp.Key] = kvp.Value;
        }

        // 5. cache_id
        if (_cacheId != null)
            response["cache_id"] = _cacheId;

        // 6. resolved
        response["resolved"] = new
        {
            pdc = _rows.FirstOrDefault()?.PdcName,
            pmu_count = pmuCount
        };

        // 7. window
        response["window"] = new { from = _windowFrom, to = _windowTo };

        // 8. meta
        if (_plotMeta != null)
            response["meta"] = _plotMeta;

        // 9. series
        response["series"] = _series;

        return response;
    }

    /// <summary>
    /// Factory method para construir resposta completa de forma fluente.
    /// Facilita construção passo-a-passo em handlers.
    /// 
    /// Exemplo:
    /// var response = SeriesResponseBuilder
    ///     .For(runId, windowFrom, windowTo, series, rows)
    ///     .WithModes(uiModes)
    ///     .WithTypeSpecificField("unit", "raw")
    ///     .WithTypeSpecificField("tri", false)
    ///     .WithTypeSpecificField("phase", "A")
    ///     .WithCacheId(cacheId)
    ///     .Build();
    /// </summary>
    public static SeriesResponseBuilder For(
        Guid runId,
        DateTime windowFrom,
        DateTime windowTo,
        List<object> series,
        IReadOnlyList<MeasurementRow> rows,
        PlotMetaDto? plotMeta = null)
        => new(runId, windowFrom, windowTo, series, rows, plotMeta);
}

/// <summary>
/// Construtor para respostas de erro padronizadas.
/// Mantém estrutura consistente para erros em endpoints de séries.
/// </summary>
public sealed class SeriesErrorResponseBuilder
{
    /// <summary>
    /// Cria resposta de dados não encontrados (404).
    /// </summary>
    public static IResult NotFound(string message) =>
        Results.NotFound(new
        {
            status = 404,
            error = message
        });

    /// <summary>
    /// Cria resposta de parâmetros inválidos (400).
    /// </summary>
    public static IResult BadRequest(string message) =>
        Results.BadRequest(new
        {
            status = 400,
            error = message
        });

    /// <summary>
    /// Cria resposta de erro interno (500).
    /// </summary>
    public static IResult InternalError(string message) =>
        Results.StatusCode(500);

    /// <summary>
    /// Cria resposta de timeout/cancelamento (408).
    /// </summary>
    public static IResult Timeout() =>
        Results.StatusCode(StatusCodes.Status408RequestTimeout);
}
