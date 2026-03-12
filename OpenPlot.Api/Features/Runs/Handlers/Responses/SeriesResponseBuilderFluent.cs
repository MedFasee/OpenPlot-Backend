using System.Collections.Generic;
using System.Globalization;

namespace OpenPlot.Features.Runs.Handlers.Responses;

/// <summary>
/// Construtor fluente para respostas padronizadas de série temporal.
/// Garante consistência em todos os endpoints /series/{type}/by-run
/// 
/// Padrão de resposta:
/// {
///   modes?: {...},                                    // UI modes (opcional)
///   run_id: guid,                                     // ID da série
///   data: "dd/MM/yyyy",                               // Data
///   cache_id?: string,                                // ID do cache (opcional)
///   [campos-específicos-tipo]: unit, tri, phase, etc. // Por tipo de série
///   resolved: { pdc, pmu_count },                     // Resolvidos
///   window: { from, to },                             // Janela temporal
///   meta: { title, x_label, y_label },                // Metadados
///   series: [...]                                     // Dados
/// }
/// </summary>
public sealed class SeriesResponseBuilderFluent
{
    private readonly Guid _runId;
    private readonly DateTime _windowFrom;
    private readonly DateTime _windowTo;
    private readonly object _series;
    private readonly object _meta;
    
    private Dictionary<string, object?>? _modes;
    private Dictionary<string, object?> _typeSpecificFields = new();
    private string? _cacheId;
    private string? _pdc;
    private int _pmuCount;

    /// <summary>
    /// Inicia construtor para resposta de série.
    /// </summary>
    public SeriesResponseBuilderFluent(
        Guid runId,
        DateTime windowFrom,
        DateTime windowTo,
        object series,
        object meta)
    {
        _runId = runId;
        _windowFrom = windowFrom;
        _windowTo = windowTo;
        _series = series;
        _meta = meta;
    }

    /// <summary>
    /// Define modos de UI (opcional).
    /// </summary>
    public SeriesResponseBuilderFluent WithModes(Dictionary<string, object?>? modes)
    {
        _modes = modes;
        return this;
    }

    /// <summary>
    /// Define ID de cache (opcional).
    /// </summary>
    public SeriesResponseBuilderFluent WithCacheId(object? cacheId)
    {
        _cacheId = cacheId?.ToString();
        return this;
    }

    /// <summary>
    /// Define PDC e PMU count (campos resolved).
    /// </summary>
    public SeriesResponseBuilderFluent WithResolved(string pdc, int pmuCount)
    {
        _pdc = pdc;
        _pmuCount = pmuCount;
        return this;
    }

    /// <summary>
    /// Adiciona um campo específico do tipo de série.
    /// </summary>
    public SeriesResponseBuilderFluent WithTypeField(string name, object? value)
    {
        _typeSpecificFields[name] = value;
        return this;
    }

    /// <summary>
    /// Adiciona múltiplos campos específicos do tipo.
    /// </summary>
    public SeriesResponseBuilderFluent WithTypeFields(Dictionary<string, object?> fields)
    {
        foreach (var kvp in fields)
        {
            _typeSpecificFields[kvp.Key] = kvp.Value;
        }
        return this;
    }

    /// <summary>
    /// Constrói a resposta final.
    /// </summary>
    public object Build()
    {
        var data = _windowFrom.Date.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);
        
        var response = new Dictionary<string, object?>();

        // 1. Modes (opcional)
        if (_modes != null)
            response["modes"] = _modes;

        // 2. Base fields
        response["run_id"] = _runId;
        response["data"] = data;

        // 3. Cache ID (opcional)
        if (_cacheId != null)
            response["cache_id"] = _cacheId;

        // 4. Type-specific fields
        foreach (var kvp in _typeSpecificFields)
        {
            response[kvp.Key] = kvp.Value;
        }

        // 5. Resolved
        response["resolved"] = new
        {
            pdc = _pdc,
            pmu_count = _pmuCount
        };

        // 6. Window
        response["window"] = new
        {
            from = _windowFrom,
            to = _windowTo
        };

        // 7. Meta
        response["meta"] = _meta;

        // 8. Series
        response["series"] = _series;

        return response;
    }
}

/// <summary>
/// Extensões para simplificar criação de builders.
/// </summary>
public static class SeriesResponseBuilderExtensions
{
    /// <summary>
    /// Cria novo builder para resposta de série.
    /// </summary>
    public static SeriesResponseBuilderFluent BuildSeriesResponse(
        Guid runId,
        DateTime windowFrom,
        DateTime windowTo,
        object series,
        object meta)
    {
        return new SeriesResponseBuilderFluent(runId, windowFrom, windowTo, series, meta);
    }
}
