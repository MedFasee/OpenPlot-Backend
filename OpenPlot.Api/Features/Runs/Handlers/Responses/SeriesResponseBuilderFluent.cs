using OpenPlot.Core.TimeSeries;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.Json;

namespace OpenPlot.Features.Runs.Handlers.Responses;

/// <summary>
/// Construtor fluente para respostas padronizadas de s�rie temporal.
/// Garante consist�ncia em todos os endpoints /series/{type}/by-run
/// 
/// Padr�o de resposta:
/// {
///   modes?: {...},                                    // UI modes (opcional)
///   run_id: guid,                                     // ID da s�rie
///   data: "dd/MM/yyyy",                               // Data
///   cache_id?: string,                                // ID do cache (opcional)
///   [campos-espec�ficos-tipo]: unit, tri, phase, etc. // Por tipo de s�rie
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
    /// Inicia construtor para resposta de s�rie.
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
    /// Adiciona um campo espec�fico do tipo de s�rie.
    /// </summary>
    public SeriesResponseBuilderFluent WithTypeField(string name, object? value)
    {
        _typeSpecificFields[name] = value;
        return this;
    }

    /// <summary>
    /// Adiciona m�ltiplos campos espec�ficos do tipo.
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
    /// Constr�i a resposta final.
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

        EnforceMaxPayloadSize(response);

        return response;
    }

    private static void EnforceMaxPayloadSize(Dictionary<string, object?> response)
    {
        var maxBytes = ResolveMaxResponseBytes();
        if (maxBytes <= 0)
            return;

        var currentSize = GetUtf8Size(response);
        if (currentSize <= maxBytes)
            return;

        var seriesLists = ExtractPointLists(response.TryGetValue("series", out var seriesObj) ? seriesObj : null);
        if (seriesLists.Count == 0)
            return;

        // Reduz em uma passada proporcional para evitar cascata de serializa��es.
        var factor = Math.Sqrt((double)maxBytes / currentSize);
        if (factor >= 0.999)
            return;

        foreach (var pointList in seriesLists)
        {
            if (pointList.Count <= 2)
                continue;

            var target = Math.Max(2, (int)Math.Floor(pointList.Count * factor));
            if (target >= pointList.Count)
                continue;

            var reduced = ReducePointsLttb(pointList, target);
            if (reduced.Count == pointList.Count)
                continue;

            pointList.Clear();
            foreach (var row in reduced)
                pointList.Add(row);
        }
    }

    private static int ResolveMaxResponseBytes()
    {
        return SeriesResponseLimits.ResolveMaxResponseBytes();
    }

    private static int GetUtf8Size(object payload)
    {
        return JsonSerializer.SerializeToUtf8Bytes(payload).Length;
    }

    private static List<IList> ExtractPointLists(object? seriesObj)
    {
        var result = new List<IList>();
        if (seriesObj is not IEnumerable enumerable || seriesObj is string)
            return result;

        foreach (var item in enumerable)
        {
            if (item is null)
                continue;

            var prop = item.GetType().GetProperty("points", BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            if (prop?.GetValue(item) is IList points && points.Count > 0)
                result.Add(points);
        }

        return result;
    }

    private static List<object> ReducePointsLttb(IList rawPoints, int threshold)
    {
        if (rawPoints.Count <= threshold)
            return rawPoints.Cast<object>().ToList();

        if (threshold < 3)
            return new List<object> { rawPoints[0], rawPoints[rawPoints.Count - 1] };

        var points = new List<(long x, double y, object raw)>(rawPoints.Count);
        foreach (var raw in rawPoints)
        {
            if (!TryReadPoint(raw, out var x, out var y))
                continue;
            points.Add((x, y, raw));
        }

        if (points.Count <= threshold)
            return points.Select(p => p.raw).ToList();

        var sampled = new List<object>(threshold) { points[0].raw };
        var bucketSize = (points.Count - 2d) / (threshold - 2d);
        var a = 0;

        for (var i = 0; i < threshold - 2; i++)
        {
            var avgRangeStart = (int)Math.Floor((i + 1) * bucketSize) + 1;
            var avgRangeEnd = (int)Math.Floor((i + 2) * bucketSize) + 1;
            if (avgRangeEnd > points.Count)
                avgRangeEnd = points.Count;

            var avgRangeLength = Math.Max(1, avgRangeEnd - avgRangeStart);
            double avgX = 0;
            double avgY = 0;
            for (var j = avgRangeStart; j < avgRangeEnd; j++)
            {
                avgX += points[j].x;
                avgY += points[j].y;
            }
            avgX /= avgRangeLength;
            avgY /= avgRangeLength;

            var rangeOffs = (int)Math.Floor(i * bucketSize) + 1;
            var rangeTo = (int)Math.Floor((i + 1) * bucketSize) + 1;
            if (rangeTo > points.Count - 1)
                rangeTo = points.Count - 1;

            var pointA = points[a];
            var maxArea = -1d;
            var nextA = rangeOffs;

            for (var j = rangeOffs; j < rangeTo; j++)
            {
                var area = Math.Abs(
                    (pointA.x - avgX) * (points[j].y - pointA.y)
                    - (pointA.x - points[j].x) * (avgY - pointA.y));

                if (area > maxArea)
                {
                    maxArea = area;
                    nextA = j;
                }
            }

            sampled.Add(points[nextA].raw);
            a = nextA;
        }

        sampled.Add(points[^1].raw);
        return sampled;
    }

    private static bool TryReadPoint(object raw, out long tsTicks, out double value)
    {
        tsTicks = 0;
        value = 0;

        if (raw is object[] arr && arr.Length >= 2)
        {
            if (!TryToTicks(arr[0], out tsTicks))
                return false;

            if (!TryToDouble(arr[1], out value))
                return false;

            return true;
        }

        return false;
    }

    private static bool TryToTicks(object? rawTs, out long ticks)
    {
        ticks = 0;
        if (rawTs is DateTime dt)
        {
            ticks = dt.Ticks;
            return true;
        }

        if (rawTs is DateTimeOffset dto)
        {
            ticks = dto.UtcDateTime.Ticks;
            return true;
        }

        return DateTime.TryParse(rawTs?.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal, out var parsed)
            && (ticks = parsed.Ticks) >= 0;
    }

    private static bool TryToDouble(object? rawVal, out double value)
    {
        value = 0;
        if (rawVal is null)
            return false;

        if (rawVal is double d)
        {
            value = d;
            return true;
        }

        if (rawVal is float f)
        {
            value = f;
            return true;
        }

        if (rawVal is decimal m)
        {
            value = (double)m;
            return true;
        }

        return double.TryParse(rawVal.ToString(), NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out value);
    }
}

/// <summary>
/// Extens�es para simplificar cria��o de builders.
/// </summary>
public static class SeriesResponseBuilderExtensions
{
    /// <summary>
    /// Cria novo builder para resposta de s�rie.
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
