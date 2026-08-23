using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.Runs.Contracts;

namespace OpenPlot.Features.Runs.Handlers;

public interface ISeriesAssemblyService
{
    List<object[]> BuildPoints(
        IEnumerable<(DateTime ts, double value)> raw,
        bool noDownsample,
        int maxPoints,
        ITimeSeriesDownsampler downsampler,
        double outputScale = 1.0);

    RowsCacheSeries BuildCacheSeries(
        int signalId,
        int pdcPmuId,
        string idName,
        string pdcName,
        string? referenceTerminal,
        string? unit,
        string? phase,
        string? quantity,
        string? component,
        IEnumerable<(DateTime ts, double value)> points);

    RowsCacheV2 BuildCachePayload(
        DateTime from,
        DateTime to,
        int selectRate,
        IEnumerable<RowsCacheSeries> series,
        bool normalizeMissingFrames = true);
}

public sealed class SeriesAssemblyService : ISeriesAssemblyService
{
    public List<object[]> BuildPoints(
        IEnumerable<(DateTime ts, double value)> raw,
        bool noDownsample,
        int maxPoints,
        ITimeSeriesDownsampler downsampler,
        double outputScale = 1.0)
    {
        // Nos novos selects Wide o preview já chega reduzido do banco.
        // Quando noDownsample=true, evita criar Point[], chamar MinMax e
        // converter tudo de volta para object[].
        if (noDownsample)
        {
            return raw
                .Select(x => new object[]
                {
                    x.ts,
                    x.value * outputScale
                })
                .ToList();
        }

        var points = raw
            .Select(x => new Point(
                x.ts,
                x.value))
            .ToList();

        var downs = downsampler.MinMax(
            points,
            maxPoints);

        return downs
            .Select(p => new object[]
            {
                p.Ts,
                p.Val * outputScale
            })
            .ToList();
    }

    public RowsCacheSeries BuildCacheSeries(
        int signalId,
        int pdcPmuId,
        string idName,
        string pdcName,
        string? referenceTerminal,
        string? unit,
        string? phase,
        string? quantity,
        string? component,
        IEnumerable<(DateTime ts, double value)> points)
    {
        // Não ordena aqui. BuildCachePayload normaliza cada série uma única
        // vez. Isso evita OrderBy + alocação duplicados em caches grandes.
        return new RowsCacheSeries
        {
            SignalId = signalId,
            PdcPmuId = pdcPmuId,
            IdName = idName,
            PdcName = pdcName,
            ReferenceTerminal = referenceTerminal,
            Unit = unit,
            Phase = phase,
            Quantity = quantity,
            Component = component,
            Points = points
                .Select(x => new RowsCachePoint
                {
                    Ts = x.ts.ToUniversalTime(),
                    Value = x.value
                })
                .ToList()
        };
    }

    public RowsCacheV2 BuildCachePayload(
        DateTime from,
        DateTime to,
        int selectRate,
        IEnumerable<RowsCacheSeries> series,
        bool normalizeMissingFrames = true)
    {
        var fromUtc = from.ToUniversalTime();
        var toUtc = to.ToUniversalTime();

        var normalizedSeries = series
            .Select(item => NormalizeSeries(
                item,
                fromUtc,
                toUtc,
                selectRate,
                normalizeMissingFrames))
            .ToList();

        return new RowsCacheV2
        {
            From = fromUtc,
            To = toUtc,
            SelectRate = selectRate,
            Series = normalizedSeries
        };
    }

    private static RowsCacheSeries NormalizeSeries(
        RowsCacheSeries series,
        DateTime fromUtc,
        DateTime toUtc,
        int selectRate,
        bool normalizeMissingFrames)
    {
        var orderedPoints = series.Points
            .Select(point => new RowsCachePoint
            {
                Ts = point.Ts.ToUniversalTime(),
                Value = point.Value
            })
            .ToList();

        EnsureOrderedByTimestamp(orderedPoints);

        if (!normalizeMissingFrames ||
            selectRate <= 0 ||
            orderedPoints.Count == 0)
        {
            return CloneSeriesWithPoints(
                series,
                orderedPoints);
        }

        if (!HasMissingFrames(
                orderedPoints,
                fromUtc,
                toUtc,
                selectRate))
        {
            return CloneSeriesWithPoints(
                series,
                orderedPoints);
        }

        // Mesma política hold-last do código anterior, mas sem materializar
        // antes uma List<DateTime> contendo toda a grade esperada.
        var holdLastPoints = ApplyHoldLast(
            orderedPoints,
            fromUtc,
            toUtc,
            selectRate);

        return CloneSeriesWithPoints(
            series,
            holdLastPoints);
    }

    private static void EnsureOrderedByTimestamp(
        List<RowsCachePoint> points)
    {
        for (var i = 1; i < points.Count; i++)
        {
            if (points[i - 1].Ts <= points[i].Ts)
                continue;

            points.Sort(
                (a, b) => a.Ts.CompareTo(b.Ts));

            return;
        }
    }

    private static bool HasMissingFrames(
        IReadOnlyList<RowsCachePoint> points,
        DateTime fromUtc,
        DateTime toUtc,
        int selectRate)
    {
        if (selectRate <= 0 ||
            toUtc < fromUtc)
        {
            return false;
        }

        var ticksPerFrame = Math.Max(
            1L,
            (long)Math.Round(
                TimeSpan.TicksPerSecond /
                (double)selectRate));

        var spanTicks = Math.Max(
            0L,
            (toUtc - fromUtc).Ticks);

        var frameCount =
            (long)(spanTicks / ticksPerFrame) + 1L;

        var pointIndex = 0;

        for (long frameIndex = 0;
             frameIndex < frameCount;
             frameIndex++)
        {
            var frame = fromUtc.AddTicks(
                frameIndex * ticksPerFrame);

            while (pointIndex < points.Count &&
                   points[pointIndex].Ts < frame)
            {
                pointIndex++;
            }

            if (pointIndex >= points.Count ||
                points[pointIndex].Ts != frame)
            {
                return true;
            }

            pointIndex++;
        }

        return false;
    }

    private static List<RowsCachePoint> ApplyHoldLast(
        IReadOnlyList<RowsCachePoint> points,
        DateTime fromUtc,
        DateTime toUtc,
        int selectRate)
    {
        var ticksPerFrame = Math.Max(
            1L,
            (long)Math.Round(
                TimeSpan.TicksPerSecond /
                (double)selectRate));

        var spanTicks = Math.Max(
            0L,
            (toUtc - fromUtc).Ticks);

        var frameCountLong =
            (long)(spanTicks / ticksPerFrame) + 1L;

        if (frameCountLong > int.MaxValue)
        {
            throw new InvalidOperationException(
                "Quantidade de frames do cache excede Int32.MaxValue.");
        }

        var output =
            new List<RowsCachePoint>(
                (int)frameCountLong);

        var pointIndex = 0;

        // Preserva a semântica anterior: antes do primeiro frame encontrado,
        // usa o valor do primeiro ponto conhecido.
        var lastValue = points[0].Value;

        for (long frameIndex = 0;
             frameIndex < frameCountLong;
             frameIndex++)
        {
            var frame = fromUtc.AddTicks(
                frameIndex * ticksPerFrame);

            while (pointIndex < points.Count &&
                   points[pointIndex].Ts <= frame)
            {
                lastValue =
                    points[pointIndex].Value;

                pointIndex++;
            }

            output.Add(new RowsCachePoint
            {
                Ts = frame,
                Value = lastValue
            });
        }

        return output;
    }

    private static RowsCacheSeries CloneSeriesWithPoints(
        RowsCacheSeries series,
        List<RowsCachePoint> points)
    {
        return new RowsCacheSeries
        {
            SignalId = series.SignalId,
            PdcPmuId = series.PdcPmuId,
            IdName = series.IdName,
            PdcName = series.PdcName,
            ReferenceTerminal = series.ReferenceTerminal,
            Unit = series.Unit,
            Phase = series.Phase,
            Quantity = series.Quantity,
            Component = series.Component,
            Points = points
        };
    }
}
