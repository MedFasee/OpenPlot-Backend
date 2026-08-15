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
        IEnumerable<RowsCacheSeries> series);
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
        var points = raw.Select(x => new Point(x.ts, x.value)).ToList();
        var downs = noDownsample ? points : downsampler.MinMax(points, maxPoints);

        return downs
            .Select(p => new object[] { p.Ts, p.Val * outputScale })
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
                .OrderBy(x => x.ts)
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
        IEnumerable<RowsCacheSeries> series)
    {
        var fromUtc = from.ToUniversalTime();
        var toUtc = to.ToUniversalTime();
        var normalizedSeries = series
            .Select(item => NormalizeSeries(item, fromUtc, toUtc, selectRate))
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
        int selectRate)
    {
        var orderedPoints = series.Points
            .OrderBy(point => point.Ts)
            .Select(point => new RowsCachePoint
            {
                Ts = point.Ts.ToUniversalTime(),
                Value = point.Value
            })
            .ToList();

        if (selectRate <= 0 || orderedPoints.Count == 0)
        {
            return CloneSeriesWithPoints(series, orderedPoints);
        }

        var expectedFrames = BuildExpectedFrames(fromUtc, toUtc, selectRate);
        if (!HasMissingFrames(orderedPoints, expectedFrames))
        {
            return CloneSeriesWithPoints(series, orderedPoints);
        }

        var holdLastPoints = ApplyHoldLast(orderedPoints, expectedFrames);
        return CloneSeriesWithPoints(series, holdLastPoints);
    }

    private static List<DateTime> BuildExpectedFrames(DateTime fromUtc, DateTime toUtc, int selectRate)
    {
        var ticksPerFrame = Math.Max(1L, (long)Math.Round(TimeSpan.TicksPerSecond / (double)selectRate));
        var spanTicks = Math.Max(0L, (toUtc - fromUtc).Ticks);
        var count = (int)(spanTicks / ticksPerFrame) + 1;
        var frames = new List<DateTime>(count);

        for (var i = 0; i < count; i++)
            frames.Add(fromUtc.AddTicks(i * ticksPerFrame));

        return frames;
    }

    private static bool HasMissingFrames(IReadOnlyList<RowsCachePoint> points, IReadOnlyList<DateTime> expectedFrames)
    {
        if (expectedFrames.Count == 0)
            return false;

        var pointIndex = 0;

        for (var frameIndex = 0; frameIndex < expectedFrames.Count; frameIndex++)
        {
            var frame = expectedFrames[frameIndex];

            while (pointIndex < points.Count && points[pointIndex].Ts < frame)
                pointIndex++;

            if (pointIndex >= points.Count || points[pointIndex].Ts != frame)
                return true;

            pointIndex++;
        }

        return false;
    }

    private static List<RowsCachePoint> ApplyHoldLast(
        IReadOnlyList<RowsCachePoint> points,
        IReadOnlyList<DateTime> expectedFrames)
    {
        var output = new List<RowsCachePoint>(expectedFrames.Count);
        var pointIndex = 0;
        var lastValue = points[0].Value;

        for (var frameIndex = 0; frameIndex < expectedFrames.Count; frameIndex++)
        {
            var frame = expectedFrames[frameIndex];

            while (pointIndex < points.Count && points[pointIndex].Ts <= frame)
            {
                lastValue = points[pointIndex].Value;
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

    private static RowsCacheSeries CloneSeriesWithPoints(RowsCacheSeries series, List<RowsCachePoint> points)
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
