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
        return new RowsCacheV2
        {
            From = from.ToUniversalTime(),
            To = to.ToUniversalTime(),
            SelectRate = selectRate,
            Series = series.ToList()
        };
    }
}
