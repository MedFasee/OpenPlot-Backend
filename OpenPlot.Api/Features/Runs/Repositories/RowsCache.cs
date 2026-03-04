using System.Text.Json;
using OpenPlot.Features.Runs.Contracts;

public static class RowsCacheV2Builder
{
    public static RowsCacheV2 Build<T>(
        IEnumerable<T> rows,
        DateTime? fromUtc,
        DateTime? toUtc,
        int selectRate,
        Func<T, int> signalId,
        Func<T, int> pdcPmuId,
        Func<T, string> idName,
        Func<T, string> pdcName,
        Func<T, string?> phase,
        Func<T, string?> component,
        Func<T, DateTime> ts,
        Func<T, double> value
    )
    {
        var list = rows.ToList();
        if (list.Count == 0)
            throw new InvalidOperationException("rows vazio.");

        var from = fromUtc ?? list.Min(ts);
        var to = toUtc ?? list.Max(ts);

        var series = list
            .GroupBy(signalId)
            .Select(g =>
            {
                var first = g.First();
                var pts = g.OrderBy(ts)
                       .Select(r => new RowsCachePoint
                       {
                           Ts = ts(r).ToUniversalTime(),
                           Value = value(r)
                       })
                       .ToList();

                return new RowsCacheSeries
                {
                    SignalId = signalId(first),
                    PdcPmuId = pdcPmuId(first),
                    IdName = idName(first),
                    PdcName = pdcName(first),
                    Phase = phase(first),
                    Component = component(first),
                    Points = pts
                };
            })
            .OrderBy(s => s.IdName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(s => s.Phase, StringComparer.OrdinalIgnoreCase)
            .ThenBy(s => s.Component, StringComparer.OrdinalIgnoreCase)
            .ToList();

        return new RowsCacheV2
        {
            From = from.ToUniversalTime(),
            To = to.ToUniversalTime(),
            SelectRate = selectRate,
            Series = series
        };
    }
}