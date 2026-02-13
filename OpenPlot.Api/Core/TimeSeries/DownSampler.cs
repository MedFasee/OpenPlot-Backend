using System;
using System.Collections.Generic;
using System.Linq;

namespace OpenPlot.Core.TimeSeries;

public readonly record struct Points(DateTime Ts, double Val);

public interface ITimeSeriesDownsampler
{
    IReadOnlyList<Point> MinMax(IReadOnlyList<Point> pts, int maxPoints);
}

public sealed class TimeBucketMinMaxDownsampler : ITimeSeriesDownsampler
{
    public IReadOnlyList<Point> MinMax(IReadOnlyList<Point> pts, int maxPoints)
    {
        if (pts is null) throw new ArgumentNullException(nameof(pts));
        if (maxPoints <= 0) return Array.Empty<Point>();

        // ordena (se já vier ordenado do repo, dá pra remover)
        var list = pts.OrderBy(p => p.Ts).ToList();
        if (list.Count <= maxPoints) return list;

        int buckets = Math.Max(1, maxPoints / 2);

        var start = list[0].Ts;
        var end = list[^1].Ts;
        var spanTicks = (end - start).Ticks;
        if (spanTicks <= 0) return list.Take(maxPoints).ToList();

        // evita bucketTicks = 0 quando span < buckets
        long bucketTicks = Math.Max(1, spanTicks / buckets);

        var result = new List<Point>(Math.Min(maxPoints, buckets * 2 + 2));
        result.Add(list[0]);

        for (int i = 0; i < buckets && result.Count < maxPoints; i++)
        {
            var bStart = start.AddTicks(bucketTicks * i);
            var bEnd = (i == buckets - 1) ? end : start.AddTicks(bucketTicks * (i + 1));

            double? minVal = null, maxVal = null;
            DateTime minTs = default, maxTs = default;

            foreach (var p in list)
            {
                if (p.Ts < bStart || p.Ts >= bEnd) continue;

                if (minVal is null || p.Val < minVal) { minVal = p.Val; minTs = p.Ts; }
                if (maxVal is null || p.Val > maxVal) { maxVal = p.Val; maxTs = p.Ts; }
            }

            if (minVal is null) continue;

            // adiciona em ordem temporal e sem duplicar timestamp
            if (minTs <= maxTs)
            {
                if (result.Count < maxPoints && result[^1].Ts != minTs) result.Add(new Point(minTs, minVal.Value));
                if (result.Count < maxPoints && result[^1].Ts != maxTs) result.Add(new Point(maxTs, maxVal!.Value));
            }
            else
            {
                if (result.Count < maxPoints && result[^1].Ts != maxTs) result.Add(new Point(maxTs, maxVal!.Value));
                if (result.Count < maxPoints && result[^1].Ts != minTs) result.Add(new Point(minTs, minVal.Value));
            }
        }

        // garante o último (se couber e não duplicar)
        if (result.Count < maxPoints && result[^1].Ts != list[^1].Ts)
            result.Add(list[^1]);

        // cap final duro
        return result.Count <= maxPoints ? result : result.Take(maxPoints).ToList();
    }
}
