using System;
using System.Collections.Generic;
using System.Linq;

namespace OpenPlot.Core.TimeSeries;

public interface ITimeSeriesDownsampler
{
    IReadOnlyList<Point> MinMax(IReadOnlyList<Point> pts, int maxPoints);
}

public sealed class TimeBucketMinMaxDownsampler : ITimeSeriesDownsampler
{
    public IReadOnlyList<Point> MinMax(IReadOnlyList<Point> pts, int maxPoints)
    {
        if (pts.Count <= maxPoints) return pts;
        var list = pts.OrderBy(p => p.Ts).ToList();

        int buckets = Math.Max(1, maxPoints / 2);
        var start = list.First().Ts;
        var end = list.Last().Ts;
        var span = (end - start).Ticks;
        if (span <= 0) return list.Take(maxPoints).ToList();

        long bucket = span / buckets;
        var result = new List<Point>(buckets * 2 + 2) { list.First() };

        for (int i = 0; i < buckets; i++)
        {
            var bStart = start.AddTicks(bucket * i);
            var bEnd = (i == buckets - 1) ? end : start.AddTicks(bucket * (i + 1));

            double? minVal = null, maxVal = null;
            DateTime minTs = default, maxTs = default;

            foreach (var p in list)
            {
                if (p.Ts < bStart || p.Ts >= bEnd) continue;

                if (minVal is null || p.Val < minVal) { minVal = p.Val; minTs = p.Ts; }
                if (maxVal is null || p.Val > maxVal) { maxVal = p.Val; maxTs = p.Ts; }
            }

            if (minVal is null) continue;

            if (minTs <= maxTs)
            {
                result.Add(new(minTs, minVal.Value));
                result.Add(new(maxTs, maxVal!.Value));
            }
            else
            {
                result.Add(new(maxTs, maxVal!.Value));
                result.Add(new(minTs, minVal.Value));
            }
        }

        result.Add(list.Last());
        return result;
    }
}
