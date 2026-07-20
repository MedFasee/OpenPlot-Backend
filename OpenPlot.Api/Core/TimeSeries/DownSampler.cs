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

        var list = pts.OrderBy(p => p.Ts).ToList();
        var targetPoints = Math.Max(2, maxPoints);

        if (list.Count <= targetPoints)
            return list;

        if (targetPoints == 2)
            return new List<Point> { list[0], list[^1] };

        return LargestTriangleThreeBuckets(list, targetPoints);
    }

    private static IReadOnlyList<Point> LargestTriangleThreeBuckets(IReadOnlyList<Point> sortedPoints, int threshold)
    {
        if (sortedPoints.Count <= threshold)
            return sortedPoints.ToList();

        if (threshold < 3)
            return new List<Point> { sortedPoints[0], sortedPoints[^1] };

        var sampled = new List<Point>(threshold) { sortedPoints[0] };

        var bucketSize = (sortedPoints.Count - 2d) / (threshold - 2d);
        var a = 0;

        for (var i = 0; i < threshold - 2; i++)
        {
            var avgRangeStart = (int)Math.Floor((i + 1) * bucketSize) + 1;
            var avgRangeEnd = (int)Math.Floor((i + 2) * bucketSize) + 1;
            if (avgRangeEnd > sortedPoints.Count)
                avgRangeEnd = sortedPoints.Count;

            var avgRangeLength = Math.Max(1, avgRangeEnd - avgRangeStart);

            double avgX = 0;
            double avgY = 0;
            for (var j = avgRangeStart; j < avgRangeEnd; j++)
            {
                avgX += sortedPoints[j].Ts.Ticks;
                avgY += sortedPoints[j].Val;
            }
            avgX /= avgRangeLength;
            avgY /= avgRangeLength;

            var rangeOffs = (int)Math.Floor(i * bucketSize) + 1;
            var rangeTo = (int)Math.Floor((i + 1) * bucketSize) + 1;
            if (rangeTo > sortedPoints.Count - 1)
                rangeTo = sortedPoints.Count - 1;

            var pointA = sortedPoints[a];
            var maxArea = -1d;
            var nextA = rangeOffs;

            for (var j = rangeOffs; j < rangeTo; j++)
            {
                var area = Math.Abs(
                    (pointA.Ts.Ticks - avgX) * (sortedPoints[j].Val - pointA.Val)
                    - (pointA.Ts.Ticks - sortedPoints[j].Ts.Ticks) * (avgY - pointA.Val));

                if (area > maxArea)
                {
                    maxArea = area;
                    nextA = j;
                }
            }

            sampled.Add(sortedPoints[nextA]);
            a = nextA;
        }

        sampled.Add(sortedPoints[^1]);
        return sampled;
    }
}
