using System;

namespace OpenPlot.Core.TimeSeries;

public static class SeriesDownsamplingPlanner
{
    private const int EstimatedBytesPerPoint = 30;
    private const int EstimatedFixedPayloadOverheadBytes = 80 * 1024;

    public static int ResolveTargetMaxPointsPerSeries(
        int requestedMaxPoints,
        bool requestedAll,
        int estimatedSeriesCount,
        DateTime windowFromUtc,
        DateTime windowToUtc,
        int? selectRate)
    {
        var seriesCount = Math.Max(1, estimatedSeriesCount);

        var maxResponseBytes = SeriesResponseLimits.ResolveMaxResponseBytes();
        var budgetBytes = Math.Max(16 * 1024, maxResponseBytes - EstimatedFixedPayloadOverheadBytes);

        var maxTotalPoints = Math.Max(2, budgetBytes / EstimatedBytesPerPoint);
        var maxPerSeriesByPayload = Math.Max(2, maxTotalPoints / seriesCount);

        var requestedCap = requestedAll ? int.MaxValue : Math.Max(2, requestedMaxPoints);
        var windowCap = EstimateWindowPointCount(windowFromUtc, windowToUtc, selectRate);

        var target = Math.Min(requestedCap, maxPerSeriesByPayload);
        target = Math.Min(target, windowCap);

        return Math.Max(2, target);
    }

    private static int EstimateWindowPointCount(DateTime fromUtc, DateTime toUtc, int? selectRate)
    {
        if (!selectRate.HasValue || selectRate.Value <= 0)
            return int.MaxValue;

        var durationSeconds = Math.Max(1.0, (toUtc - fromUtc).TotalSeconds);
        var estimated = (long)Math.Ceiling(durationSeconds * selectRate.Value) + 1;
        return estimated >= int.MaxValue ? int.MaxValue : (int)estimated;
    }
}