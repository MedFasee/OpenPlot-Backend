using System;

namespace OpenPlot.Ingestor.Gsf.Hosting;

internal sealed record ChunkIngestMetrics(
    string Pmu,
    DateTime FromUtc,
    DateTime ToUtc,
    TimeSpan ProcessingTime,
    int ExpectedFrames,
    int? PresentFrames,
    int? MissingFrames,
    int? BadQualityFrames,
    string Status,
    string? Details = null)
{
    public string Key => $"{Pmu}|{FromUtc.Ticks}|{ToUtc.Ticks}";
}
