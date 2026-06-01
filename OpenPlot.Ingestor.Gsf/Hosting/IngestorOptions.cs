using System;

namespace OpenPlot.Ingestor.Gsf.Hosting;

internal sealed record IngestorOptions(
    string PgConnString,
    int PollIntervalSeconds,
    int ChunkMinutes,
    int MaxParallelChunks,
    int MaxParallelJobs,
    int GlobalMaxParallelChunks)
{
    internal TimeSpan PollInterval => TimeSpan.FromSeconds(PollIntervalSeconds);
}
