using System;
using System.Configuration;

namespace OpenPlot.Ingestor.Gsf.Hosting;

internal static class IngestorConfigurationLoader
{
    internal static IngestorRuntimeContext LoadFromAppConfig()
    {
        var connectionString = ConfigurationManager.AppSettings["Db"];
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new Exception("App.config: defina AppSettings key=Db.");

        var cpuCount = Math.Max(1, Environment.ProcessorCount);
        var globalMaxParallelChunks = Math.Max(1, Math.Min(ReadInt("GlobalMaxParallelChunks", cpuCount), cpuCount));
        var maxParallelChunks = Math.Max(1, Math.Min(ReadInt("MaxParallelChunks", 4), globalMaxParallelChunks));
        var maxParallelJobs = Math.Max(1, Math.Min(ReadInt("MaxParallelJobs", Math.Min(2, globalMaxParallelChunks)), globalMaxParallelChunks));

        var options = new IngestorOptions(
            PgConnString: connectionString,
            PollIntervalSeconds: ReadInt("PollIntervalSeconds", 2),
            ChunkMinutes: ReadInt("ChunkMinutes", 5),
            MaxParallelChunks: maxParallelChunks,
            MaxParallelJobs: maxParallelJobs,
            GlobalMaxParallelChunks: globalMaxParallelChunks);

        return new IngestorRuntimeContext(options);
    }

    private static int ReadInt(string key, int defaultValue)
    {
        var rawValue = ConfigurationManager.AppSettings[key];
        return int.TryParse(rawValue, out var value) ? value : defaultValue;
    }
}
