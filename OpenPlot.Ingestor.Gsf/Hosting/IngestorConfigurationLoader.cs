using System;
using System.Configuration;

namespace OpenPlot.Ingestor.Gsf.Hosting;

internal static class IngestorConfigurationLoader
{
    internal static IngestorRuntimeContext LoadFromAppConfig()
    {
        var connectionString = ReadString("Db", "OPENPLOT_INGESTOR__DB");
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new Exception("App.config: defina AppSettings key=Db.");

        var cpuCount = Math.Max(1, Environment.ProcessorCount);
        var globalMaxParallelChunks = Math.Max(1, Math.Min(ReadInt("GlobalMaxParallelChunks", cpuCount, "OPENPLOT_INGESTOR__GLOBAL_MAX_PARALLEL_CHUNKS"), cpuCount));
        var maxParallelChunks = Math.Max(1, Math.Min(ReadInt("MaxParallelChunks", 4, "OPENPLOT_INGESTOR__MAX_PARALLEL_CHUNKS"), globalMaxParallelChunks));
        var maxParallelJobs = Math.Max(1, Math.Min(ReadInt("MaxParallelJobs", Math.Min(2, globalMaxParallelChunks), "OPENPLOT_INGESTOR__MAX_PARALLEL_JOBS"), globalMaxParallelChunks));

        var options = new IngestorOptions(
            PgConnString: connectionString,
            PollIntervalSeconds: ReadInt("PollIntervalSeconds", 2, "OPENPLOT_INGESTOR__POLL_INTERVAL_SECONDS"),
            ChunkMinutes: ReadInt("ChunkMinutes", 5, "OPENPLOT_INGESTOR__CHUNK_MINUTES"),
            MaxParallelChunks: maxParallelChunks,
            MaxParallelJobs: maxParallelJobs,
            GlobalMaxParallelChunks: globalMaxParallelChunks);

        return new IngestorRuntimeContext(options);
    }

    private static int ReadInt(string key, int defaultValue, params string[] envKeys)
    {
        var rawValue = ReadString(key, envKeys);
        return int.TryParse(rawValue, out var value) ? value : defaultValue;
    }

    private static string? ReadString(string appSettingKey, params string[] envKeys)
    {
        foreach (var envKey in envKeys)
        {
            var envValue = Environment.GetEnvironmentVariable(envKey);
            if (!string.IsNullOrWhiteSpace(envValue))
                return envValue;
        }

        return ConfigurationManager.AppSettings[appSettingKey];
    }
}
