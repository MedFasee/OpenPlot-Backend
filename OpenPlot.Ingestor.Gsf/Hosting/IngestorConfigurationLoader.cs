using System;
using Microsoft.Extensions.Configuration;

namespace OpenPlot.Ingestor.Gsf.Hosting;

internal static class IngestorConfigurationLoader
{
    internal static IngestorRuntimeContext LoadFromConfiguration(IConfiguration configuration)
    {
        var connectionString =
            configuration["OPENPLOT_INGESTOR__DB"]
            ?? configuration["OPENPLOT_INGESTOR:DB"]
            ?? configuration["BackgroundWorkers:Ingestor:Db"]
            ?? configuration.GetConnectionString("Db");

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new Exception("Defina OPENPLOT_INGESTOR__DB, BackgroundWorkers:Ingestor:Db ou ConnectionStrings:Db.");

        var cpuCount = Math.Max(1, Environment.ProcessorCount);
        var globalMaxParallelChunks = Math.Max(1, Math.Min(ReadInt(configuration, "GlobalMaxParallelChunks", cpuCount), cpuCount));
        var maxParallelChunks = Math.Max(1, Math.Min(ReadInt(configuration, "MaxParallelChunks", 4), globalMaxParallelChunks));
        var maxParallelJobs = Math.Max(1, Math.Min(ReadInt(configuration, "MaxParallelJobs", Math.Min(2, globalMaxParallelChunks)), globalMaxParallelChunks));

        var options = new IngestorOptions(
            PgConnString: connectionString,
            PollIntervalSeconds: ReadInt(configuration, "PollIntervalSeconds", 2),
            ChunkMinutes: ReadInt(configuration, "ChunkMinutes", 5),
            MaxParallelChunks: maxParallelChunks,
            MaxParallelJobs: maxParallelJobs,
            GlobalMaxParallelChunks: globalMaxParallelChunks);

        return new IngestorRuntimeContext(options);
    }

    internal static IngestorRuntimeContext LoadFromAppConfig()
    {
        var connectionString =
            Environment.GetEnvironmentVariable("OPENPLOT_INGESTOR__DB")
            ?? System.Configuration.ConfigurationManager.AppSettings["Db"];

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new Exception("Defina OPENPLOT_INGESTOR__DB ou App.config AppSettings key=Db.");

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

    private static int ReadInt(IConfiguration configuration, string key, int defaultValue)
    {
        var rawValue =
            configuration[$"OPENPLOT_INGESTOR__{key}"]
            ?? configuration[$"OPENPLOT_INGESTOR:{key}"]
            ?? configuration[$"BackgroundWorkers:Ingestor:{key}"];

        return int.TryParse(rawValue, out var value) ? value : defaultValue;
    }

    private static int ReadInt(string key, int defaultValue)
    {
        var rawValue = System.Configuration.ConfigurationManager.AppSettings[key];
        return int.TryParse(rawValue, out var value) ? value : defaultValue;
    }
}
