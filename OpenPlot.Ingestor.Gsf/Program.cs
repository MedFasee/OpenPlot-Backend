using System;
using System.Threading.Tasks;
using Gemstone.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using OpenPlot.Ingestor.Gsf.Hosting;

namespace OpenPlot.Ingestor.Gsf;

internal static class Program
{
    private static async Task Main(string[] args)
    {
        try
        {
            _ = new Settings();
            _ = SnapDB.Snap.Library.Encodings;

            var runtimeContext = LoadRuntimeContext();
            Console.WriteLine(
                "[startup] Ingestor options: PollIntervalSeconds=" + runtimeContext.Options.PollIntervalSeconds +
                ", ChunkMinutes=" + runtimeContext.Options.ChunkMinutes +
                ", MaxParallelChunks=" + runtimeContext.Options.MaxParallelChunks +
                ", MaxParallelJobs=" + runtimeContext.Options.MaxParallelJobs +
                ", GlobalMaxParallelChunks=" + runtimeContext.Options.GlobalMaxParallelChunks +
                ", ProcessorCount=" + Environment.ProcessorCount);

            ValidateExternalDatabase(runtimeContext);

            using var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices(services =>
                {
                    services.AddSingleton(runtimeContext);
                    services.AddSingleton<IChunkExecutionCoordinator, PostgresAdvisoryLockChunkExecutionCoordinator>();
                    services.AddSingleton<IIngestorChunkPipeline, IngestorChunkPipeline>();
                    services.AddSingleton<IQueuedJobPicker, QueuedJobPicker>();
                    services.AddSingleton<IIngestorJobProcessor, IngestorJobProcessor>();
                    services.AddSingleton<IIngestorJobService, IngestorJobService>();
                    services.AddHostedService<IngestorWorker>();
                })
                .Build();

            await host.RunAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine("[fatal] " + ex.Message);
        }
    }

    private static IngestorRuntimeContext LoadRuntimeContext()
    {
        var configuration = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

        try
        {
            return IngestorConfigurationLoader.LoadFromConfiguration(configuration);
        }
        catch
        {
            return IngestorConfigurationLoader.LoadFromAppConfig();
        }
    }

    private static void ValidateExternalDatabase(IngestorRuntimeContext runtimeContext)
    {
        using var conn = new NpgsqlConnection(runtimeContext.Options.PgConnString);
        conn.Open();
        DbOps.ValidateRequiredSchema(conn);
    }
}