using System;
using System.Threading.Tasks;
using Gemstone.Configuration;
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

            var runtimeContext = IngestorConfigurationLoader.LoadFromAppConfig();
            EnsureSchema(runtimeContext);

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

    private static void EnsureSchema(IngestorRuntimeContext runtimeContext)
    {
        using var conn = new NpgsqlConnection(runtimeContext.Options.PgConnString);
        conn.Open();
        DbOps.EnsureSchema(conn);
    }
}