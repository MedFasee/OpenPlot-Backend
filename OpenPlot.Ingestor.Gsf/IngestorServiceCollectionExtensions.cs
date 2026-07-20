using Gemstone.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using OpenPlot.Ingestor.Gsf.Hosting;

namespace OpenPlot.Ingestor.Gsf;

public static class IngestorServiceCollectionExtensions
{
    public static IServiceCollection AddOpenPlotIngestorBackgroundServices(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var enabled = configuration.GetValue<bool>("BackgroundWorkers:Ingestor:Enabled");
        if (!enabled)
            return services;

        _ = new Settings();
        _ = SnapDB.Snap.Library.Encodings;

        var runtimeContext = IngestorConfigurationLoader.LoadFromConfiguration(configuration);
        EnsureSchema(runtimeContext);

        services.AddSingleton(runtimeContext);
        services.AddSingleton<IChunkExecutionCoordinator, PostgresAdvisoryLockChunkExecutionCoordinator>();
        services.AddSingleton<IIngestorChunkPipeline, IngestorChunkPipeline>();
        services.AddSingleton<IQueuedJobPicker, QueuedJobPicker>();
        services.AddSingleton<IIngestorJobProcessor, IngestorJobProcessor>();
        services.AddSingleton<IIngestorJobService, IngestorJobService>();
        services.AddHostedService<IngestorWorker>();

        return services;
    }

    private static void EnsureSchema(IngestorRuntimeContext runtimeContext)
    {
        using var conn = new NpgsqlConnection(runtimeContext.Options.PgConnString);
        conn.Open();
        DbOps.EnsureSchema(conn);
    }
}
