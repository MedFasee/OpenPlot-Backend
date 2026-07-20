using System.Data;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using OpenPlot.ExportWorker.Build;
using OpenPlot.ExportWorker.Comtrade;
using OpenPlot.ExportWorker.Data;
using OpenPlot.ExportWorker.Options;
using OpenPlot.ExportWorker.Storage;

namespace OpenPlot.ExportWorker;

public static class ExportWorkerServiceCollectionExtensions
{
    public static IServiceCollection AddOpenPlotExportWorkerServices(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<ExportOptions>(configuration.GetSection("Exports"));

        services.AddScoped<IDbConnection>(_ =>
        {
            var cs = configuration.GetConnectionString("Db")
                ?? throw new InvalidOperationException("ConnectionStrings:Db ausente.");
            var conn = new NpgsqlConnection(cs);
            conn.Open();
            return conn;
        });

        services.AddScoped<Db>();
        services.AddScoped<RunComtradeRepo>();
        services.AddScoped<SearchRunsRepo>();
        services.AddScoped<MeasurementsRepo>();
        services.AddScoped<PdcRepo>();
        services.AddSingleton<IExportRunProcessor, ExportRunProcessor>();
        services.AddSingleton<IExportArtifactStore, DiskExportStore>();
        services.AddSingleton<DiskExportStore>();
        services.AddSingleton<ComtradeBuildService>();
        services.AddSingleton<Comtrade2013Writer>();

        services.AddHostedService<Worker>();

        return services;
    }
}
