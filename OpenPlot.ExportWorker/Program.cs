using System.Data;
using Microsoft.Extensions.Options;
using Npgsql;
using OpenPlot.ExportWorker;
using OpenPlot.ExportWorker.Data;
using OpenPlot.ExportWorker.Options;
using OpenPlot.ExportWorker.Build;
using OpenPlot.ExportWorker.Comtrade;
using OpenPlot.ExportWorker.Storage;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((ctx, services) =>
    {
        services.Configure<ExportOptions>(ctx.Configuration.GetSection("Exports"));

        services.AddScoped<IDbConnection>(_ =>
        {
            var cs = ctx.Configuration.GetConnectionString("Db")
                     ?? ctx.Configuration.GetConnectionString("OpenPlotDb")
                     ?? Environment.GetEnvironmentVariable("OPENPLOT_DB_CONNECTION")
                     ?? throw new InvalidOperationException("Defina ConnectionStrings:Db (ou legado OpenPlotDb) ou OPENPLOT_DB_CONNECTION.");
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
    })
    .Build();

await host.RunAsync();