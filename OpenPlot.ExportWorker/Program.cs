using OpenPlot.ExportWorker;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((ctx, services) =>
    {
        services.AddOpenPlotExportWorkerServices(ctx.Configuration);
    })
    .Build();

await host.RunAsync();