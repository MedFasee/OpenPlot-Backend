using Microsoft.Extensions.Configuration;

namespace OpenPlot.ExportWorker;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _log;
    private readonly IConfiguration _configuration;

    public Worker(
        ILogger<Worker> log,
        IConfiguration configuration)
    {
        _log = log;
        _configuration = configuration;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Polling desligado: exportações são acionadas sincronamente pela API via ProcessByIdAsync
        _log.LogInformation("Export worker polling disabled. Use synchronous ProcessByIdAsync from API instead.");
        await Task.CompletedTask;
    }
}