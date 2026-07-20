using Microsoft.Extensions.Hosting;

namespace OpenPlot.Features.Import;

public sealed class XmlImportBackgroundService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IConfiguration _configuration;
    private readonly ILogger<XmlImportBackgroundService> _logger;

    public XmlImportBackgroundService(
        IServiceScopeFactory scopeFactory,
        IConfiguration configuration,
        ILogger<XmlImportBackgroundService> logger)
    {
        _scopeFactory = scopeFactory;
        _configuration = configuration;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var enabled = _configuration.GetValue<bool>("BackgroundWorkers:XmlImporter:Enabled");
        if (!enabled)
        {
            _logger.LogInformation("Xml importer background worker disabled.");
            return;
        }

        var path = _configuration["BackgroundWorkers:XmlImporter:Path"]
            ?? _configuration["XmlFolder"];

        if (string.IsNullOrWhiteSpace(path))
        {
            _logger.LogWarning("Xml importer background worker enabled, but no path was configured.");
            return;
        }

        var runOnStartup = _configuration.GetValue("BackgroundWorkers:XmlImporter:RunOnStartup", true);
        var pollIntervalSeconds = _configuration.GetValue("BackgroundWorkers:XmlImporter:PollIntervalSeconds", 0);

        if (runOnStartup)
            await ImportOnce(path, stoppingToken);

        if (pollIntervalSeconds <= 0)
        {
            _logger.LogInformation("Xml importer background worker finished after startup run (polling disabled).");
            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(pollIntervalSeconds), stoppingToken);
                await ImportOnce(path, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private async Task ImportOnce(string path, CancellationToken ct)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var importService = scope.ServiceProvider.GetRequiredService<IXmlImportService>();

            var summaries = await importService.ImportAsync(path, ct);
            var pdcCount = summaries.Sum(x => x.PdcId > 0 ? 1 : 0);
            var pmuCount = summaries.Sum(x => x.Pmus);
            var signalCount = summaries.Sum(x => x.Signals);

            _logger.LogInformation(
                "Xml importer completed for path {Path}. Files={Files} Pdcs={Pdcs} Pmus={Pmus} Signals={Signals}",
                path,
                summaries.Count,
                pdcCount,
                pmuCount,
                signalCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Xml importer background worker failed for path {Path}", path);
        }
    }
}
