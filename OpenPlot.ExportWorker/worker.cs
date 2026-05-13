using Microsoft.Extensions.Options;
using OpenPlot.ExportWorker.Data;
using OpenPlot.ExportWorker.Options;
using OpenPlot.ExportWorker.Build;
using OpenPlot.ExportWorker.Comtrade;
using OpenPlot.ExportWorker.Storage;

namespace OpenPlot.ExportWorker;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _log;
    private readonly ExportOptions _opt;
    private readonly IExportRunProcessor _runProcessor;

    public Worker(
        ILogger<Worker> log,
        IOptions<ExportOptions> opt,
        IExportRunProcessor runProcessor)
    {
        _log = log;
        _opt = opt.Value;
        _runProcessor = runProcessor;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var parallelism = ResolveParallelism(_opt);

        _log.LogInformation(
            "ExportWorker (COMTRADE) iniciado. RootDir={RootDir} Parallelism={Parallelism} CpuLimitPercent={CpuLimitPercent}",
            _opt.RootDir,
            parallelism,
            Math.Clamp(_opt.MaxCpuUsagePercent, 1, 100));

        var workers = new Task[parallelism];
        for (var i = 0; i < parallelism; i++)
            workers[i] = RunWorkerSlotAsync(i + 1, stoppingToken);

        await Task.WhenAll(workers);
    }

    private async Task RunWorkerSlotAsync(int slot, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var processed = await _runProcessor.ProcessNextAsync(slot, stoppingToken);
                if (!processed)
                {
                    await Task.Delay(_opt.PollIntervalMs, stoppingToken);
                    continue;
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Erro no worker slot={Slot}", slot);

                await Task.Delay(1000, stoppingToken);
            }
        }
    }

    private static int ResolveParallelism(ExportOptions opt)
    {
        var cpuCount = Math.Max(1, Environment.ProcessorCount);
        var cpuLimitPercent = Math.Clamp(opt.MaxCpuUsagePercent, 1, 100);
        var cpuLimitedParallelism = Math.Max(1, (int)Math.Floor(cpuCount * (cpuLimitPercent / 100d)));

        if (opt.MaxParallelJobs <= 0)
            return cpuLimitedParallelism;

        return Math.Max(1, Math.Min(opt.MaxParallelJobs, cpuLimitedParallelism));
    }
}