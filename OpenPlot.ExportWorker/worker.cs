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
    private readonly IServiceProvider _sp;
    private readonly ExportOptions _opt;
    private readonly DiskExportStore _store;
    private readonly ComtradeBuildService _builder;
    private readonly Comtrade2013Writer _writer;

    public Worker(
        ILogger<Worker> log,
        IServiceProvider sp,
        IOptions<ExportOptions> opt,
        DiskExportStore store,
        ComtradeBuildService builder,
        Comtrade2013Writer writer)
    {
        _log = log;
        _sp = sp;
        _opt = opt.Value;
        _store = store;
        _builder = builder;
        _writer = writer;
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
            Guid? runId = null;

            try
            {
                using var scope = _sp.CreateScope();
                var runRepo = scope.ServiceProvider.GetRequiredService<RunComtradeRepo>();
                var srRepo = scope.ServiceProvider.GetRequiredService<SearchRunsRepo>();
                var mRepo = scope.ServiceProvider.GetRequiredService<MeasurementsRepo>();
                var pdcRepo = scope.ServiceProvider.GetRequiredService<PdcRepo>();

                runId = await runRepo.TryDequeueAsync(stoppingToken);
                if (runId is null)
                {
                    await Task.Delay(_opt.PollIntervalMs, stoppingToken);
                    continue;
                }

                _log.LogInformation("Dequeue run_id={RunId} slot={Slot}", runId, slot);

                var ctx = await srRepo.LoadRunContextAsync(runId.Value, stoppingToken);

                if (ctx is null)
                {
                    await runRepo.MarkFailedAsync(runId.Value, "search_runs não encontrado para este run_id.", stoppingToken);
                    continue;
                }

                var nominal = await pdcRepo.GetFpsByNameAsync(ctx.PdcName, stoppingToken)
                             ?? _opt.NominalFrequencyFallback;
                await runRepo.UpdateProgressAsync(runId.Value, 5, "Carregando medições...", stoppingToken);

                var rows = await mRepo.LoadMeasurementsForComtradeAsync(
                    runId: ctx.RunId,
                    fromUtc: ctx.FromUtc,
                    toUtc: ctx.ToUtc,
                    pmusOverride: ctx.RunPmus,
                    ct: stoppingToken);

                if (rows.Count == 0)
                {
                    await runRepo.MarkFailedAsync(runId.Value, "Nenhuma medição encontrada para este run_id (openplot.measurements).", stoppingToken);
                    continue;
                }

                await runRepo.UpdateProgressAsync(runId.Value, 20, $"Montando PMUs/canais ({rows.Count} pontos)...", stoppingToken);

                var pmus = _builder.Build(ctx, rows, onProgress: async (p, msg) =>
                {
                    await runRepo.UpdateProgressAsync(runId.Value, p, msg, stoppingToken);
                });

                if (pmus.Count == 0)
                {
                    await runRepo.MarkFailedAsync(runId.Value, "Não foi possível montar PMUs/canais para COMTRADE.", stoppingToken);
                    continue;
                }

                await runRepo.UpdateProgressAsync(runId.Value, 65, $"Gerando ZIP COMTRADE ({pmus.Count} PMUs)...", stoppingToken);

                var export = _store.ResolveRunZipPath(_opt.RootDir, runId.Value, ctx.Label);

                var result = await _store.WriteZipAtomicallyAsync(
                    finalDir: export.DirPath,
                    finalFileName: export.FileName,
                    writeToStream: (stream) =>
                    {
                        _writer.WriteZipToStream(
                            stream: stream,
                            run: ctx,
                            pmus: pmus,
                            nominalFrequency: nominal,
                            timeCodeMode: _opt.TimeCodeMode,
                            tmqCode: _opt.TmqCode,
                            leapSec: _opt.LeapSec,
                            fileType: _opt.FileType
                        );
                    },
                    ct: stoppingToken);

                await runRepo.MarkDoneAsync(
                    runId.Value,
                    dirPath: export.DirPath,
                    fileName: export.FileName,
                    sizeBytes: result.SizeBytes,
                    sha256: result.Sha256,
                    ct: stoppingToken);

                _log.LogInformation(
                    "Done run_id={RunId} slot={Slot} zip={Zip} size={Size} sha={Sha}",
                    runId,
                    slot,
                    Path.Combine(export.DirPath, export.FileName),
                    result.SizeBytes,
                    result.Sha256);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Erro no worker slot={Slot} run_id={RunId}", slot, runId);

                if (runId is not null)
                    await TryMarkFailedAsync(runId.Value, ex.Message);

                await Task.Delay(1000, stoppingToken);
            }
        }
    }

    private async Task TryMarkFailedAsync(Guid runId, string error)
    {
        try
        {
            using var scope = _sp.CreateScope();
            var runRepo = scope.ServiceProvider.GetRequiredService<RunComtradeRepo>();
            await runRepo.MarkFailedAsync(runId, error, CancellationToken.None);
        }
        catch (Exception markEx)
        {
            _log.LogError(markEx, "Falha ao marcar run_id={RunId} como failed.", runId);
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