using Microsoft.Extensions.Options;
using System.Diagnostics;
using OpenPlot.ExportWorker.Build;
using OpenPlot.ExportWorker.Comtrade;
using OpenPlot.ExportWorker.Data;
using OpenPlot.ExportWorker.Options;
using OpenPlot.ExportWorker.Storage;

namespace OpenPlot.ExportWorker;

public interface IExportRunProcessor
{
    Task<bool> ProcessNextAsync(int slot, CancellationToken stoppingToken);
}

public sealed class ExportRunProcessor : IExportRunProcessor
{
    private readonly ILogger<ExportRunProcessor> _log;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ExportOptions _opt;
    private readonly IExportArtifactStore _store;
    private readonly ComtradeBuildService _builder;
    private readonly Comtrade2013Writer _writer;

    public ExportRunProcessor(
        ILogger<ExportRunProcessor> log,
        IServiceScopeFactory scopeFactory,
        IOptions<ExportOptions> opt,
        IExportArtifactStore store,
        ComtradeBuildService builder,
        Comtrade2013Writer writer)
    {
        _log = log;
        _scopeFactory = scopeFactory;
        _opt = opt.Value;
        _store = store;
        _builder = builder;
        _writer = writer;
    }

    public async Task<bool> ProcessNextAsync(int slot, CancellationToken stoppingToken)
    {
        Guid? runId = null;

        try
        {
            using var scope = _scopeFactory.CreateScope();
            var runRepo = scope.ServiceProvider.GetRequiredService<RunComtradeRepo>();
            var srRepo = scope.ServiceProvider.GetRequiredService<SearchRunsRepo>();
            var mRepo = scope.ServiceProvider.GetRequiredService<MeasurementsRepo>();
            var pdcRepo = scope.ServiceProvider.GetRequiredService<PdcRepo>();

            runId = await runRepo.TryDequeueAsync(stoppingToken);
            if (runId is null)
                return false;

            _log.LogInformation("Dequeue run_id={RunId} slot={Slot}", runId, slot);

            var ctx = await srRepo.LoadRunContextAsync(runId.Value, stoppingToken);
            if (ctx is null)
            {
                await runRepo.MarkFailedAsync(runId.Value, "search_runs não encontrado para este run_id.", stoppingToken);
                return true;
            }

            var pdcFps = await pdcRepo.GetFpsByNameAsync(ctx.PdcName, stoppingToken)
                         ?? _opt.NominalFrequencyFallback;
            await runRepo.UpdateProgressAsync(runId.Value, 5, "Carregando medições...", stoppingToken);

            _log.LogInformation(
                "Carregando medições COMTRADE run_id={RunId} slot={Slot} pdc={PdcName} from={FromUtc:o} to={ToUtc:o} pmus={PmuCount}",
                ctx.RunId,
                slot,
                ctx.PdcName,
                ctx.FromUtc,
                ctx.ToUtc,
                ctx.RunPmus.Length);

            var loadMeasurementsStopwatch = Stopwatch.StartNew();
            var rows = new List<Domain.MeasurementRow>();

            if (ctx.RunPmus.Length == 0)
            {
                rows = await mRepo.LoadMeasurementsForComtradeAsync(
                    runId: ctx.RunId,
                    fromUtc: ctx.FromUtc,
                    toUtc: ctx.ToUtc,
                    pmusOverride: ctx.RunPmus,
                    ct: stoppingToken);
            }
            else
            {
                for (var i = 0; i < ctx.RunPmus.Length; i++)
                {
                    var pmu = ctx.RunPmus[i];
                    await runRepo.UpdateProgressAsync(
                        runId.Value,
                        5 + (int)Math.Round(15.0 * i / Math.Max(1, ctx.RunPmus.Length)),
                        $"Carregando medições da PMU {i + 1}/{ctx.RunPmus.Length} ({pmu})...",
                        stoppingToken);

                    _log.LogInformation(
                        "Carregando medições COMTRADE run_id={RunId} slot={Slot} pmu={Pmu} index={Index}/{TotalPmus}",
                        ctx.RunId,
                        slot,
                        pmu,
                        i + 1,
                        ctx.RunPmus.Length);

                    var pmuRows = await mRepo.LoadMeasurementsForComtradeAsync(
                        runId: ctx.RunId,
                        fromUtc: ctx.FromUtc,
                        toUtc: ctx.ToUtc,
                        pmusOverride: [pmu],
                        ct: stoppingToken);

                    rows.AddRange(pmuRows);

                    _log.LogInformation(
                        "Medições COMTRADE carregadas run_id={RunId} slot={Slot} pmu={Pmu} rows={RowCount} total_rows={TotalRows}",
                        ctx.RunId,
                        slot,
                        pmu,
                        pmuRows.Count,
                        rows.Count);
                }
            }

            loadMeasurementsStopwatch.Stop();

            _log.LogInformation(
                "Medições COMTRADE carregadas run_id={RunId} slot={Slot} rows={RowCount} elapsed_ms={ElapsedMs}",
                ctx.RunId,
                slot,
                rows.Count,
                loadMeasurementsStopwatch.ElapsedMilliseconds);

            if (rows.Count == 0)
            {
                await runRepo.MarkFailedAsync(runId.Value, "Nenhuma medição encontrada para este run_id (openplot.measurements).", stoppingToken);
                return true;
            }

            await runRepo.UpdateProgressAsync(runId.Value, 20, $"Montando PMUs/canais ({rows.Count} pontos)...", stoppingToken);

            var pmus = _builder.Build(ctx, rows, nominalFps: pdcFps, onProgress: async (p, msg) =>
            {
                await runRepo.UpdateProgressAsync(runId.Value, p, msg, stoppingToken);
            });

            if (pmus.Count == 0)
            {
                await runRepo.MarkFailedAsync(runId.Value, "Não foi possível montar PMUs/canais para COMTRADE.", stoppingToken);
                return true;
            }

            await runRepo.UpdateProgressAsync(runId.Value, 65, $"Gerando ZIP COMTRADE ({pmus.Count} PMUs)...", stoppingToken);

            var export = _store.ResolveRunZipPath(_opt.RootDir, runId.Value, ctx.Label);

            var result = await _store.WriteZipAtomicallyAsync(
                finalDir: export.DirPath,
                finalFileName: export.FileName,
                writeToStream: stream =>
                {
                    _writer.WriteZipToStream(
                        stream: stream,
                        run: ctx,
                        pmus: pmus,
                        nominalFrequency: _opt.NominalFrequencyFallback,
                        timeCodeMode: _opt.TimeCodeMode,
                        tmqCode: _opt.TmqCode,
                        leapSec: _opt.LeapSec,
                        fileType: _opt.FileType);
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

            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
        {
            _log.LogError(ex, "Erro no processador de export slot={Slot} run_id={RunId}", slot, runId);

            if (runId is not null)
                await TryMarkFailedAsync(runId.Value, ex.Message);

            throw;
        }
    }

    private async Task TryMarkFailedAsync(Guid runId, string error)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var runRepo = scope.ServiceProvider.GetRequiredService<RunComtradeRepo>();
            await runRepo.MarkFailedAsync(runId, error, CancellationToken.None);
        }
        catch (Exception markEx)
        {
            _log.LogError(markEx, "Falha ao marcar run_id={RunId} como failed.", runId);
        }
    }
}
