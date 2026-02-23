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
        _log.LogInformation("ExportWorker (COMTRADE) iniciado. RootDir={RootDir}", _opt.RootDir);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _sp.CreateScope();
                var runRepo = scope.ServiceProvider.GetRequiredService<RunComtradeRepo>();
                var srRepo = scope.ServiceProvider.GetRequiredService<SearchRunsRepo>();
                var mRepo = scope.ServiceProvider.GetRequiredService<MeasurementsRepo>();

                // 1) pega um run_id queued e marca running (SKIP LOCKED)
                var runId = await runRepo.TryDequeueAsync(stoppingToken);
                if (runId is null)
                {
                    await Task.Delay(_opt.PollIntervalMs, stoppingToken);
                    continue;
                }

                _log.LogInformation("Dequeue run_id={RunId}", runId);

                // 2) carrega contexto do run
                var ctx = await srRepo.LoadRunContextAsync(runId.Value, stoppingToken);
                
                if (ctx is null)
                {
                    await runRepo.MarkFailedAsync(runId.Value, "search_runs não encontrado para este run_id.", stoppingToken);
                    continue;
                }
                var pdcRepo = scope.ServiceProvider.GetRequiredService<PdcRepo>();

                var nominal = await pdcRepo.GetFpsByNameAsync(ctx.PdcName, stoppingToken)
                             ?? _opt.NominalFrequencyFallback; // ou 60 direto
                await runRepo.UpdateProgressAsync(runId.Value, 5, "Carregando medições...", stoppingToken);

                // 3) carrega medidas (filtra por janela do run por padrão)
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

                // 4) constrói PMUs e séries alinhadas
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

                // 5) define caminho/nome do zip
                var export = _store.ResolveRunZipPath(_opt.RootDir, runId.Value, ctx.Label);

                // 6) escreve zip (.tmp -> rename) e calcula sha/size
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

                _log.LogInformation("Done run_id={RunId} zip={Zip} size={Size} sha={Sha}",
                    runId, Path.Combine(export.DirPath, export.FileName), result.SizeBytes, result.Sha256);
            }
            catch (OperationCanceledException)
            {
                // shutdown normal
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Erro no loop do worker.");
                await Task.Delay(1000, stoppingToken);
            }
        }

        _log.LogInformation("ExportWorker finalizado.");
    }
}