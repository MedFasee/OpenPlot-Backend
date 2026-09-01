using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace OpenPlot.Services.BackgroundCache;

public sealed class BackgroundCacheWorker : BackgroundService
{
    // Protege o banco e a API contra tempestade de caches RAW concorrentes.
    // Começar com 2 e medir antes de elevar para 4.
    private const int MaxParallelCacheJobs = 2;

    private readonly BackgroundCacheQueue _queue;
    private readonly IBackgroundCacheCoordinator _coordinator;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<BackgroundCacheWorker> _logger;

    public BackgroundCacheWorker(
        BackgroundCacheQueue queue,
        IBackgroundCacheCoordinator coordinator,
        IServiceScopeFactory scopeFactory,
        ILogger<BackgroundCacheWorker> logger)
    {
        _queue = queue;
        _coordinator = coordinator;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var workers = Enumerable
            .Range(0, MaxParallelCacheJobs)
            .Select(workerId => WorkerLoopAsync(workerId, stoppingToken));

        return Task.WhenAll(workers);
    }

    private async Task WorkerLoopAsync(
        int workerId,
        CancellationToken stoppingToken)
    {
        await foreach (var item in _queue.ReadAllAsync(stoppingToken))
        {
            _queue.MarkRunning(item.CacheId);

            try
            {
                _logger.LogInformation(
                    "[CACHE-WORKER][START] worker={Worker} type={Type} runId={RunId} cacheId={CacheId}",
                    workerId,
                    item.Name,
                    item.RunId,
                    item.CacheId);

                // IMPORTANTÍSSIMO:
                // nenhum serviço Scoped do request é capturado.
                // O job ganha um scope próprio e válido até terminar.
                using var scope = _scopeFactory.CreateScope();

                // Marca a prioridade ambiente como Background para que o
                // IMeasurementQueryScheduler distinga este trabalho de leituras
                // interativas (FRONT) feitas por handlers HTTP.
                using var _ = OpenPlot.Features.Runs.Repositories.MeasurementQueryContext.BeginScope(
                    OpenPlot.Features.Runs.Repositories.QueryPriority.Background);

                await item.ExecuteAsync(
                    scope.ServiceProvider,
                    stoppingToken);

                _queue.MarkCompleted(item.CacheId);
                _coordinator.Complete(item.WorkKey, item.CacheId);

                _logger.LogInformation(
                    "[CACHE-WORKER][END] worker={Worker} type={Type} runId={RunId} cacheId={CacheId}",
                    workerId,
                    item.Name,
                    item.RunId,
                    item.CacheId);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _queue.MarkFailed(item.CacheId);
                await _coordinator.FailAsync(item.WorkKey, item.CacheId, CancellationToken.None);
                break;
            }
            catch (Exception ex)
            {
                _queue.MarkFailed(item.CacheId);
                await _coordinator.FailAsync(item.WorkKey, item.CacheId, CancellationToken.None);

                _logger.LogError(
                    ex,
                    "[CACHE-WORKER][ERROR] worker={Worker} type={Type} runId={RunId} cacheId={CacheId}",
                    workerId,
                    item.Name,
                    item.RunId,
                    item.CacheId);
            }
        }
    }
}
