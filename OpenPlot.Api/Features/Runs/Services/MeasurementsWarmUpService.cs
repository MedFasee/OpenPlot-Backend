using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.Features.Runs.Services;

public interface IMeasurementsWarmUpQueue
{
    bool TryEnqueue(Guid runId);
}

public sealed class MeasurementsWarmUpService : BackgroundService, IMeasurementsWarmUpQueue
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<MeasurementsWarmUpService> _logger;
    private readonly Channel<Guid> _queue;
    private readonly ConcurrentDictionary<Guid, byte> _scheduledRuns = new();

    public MeasurementsWarmUpService(
        IServiceScopeFactory scopeFactory,
        ILogger<MeasurementsWarmUpService> logger)
    {
        _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _queue = Channel.CreateUnbounded<Guid>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
    }

    public bool TryEnqueue(Guid runId)
    {
        if (runId == Guid.Empty)
            return false;

        if (!_scheduledRuns.TryAdd(runId, 0))
            return false;

        if (_queue.Writer.TryWrite(runId))
            return true;

        _scheduledRuns.TryRemove(runId, out _);
        return false;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var runId in _queue.Reader.ReadAllAsync(stoppingToken))
        {
            try
            {
                await WarmUpRunAsync(runId, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Falha no warm-up de measurements para o run {RunId}.", runId);
            }
            finally
            {
                _scheduledRuns.TryRemove(runId, out _);
            }
        }
    }

    private async Task WarmUpRunAsync(Guid runId, CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();

        var runRepository = scope.ServiceProvider.GetRequiredService<IRunContextRepository>();
        var measurementsRepository = scope.ServiceProvider.GetRequiredService<IMeasurementsRepository>();

        var ctx = await runRepository.ResolveAsync(runId, null, null, ct);
        if (ctx is null)
        {
            _logger.LogDebug("Warm-up ignorado porque o run {RunId} não foi encontrado.", runId);
            return;
        }

        if (ctx.PmuNames.Count == 0)
        {
            _logger.LogDebug("Warm-up ignorado porque o run {RunId} não possui PMUs resolvidas.", runId);
            return;
        }

        await measurementsRepository.WarmUpAsync(ctx, ct);
        _logger.LogDebug("Warm-up de measurements concluído para o run {RunId}.", runId);
    }
}
