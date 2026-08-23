using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.AspNetCore.Http;

namespace OpenPlot.Services.BackgroundCache;

public enum BackgroundCacheStatus
{
    Unknown = 0,
    Pending = 1,
    Running = 2,
    Failed = 3
}

public sealed record BackgroundCacheWorkItem(
    string Name,
    Guid RunId,
    Guid CacheId,
    Func<IServiceProvider, CancellationToken, Task> ExecuteAsync);

public interface IBackgroundCacheQueue
{
    /// <summary>
    /// Agenda o trabalho para entrar na fila somente depois que a resposta HTTP
    /// terminar. Quando não existe HttpContext (ex.: teste), enfileira imediatamente.
    /// </summary>
    bool ScheduleAfterResponse(
        HttpContext? httpContext,
        BackgroundCacheWorkItem item);

    BackgroundCacheStatus GetStatus(Guid cacheId);

    bool IsPending(Guid cacheId);
}

public sealed class BackgroundCacheQueue : IBackgroundCacheQueue
{
    private readonly Channel<BackgroundCacheWorkItem> _channel;

    private readonly ConcurrentDictionary<Guid, BackgroundCacheStatus>
        _status = new();

    public BackgroundCacheQueue()
    {
        // A fila guarda somente descritores pequenos.
        // A massa de dados é carregada pelo worker apenas quando houver slot.
        _channel = Channel.CreateUnbounded<BackgroundCacheWorkItem>(
            new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });
    }

    public bool ScheduleAfterResponse(
        HttpContext? httpContext,
        BackgroundCacheWorkItem item)
    {
        _status[item.CacheId] = BackgroundCacheStatus.Pending;

        if (httpContext is null)
            return TryEnqueue(item);

        // Se o request morrer antes de terminar, não deixamos status pendente
        // eternamente. Se a resposta já tiver iniciado, OnCompleted é quem decide.
        var registration = httpContext.RequestAborted.Register(() =>
        {
            if (!httpContext.Response.HasStarted)
                _status.TryRemove(item.CacheId, out _);
        });

        httpContext.Response.OnCompleted(() =>
        {
            registration.Dispose();

            if (!TryEnqueue(item))
                _status[item.CacheId] = BackgroundCacheStatus.Failed;

            return Task.CompletedTask;
        });

        return true;
    }

    public BackgroundCacheStatus GetStatus(Guid cacheId)
        => _status.TryGetValue(cacheId, out var status)
            ? status
            : BackgroundCacheStatus.Unknown;

    public bool IsPending(Guid cacheId)
    {
        var status = GetStatus(cacheId);
        return status is BackgroundCacheStatus.Pending
            or BackgroundCacheStatus.Running;
    }

    internal IAsyncEnumerable<BackgroundCacheWorkItem> ReadAllAsync(
        CancellationToken ct)
        => _channel.Reader.ReadAllAsync(ct);

    internal void MarkRunning(Guid cacheId)
        => _status[cacheId] = BackgroundCacheStatus.Running;

    internal void MarkCompleted(Guid cacheId)
        => _status.TryRemove(cacheId, out _);

    internal void MarkFailed(Guid cacheId)
        => _status[cacheId] = BackgroundCacheStatus.Failed;

    private bool TryEnqueue(BackgroundCacheWorkItem item)
        => _channel.Writer.TryWrite(item);
}
