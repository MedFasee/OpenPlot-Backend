using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace OpenPlot.Services.BackgroundCache;

public sealed record CacheWorkKey(
    string Type,
    Guid RunId,
    DateTime? WindowFromUtc,
    DateTime? WindowToUtc,
    string Parameters)
{
    public string CacheKey => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(
        JsonSerializer.Serialize(new
        {
            Type,
            RunId,
            WindowFromUtc,
            WindowToUtc,
            Parameters
        }))));
    public static CacheWorkKey Create(
        string type,
        Guid runId,
        DateTime? windowFromUtc,
        DateTime? windowToUtc,
        params (string Name, string? Value)[] parameters)
    {
        var canonicalParameters = parameters
            .OrderBy(parameter => parameter.Name, StringComparer.Ordinal)
            .Select(parameter => new[]
            {
                Normalize(parameter.Name),
                Normalize(parameter.Value)
            });

        return new CacheWorkKey(
            Normalize(type),
            runId,
            NormalizeUtc(windowFromUtc),
            NormalizeUtc(windowToUtc),
            JsonSerializer.Serialize(canonicalParameters));
    }

    public static string? NormalizeCollection(IEnumerable<string>? values)
    {
        return JsonSerializer.Serialize((values ?? Array.Empty<string>())
            .Select(Normalize)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(value => value, StringComparer.Ordinal));
    }

    private static DateTime? NormalizeUtc(DateTime? value)
        => value?.ToUniversalTime();

    private static string Normalize(string? value)
        => string.IsNullOrWhiteSpace(value)
            ? "<null>"
            : value.Trim().ToUpperInvariant();
}

public sealed record CacheReservation(Guid CacheId, bool IsOwner);

public interface IBackgroundCacheCoordinator
{
    Task<CacheReservation> ReserveOrGetAsync(CacheWorkKey key, CancellationToken ct);

    void Complete(CacheWorkKey key, Guid cacheId);

    Task FailAsync(CacheWorkKey key, Guid cacheId, CancellationToken ct = default);
}

public sealed class BackgroundCacheCoordinator : IBackgroundCacheCoordinator
{
    private readonly ConcurrentDictionary<CacheWorkKey, Lazy<Task<ReservationEntry>>> _reservations = new();
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly Func<IAnalysisCacheRepository>? _repositoryFactory;
    private readonly ILogger<BackgroundCacheCoordinator> _logger;

    public BackgroundCacheCoordinator(IServiceScopeFactory scopeFactory, ILogger<BackgroundCacheCoordinator> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    public BackgroundCacheCoordinator(Func<IAnalysisCacheRepository> repositoryFactory, ILogger<BackgroundCacheCoordinator> logger)
    {
        _repositoryFactory = repositoryFactory;
        _scopeFactory = null!;
        _logger = logger;
    }

    public async Task<CacheReservation> ReserveOrGetAsync(CacheWorkKey key, CancellationToken ct)
    {
        var candidate = new Lazy<Task<ReservationEntry>>(
            () => ReserveAsync(key, ct),
            LazyThreadSafetyMode.ExecutionAndPublication);
        var lazy = _reservations.GetOrAdd(key, candidate);
        var entry = await lazy.Value;
        var isOwner = ReferenceEquals(lazy, candidate) && entry.IsOwner;

        if (!entry.IsOwner)
            _reservations.TryRemove(key, out _);

        _logger.LogInformation(
            isOwner ? "[CACHE-COALESCE][MISS] type={Type} runId={RunId} cacheId={CacheId}" : "[CACHE-COALESCE][ACTIVE-HIT] type={Type} runId={RunId} cacheId={CacheId}",
            key.Type,
            key.RunId,
            entry.CacheId);

        return new CacheReservation(entry.CacheId, isOwner);
    }

    public void Complete(CacheWorkKey key, Guid cacheId)
    {
        Remove(key, cacheId);
        _logger.LogInformation(
            "[CACHE-COALESCE][COMPLETE] type={Type} runId={RunId} cacheId={CacheId}",
            key.Type,
            key.RunId,
            cacheId);
    }

    public async Task FailAsync(CacheWorkKey key, Guid cacheId, CancellationToken ct = default)
    {
        Remove(key, cacheId);
        using var scope = _repositoryFactory is null ? _scopeFactory.CreateScope() : null;
        var repository = _repositoryFactory?.Invoke() ?? scope!.ServiceProvider.GetRequiredService<IAnalysisCacheRepository>();
        await repository.ReleaseReservationAsync(key.CacheKey, cacheId, ct);
        _logger.LogInformation(
            "[CACHE-COALESCE][FAIL] type={Type} runId={RunId} cacheId={CacheId}",
            key.Type,
            key.RunId,
            cacheId);
    }

    private void Remove(CacheWorkKey key, Guid cacheId)
    {
        if (_reservations.TryGetValue(key, out var lazy) && lazy.IsValueCreated &&
            lazy.Value.IsCompletedSuccessfully && lazy.Value.Result.CacheId == cacheId)
        {
            ((ICollection<KeyValuePair<CacheWorkKey, Lazy<Task<ReservationEntry>>>>)_reservations)
                .Remove(new KeyValuePair<CacheWorkKey, Lazy<Task<ReservationEntry>>>(key, lazy));
        }
    }

    private async Task<ReservationEntry> ReserveAsync(CacheWorkKey key, CancellationToken ct)
    {
        using var scope = _repositoryFactory is null ? _scopeFactory.CreateScope() : null;
        var repository = _repositoryFactory?.Invoke() ?? scope!.ServiceProvider.GetRequiredService<IAnalysisCacheRepository>();
        var existing = await repository.FindByCacheKeyAsync(key.CacheKey, ct);
        if (existing is not null)
        {
            _logger.LogInformation("[CACHE-COALESCE][PERSISTED-HIT] type={Type} runId={RunId} cacheId={CacheId}", key.Type, key.RunId, existing.CacheId);
            return new ReservationEntry(existing.CacheId, false);
        }

        var cacheId = Guid.NewGuid();
        var winner = await repository.ReserveAsync(cacheId, key.RunId, key.CacheKey, ct);
        if (winner != cacheId)
        {
            _logger.LogInformation("[CACHE-COALESCE][DB-RACE-REUSED] type={Type} runId={RunId} cacheId={CacheId}", key.Type, key.RunId, winner);
            return new ReservationEntry(winner, false);
        }

        return new ReservationEntry(cacheId, true);
    }

    private sealed record ReservationEntry(Guid CacheId, bool IsOwner);
}