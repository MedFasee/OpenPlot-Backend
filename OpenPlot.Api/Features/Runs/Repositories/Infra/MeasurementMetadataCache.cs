using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace OpenPlot.Features.Runs.Repositories;

/// <summary>
/// Cache unico de metadados (PMU scope / Signal scope / ABC signal scope)
/// consumido por todas as familias de series. Nunca cacheia measurements, nem
/// falhas, nem resultados obtidos durante cancelamento. TTL fixo de 5 minutos.
/// </summary>
public interface IMeasurementMetadataCache
{
    Task<IReadOnlyList<PmuScopeRow>> GetOrAddPmuScopeAsync(
        int pdcId,
        IReadOnlyList<string> pmuNames,
        Func<CancellationToken, Task<List<PmuScopeRow>>> factory,
        CancellationToken ct);

    Task<IReadOnlyList<SignalScopeRow>> GetOrAddSignalScopeAsync(
        int pdcId,
        IReadOnlyList<string> pmuNames,
        string quantity,
        string component,
        PhaseMode phaseMode,
        string? phase,
        Func<CancellationToken, Task<List<SignalScopeRow>>> factory,
        CancellationToken ct);

    Task<IReadOnlyList<SignalScopeRow>> GetOrAddAbcSignalScopeAsync(
        int pdcId,
        IReadOnlyList<string> pmuNames,
        string kind,
        Func<CancellationToken, Task<List<SignalScopeRow>>> factory,
        CancellationToken ct);
}

public sealed class MeasurementMetadataCache : IMeasurementMetadataCache
{
    private static readonly TimeSpan Ttl = TimeSpan.FromMinutes(5);

    private readonly IMemoryCache _cache;
    private readonly ILogger<MeasurementMetadataCache> _logger;

    public MeasurementMetadataCache(IMemoryCache cache, ILogger<MeasurementMetadataCache> logger)
    {
        _cache = cache ?? throw new ArgumentNullException(nameof(cache));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public Task<IReadOnlyList<PmuScopeRow>> GetOrAddPmuScopeAsync(
        int pdcId,
        IReadOnlyList<string> pmuNames,
        Func<CancellationToken, Task<List<PmuScopeRow>>> factory,
        CancellationToken ct)
    {
        var key = new PmuScopeCacheKey(pdcId, MeasurementKeyNormalization.NormalizePmuKey(pmuNames));
        return GetOrAddAsync("PmuScope", key, factory, ct);
    }

    public Task<IReadOnlyList<SignalScopeRow>> GetOrAddSignalScopeAsync(
        int pdcId,
        IReadOnlyList<string> pmuNames,
        string quantity,
        string component,
        PhaseMode phaseMode,
        string? phase,
        Func<CancellationToken, Task<List<SignalScopeRow>>> factory,
        CancellationToken ct)
    {
        var key = new SignalScopeCacheKey(
            pdcId,
            MeasurementKeyNormalization.NormalizePmuKey(pmuNames),
            (quantity ?? string.Empty).Trim().ToLowerInvariant(),
            (component ?? string.Empty).Trim().ToUpperInvariant(),
            phaseMode.ToString(),
            (phase ?? string.Empty).Trim().ToUpperInvariant());

        return GetOrAddAsync("SignalScope", key, factory, ct);
    }

    public Task<IReadOnlyList<SignalScopeRow>> GetOrAddAbcSignalScopeAsync(
        int pdcId,
        IReadOnlyList<string> pmuNames,
        string kind,
        Func<CancellationToken, Task<List<SignalScopeRow>>> factory,
        CancellationToken ct)
    {
        var key = new AbcSignalScopeCacheKey(
            pdcId,
            MeasurementKeyNormalization.NormalizePmuKey(pmuNames),
            (kind ?? string.Empty).Trim().ToLowerInvariant());

        return GetOrAddAsync("AbcSignalScope", key, factory, ct);
    }

    private async Task<IReadOnlyList<TRow>> GetOrAddAsync<TKey, TRow>(
        string scope,
        TKey key,
        Func<CancellationToken, Task<List<TRow>>> factory,
        CancellationToken ct)
        where TKey : notnull
    {
        if (_cache.TryGetValue(key, out List<TRow>? cached) && cached is not null)
        {
            _logger.LogDebug("[METADATA-CACHE][HIT] scope={Scope} key={Key}", scope, key);
            return cached;
        }

        _logger.LogDebug("[METADATA-CACHE][MISS] scope={Scope} key={Key}", scope, key);

        var result = await factory(ct);

        // Nao cachear falhas (o factory ja teria lancado antes deste ponto)
        // nem resultados obtidos durante cancelamento.
        if (!ct.IsCancellationRequested)
            _cache.Set(key, result, Ttl);

        return result;
    }

    private readonly record struct PmuScopeCacheKey(int PdcId, string PmuKey);

    private readonly record struct SignalScopeCacheKey(
        int PdcId,
        string PmuKey,
        string Quantity,
        string Component,
        string PhaseMode,
        string Phase);

    private readonly record struct AbcSignalScopeCacheKey(int PdcId, string PmuKey, string Kind);
}
