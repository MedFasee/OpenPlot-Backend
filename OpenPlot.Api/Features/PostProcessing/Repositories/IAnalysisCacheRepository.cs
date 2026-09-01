public sealed record ExistingCache(Guid CacheId, Guid RunId);

public interface IAnalysisCacheRepository
{
    Task<ExistingCache?> FindByCacheKeyAsync(string cacheKey, CancellationToken ct);
    Task<Guid> ReserveAsync(Guid cacheId, Guid runId, string cacheKey, CancellationToken ct);
    Task ReleaseReservationAsync(string cacheKey, Guid cacheId, CancellationToken ct);
    Task<Guid> SaveAsync(Guid jobId, object payload, CancellationToken ct);
    Task<Guid> SaveAsync(Guid cacheId, Guid jobId, object payload, CancellationToken ct);
    Task<Guid> SaveAsync(Guid cacheId, Guid jobId, string cacheKey, object payload, CancellationToken ct);
    Task<T?> GetAsync<T>(Guid cacheId, CancellationToken ct);
}
