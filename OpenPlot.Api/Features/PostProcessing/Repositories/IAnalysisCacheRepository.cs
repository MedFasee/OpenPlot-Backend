public interface IAnalysisCacheRepository
{
    Task<Guid> SaveAsync(Guid jobId, object payload, CancellationToken ct);
    Task<T?> GetAsync<T>(Guid cacheId, CancellationToken ct);
}