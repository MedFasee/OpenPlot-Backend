using System.Text.Json;
using Dapper;

public sealed class AnalysisCacheRepository : IAnalysisCacheRepository
{
    private readonly IDbConnectionFactory _dbf;

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public AnalysisCacheRepository(IDbConnectionFactory dbf)
    {
        _dbf = dbf;
    }

    public Task<Guid> SaveAsync(Guid jobId, object payload, CancellationToken ct)
    {
        var cacheId = Guid.NewGuid();
        return SaveAsync(cacheId, jobId, null!, payload, ct);
    }

    public Task<Guid> SaveAsync(Guid cacheId, Guid jobId, object payload, CancellationToken ct)
        => SaveAsync(cacheId, jobId, null!, payload, ct);

    public async Task<Guid> SaveAsync(Guid cacheId, Guid jobId, string cacheKey, object payload, CancellationToken ct)
    {
        const string sql = """
        INSERT INTO openplot.analysis_cache (cache_id, job_id, cache_key, payload, last_accessed_at)
        VALUES (@cache_id, @job_id, @cache_key, CAST(@payload AS jsonb), now())
        ON CONFLICT (cache_key)
        DO UPDATE SET payload = EXCLUDED.payload, job_id = EXCLUDED.job_id, last_accessed_at = now()
        RETURNING cache_id;

        DELETE FROM openplot.analysis_cache WHERE last_accessed_at < now() - INTERVAL '24 hours';
        """;

        using var db = _dbf.Create();
        var winner = await db.QuerySingleAsync<Guid>(sql, new
        {
            cache_id = cacheId,
            job_id = jobId,
            cache_key = cacheKey,
            payload = JsonSerializer.Serialize(payload, JsonOpts)
        });

        return winner;
    }

    public async Task<ExistingCache?> FindByCacheKeyAsync(string cacheKey, CancellationToken ct)
    {
        using var db = _dbf.Create();
        return await db.QuerySingleOrDefaultAsync<ExistingCache>("SELECT cache_id AS CacheId, job_id AS RunId FROM openplot.analysis_cache WHERE cache_key = @cacheKey AND payload IS NOT NULL LIMIT 1", new { cacheKey });
    }

    public async Task<Guid> ReserveAsync(Guid cacheId, Guid runId, string cacheKey, CancellationToken ct)
    {
        using var db = _dbf.Create();
        return await db.QuerySingleAsync<Guid>("INSERT INTO openplot.analysis_cache (cache_id, job_id, cache_key, payload, last_accessed_at) VALUES (@cacheId, @runId, @cacheKey, NULL, now()) ON CONFLICT (cache_key) DO UPDATE SET last_accessed_at = now() RETURNING cache_id", new { cacheId, runId, cacheKey });
    }

    public async Task ReleaseReservationAsync(string cacheKey, Guid cacheId, CancellationToken ct)
    {
        using var db = _dbf.Create();
        await db.ExecuteAsync("DELETE FROM openplot.analysis_cache WHERE cache_key = @cacheKey AND cache_id = @cacheId AND payload IS NULL", new { cacheKey, cacheId });
    }

    public async Task<T?> GetAsync<T>(Guid cacheId, CancellationToken ct)
    {
        const string sql = """
        UPDATE openplot.analysis_cache
           SET last_accessed_at = now()
         WHERE cache_id = @cache_id
        RETURNING payload::text;
        """;

        using var db = _dbf.Create();
        var json = await db.QueryFirstOrDefaultAsync<string>(sql, new { cache_id = cacheId });

        if (string.IsNullOrWhiteSpace(json))
            return default;

        return JsonSerializer.Deserialize<T>(json, JsonOpts);
    }
}