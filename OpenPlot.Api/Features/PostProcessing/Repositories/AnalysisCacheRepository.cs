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

    public async Task<Guid> SaveAsync(Guid jobId, object payload, CancellationToken ct)
    {
        const string sql = """
        INSERT INTO openplot.analysis_cache (cache_id, job_id, payload, last_accessed_at)
        VALUES (@cache_id, @job_id, CAST(@payload AS jsonb), now());
        """;

        var cacheId = Guid.NewGuid();

        using var db = _dbf.Create();
        await db.ExecuteAsync(sql, new
        {
            cache_id = cacheId,
            job_id = jobId,
            payload = JsonSerializer.Serialize(payload, JsonOpts)
        });

        return cacheId;
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