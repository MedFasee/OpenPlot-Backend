using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace OpenPlot.Api.Services.Logging;

public sealed class ApiRequestLogRepository : IApiRequestLogRepository
{
    private readonly IDbConnectionFactory _dbf;

    public ApiRequestLogRepository(IDbConnectionFactory dbf)
    {
        _dbf = dbf;
    }

    public async Task InsertAsync(ApiRequestLogEntry entry, CancellationToken ct = default)
    {
        const string sql = @"
            INSERT INTO openplot.api_request_log
            (timestamp_utc, method, path, status_code, elapsed_ms,
             user_name, user_id, ip, correlation_id, user_agent,
             protocol, content_type, content_length, request_body, query_string)
            VALUES
            (@timestamp_utc, @method, @path, @status_code, @elapsed_ms,
             @user_name, @user_id, @ip, @correlation_id, @user_agent,
             @protocol, @content_type, @content_length, @request_body, @query_string);";

        await using var conn = (DbConnection)_dbf.Create();
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        AddParam(cmd, "timestamp_utc", entry.TimestampUtc);
        AddParam(cmd, "method", entry.Method);
        AddParam(cmd, "path", entry.Path);
        AddParam(cmd, "status_code", entry.StatusCode);
        AddParam(cmd, "elapsed_ms", entry.ElapsedMs);
        AddParam(cmd, "user_name", (object?)entry.UserName ?? DBNull.Value);
        AddParam(cmd, "user_id", (object?)entry.UserId ?? DBNull.Value);
        AddParam(cmd, "ip", (object?)entry.Ip ?? DBNull.Value);
        AddParam(cmd, "correlation_id", (object?)entry.CorrelationId ?? DBNull.Value);
        AddParam(cmd, "user_agent", (object?)entry.UserAgent ?? DBNull.Value);
        AddParam(cmd, "protocol", (object?)entry.Protocol ?? DBNull.Value);
        AddParam(cmd, "content_type", (object?)entry.ContentType ?? DBNull.Value);
        AddParam(cmd, "content_length", (object?)entry.ContentLength ?? DBNull.Value);
        AddParam(cmd, "request_body", (object?)entry.RequestBody ?? DBNull.Value);
        AddParam(cmd, "query_string", (object?)entry.QueryString ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static void AddParam(DbCommand cmd, string name, object value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        cmd.Parameters.Add(p);
    }
}
