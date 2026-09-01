using System.Data.Common;
using Dapper;

namespace OpenPlot.Features.Sso.Repositories;

public sealed class SsoAuthRepository : ISsoAuthRepository
{
    private readonly IDbConnectionFactory _dbf;

    public SsoAuthRepository(IDbConnectionFactory dbf)
    {
        _dbf = dbf;
    }

    public async Task<bool> TryRegisterNonceAsync(
        string clientId,
        string nonce,
        DateTime createdAtUtc,
        DateTime expiresAtUtc,
        CancellationToken ct = default)
    {
        await using var conn = (DbConnection)_dbf.Create();
        await conn.OpenAsync(ct);

        await PurgeExpiredNoncesAsync(conn, ct);

        const string sql = @"
INSERT INTO openplot.sso_request_nonce (
    id,
    client_id,
    nonce,
    created_at,
    expires_at
)
VALUES (
    @Id,
    @ClientId,
    @Nonce,
    @CreatedAtUtc,
    @ExpiresAtUtc
)
ON CONFLICT (client_id, nonce) DO NOTHING;";

        var affectedRows = await conn.ExecuteAsync(
            new CommandDefinition(
                sql,
                new
                {
                    Id = Guid.NewGuid(),
                    ClientId = clientId,
                    Nonce = nonce,
                    CreatedAtUtc = createdAtUtc,
                    ExpiresAtUtc = expiresAtUtc
                },
                cancellationToken: ct));

        return affectedRows > 0;
    }

    public async Task CreateLoginTokenAsync(
        string token,
        string consultaId,
        string originClient,
        DateTime createdAtUtc,
        DateTime expiresAtUtc,
        CancellationToken ct = default)
    {
        await using var conn = (DbConnection)_dbf.Create();
        await conn.OpenAsync(ct);

        await PurgeExpiredLoginTokensAsync(conn, ct);

        const string sql = @"
INSERT INTO openplot.sso_login_token (
    id,
    token,
    consulta_id,
    origin_client,
    created_at,
    expires_at,
    used
)
VALUES (
    @Id,
    @Token,
    @ConsultaId,
    @OriginClient,
    @CreatedAtUtc,
    @ExpiresAtUtc,
    FALSE
);";

        await conn.ExecuteAsync(
            new CommandDefinition(
                sql,
                new
                {
                    Id = Guid.NewGuid(),
                    Token = token,
                    ConsultaId = consultaId,
                    OriginClient = originClient,
                    CreatedAtUtc = createdAtUtc,
                    ExpiresAtUtc = expiresAtUtc
                },
                cancellationToken: ct));
    }

    public async Task<ConsumedSsoLoginToken?> ConsumeLoginTokenAsync(
        string token,
        CancellationToken ct = default)
    {
        await using var conn = (DbConnection)_dbf.Create();
        await conn.OpenAsync(ct);

        await PurgeExpiredLoginTokensAsync(conn, ct);

        const string sql = @"
WITH matched AS (
    SELECT id
    FROM openplot.sso_login_token
    WHERE token = @Token
      AND used = FALSE
      AND expires_at >= now()
    FOR UPDATE
)
UPDATE openplot.sso_login_token token_row
SET used = TRUE,
    used_at = now()
FROM matched
WHERE token_row.id = matched.id
RETURNING
    token_row.id,
    token_row.consulta_id AS ConsultaId,
    token_row.origin_client AS OriginClient,
    token_row.created_at AS CreatedAtUtc,
    token_row.expires_at AS ExpiresAtUtc;";

        return await conn.QuerySingleOrDefaultAsync<ConsumedSsoLoginToken>(
            new CommandDefinition(
                sql,
                new { Token = token },
                cancellationToken: ct));
    }

    private static Task PurgeExpiredNoncesAsync(
        DbConnection conn,
        CancellationToken ct)
        => conn.ExecuteAsync(
            new CommandDefinition(
                @"
DELETE FROM openplot.sso_request_nonce
WHERE expires_at < now();",
                cancellationToken: ct));

    private static Task PurgeExpiredLoginTokensAsync(
        DbConnection conn,
        CancellationToken ct)
        => conn.ExecuteAsync(
            new CommandDefinition(
                @"
DELETE FROM openplot.sso_login_token
WHERE expires_at < now()
   OR used = TRUE;",
                cancellationToken: ct));
}