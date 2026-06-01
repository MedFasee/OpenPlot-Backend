using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Dapper;

public interface IOpenPlotDatabaseBootstrapper
{
    Task EnsureInitializedAsync(CancellationToken ct = default);
}

public sealed class OpenPlotDatabaseBootstrapper : IOpenPlotDatabaseBootstrapper
{
    private const string BootstrapSql = @"
CREATE SCHEMA IF NOT EXISTS openplot;

CREATE UNIQUE INDEX IF NOT EXISTS ux_signal_pdc_pmu_name_phase_component
    ON openplot.signal (pdc_pmu_id, name, phase, component);

CREATE TABLE IF NOT EXISTS openplot.sso_request_nonce (
    id UUID PRIMARY KEY,
    client_id VARCHAR(100) NOT NULL,
    nonce VARCHAR(200) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_sso_request_nonce_client_nonce
    ON openplot.sso_request_nonce (client_id, nonce);

CREATE TABLE IF NOT EXISTS openplot.sso_login_token (
    id UUID PRIMARY KEY,
    token VARCHAR(500) NOT NULL,
    consulta_id VARCHAR(100) NOT NULL,
    origin_client VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    used BOOLEAN NOT NULL DEFAULT FALSE,
    used_at TIMESTAMPTZ NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_sso_login_token_token
    ON openplot.sso_login_token (token);
";

    private readonly IDbConnectionFactory _dbf;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool _initialized;

    public OpenPlotDatabaseBootstrapper(IDbConnectionFactory dbf)
    {
        _dbf = dbf;
    }

    public async Task EnsureInitializedAsync(CancellationToken ct = default)
    {
        if (_initialized)
            return;

        await _gate.WaitAsync(ct);
        try
        {
            if (_initialized)
                return;

            await using var conn = (DbConnection)_dbf.Create();
            await conn.OpenAsync(ct);
            await conn.ExecuteAsync(new CommandDefinition(BootstrapSql, cancellationToken: ct));

            _initialized = true;
        }
        finally
        {
            _gate.Release();
        }
    }
}