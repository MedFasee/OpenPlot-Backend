using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace OpenPlot.Features.Runs.Repositories;

public enum PhaseMode { Any, ABC, Single }

public sealed record MeasurementsQuery(
    string Quantity,
    string Component,
    PhaseMode PhaseMode = PhaseMode.Any,
    string? Phase = null,
    IReadOnlyList<string>? PmuNames = null, // id_name
    string? Unit = null
);

public sealed record MeasurementRow(
    int SignalId,
    int PdcPmuId,
    string IdName,
    string PdcName,
    DateTime Ts,
    double Value
);

public interface IMeasurementsRepository
{
    Task<IReadOnlyList<MeasurementRow>> QueryAsync(
        RunContext ctx,
        MeasurementsQuery q,
        CancellationToken ct);
}

public sealed class MeasurementsRepository : IMeasurementsRepository
{
    private readonly IDbConnectionFactory _dbf;
    public MeasurementsRepository(IDbConnectionFactory dbf) => _dbf = dbf;

    public async Task<IReadOnlyList<MeasurementRow>> QueryAsync(
        RunContext ctx,
        MeasurementsQuery q,
        CancellationToken ct)
    {
        using var db = _dbf.Create();

        var pmuFilter = (q.PmuNames is { Count: > 0 })
            ? "LOWER(pmus.id_name) = ANY(@pmu_names)"
            : "TRUE";

        // filtro fase
        var phaseClause = q.PhaseMode switch
        {
            PhaseMode.ABC => "UPPER(s.phase::text) IN ('A','B','C')",
            PhaseMode.Single => "UPPER(s.phase::text) = UPPER(@phase)",
            _ => "TRUE"
        };

        var sql = $@"
WITH ctx AS (
  SELECT
    @pdc_id::int           AS pdc_id,
    @pdc_name::text        AS pdc_name,
    @from_utc::timestamptz AS from_utc,
    @to_utc::timestamptz   AS to_utc
),
pmus AS (
  SELECT pmu_id, id_name
  FROM openplot.pmu
  WHERE id_name = ANY(@all_run_pmus)
),
sel AS (
  SELECT pmus.id_name, pmus.pmu_id, c.pdc_name, c.pdc_id
  FROM pmus
  CROSS JOIN ctx c
  WHERE {pmuFilter}
),
sig AS (
  SELECT
    s.signal_id,
    s.pdc_pmu_id,
    sel.id_name,
    sel.pdc_name
  FROM sel
  JOIN openplot.pdc_pmu pp ON pp.pdc_id = sel.pdc_id AND pp.pmu_id = sel.pmu_id
  JOIN openplot.signal s   ON s.pdc_pmu_id = pp.pdc_pmu_id
  WHERE LOWER(s.quantity::text)   = LOWER(@quantity)
    AND LOWER(s.component::text)  = LOWER(@component)
    AND {phaseClause}
)
SELECT
  sig.signal_id  AS SignalId,
  sig.pdc_pmu_id AS PdcPmuId,
  sig.id_name    AS IdName,
  sig.pdc_name   AS PdcName,
  m.ts           AS Ts,
  m.value        AS Value
FROM sig
JOIN openplot.measurements m
  ON m.signal_id = sig.signal_id
WHERE m.ts >= (SELECT from_utc FROM ctx)
  AND m.ts <= (SELECT to_utc   FROM ctx)
ORDER BY sig.signal_id, m.ts;
";

        var args = new
        {
            pdc_id = ctx.PdcId,
            pdc_name = ctx.PdcName,
            from_utc = ctx.FromUtc,
            to_utc = ctx.ToUtc,
            quantity = q.Quantity,
            component = q.Component,
            phase = q.Phase,

            all_run_pmus = ctx.PmuNames.Select(x => x).ToArray(),
            pmu_names = (q.PmuNames ?? Array.Empty<string>())
                           .Select(x => x.ToLowerInvariant())
                           .ToArray(),
        };

        var rows = await db.QueryAsync<MeasurementRow>(
            new CommandDefinition(sql, args, cancellationToken: ct));

        return rows.ToList();
    }
}

