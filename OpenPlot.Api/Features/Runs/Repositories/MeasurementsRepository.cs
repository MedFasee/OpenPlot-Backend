using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace OpenPlot.Features.Runs.Repositories;

public enum PhaseMode
{
    Any,
    Single,
    ABC,        // quando for “ABC cru”
    ThreePhase, // equivalente ao three_phase do front (usa terminal)
    Deseq,      // desequilíbrio
    SeqPos,
    SeqNeg,
    SeqZero
}


public enum SeqKind { Voltage, Current }
public enum SeqType { Pos, Neg, Zero }

public sealed record SeqRequest(SeqKind Kind, SeqType Seq);
public sealed record UnbalanceRequest(SeqKind Kind);

public sealed record MeasurementsQuery(
    string Quantity,
    string Component,
    PhaseMode PhaseMode = PhaseMode.Any,
    string? Phase = null,
    IReadOnlyList<string>? PmuNames = null, // id_name
    string? Unit = null,
    string? ReferenceTerminal = null   // <-- novo
);

public sealed record MeasurementRow(
    int SignalId,
    int PdcPmuId,
    string IdName,
    string PdcName,
    DateTime Ts,
    double Value
);

public sealed record PhasorMeasurementRow(
    int SignalId,
    int PdcPmuId,
    string IdName,
    string PdcName,
    string Phase,
    string Component,
    int? VoltLevel,
    DateTime Ts,
    double Value
);

/// <summary>
/// Row rico para SEQ/UNBALANCE: MAG+ANG e fases A/B/C + volt_level.
/// </summary>
public sealed record PhasorAbcRow(
    int SignalId,
    int PdcPmuId,
    string IdName,
    string PdcName,
    string Phase,        // A|B|C
    string Component,    // MAG|ANG
    double? VoltLevel,   // como vier do banco
    DateTime Ts,
    double Value
);

public interface IMeasurementsRepository
{
    Task<IReadOnlyList<MeasurementRow>> QueryAsync(
        RunContext ctx,
        MeasurementsQuery q,
        CancellationToken ct);

    Task<IReadOnlyList<PhasorMeasurementRow>> QueryPhasorAsync(
        RunContext ctx,
        MeasurementsQuery q,
        CancellationToken ct);

    Task<IReadOnlyList<PhasorAbcRow>> QueryAbcMagAngAsync(
        RunContext ctx,
        string kind,                    // "voltage" | "current"
        IReadOnlyList<string>? pmuNames,
        DateTime? fromUtc,
        DateTime? toUtc,
        CancellationToken ct);
}

public sealed class MeasurementsRepository : IMeasurementsRepository
{
    private readonly IDbConnectionFactory _dbf;
    public MeasurementsRepository(IDbConnectionFactory dbf) => _dbf = dbf;

    // ============================================================
    // 1) SIMPLE: para freq/dfreq/etc (retorna MeasurementRow)
    // ============================================================
    public async Task<IReadOnlyList<MeasurementRow>> QueryAsync(
        RunContext ctx,
        MeasurementsQuery q,
        CancellationToken ct)
    {
        using var db = _dbf.Create();

        var pmuFilter = (q.PmuNames is { Count: > 0 })
            ? "LOWER(pmus.id_name) = ANY(@pmu_names)"
            : "TRUE";

        var phaseClause = q.PhaseMode switch
        {
            PhaseMode.ABC => "UPPER(s.phase::text) IN ('A','B','C')",
            PhaseMode.Single => "UPPER(s.phase::text) = UPPER(@phase)",
            _ => "TRUE"
        };

        // NOTE: aqui NÃO traz phase/component/volt_level no SELECT final
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
  WHERE LOWER(s.quantity::text)  = LOWER(@quantity)
    AND LOWER(s.component::text) = LOWER(@component)
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

            all_run_pmus = ctx.PmuNames.ToArray(),
            pmu_names = (q.PmuNames ?? Array.Empty<string>())
                .Select(x => x.ToLowerInvariant())
                .ToArray(),
        };

        var rows = await db.QueryAsync<MeasurementRow>(
            new CommandDefinition(sql, args, cancellationToken: ct));

        return rows.ToList();
    }

    // ============================================================
    // 2) PHASOR: voltage/current MAG (retorna PhasorMeasurementRow)
    // ============================================================
    public async Task<IReadOnlyList<PhasorMeasurementRow>> QueryPhasorAsync(
        RunContext ctx,
        MeasurementsQuery q,
        CancellationToken ct)
    {
        using var db = _dbf.Create();

        var pmuFilter = (q.PmuNames is { Count: > 0 })
            ? "LOWER(pmus.id_name) = ANY(@pmu_names)"
            : "TRUE";

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
  SELECT pmu_id, id_name, volt_level
  FROM openplot.pmu
  WHERE id_name = ANY(@all_run_pmus)
),
sel AS (
  SELECT pmus.id_name, pmus.pmu_id, pmus.volt_level, c.pdc_name, c.pdc_id
  FROM pmus
  CROSS JOIN ctx c
  WHERE {pmuFilter}
),
sig AS (
  SELECT
    s.signal_id,
    s.pdc_pmu_id,
    sel.id_name,
    sel.pdc_name,
    UPPER(s.phase::text)     AS phase,
    UPPER(s.component::text) AS component,
    sel.volt_level           AS volt_level
  FROM sel
  JOIN openplot.pdc_pmu pp ON pp.pdc_id = sel.pdc_id AND pp.pmu_id = sel.pmu_id
  JOIN openplot.signal s   ON s.pdc_pmu_id = pp.pdc_pmu_id
  WHERE LOWER(s.quantity::text)  = LOWER(@quantity)
    AND LOWER(s.component::text) = LOWER(@component)
    AND {phaseClause}
)
SELECT
  sig.signal_id  AS SignalId,
  sig.pdc_pmu_id AS PdcPmuId,
  sig.id_name    AS IdName,
  sig.pdc_name   AS PdcName,
  sig.phase      AS Phase,
  sig.component  AS Component,
  sig.volt_level AS VoltLevel,
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

            all_run_pmus = ctx.PmuNames.ToArray(),
            pmu_names = (q.PmuNames ?? Array.Empty<string>())
                .Select(x => x.ToLowerInvariant())
                .ToArray(),
        };

        var rows = await db.QueryAsync<PhasorMeasurementRow>(
            new CommandDefinition(sql, args, cancellationToken: ct));

        return rows.ToList();
    }

    // ============================================================
    // 3) ABC MAG+ANG: para SEQ/UNBALANCE (retorna PhasorAbcRow)
    // ============================================================
    public async Task<IReadOnlyList<PhasorAbcRow>> QueryAbcMagAngAsync(
        RunContext ctx,
        string kind,
        IReadOnlyList<string>? pmuNames,
        DateTime? fromUtc,
        DateTime? toUtc,
        CancellationToken ct)
    {
        using var db = _dbf.Create();

        var k = (kind ?? "").Trim().ToLowerInvariant();
        if (k is not ("voltage" or "current"))
            throw new ArgumentException("kind deve ser 'voltage' ou 'current'.", nameof(kind));

        var effFrom = fromUtc ?? ctx.FromUtc;
        var effTo = toUtc ?? ctx.ToUtc;

        var pmuFilter = (pmuNames is { Count: > 0 })
            ? "LOWER(pmus.id_name) = ANY(@pmu_names)"
            : "TRUE";

        var sql = $@"
WITH ctx AS (
  SELECT
    @pdc_id::int           AS pdc_id,
    @pdc_name::text        AS pdc_name,
    @from_utc::timestamptz AS from_utc,
    @to_utc::timestamptz   AS to_utc
),
pmus AS (
  SELECT pmu_id, id_name, volt_level
  FROM openplot.pmu
  WHERE id_name = ANY(@all_run_pmus)
),
sel AS (
  SELECT pmus.id_name, pmus.pmu_id, pmus.volt_level, c.pdc_name, c.pdc_id
  FROM pmus
  CROSS JOIN ctx c
  WHERE {pmuFilter}
),
sig AS (
  SELECT
    s.signal_id,
    s.pdc_pmu_id,
    sel.id_name,
    sel.pdc_name,
    UPPER(s.phase::text)     AS phase,
    UPPER(s.component::text) AS component,
    sel.volt_level::double precision AS volt_level
  FROM sel
  JOIN openplot.pdc_pmu pp ON pp.pdc_id = sel.pdc_id AND pp.pmu_id = sel.pmu_id
  JOIN openplot.signal s   ON s.pdc_pmu_id = pp.pdc_pmu_id
  WHERE
    (
      (@kind = 'voltage' AND LOWER(s.quantity::text) IN ('voltage','v'))
      OR
      (@kind = 'current' AND LOWER(s.quantity::text) IN ('current','i'))
    )
    AND UPPER(s.phase::text) IN ('A','B','C')
    AND UPPER(s.component::text) IN ('MAG','ANG')
)
SELECT
  sig.signal_id  AS SignalId,
  sig.pdc_pmu_id AS PdcPmuId,
  sig.id_name    AS IdName,
  sig.pdc_name   AS PdcName,
  sig.phase      AS Phase,
  sig.component  AS Component,
  sig.volt_level AS VoltLevel,
  m.ts           AS Ts,
  m.value        AS Value
FROM sig
JOIN openplot.measurements m
  ON m.signal_id = sig.signal_id
WHERE m.ts >= (SELECT from_utc FROM ctx)
  AND m.ts <= (SELECT to_utc   FROM ctx)
ORDER BY sig.id_name, sig.signal_id, m.ts;
";

        var args = new
        {
            pdc_id = ctx.PdcId,
            pdc_name = ctx.PdcName,
            from_utc = effFrom,
            to_utc = effTo,
            kind = k,

            all_run_pmus = ctx.PmuNames.ToArray(),
            pmu_names = (pmuNames ?? Array.Empty<string>())
                .Select(x => x.ToLowerInvariant())
                .ToArray(),
        };

        var rows = await db.QueryAsync<PhasorAbcRow>(
            new CommandDefinition(sql, args, cancellationToken: ct));

        return rows.ToList();
    }
}
