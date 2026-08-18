using Dapper;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace OpenPlot.Features.Runs.Repositories;

public enum PhaseMode
{
    Any,
    Single,
    ABC,
    ThreePhase,
    Deseq,
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
    IReadOnlyList<string>? PmuNames = null,
    string? Unit = null,
    string? ReferenceTerminal = null
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

public sealed record PhasorAbcRow(
    int SignalId,
    int PdcPmuId,
    string IdName,
    string PdcName,
    string Phase,
    string Component,
    double? VoltLevel,
    DateTime Ts,
    double Value
);

public interface IMeasurementsRepository
{
    Task<IReadOnlyList<MeasurementRow>> QueryAsync(
        RunContext ctx,
        MeasurementsQuery q,
        CancellationToken ct,
        int? maxPoints = null);

    Task<IReadOnlyList<PhasorMeasurementRow>> QueryPhasorAsync(
        RunContext ctx,
        MeasurementsQuery q,
        CancellationToken ct,
        int? maxPoints = null);

    Task<IReadOnlyList<PhasorAbcRow>> QueryAbcMagAngAsync(
        RunContext ctx,
        string kind,
        IReadOnlyList<string>? pmuNames,
        DateTime? fromUtc,
        DateTime? toUtc,
        CancellationToken ct,
        int? maxPoints = null);

    Task WarmUpAsync(
        RunContext ctx,
        CancellationToken ct);
}

public sealed class MeasurementsRepository : IMeasurementsRepository
{
    private const int ByRunMeasurementQuality = 29;

    private static readonly string WarmUpSimpleSql = BuildWarmUpSimpleSql();
    private static readonly string WarmUpPhasorSql = BuildWarmUpPhasorSql();
    private static readonly string WarmUpAbcMagAngSql = BuildWarmUpAbcMagAngSql();

    private readonly IDbConnectionFactory _dbf;
    private readonly ILogger<MeasurementsRepository> _logger;

    public MeasurementsRepository(
        IDbConnectionFactory dbf,
        ILogger<MeasurementsRepository> logger)
    {
        _dbf = dbf ?? throw new ArgumentNullException(nameof(dbf));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    // ============================================================
    // 1) SIMPLE: frequency / dfreq / digital / etc.
    // Preview:
    //   - Toolkit disponível: LTTB no TimescaleDB.
    //   - Sem Toolkit: time_bucket + first(), também no TimescaleDB.
    // Processamento:
    //   - maxPoints == null => massa completa.
    // ============================================================
    public async Task<IReadOnlyList<MeasurementRow>> QueryAsync(
        RunContext ctx,
        MeasurementsQuery q,
        CancellationToken ct,
        int? maxPoints = null)
    {
        using var db = _dbf.Create();

        var pmuFilter = q.PmuNames is { Count: > 0 }
            ? "LOWER(pmus.id_name) = ANY(@pmu_names)"
            : "TRUE";

        var phaseClause = q.PhaseMode switch
        {
            PhaseMode.ABC => "UPPER(s.phase::text) IN ('A','B','C')",
            PhaseMode.Single => "UPPER(s.phase::text) = UPPER(@phase)",
            _ => "TRUE"
        };

        var valueCase = WideSignalColumnMap.BuildValueCaseSql("sig", "mw");
        var isPreview = maxPoints is > 0;
        var useToolkitLttb = isPreview && await HasTimescaleToolkitAsync(db, ct);

        var sql = BuildSimpleQuerySql(
            pmuFilter,
            phaseClause,
            valueCase,
            isPreview,
            useToolkitLttb);

        var args = new
        {
            pdc_id = ctx.PdcId,
            pdc_name = ctx.PdcName,
            from_utc = ctx.FromUtc,
            to_utc = ctx.ToUtc,
            quality = ByRunMeasurementQuality,
            quantity = q.Quantity,
            component = q.Component,
            phase = q.Phase,
            max_points = maxPoints,
            all_run_pmus = ctx.PmuNames.ToArray(),
            pmu_names = (q.PmuNames ?? Array.Empty<string>())
                .Select(x => x.ToLowerInvariant())
                .ToArray()
        };

        _logger.LogInformation(
            "[DATA-REQ][QueryAsync][START] pdc={Pdc} quantity={Quantity} component={Component} phase={Phase} window=[{From:o}..{To:o}] pmus={Pmus} maxPoints={MaxPoints} downsample={Downsample}",
            ctx.PdcName,
            q.Quantity,
            q.Component,
            q.Phase,
            ctx.FromUtc,
            ctx.ToUtc,
            string.Join(',', q.PmuNames ?? ctx.PmuNames),
            maxPoints,
            isPreview ? (useToolkitLttb ? "lttb" : "time_bucket_first") : "none");

        var stopwatch = Stopwatch.StartNew();

        var rows = (await db.QueryAsync<MeasurementRow>(
            new CommandDefinition(
                sql,
                args,
                commandTimeout: 30,
                cancellationToken: ct)))
            .ToList();

        stopwatch.Stop();

        _logger.LogInformation(
            "[DATA-REQ][QueryAsync][END] elapsedMs={ElapsedMs} rows={Rows} pdc_pmu_ids={PdcPmuIds}",
            stopwatch.ElapsedMilliseconds,
            rows.Count,
            string.Join(',', rows.Select(r => r.PdcPmuId).Distinct().OrderBy(x => x)));

        return rows;
    }

    // ============================================================
    // 2) PHASOR: série única / ABC por component.
    // Preview usa downsampling no TimescaleDB.
    // ============================================================
    public async Task<IReadOnlyList<PhasorMeasurementRow>> QueryPhasorAsync(
        RunContext ctx,
        MeasurementsQuery q,
        CancellationToken ct,
        int? maxPoints = null)
    {
        using var db = _dbf.Create();

        var pmuFilter = q.PmuNames is { Count: > 0 }
            ? "LOWER(pmus.id_name) = ANY(@pmu_names)"
            : "TRUE";

        var phaseClause = q.PhaseMode switch
        {
            PhaseMode.ABC => "UPPER(s.phase::text) IN ('A','B','C')",
            PhaseMode.Single => "UPPER(s.phase::text) = UPPER(@phase)",
            _ => "TRUE"
        };

        var valueCase = WideSignalColumnMap.BuildValueCaseSql("sig", "mw");
        var isPreview = maxPoints is > 0;
        var useToolkitLttb = isPreview && await HasTimescaleToolkitAsync(db, ct);

        var sql = BuildPhasorQuerySql(
            pmuFilter,
            phaseClause,
            valueCase,
            isPreview,
            useToolkitLttb);

        var args = new
        {
            pdc_id = ctx.PdcId,
            pdc_name = ctx.PdcName,
            from_utc = ctx.FromUtc,
            to_utc = ctx.ToUtc,
            quality = ByRunMeasurementQuality,
            quantity = q.Quantity,
            component = q.Component,
            phase = q.Phase,
            max_points = maxPoints,
            all_run_pmus = ctx.PmuNames.ToArray(),
            pmu_names = (q.PmuNames ?? Array.Empty<string>())
                .Select(x => x.ToLowerInvariant())
                .ToArray()
        };

        _logger.LogInformation(
            "[DATA-REQ][QueryPhasorAsync][START] pdc={Pdc} quantity={Quantity} component={Component} phase={Phase} window=[{From:o}..{To:o}] pmus={Pmus} maxPoints={MaxPoints} downsample={Downsample}",
            ctx.PdcName,
            q.Quantity,
            q.Component,
            q.Phase,
            ctx.FromUtc,
            ctx.ToUtc,
            string.Join(',', q.PmuNames ?? ctx.PmuNames),
            maxPoints,
            isPreview ? (useToolkitLttb ? "lttb" : "time_bucket_first") : "none");

        var stopwatch = Stopwatch.StartNew();

        var rows = (await db.QueryAsync<PhasorMeasurementRow>(
            new CommandDefinition(
                sql,
                args,
                commandTimeout: 30,
                cancellationToken: ct)))
            .ToList();

        stopwatch.Stop();

        _logger.LogInformation(
            "[DATA-REQ][QueryPhasorAsync][END] elapsedMs={ElapsedMs} rows={Rows} pdc_pmu_ids={PdcPmuIds}",
            stopwatch.ElapsedMilliseconds,
            rows.Count,
            string.Join(',', rows.Select(r => r.PdcPmuId).Distinct().OrderBy(x => x)));

        return rows;
    }

    // ============================================================
    // 3) ABC MAG+ANG: SEQ / UNBALANCE / POWER.
    //
    // IMPORTANTE:
    // O downsampling ocorre na LINHA WIDE, antes da expansão em SignalId.
    // Isso evita multiplicar frames por 6 e mantém MAG/ANG/ABC no mesmo
    // conjunto de timestamps.
    //
    // Para preview usa time_bucket + first() do próprio TimescaleDB.
    // LTTB não é usado aqui porque cada coluna poderia escolher timestamps
    // diferentes, quebrando o alinhamento fasorial.
    // ============================================================
    public async Task<IReadOnlyList<PhasorAbcRow>> QueryAbcMagAngAsync(
        RunContext ctx,
        string kind,
        IReadOnlyList<string>? pmuNames,
        DateTime? fromUtc,
        DateTime? toUtc,
        CancellationToken ct,
        int? maxPoints = null)
    {
        using var db = _dbf.Create();

        var k = (kind ?? string.Empty).Trim().ToLowerInvariant();
        if (k is not ("voltage" or "current"))
            throw new ArgumentException("kind deve ser 'voltage' ou 'current'.", nameof(kind));

        var effFrom = fromUtc ?? ctx.FromUtc;
        var effTo = toUtc ?? ctx.ToUtc;

        var pmuFilter = pmuNames is { Count: > 0 }
            ? "LOWER(pmus.id_name) = ANY(@pmu_names)"
            : "TRUE";

        var isPreview = maxPoints is > 0;
        var sql = BuildAbcMagAngQuerySql(pmuFilter, isPreview);

        var args = new
        {
            pdc_id = ctx.PdcId,
            pdc_name = ctx.PdcName,
            from_utc = effFrom,
            to_utc = effTo,
            quality = ByRunMeasurementQuality,
            kind = k,
            max_points = maxPoints,
            all_run_pmus = ctx.PmuNames.ToArray(),
            pmu_names = (pmuNames ?? Array.Empty<string>())
                .Select(x => x.ToLowerInvariant())
                .ToArray()
        };

        _logger.LogInformation(
            "[DATA-REQ][QueryAbcMagAngAsync][START] pdc={Pdc} kind={Kind} window=[{From:o}..{To:o}] pmus={Pmus} maxPoints={MaxPoints} downsample={Downsample}",
            ctx.PdcName,
            k,
            effFrom,
            effTo,
            string.Join(',', pmuNames ?? ctx.PmuNames),
            maxPoints,
            isPreview ? "time_bucket_first_wide" : "none");

        var stopwatch = Stopwatch.StartNew();

        var rows = (await db.QueryAsync<PhasorAbcRow>(
            new CommandDefinition(
                sql,
                args,
                commandTimeout: 300,
                cancellationToken: ct)))
            .ToList();

        stopwatch.Stop();

        _logger.LogInformation(
            "[DATA-REQ][QueryAbcMagAngAsync][END] elapsedMs={ElapsedMs} rows={Rows} pdc_pmu_ids={PdcPmuIds}",
            stopwatch.ElapsedMilliseconds,
            rows.Count,
            string.Join(',', rows.Select(r => r.PdcPmuId).Distinct().OrderBy(x => x)));

        return rows;
    }

    private async Task<bool> HasTimescaleToolkitAsync(
        IDbConnection db,
        CancellationToken ct)
    {
        const string sql = @"
SELECT EXISTS (
    SELECT 1
    FROM pg_extension
    WHERE extname = 'timescaledb_toolkit'
);";

        try
        {
            return await db.ExecuteScalarAsync<bool>(
                new CommandDefinition(
                    sql,
                    commandTimeout: 10,
                    cancellationToken: ct));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Falha ao verificar timescaledb_toolkit; fallback para time_bucket será usado.");
            return false;
        }
    }

    private static string BuildSimpleQuerySql(
        string pmuFilter,
        string phaseClause,
        string valueCase,
        bool isPreview,
        bool useToolkitLttb)
    {
        var downsamplingCte = isPreview
            ? (useToolkitLttb
                ? @",
grouped_rows AS (
  SELECT
    br.SignalId,
    br.PdcPmuId,
    br.IdName,
    br.PdcName,
    lttb(br.Ts, br.Value, @max_points) AS sampled
  FROM base_rows br
  GROUP BY br.SignalId, br.PdcPmuId, br.IdName, br.PdcName
),
downsampled_rows AS (
  SELECT
    g.SignalId,
    g.PdcPmuId,
    g.IdName,
    g.PdcName,
    p.time AS Ts,
    p.value AS Value
  FROM grouped_rows g
  CROSS JOIN LATERAL unnest(g.sampled) AS p(time, value)
)"
                : @",
downsample_cfg AS (
  SELECT
    GREATEST(
      INTERVAL '1 millisecond',
      ((SELECT to_utc FROM ctx) - (SELECT from_utc FROM ctx))
        / COALESCE(NULLIF(@max_points, 0)::double precision, 1.0)
    ) AS bucket_width
),
downsampled_rows AS (
  SELECT
    br.SignalId,
    br.PdcPmuId,
    br.IdName,
    br.PdcName,
    first(br.Ts, br.Ts) AS Ts,
    first(br.Value, br.Ts) AS Value
  FROM base_rows br
  CROSS JOIN downsample_cfg cfg
  GROUP BY
    br.SignalId,
    br.PdcPmuId,
    br.IdName,
    br.PdcName,
    time_bucket(cfg.bucket_width, br.Ts)
)")
            : string.Empty;

        var sourceCte = isPreview ? "downsampled_rows" : "base_rows";

        return $@"
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
    s.name,
    s.quantity,
    s.phase,
    s.component,
    sel.id_name,
    sel.pdc_name
  FROM sel
  JOIN openplot.pdc_pmu pp
    ON pp.pdc_id = sel.pdc_id
   AND pp.pmu_id = sel.pmu_id
  JOIN openplot.signal s
    ON s.pdc_pmu_id = pp.pdc_pmu_id
  WHERE LOWER(s.quantity::text) = LOWER(@quantity)
    AND LOWER(s.component::text) = LOWER(@component)
    AND {phaseClause}
),
base_rows AS (
  SELECT
    sig.signal_id  AS SignalId,
    sig.pdc_pmu_id AS PdcPmuId,
    sig.id_name    AS IdName,
    sig.pdc_name   AS PdcName,
    mw.ts          AS Ts,
    {valueCase}    AS Value
  FROM sig
  JOIN openplot.measurements_wide mw
    ON mw.pdc_pmu_id = sig.pdc_pmu_id
  WHERE mw.ts >= (SELECT from_utc FROM ctx)
    AND mw.ts <= (SELECT to_utc FROM ctx)
    AND (mw.quality = @quality OR mw.quality IS NULL)
    AND ({valueCase}) IS NOT NULL
)
{downsamplingCte}
SELECT
  SignalId,
  PdcPmuId,
  IdName,
  PdcName,
  Ts,
  Value
FROM {sourceCte}
ORDER BY SignalId, Ts;
";
    }

    private static string BuildPhasorQuerySql(
        string pmuFilter,
        string phaseClause,
        string valueCase,
        bool isPreview,
        bool useToolkitLttb)
    {
        var downsamplingCte = isPreview
            ? (useToolkitLttb
                ? @",
grouped_rows AS (
  SELECT
    br.SignalId,
    br.PdcPmuId,
    br.IdName,
    br.PdcName,
    br.Phase,
    br.Component,
    br.VoltLevel,
    lttb(br.Ts, br.Value, @max_points) AS sampled
  FROM base_rows br
  GROUP BY
    br.SignalId,
    br.PdcPmuId,
    br.IdName,
    br.PdcName,
    br.Phase,
    br.Component,
    br.VoltLevel
),
downsampled_rows AS (
  SELECT
    g.SignalId,
    g.PdcPmuId,
    g.IdName,
    g.PdcName,
    g.Phase,
    g.Component,
    g.VoltLevel,
    p.time AS Ts,
    p.value AS Value
  FROM grouped_rows g
  CROSS JOIN LATERAL unnest(g.sampled) AS p(time, value)
)"
                : @",
downsample_cfg AS (
  SELECT
    GREATEST(
      INTERVAL '1 millisecond',
      ((SELECT to_utc FROM ctx) - (SELECT from_utc FROM ctx))
        / COALESCE(NULLIF(@max_points, 0)::double precision, 1.0)
    ) AS bucket_width
),
downsampled_rows AS (
  SELECT
    br.SignalId,
    br.PdcPmuId,
    br.IdName,
    br.PdcName,
    br.Phase,
    br.Component,
    br.VoltLevel,
    first(br.Ts, br.Ts) AS Ts,
    first(br.Value, br.Ts) AS Value
  FROM base_rows br
  CROSS JOIN downsample_cfg cfg
  GROUP BY
    br.SignalId,
    br.PdcPmuId,
    br.IdName,
    br.PdcName,
    br.Phase,
    br.Component,
    br.VoltLevel,
    time_bucket(cfg.bucket_width, br.Ts)
)")
            : string.Empty;

        var sourceCte = isPreview ? "downsampled_rows" : "base_rows";

        return $@"
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
  SELECT
    pmus.id_name,
    pmus.pmu_id,
    pmus.volt_level,
    c.pdc_name,
    c.pdc_id
  FROM pmus
  CROSS JOIN ctx c
  WHERE {pmuFilter}
),
sig AS (
  SELECT
    s.signal_id,
    s.pdc_pmu_id,
    s.name,
    s.quantity,
    s.phase,
    s.component,
    sel.id_name,
    sel.pdc_name,
    UPPER(s.phase::text) AS phase_upper,
    UPPER(s.component::text) AS component_upper,
    sel.volt_level AS volt_level
  FROM sel
  JOIN openplot.pdc_pmu pp
    ON pp.pdc_id = sel.pdc_id
   AND pp.pmu_id = sel.pmu_id
  JOIN openplot.signal s
    ON s.pdc_pmu_id = pp.pdc_pmu_id
  WHERE LOWER(s.quantity::text) = LOWER(@quantity)
    AND LOWER(s.component::text) = LOWER(@component)
    AND {phaseClause}
),
base_rows AS (
  SELECT
    sig.signal_id       AS SignalId,
    sig.pdc_pmu_id      AS PdcPmuId,
    sig.id_name         AS IdName,
    sig.pdc_name        AS PdcName,
    sig.phase_upper     AS Phase,
    sig.component_upper AS Component,
    sig.volt_level      AS VoltLevel,
    mw.ts               AS Ts,
    {valueCase}         AS Value
  FROM sig
  JOIN openplot.measurements_wide mw
    ON mw.pdc_pmu_id = sig.pdc_pmu_id
  WHERE mw.ts >= (SELECT from_utc FROM ctx)
    AND mw.ts <= (SELECT to_utc FROM ctx)
    AND (mw.quality = @quality OR mw.quality IS NULL)
    AND ({valueCase}) IS NOT NULL
)
{downsamplingCte}
SELECT
  SignalId,
  PdcPmuId,
  IdName,
  PdcName,
  Phase,
  Component,
  VoltLevel,
  Ts,
  Value
FROM {sourceCte}
ORDER BY SignalId, Ts;
";
    }

    private static string BuildAbcMagAngQuerySql(
        string pmuFilter,
        bool isPreview)
    {
        // Este CASE é intencionalmente específico para ABC MAG/ANG.
        // Não usa o CASE genérico do WideSignalColumnMap porque o alias ds
        // contém somente as colunas fasoriais necessárias. Isso elimina o
        // erro "ds.cthd_a_pct não existe" e evita agregar colunas irrelevantes.
        const string abcValueCase = @"
CASE
  WHEN @kind = 'voltage' AND sig.phase_upper = 'A' AND sig.component_upper = 'MAG' THEN ds.va_mod_v
  WHEN @kind = 'voltage' AND sig.phase_upper = 'A' AND sig.component_upper = 'ANG' THEN ds.va_ang_deg
  WHEN @kind = 'voltage' AND sig.phase_upper = 'B' AND sig.component_upper = 'MAG' THEN ds.vb_mod_v
  WHEN @kind = 'voltage' AND sig.phase_upper = 'B' AND sig.component_upper = 'ANG' THEN ds.vb_ang_deg
  WHEN @kind = 'voltage' AND sig.phase_upper = 'C' AND sig.component_upper = 'MAG' THEN ds.vc_mod_v
  WHEN @kind = 'voltage' AND sig.phase_upper = 'C' AND sig.component_upper = 'ANG' THEN ds.vc_ang_deg

  WHEN @kind = 'current' AND sig.phase_upper = 'A' AND sig.component_upper = 'MAG' THEN ds.ia_mod_a
  WHEN @kind = 'current' AND sig.phase_upper = 'A' AND sig.component_upper = 'ANG' THEN ds.ia_ang_deg
  WHEN @kind = 'current' AND sig.phase_upper = 'B' AND sig.component_upper = 'MAG' THEN ds.ib_mod_a
  WHEN @kind = 'current' AND sig.phase_upper = 'B' AND sig.component_upper = 'ANG' THEN ds.ib_ang_deg
  WHEN @kind = 'current' AND sig.phase_upper = 'C' AND sig.component_upper = 'MAG' THEN ds.ic_mod_a
  WHEN @kind = 'current' AND sig.phase_upper = 'C' AND sig.component_upper = 'ANG' THEN ds.ic_ang_deg
END";

        var downsamplingCte = isPreview
            ? @",
downsample_cfg AS (
  SELECT
    GREATEST(
      INTERVAL '1 millisecond',
      ((SELECT to_utc FROM ctx) - (SELECT from_utc FROM ctx))
        / COALESCE(NULLIF(@max_points, 0)::double precision, 1.0)
    ) AS bucket_width
),
downsampled_wide AS (
  SELECT
    fw.pdc_pmu_id,
    fw.id_name,
    fw.pdc_name,
    fw.volt_level,

    first(fw.ts, fw.ts) AS ts,

    first(fw.va_mod_v, fw.ts) AS va_mod_v,
    first(fw.va_ang_deg, fw.ts) AS va_ang_deg,
    first(fw.vb_mod_v, fw.ts) AS vb_mod_v,
    first(fw.vb_ang_deg, fw.ts) AS vb_ang_deg,
    first(fw.vc_mod_v, fw.ts) AS vc_mod_v,
    first(fw.vc_ang_deg, fw.ts) AS vc_ang_deg,

    first(fw.ia_mod_a, fw.ts) AS ia_mod_a,
    first(fw.ia_ang_deg, fw.ts) AS ia_ang_deg,
    first(fw.ib_mod_a, fw.ts) AS ib_mod_a,
    first(fw.ib_ang_deg, fw.ts) AS ib_ang_deg,
    first(fw.ic_mod_a, fw.ts) AS ic_mod_a,
    first(fw.ic_ang_deg, fw.ts) AS ic_ang_deg

  FROM filtered_wide fw
  CROSS JOIN downsample_cfg cfg
  GROUP BY
    fw.pdc_pmu_id,
    fw.id_name,
    fw.pdc_name,
    fw.volt_level,
    time_bucket(cfg.bucket_width, fw.ts)
)"
            : @",
downsampled_wide AS (
  SELECT
    fw.pdc_pmu_id,
    fw.id_name,
    fw.pdc_name,
    fw.volt_level,
    fw.ts,

    fw.va_mod_v,
    fw.va_ang_deg,
    fw.vb_mod_v,
    fw.vb_ang_deg,
    fw.vc_mod_v,
    fw.vc_ang_deg,

    fw.ia_mod_a,
    fw.ia_ang_deg,
    fw.ib_mod_a,
    fw.ib_ang_deg,
    fw.ic_mod_a,
    fw.ic_ang_deg

  FROM filtered_wide fw
)";

        return $@"
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
  SELECT
    pmus.id_name,
    pmus.pmu_id,
    pmus.volt_level,
    c.pdc_name,
    c.pdc_id
  FROM pmus
  CROSS JOIN ctx c
  WHERE {pmuFilter}
),

-- Uma linha por PMU/PDC. NÃO junta signal aqui.
-- A versão anterior multiplicava cada frame Wide pelo número de sinais.
target_pdc_pmu AS (
  SELECT
    pp.pdc_pmu_id,
    sel.id_name,
    sel.pdc_name,
    sel.volt_level::double precision AS volt_level
  FROM sel
  JOIN openplot.pdc_pmu pp
    ON pp.pdc_id = sel.pdc_id
   AND pp.pmu_id = sel.pmu_id
),

-- Filtra a hypertable primeiro usando a PK (pdc_pmu_id, ts).
filtered_wide AS (
  SELECT
    mw.pdc_pmu_id,
    tpp.id_name,
    tpp.pdc_name,
    tpp.volt_level,
    mw.ts,

    mw.va_mod_v,
    mw.va_ang_deg,
    mw.vb_mod_v,
    mw.vb_ang_deg,
    mw.vc_mod_v,
    mw.vc_ang_deg,

    mw.ia_mod_a,
    mw.ia_ang_deg,
    mw.ib_mod_a,
    mw.ib_ang_deg,
    mw.ic_mod_a,
    mw.ic_ang_deg

  FROM target_pdc_pmu tpp
  JOIN openplot.measurements_wide mw
    ON mw.pdc_pmu_id = tpp.pdc_pmu_id
   AND mw.ts >= (SELECT from_utc FROM ctx)
   AND mw.ts <= (SELECT to_utc FROM ctx)
  WHERE mw.quality = @quality
     OR mw.quality IS NULL
)
{downsamplingCte},

-- Só depois do downsampling recupera os SignalIds lógicos.
sig AS (
  SELECT
    s.signal_id,
    s.pdc_pmu_id,
    tpp.id_name,
    tpp.pdc_name,
    UPPER(s.phase::text) AS phase_upper,
    UPPER(s.component::text) AS component_upper,
    tpp.volt_level AS volt_level
  FROM target_pdc_pmu tpp
  JOIN openplot.signal s
    ON s.pdc_pmu_id = tpp.pdc_pmu_id
  WHERE (
          @kind = 'voltage'
          AND LOWER(s.quantity::text) IN ('voltage','v')
        )
     OR (
          @kind = 'current'
          AND LOWER(s.quantity::text) IN ('current','i')
        )
),
abc_sig AS (
  SELECT *
  FROM sig
  WHERE phase_upper IN ('A','B','C')
    AND component_upper IN ('MAG','ANG')
),
base_rows AS (
  SELECT
    sig.signal_id       AS SignalId,
    sig.pdc_pmu_id      AS PdcPmuId,
    sig.id_name         AS IdName,
    sig.pdc_name        AS PdcName,
    sig.phase_upper     AS Phase,
    sig.component_upper AS Component,
    sig.volt_level      AS VoltLevel,
    ds.ts               AS Ts,
    {abcValueCase}      AS Value
  FROM abc_sig sig
  JOIN downsampled_wide ds
    ON ds.pdc_pmu_id = sig.pdc_pmu_id
)
SELECT
  SignalId,
  PdcPmuId,
  IdName,
  PdcName,
  Phase,
  Component,
  VoltLevel,
  Ts,
  Value
FROM base_rows
WHERE Value IS NOT NULL
ORDER BY IdName, SignalId, Ts;
";
    }

    public async Task WarmUpAsync(
        RunContext ctx,
        CancellationToken ct)
    {
        if (ctx.PmuNames.Count == 0)
            return;

        using var db = _dbf.Create();

        await db.ExecuteScalarAsync<long>(
            new CommandDefinition(
                WarmUpSimpleSql,
                BuildWarmUpArgs(ctx, quantity: "frequency", component: "freq"),
                commandTimeout: 300,
                cancellationToken: ct));

        await db.ExecuteScalarAsync<long>(
            new CommandDefinition(
                WarmUpSimpleSql,
                BuildWarmUpArgs(ctx, quantity: "frequency", component: "dfreq"),
                commandTimeout: 300,
                cancellationToken: ct));

        await db.ExecuteScalarAsync<long>(
            new CommandDefinition(
                WarmUpSimpleSql,
                BuildWarmUpArgs(ctx, quantity: "digital", component: "dig"),
                commandTimeout: 300,
                cancellationToken: ct));

        await db.ExecuteScalarAsync<long>(
            new CommandDefinition(
                WarmUpPhasorSql,
                BuildWarmUpArgs(ctx, quantity: "voltage", component: "thd"),
                commandTimeout: 300,
                cancellationToken: ct));

        await db.ExecuteScalarAsync<long>(
            new CommandDefinition(
                WarmUpPhasorSql,
                BuildWarmUpArgs(ctx, quantity: "current", component: "thd"),
                commandTimeout: 300,
                cancellationToken: ct));

        await db.ExecuteScalarAsync<long>(
            new CommandDefinition(
                WarmUpAbcMagAngSql,
                BuildWarmUpArgs(ctx, kind: "voltage"),
                commandTimeout: 300,
                cancellationToken: ct));

        await db.ExecuteScalarAsync<long>(
            new CommandDefinition(
                WarmUpAbcMagAngSql,
                BuildWarmUpArgs(ctx, kind: "current"),
                commandTimeout: 300,
                cancellationToken: ct));
    }

    private static string BuildWarmUpSimpleSql()
    {
        return @"
WITH ctx AS (
  SELECT
    @pdc_id::int AS pdc_id,
    @from_utc::timestamptz AS from_utc,
    @to_utc::timestamptz AS to_utc
),
pmus AS (
  SELECT pmu_id
  FROM openplot.pmu
  WHERE id_name = ANY(@all_run_pmus)
),
target_pdc_pmu AS (
  SELECT pp.pdc_pmu_id
  FROM pmus
  CROSS JOIN ctx c
  JOIN openplot.pdc_pmu pp
    ON pp.pdc_id = c.pdc_id
   AND pp.pmu_id = pmus.pmu_id
)
SELECT COUNT(*)
FROM target_pdc_pmu t
JOIN openplot.measurements_wide mw
  ON mw.pdc_pmu_id = t.pdc_pmu_id
 AND mw.ts >= (SELECT from_utc FROM ctx)
 AND mw.ts <= (SELECT to_utc FROM ctx)
WHERE (mw.quality = @quality OR mw.quality IS NULL)
  AND (
       (LOWER(@quantity) = 'frequency' AND LOWER(@component) = 'freq'  AND mw.frequency_hz IS NOT NULL)
    OR (LOWER(@quantity) = 'frequency' AND LOWER(@component) = 'dfreq' AND mw.delta_freq_hz IS NOT NULL)
    OR (LOWER(@quantity) = 'digital'   AND LOWER(@component) = 'dig'   AND mw.cfds IS NOT NULL)
  );";
    }

    private static string BuildWarmUpPhasorSql()
    {
        return @"
WITH ctx AS (
  SELECT
    @pdc_id::int AS pdc_id,
    @from_utc::timestamptz AS from_utc,
    @to_utc::timestamptz AS to_utc
),
pmus AS (
  SELECT pmu_id
  FROM openplot.pmu
  WHERE id_name = ANY(@all_run_pmus)
),
target_pdc_pmu AS (
  SELECT pp.pdc_pmu_id
  FROM pmus
  CROSS JOIN ctx c
  JOIN openplot.pdc_pmu pp
    ON pp.pdc_id = c.pdc_id
   AND pp.pmu_id = pmus.pmu_id
)
SELECT COUNT(*)
FROM target_pdc_pmu t
JOIN openplot.measurements_wide mw
  ON mw.pdc_pmu_id = t.pdc_pmu_id
 AND mw.ts >= (SELECT from_utc FROM ctx)
 AND mw.ts <= (SELECT to_utc FROM ctx)
WHERE (mw.quality = @quality OR mw.quality IS NULL)
  AND LOWER(@component) = 'thd'
  AND (
       (LOWER(@quantity) IN ('voltage','v')
        AND (mw.vthd_a_pct IS NOT NULL OR mw.vthd_b_pct IS NOT NULL OR mw.vthd_c_pct IS NOT NULL))
    OR (LOWER(@quantity) IN ('current','i')
        AND (mw.cthd_a_pct IS NOT NULL OR mw.cthd_b_pct IS NOT NULL OR mw.cthd_c_pct IS NOT NULL))
  );";
    }

    private static string BuildWarmUpAbcMagAngSql()
    {
        return @"
WITH ctx AS (
  SELECT
    @pdc_id::int AS pdc_id,
    @from_utc::timestamptz AS from_utc,
    @to_utc::timestamptz AS to_utc
),
pmus AS (
  SELECT pmu_id
  FROM openplot.pmu
  WHERE id_name = ANY(@all_run_pmus)
),
target_pdc_pmu AS (
  SELECT pp.pdc_pmu_id
  FROM pmus
  CROSS JOIN ctx c
  JOIN openplot.pdc_pmu pp
    ON pp.pdc_id = c.pdc_id
   AND pp.pmu_id = pmus.pmu_id
)
SELECT COUNT(*)
FROM target_pdc_pmu t
JOIN openplot.measurements_wide mw
  ON mw.pdc_pmu_id = t.pdc_pmu_id
 AND mw.ts >= (SELECT from_utc FROM ctx)
 AND mw.ts <= (SELECT to_utc FROM ctx)
WHERE (mw.quality = @quality OR mw.quality IS NULL)
  AND (
       (@kind = 'voltage' AND (
            mw.va_mod_v IS NOT NULL OR mw.va_ang_deg IS NOT NULL OR
            mw.vb_mod_v IS NOT NULL OR mw.vb_ang_deg IS NOT NULL OR
            mw.vc_mod_v IS NOT NULL OR mw.vc_ang_deg IS NOT NULL))
    OR (@kind = 'current' AND (
            mw.ia_mod_a IS NOT NULL OR mw.ia_ang_deg IS NOT NULL OR
            mw.ib_mod_a IS NOT NULL OR mw.ib_ang_deg IS NOT NULL OR
            mw.ic_mod_a IS NOT NULL OR mw.ic_ang_deg IS NOT NULL))
  );";
    }

    private static object BuildWarmUpArgs(
        RunContext ctx,
        string? quantity = null,
        string? component = null,
        string? kind = null) => new
        {
            pdc_id = ctx.PdcId,
            from_utc = ctx.FromUtc,
            to_utc = ctx.ToUtc,
            quality = ByRunMeasurementQuality,
            all_run_pmus = ctx.PmuNames.ToArray(),
            quantity,
            component,
            kind
        };
}