using System.Data;
using System.Globalization;
using System.Numerics;
using Dapper;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Abstractions;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.Features.Runs.Handlers;

/// <summary>
/// Query parameters for angle difference series handler.
/// </summary>
public sealed class AngleDiffQuery : ISeriesQuery
{
    public Guid RunId { get; init; }
    public string? MaxPoints { get; init; }
    public string? Kind { get; init; } // voltage|current
    public string? Reference { get; init; } // PMU reference name
    public string? Phase { get; init; } // A|B|C
    public string? Sequence { get; init; } // pos|neg|zero

    public bool MaxPointsIsAll =>
        string.Equals(MaxPoints?.Trim(), "all", StringComparison.OrdinalIgnoreCase);

    public int ResolveMaxPoints(int @default = 5000)
    {
        if (MaxPointsIsAll) return int.MaxValue;
        if (string.IsNullOrWhiteSpace(MaxPoints)) return @default;
        return int.TryParse(MaxPoints, out var n) && n > 0 ? n : @default;
    }
}

/// <summary>
/// Handler for calculating phase angle differences between reference and measurement PMUs.
/// Supports both phase-based (A|B|C) and sequence-based (pos|neg|zero) calculations.
/// 
/// Architecture:
/// - Validates input parameters (kind, reference, phase XOR sequence)
/// - Executes complex SQL query with PMU/signal resolution
/// - Calculates sequence angles using Complex number math
/// - Computes angle differences with time-series alignment
/// - Applies min/max downsampling for visualization
/// </summary>
public sealed class AngleDiffSeriesHandler
{
    private readonly IDbConnectionFactory _dbFactory;
    private readonly IRunContextRepository _runRepository;
    private readonly IAnalysisCacheRepository _cacheRepo;
    private readonly ITimeSeriesDownsampler _downsampler;
    private readonly IPmuQueryHelper _pmuHelper;
    private readonly ISeriesAssemblyService _seriesAssembly;

    public AngleDiffSeriesHandler(
        IRunContextRepository runRepository,
        IAnalysisCacheRepository cacheRepo,
        IDbConnectionFactory dbFactory,
        ITimeSeriesDownsampler downsampler,
        IPmuQueryHelper pmuHelper,
        ISeriesAssemblyService seriesAssembly)
    {
        _runRepository = runRepository ?? throw new ArgumentNullException(nameof(runRepository));
        _cacheRepo = cacheRepo ?? throw new ArgumentNullException(nameof(cacheRepo));
        _dbFactory = dbFactory ?? throw new ArgumentNullException(nameof(dbFactory));
        _downsampler = downsampler ?? throw new ArgumentNullException(nameof(downsampler));
        _pmuHelper = pmuHelper ?? throw new ArgumentNullException(nameof(pmuHelper));
        _seriesAssembly = seriesAssembly ?? throw new ArgumentNullException(nameof(seriesAssembly));
    }

    /// <summary>
    /// Main handler method for angle difference series calculation.
    /// </summary>
    public async Task<IResult> HandleAsync(
        AngleDiffQuery query,
        WindowQuery window,
        string[]? pmuArray,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        // Validate input
        var validation = ValidateInput(query);
        if (!validation.isValid)
            return Results.BadRequest(validation.errorMessage);

        try
        {
            var kind = query.Kind!.Trim().ToLowerInvariant();
            var refPmu = query.Reference!.Trim();
            var hasPhase = !string.IsNullOrWhiteSpace(query.Phase);
            var hasSeq = !string.IsNullOrWhiteSpace(query.Sequence);

            // Process PMU list
            var pmuList = _pmuHelper.NormalizeExcluding(refPmu, pmuArray).ToList();

            var maxPts = query.ResolveMaxPoints(@default: 5000);
            var fromUtc = window.FromUtc;
            var toUtc = window.ToUtc;

            var ctx = await _runRepository.ResolveAsync(query.RunId, fromUtc, toUtc, ct);
            if (ctx is null)
                return Results.NotFound("run_id não encontrado.");

            // Query data
            var rows = await QueryDataAsync(query, window, pmuList, ct);
            if (rows.Count == 0)
                return Results.NotFound("Nenhuma série encontrada para este run/filtros.");

            // Separate reference and measurement data
            var refRows = rows
                .Where(r => (r.IdName ?? "").Equals(refPmu, StringComparison.OrdinalIgnoreCase))
                .ToList();

            if (refRows.Count == 0)
                return Results.BadRequest("PMU de referência não encontrada dentro do run/filtros.");

            // Calculate reference angle series
            var refAngSeries = hasPhase
                ? ExtractPhaseSeries(refRows)
                : CalculateSequenceSeries(refRows, query.Sequence!);

            if (refAngSeries.Count == 0)
                return Results.BadRequest("Não foi possível calcular série de referência (ângulo).");

            // Process target PMUs
            IEnumerable<IGrouping<string, (string IdName, string PdcName, string Phase, string Component, DateTime Ts, double Value)>> targetGroups;

            if (pmuList.Count > 0)
            {
                targetGroups = rows
                    .Where(r => pmuList.Contains(r.IdName ?? "", StringComparer.OrdinalIgnoreCase))
                    .GroupBy(r => r.IdName!);
            }
            else
            {
                targetGroups = rows
                    .Where(r => !(r.IdName ?? "").Equals(refPmu, StringComparison.OrdinalIgnoreCase))
                    .GroupBy(r => r.IdName!);
            }

            var series = new List<object>();
            var cachePoints = new List<(string pmuId, DateTime ts, double value)>();
            var tol = TimeSpan.FromMilliseconds(3);

            foreach (var g in targetGroups)
            {
                var sigRows = g.ToList();
                if (sigRows.Count == 0) continue;

                var first = sigRows.First();
                var pmuName = g.Key;
                var pdcName = rows.First(r => r.IdName == pmuName).PdcName;

                var measAngSeries = hasPhase
                    ? ExtractPhaseSeries(sigRows.Select(s => (s.IdName, s.PdcName, s.Phase, s.Component, s.Ts, s.Value)))
                    : CalculateSequenceSeries(sigRows.Select(s => (s.IdName, s.PdcName, s.Phase, s.Component, s.Ts, s.Value)), query.Sequence!);

                if (measAngSeries.Count == 0) continue;

                var dif = ComputeAngleDifference(measAngSeries, refAngSeries, tol);
                if (dif.Count == 0) continue;

                foreach (var p in dif)
                    cachePoints.Add((pmuName, p.ts, p.difDeg));

                var points = _seriesAssembly.BuildPoints(
                    dif.Select(x => (x.ts, x.difDeg)),
                    noDownsample: query.MaxPointsIsAll,
                    maxPoints: maxPts,
                    downsampler: _downsampler);

                series.Add(new
                {
                    pmu = pmuName,
                    pdc = pdcName,
                    reference = refPmu,
                    kind = kind,
                    mode = hasPhase ? "phase" : "sequence",
                    phase = hasPhase ? query.Phase!.ToUpperInvariant() : null,
                    seq = hasSeq ? NormalizeSeq(query.Sequence!) : null,
                    unit = "deg",
                    points
                });
            }

            if (series.Count == 0)
                return Results.BadRequest("Nenhuma PMU pôde ser processada (faltam sinais ou alinhamento falhou).");

            var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
            var windowTo = toUtc ?? rows.Max(r => r.Ts);
            var dataStr = windowFrom.Date.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

            var modeLabel = hasPhase ? "phase" : "sequence";
            var componentLabel = hasPhase ? "angle_diff_phase" : "angle_diff_sequence";

            var cacheSeries = cachePoints
                .GroupBy(x => x.pmuId)
                .Select(g => _seriesAssembly.BuildCacheSeries(
                    signalId: 0,
                    pdcPmuId: 0,
                    idName: g.Key,
                    pdcName: ctx.PdcName,
                    unit: "deg",
                    phase: hasPhase ? query.Phase?.ToUpperInvariant() : NormalizeSeq(query.Sequence!),
                    quantity: kind,
                    component: componentLabel,
                    points: g.Select(x => (x.ts, x.value))))
                .ToList();

            var cachePayload = _seriesAssembly.BuildCachePayload(
                windowFrom,
                windowTo,
                (int)ctx.SelectRate,
                cacheSeries);

            var cacheId = await _cacheRepo.SaveAsync(query.RunId, cachePayload, ct);

            return Results.Ok(new
            {
                run_id = query.RunId,
                data = dataStr,
                kind = kind,
                reference = refPmu,
                mode = modeLabel,
                phase = hasPhase ? query.Phase!.ToUpperInvariant() : null,
                seq = hasSeq ? NormalizeSeq(query.Sequence!) : null,
                unit = "deg",
                cache_id = cacheId.ToString(),
                pmu_count = series.Count,
                window = new { from = windowFrom, to = windowTo },
                modes = modes,
                series
            });
        }
        catch (OperationCanceledException)
        {
            return Results.StatusCode(StatusCodes.Status408RequestTimeout);
        }
        catch (Exception ex)
        {
            return Results.StatusCode(StatusCodes.Status500InternalServerError);
        }
    }

    /// <summary>
    /// Validates input parameters for angle difference calculation.
    /// </summary>
    private (bool isValid, string? errorMessage) ValidateInput(AngleDiffQuery query)
    {
        if (query.RunId == Guid.Empty)
            return (false, "run_id é obrigatório.");

        if (string.IsNullOrWhiteSpace(query.Kind))
            return (false, "kind é obrigatório (voltage|current).");

        var kind = query.Kind.Trim().ToLowerInvariant();
        if (kind is not ("voltage" or "current"))
            return (false, "kind deve ser 'voltage' ou 'current'.");

        if (string.IsNullOrWhiteSpace(query.Reference))
            return (false, "ref é obrigatório (id_name da PMU referência).");

        var hasPhase = !string.IsNullOrWhiteSpace(query.Phase);
        var hasSeq = !string.IsNullOrWhiteSpace(query.Sequence);

        if (hasPhase == hasSeq)
            return (false, "informe exatamente um dos parâmetros: phase (A|B|C) OU seq (pos|neg|zero).");

        if (hasPhase)
        {
            var phase = query.Phase!.Trim().ToUpperInvariant();
            if (phase is not ("A" or "B" or "C"))
                return (false, "phase deve ser A, B ou C.");
        }
        else
        {
            var seq = query.Sequence!.Trim().ToLowerInvariant();
            var normalized = NormalizeSeq(seq);
            if (normalized == "")
                return (false, "seq inválida. Use pos|neg|zero (ou seq+|seq-|seq0).");
        }

        return (true, null);
    }

    /// <summary>
    /// Executes SQL query to fetch signal data from database.
    /// </summary>
    private async Task<List<(string IdName, string PdcName, string Phase, string Component, DateTime Ts, double Value)>> 
        QueryDataAsync(AngleDiffQuery query, WindowQuery window, IReadOnlyList<string> pmuList, CancellationToken ct)
    {
        var kind = query.Kind!.Trim().ToLowerInvariant();
        var hasSeq = !string.IsNullOrWhiteSpace(query.Sequence);

        var componentClause = hasSeq
            ? "UPPER(s.component::text) IN ('MAG','ANG')"
            : "UPPER(s.component::text) IN ('ANG')";

        var phaseClause = hasSeq
            ? "UPPER(s.phase::text) IN ('A','B','C')"
            : "UPPER(s.phase::text) = UPPER(@phase)";

        var pmuFilter = _pmuHelper.BuildOrSqlFilter("c.id_name", pmuList);

        const string sqlTemplate = @"
WITH run AS (
  SELECT id, source AS pdc_name, from_ts, to_ts, COALESCE(pmus_ok, pmus) AS pmus, signals
  FROM openplot.search_runs
  WHERE id = @run_id::uuid
),
run_window AS (
  SELECT
    CASE WHEN pg_typeof(r.from_ts)::text = 'timestamp without time zone'
         THEN r.from_ts::timestamptz ELSE r.from_ts END AS from_utc,
    CASE WHEN pg_typeof(r.to_ts)::text = 'timestamp without time zone'
         THEN r.to_ts::timestamptz ELSE r.to_ts   END AS to_utc,
    r.pdc_name, r.signals, r.pmus
  FROM run r
),
win AS (
  SELECT
    COALESCE(@from_utc, rw.from_utc) AS from_utc,
    COALESCE(@to_utc,   rw.to_utc)   AS to_utc,
    rw.pdc_name, rw.signals, rw.pmus
  FROM run_window rw
),
src AS (
  SELECT w.pdc_name,
         w.from_utc AS from_ts,
         w.to_utc   AS to_ts,
         CASE
           WHEN jsonb_typeof(w.signals) = 'array' AND jsonb_array_length(w.signals) > 0 THEN w.signals
           WHEN jsonb_typeof(w.pmus)    = 'array' AND jsonb_array_length(w.pmus)    > 0 THEN w.pmus
           ELSE '[]'::jsonb
         END AS arr
  FROM win w
),
elems AS (
  SELECT pdc_name, from_ts, to_ts, jsonb_array_elements(arr) AS elem
  FROM src
),
pmu_ids AS (
  SELECT r.pdc_name, r.from_ts, r.to_ts, p.pmu_id, p.id_name
  FROM elems r
  JOIN openplot.pmu p ON p.id_name = btrim(r.elem::text, '""')
  WHERE jsonb_typeof(r.elem) = 'string'
  UNION ALL
  SELECT r.pdc_name, r.from_ts, r.to_ts, p.pmu_id, p.id_name
  FROM elems r
  JOIN LATERAL (
    SELECT NULLIF(TRIM(r.elem->>'pmu'), '')     AS key_pmu,
           NULLIF(TRIM(r.elem->>'id_name'), '') AS key_idname
  ) k ON TRUE
  JOIN openplot.pmu p ON p.id_name = COALESCE(k.key_pmu, k.key_idname)
  WHERE jsonb_typeof(r.elem) = 'object'
    AND COALESCE(k.key_pmu, k.key_idname) IS NOT NULL
  UNION ALL
  SELECT r.pdc_name, r.from_ts, r.to_ts, p.pmu_id, p.id_name
  FROM elems r
  JOIN LATERAL (SELECT NULLIF(r.elem->>'pdc_pmu_id','')::int AS key_pdc_pmu_id) k ON TRUE
  JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id = k.key_pdc_pmu_id
  JOIN openplot.pmu p ON p.pmu_id = ppm.pmu_id
  WHERE jsonb_typeof(r.elem) = 'object'
  UNION ALL
  SELECT r.pdc_name, r.from_ts, r.to_ts, p.pmu_id, p.id_name
  FROM elems r
  JOIN LATERAL (SELECT NULLIF(r.elem->>'signal_id','')::int AS key_signal_id) k ON TRUE
  JOIN openplot.signal s ON s.signal_id = k.key_signal_id
  JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id = s.pdc_pmu_id
  JOIN openplot.pmu p ON p.pmu_id = ppm.pmu_id
  WHERE jsonb_typeof(r.elem) = 'object'
),
pdc_ctx AS (
  SELECT w.pdc_name, w.from_ts, w.to_ts, pdc.pdc_id
  FROM src w
  JOIN openplot.pdc pdc ON LOWER(pdc.name) = LOWER(w.pdc_name)
),
ctx AS (
  SELECT pc.pdc_name, pc.from_ts, pc.to_ts, pid.id_name, pid.pmu_id, pc.pdc_id
  FROM pdc_ctx pc
  JOIN pmu_ids pid ON pid.pdc_name = pc.pdc_name
),
sig AS (
  SELECT s.signal_id, s.pdc_pmu_id, s.phase, s.component,
         c.id_name, c.pdc_name
  FROM ctx c
  JOIN openplot.pdc_pmu pp ON pp.pdc_id = c.pdc_id AND pp.pmu_id = c.pmu_id
  JOIN openplot.signal s   ON s.pdc_pmu_id = pp.pdc_pmu_id
  JOIN openplot.pmu pmu    ON pmu.pmu_id   = c.pmu_id
  WHERE (
    (@kind = 'voltage' AND LOWER(s.quantity::text) IN ('voltage','v'))
    OR
    (@kind = 'current' AND LOWER(s.quantity::text) IN ('current','i'))
  )
  AND {PHASE_CLAUSE}
  AND {COMPONENT_CLAUSE}
  AND ({PMU_FILTER})
),
raw AS (
  SELECT m.signal_id, m.ts, m.value
  FROM openplot.measurements m
  WHERE m.ts >= (SELECT from_utc FROM win)
    AND m.ts <= (SELECT to_utc   FROM win)
)
SELECT
  s.signal_id, s.pdc_pmu_id, s.phase, s.component,
  s.id_name, s.pdc_name,
  r.ts, r.value
FROM sig s
JOIN raw r USING (signal_id)
ORDER BY s.id_name, s.signal_id, r.ts;
";

        var sql = sqlTemplate
            .Replace("{PHASE_CLAUSE}", phaseClause)
            .Replace("{COMPONENT_CLAUSE}", componentClause)
            .Replace("{PMU_FILTER}", pmuFilter);

        var dyn = new DynamicParameters();
        dyn.Add("run_id", query.RunId);
        dyn.Add("kind", kind);
        dyn.Add("from_utc", window.FromUtc);
        dyn.Add("to_utc", window.ToUtc);
        dyn.Add("phase", query.Phase?.ToUpperInvariant());

        _pmuHelper.AddSqlParameters(dyn, pmuList);

        using var db = _dbFactory.Create();
        var dynamicRows = await db.QueryAsync(sql, dyn, commandTimeout: 300);

        var result = new List<(string IdName, string PdcName, string Phase, string Component, DateTime Ts, double Value)>();
        foreach (dynamic row in dynamicRows)
        {
            result.Add((
                IdName: (string)row.id_name,
                PdcName: (string)row.pdc_name,
                Phase: (string)(row.phase ?? ""),
                Component: (string)row.component,
                Ts: (DateTime)row.ts,
                Value: (double)row.value
            ));
        }

        return result;
    }

    /// <summary>
    /// Extracts phase angle series from measurement rows (mode: phase A|B|C).
    /// </summary>
    private static List<(DateTime ts, double angDeg)> ExtractPhaseSeries(
        IEnumerable<(string IdName, string PdcName, string Phase, string Component, DateTime Ts, double Value)> rows)
    {
        return rows
            .Where(r => r.Component.Equals("ANG", StringComparison.OrdinalIgnoreCase))
            .Select(r => (r.Ts, r.Value))
            .OrderBy(x => x.Ts)
            .ToList();
    }

    /// <summary>
    /// Calculates sequence angle series from measurement rows (mode: sequence pos/neg/zero).
    /// Uses complex number math: a = e^(j*120°), a² = e^(j*240°)
    /// </summary>
    private static List<(DateTime ts, double angDeg)> CalculateSequenceSeries(
        IEnumerable<(string IdName, string PdcName, string Phase, string Component, DateTime Ts, double Value)> rows,
        string seq)
    {
        var rowList = rows.ToList();
        
        var vaMod = new List<(DateTime ts, double mag)>();
        var vbMod = new List<(DateTime ts, double mag)>();
        var vcMod = new List<(DateTime ts, double mag)>();
        var vaAng = new List<(DateTime ts, double angDeg)>();
        var vbAng = new List<(DateTime ts, double angDeg)>();
        var vcAng = new List<(DateTime ts, double angDeg)>();

        foreach (var r in rowList)
        {
            var ph = r.Phase.ToUpperInvariant();
            var cp = r.Component.ToUpperInvariant();

            if (ph == "A" && cp == "MAG") vaMod.Add((r.Ts, r.Value));
            else if (ph == "A" && cp == "ANG") vaAng.Add((r.Ts, r.Value));
            else if (ph == "B" && cp == "MAG") vbMod.Add((r.Ts, r.Value));
            else if (ph == "B" && cp == "ANG") vbAng.Add((r.Ts, r.Value));
            else if (ph == "C" && cp == "MAG") vcMod.Add((r.Ts, r.Value));
            else if (ph == "C" && cp == "ANG") vcAng.Add((r.Ts, r.Value));
        }

        if (vaMod.Count == 0 || vbMod.Count == 0 || vcMod.Count == 0 ||
            vaAng.Count == 0 || vbAng.Count == 0 || vcAng.Count == 0)
            return new List<(DateTime ts, double angDeg)>();

        vaMod.Sort((a, b) => a.ts.CompareTo(b.ts));
        vbMod.Sort((a, b) => a.ts.CompareTo(b.ts));
        vcMod.Sort((a, b) => a.ts.CompareTo(b.ts));
        vaAng.Sort((a, b) => a.ts.CompareTo(b.ts));
        vbAng.Sort((a, b) => a.ts.CompareTo(b.ts));
        vcAng.Sort((a, b) => a.ts.CompareTo(b.ts));

        return ComputeSequenceAngle(vaMod, vbMod, vcMod, vaAng, vbAng, vcAng, seq);
    }

    /// <summary>
    /// Computes sequence angle from three-phase measurements using complex number math.
    /// Sequence operators: a = e^(j*120°), a² = e^(j*240°)
    /// </summary>
    private static List<(DateTime ts, double angDeg)> ComputeSequenceAngle(
        List<(DateTime ts, double mag)> vaMod,
        List<(DateTime ts, double mag)> vbMod,
        List<(DateTime ts, double mag)> vcMod,
        List<(DateTime ts, double angDeg)> vaAng,
        List<(DateTime ts, double angDeg)> vbAng,
        List<(DateTime ts, double angDeg)> vcAng,
        string seq)
    {
        var result = new List<(DateTime ts, double angDeg)>();
        var tolerance = TimeSpan.FromMilliseconds(3);

        int ia = 0, ib = 0, ic = 0;
        const double Deg2Rad = Math.PI / 180.0;
        const double Rad2Deg = 180.0 / Math.PI;

        var a = Complex.FromPolarCoordinates(1.0, 120.0 * Deg2Rad);
        var a2 = Complex.FromPolarCoordinates(1.0, 240.0 * Deg2Rad);

        while (ia < vaMod.Count && ib < vbMod.Count && ic < vcMod.Count)
        {
            var tA = vaMod[ia].ts;
            var tB = vbMod[ib].ts;
            var tC = vcMod[ic].ts;
            var maxTime = new[] { tA, tB, tC }.Max();

            while (ia < vaMod.Count && vaMod[ia].ts < maxTime && (maxTime - vaMod[ia].ts) > tolerance) ia++;
            while (ib < vbMod.Count && vbMod[ib].ts < maxTime && (maxTime - vbMod[ib].ts) > tolerance) ib++;
            while (ic < vcMod.Count && vcMod[ic].ts < maxTime && (maxTime - vcMod[ic].ts) > tolerance) ic++;

            if (ia >= vaMod.Count || ib >= vbMod.Count || ic >= vcMod.Count) break;

            tA = vaMod[ia].ts;
            tB = vbMod[ib].ts;
            tC = vcMod[ic].ts;

            if (Math.Abs((tA - maxTime).TotalMilliseconds) > 3 ||
                Math.Abs((tB - maxTime).TotalMilliseconds) > 3 ||
                Math.Abs((tC - maxTime).TotalMilliseconds) > 3)
            {
                var minTime = new[] { tA, tB, tC }.Min();
                if (minTime == tA && ia < vaMod.Count) ia++;
                else if (minTime == tB && ib < vbMod.Count) ib++;
                else if (minTime == tC && ic < vcMod.Count) ic++;
                continue;
            }

            var Va = Complex.FromPolarCoordinates(vaMod[ia].mag, vaAng[ia].angDeg * Deg2Rad);
            var Vb = Complex.FromPolarCoordinates(vbMod[ib].mag, vbAng[ib].angDeg * Deg2Rad);
            var Vc = Complex.FromPolarCoordinates(vcMod[ic].mag, vcAng[ic].angDeg * Deg2Rad);

            var Vseq = seq switch
            {
                "pos" => (Va + a * Vb + a2 * Vc) / 3.0,
                "neg" => (Va + a2 * Vb + a * Vc) / 3.0,
                "zero" => (Va + Vb + Vc) / 3.0,
                _ => throw new ArgumentException("seq deve ser: pos | neg | zero")
            };

            result.Add((maxTime, Vseq.Phase * Rad2Deg));
            ia++; ib++; ic++;
        }

        return result;
    }

    /// <summary>
    /// Computes angle difference between measurement and reference series with time-series alignment.
    /// </summary>
    private static List<(DateTime ts, double difDeg)> ComputeAngleDifference(
        List<(DateTime ts, double angDeg)> meas,
        List<(DateTime ts, double angDeg)> refe,
        TimeSpan tol)
    {
        meas.Sort((a, b) => a.ts.CompareTo(b.ts));
        refe.Sort((a, b) => a.ts.CompareTo(b.ts));

        int im = 0, ir = 0;
        var outp = new List<(DateTime ts, double difDeg)>();

        while (im < meas.Count && ir < refe.Count)
        {
            var tm = meas[im].ts;
            var tr = refe[ir].ts;
            var t = tm > tr ? tm : tr;

            while (im < meas.Count && meas[im].ts < t && (t - meas[im].ts) > tol) im++;
            while (ir < refe.Count && refe[ir].ts < t && (t - refe[ir].ts) > tol) ir++;

            if (im >= meas.Count || ir >= refe.Count) break;

            tm = meas[im].ts;
            tr = refe[ir].ts;

            if (Math.Abs((tm - t).TotalMilliseconds) > tol.TotalMilliseconds ||
                Math.Abs((tr - t).TotalMilliseconds) > tol.TotalMilliseconds)
            {
                var minT = tm < tr ? tm : tr;
                if (minT == tm) im++; else ir++;
                continue;
            }

            var dif = Wrap180(meas[im].angDeg - refe[ir].angDeg);
            outp.Add((t, dif));
            im++; ir++;
        }

        return outp;
    }

    /// <summary>
    /// Normalizes angle to [-180, +180] range.
    /// </summary>
    private static double Wrap180(double difDeg)
    {
        if (difDeg > 180.0) return difDeg - 360.0;
        if (difDeg < -180.0) return difDeg + 360.0;
        return difDeg;
    }

    /// <summary>
    /// Normalizes sequence notation (pos/neg/zero).
    /// </summary>
    private static string NormalizeSeq(string seq)
    {
        return seq.Trim().ToLowerInvariant() switch
        {
            "pos" or "seq+" or "1" => "pos",
            "neg" or "seq-" or "2" => "neg",
            "zero" or "seq0" or "0" => "zero",
            _ => ""
        };
    }
}
