using System.Data;
using System.Globalization;
using System.Numerics;
using Dapper;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Data.Dtos;

namespace OpenPlot.Features.Runs.Handlers;

/// <summary>
/// Handler para cálculo de potência ativa/reativa.
/// Recebe V/I em phasores (MAG+ANG) e calcula P/Q por fase ou total.
/// Responsabilidade: validação, SQL, cálculo de potência e resposta.
/// </summary>
public sealed class PowerSeriesHandler
{
    private readonly IDbConnectionFactory _dbFactory;
    private readonly IRunContextRepository _runRepository;
    private readonly IAnalysisCacheRepository _cacheRepo;
    private readonly IPlotMetaBuilder _metaBuilder;
    private readonly IPmuQueryHelper _pmuHelper;
    private readonly ISeriesAssemblyService _seriesAssembly;

    public PowerSeriesHandler(
        IRunContextRepository runRepository,
        IDbConnectionFactory dbFactory,
        IAnalysisCacheRepository cacheRepo,
        IPlotMetaBuilder metaBuilder,
        IPmuQueryHelper pmuHelper,
        ISeriesAssemblyService seriesAssembly)
    {
        _runRepository = runRepository ?? throw new ArgumentNullException(nameof(runRepository));
        _dbFactory = dbFactory ?? throw new ArgumentNullException(nameof(dbFactory));
        _cacheRepo = cacheRepo ?? throw new ArgumentNullException(nameof(cacheRepo));
        _metaBuilder = metaBuilder ?? throw new ArgumentNullException(nameof(metaBuilder));
        _pmuHelper = pmuHelper ?? throw new ArgumentNullException(nameof(pmuHelper));
        _seriesAssembly = seriesAssembly ?? throw new ArgumentNullException(nameof(seriesAssembly));
    }

    /// <summary>
    /// Processa requisição de potência ativa/reativa.
    /// </summary>
    public async Task<IResult> HandleAsync(
        PowerPlotQuery query,
        WindowQuery window,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        // =============================
        // Validação
        // =============================
        var validation = ValidatePowerQuery(query);
        if (!validation.isValid)
            return Results.BadRequest(validation.errorMessage);

        var which = (query.Which ?? "active").Trim().ToLowerInvariant();
        var u = (query.Unit ?? "raw").Trim().ToLowerInvariant();
        var maxPts = Math.Max(query.ResolveMaxPoints(@default: 100), 100);

        DateTime? fromUtc = window.FromUtc;
        DateTime? toUtc = window.ToUtc;
        if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
            return Results.BadRequest("from < to");

        var ctx = await _runRepository.ResolveAsync(query.RunId, fromUtc, toUtc, ct);
        if (ctx is null)
            return Results.NotFound("run_id não encontrado.");

        var pmuList = _pmuHelper.Normalize(query.Pmu).ToList();

        var tri = query.Tri ?? false;
        var total = query.Total ?? false;
        var phase = !tri && !total ? query.Phase : null;

        // =============================
        // Query SQL
        // =============================
        using var db = _dbFactory.Create();

        var pmuFilter = _pmuHelper.BuildOrSqlFilter("c.id_name", pmuList);

        string phaseClause = (tri || total)
            ? "UPPER(s.phase::text) IN ('A','B','C')"
            : "UPPER(s.phase::text) = UPPER(@phase)";

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
  SELECT s.signal_id, s.pdc_pmu_id, s.phase, s.component, s.quantity,
         c.id_name, c.pdc_name
  FROM ctx c
  JOIN openplot.pdc_pmu pp ON pp.pdc_id = c.pdc_id AND pp.pmu_id = c.pmu_id
  JOIN openplot.signal s   ON s.pdc_pmu_id = pp.pdc_pmu_id
  WHERE ({PMU_FILTER})
    AND {PHASE_CLAUSE}
    AND UPPER(s.component::text) IN ('MAG','ANG')
    AND LOWER(s.quantity::text) IN ('voltage','v','current','i')
),
raw AS (
  SELECT m.signal_id, m.ts, m.value
  FROM openplot.measurements m
  WHERE m.ts >= (SELECT from_utc FROM win)
    AND m.ts <= (SELECT to_utc   FROM win)
)
SELECT
  s.signal_id, s.pdc_pmu_id, s.phase, s.component, s.quantity,
  s.id_name, s.pdc_name,
  r.ts, r.value
FROM sig s
JOIN raw r USING (signal_id)
ORDER BY s.id_name, s.quantity, s.phase, s.component, r.ts;
";

        var sql = sqlTemplate
            .Replace("{PMU_FILTER}", pmuFilter)
            .Replace("{PHASE_CLAUSE}", phaseClause);

        var dyn = new DynamicParameters();
        dyn.Add("run_id", query.RunId);
        dyn.Add("from_utc", fromUtc);
        dyn.Add("to_utc", toUtc);
        dyn.Add("phase", phase);

        _pmuHelper.AddSqlParameters(dyn, pmuList);

        var rows = (await db.QueryAsync<PowerRow>(sql, dyn)).ToList();
        if (rows.Count == 0)
            return Results.NotFound("Nada encontrado para esse run/filtro no intervalo solicitado.");

        // =============================
        // Cálculo de potência
        // =============================
        var tol = TimeSpan.FromMilliseconds(3);
        var seriesOut = new List<object>();
        var cachePoints = new List<(string pmuId, DateTime ts, double value)>();

        foreach (var pmuGroup in rows.GroupBy(r => r.Id_Name))
        {
            var d = new Dictionary<string, List<(DateTime ts, double v)>>();

            foreach (var r in pmuGroup)
            {
                var qty = (r.Quantity ?? "").ToLowerInvariant();
                var phs = (r.Phase ?? "").ToUpperInvariant();
                var cmp = (r.Component ?? "").ToUpperInvariant();

                if (qty is not ("voltage" or "v" or "current" or "i")) continue;
                if (phs is not ("A" or "B" or "C")) continue;
                if (cmp is not ("MAG" or "ANG")) continue;

                var qn = qty == "v" ? "voltage" : (qty == "i" ? "current" : qty);
                if (!d.TryGetValue($"{qn}_{phs}_{cmp}", out var list))
                    d[$"{qn}_{phs}_{cmp}"] = list = new();
                list.Add((r.Ts, r.Value));
            }

            bool Need(string key) => d.ContainsKey(key) && d[key].Count > 0;

            List<(DateTime ts, double val)> MakePhase(string phs)
            {
                var vMagK = $"voltage_{phs}_MAG";
                var vAngK = $"voltage_{phs}_ANG";
                var iMagK = $"current_{phs}_MAG";
                var iAngK = $"current_{phs}_ANG";

                if (!Need(vMagK) || !Need(vAngK) || !Need(iMagK) || !Need(iAngK))
                    return new List<(DateTime ts, double val)>();

                return ComputePower1Phase(d[vMagK], d[vAngK], d[iMagK], d[iAngK], tol, which);
            }

            double scale = 1e-6;
            var any = pmuGroup.First();

            if (tri)
            {
                foreach (var phs in new[] { "A", "B", "C" })
                {
                    var pts = MakePhase(phs);
                    if (pts.Count == 0) continue;

                    // Armazena para cache
                    foreach (var pt in pts)
                    {
                        cachePoints.Add((any.Id_Name, pt.ts, pt.val * scale));
                    }

                    var down = TimeBucketDownsampleMinMax(pts.Select(x => (x.ts, x.val * scale)), maxPts);

                    seriesOut.Add(new
                    {
                        pmu = any.Id_Name,
                        pdc = any.Pdc_Name,
                        meta = new { phase = phs },
                        unit = (u == "mw") ? (which == "active" ? "MW" : "MVAr") : "raw",
                        points = down.Select(p => new object[] { p.ts, p.val })
                    });
                }
            }
            else if (total)
            {
                var aPts = MakePhase("A");
                var bPts = MakePhase("B");
                var cPts = MakePhase("C");
                if (aPts.Count == 0 || bPts.Count == 0 || cPts.Count == 0) continue;

                var sum = Sum3PhasePointwise(aPts, bPts, cPts, tol);
                if (sum.Count == 0) continue;

                // Armazena para cache
                foreach (var pt in sum)
                {
                    cachePoints.Add((any.Id_Name, pt.ts, pt.val * scale));
                }

                var down = TimeBucketDownsampleMinMax(sum.Select(x => (x.ts, x.val * scale)), maxPts);

                seriesOut.Add(new
                {
                    pmu = any.Id_Name,
                    pdc = any.Pdc_Name,
                    meta = new { total = true },
                    unit = (u == "mw") ? (which == "active" ? "MW" : "MVAr") : "raw",
                    points = down.Select(p => new object[] { p.ts, p.val })
                });
            }
            else
            {
                var pts = MakePhase(phase!);
                if (pts.Count == 0) continue;

                // Armazena para cache
                foreach (var pt in pts)
                {
                    cachePoints.Add((any.Id_Name, pt.ts, pt.val * scale));
                }

                var down = TimeBucketDownsampleMinMax(pts.Select(x => (x.ts, x.val * scale)), maxPts);

                seriesOut.Add(new
                {
                    pmu = any.Id_Name,
                    pdc = any.Pdc_Name,
                    meta = new { phase = phase },
                    unit = (u == "mw") ? (which == "active" ? "MW" : "MVAr") : "raw",
                    points = down.Select(p => new object[] { p.ts, p.val })
                });
            }
        }

        if (seriesOut.Count == 0)
            return Results.BadRequest("Nenhuma PMU pôde ser processada (faltam sinais MAG/ANG de V/I ou alinhamento falhou).");

        var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
        var windowTo = toUtc ?? rows.Max(r => r.Ts);
        var unitDisplay = (u == "mw") ? (which == "active" ? "MW" : "MVAr") : "raw";

        // ===== CACHE =====
        var cacheSeries = cachePoints
            .GroupBy(x => x.pmuId)
            .Select(g => _seriesAssembly.BuildCacheSeries(
                signalId: 0,
                pdcPmuId: 0,
                idName: g.Key,
                pdcName: ctx.PdcName,
                unit: unitDisplay,
                phase: null,
                quantity: which,
                component: "power",
                points: g.Select(x => (x.ts, x.value))))
            .ToList();

        var cachePayload = _seriesAssembly.BuildCachePayload(
            windowFrom,
            windowTo,
            (int)ctx.SelectRate,
            cacheSeries);

        var cacheId = await _cacheRepo.SaveAsync(query.RunId, cachePayload, ct);
        // =======================================================

        var meas = new MeasurementsQuery(
            Quantity: which,
            Component: "power",
            PhaseMode: PhaseMode.Any,
            Unit: unitDisplay
        );

        var plotMeta = _metaBuilder.Build(window, ctx, meas);

        var response = SeriesResponseBuilderExtensions
            .BuildSeriesResponse(query.RunId, windowFrom, windowTo, seriesOut, plotMeta)
            .WithModes(modes)
            .WithCacheId(cacheId)
            .WithResolved(ctx.PdcName, seriesOut.Count)
            .WithTypeFields(new Dictionary<string, object?>
            {
                ["unit"] = unitDisplay,
                ["type"] = which,
                ["tri"] = tri,
                ["total"] = total,
                ["phase"] = phase
            })
            .Build();

        return Results.Ok(response);
    }

    private (bool isValid, string? errorMessage) ValidatePowerQuery(PowerPlotQuery query)
    {
        if (query.RunId == Guid.Empty)
            return (false, "run_id é obrigatório.");

        var which = (query.Which ?? "active").Trim().ToLowerInvariant();
        if (which is not ("active" or "reactive"))
            return (false, "which deve ser 'active' ou 'reactive'.");

        var unit = (query.Unit ?? "raw").Trim().ToLowerInvariant();
        if (unit is not ("raw" or "mw"))
            return (false, "unit deve ser 'raw' ou 'mw'.");

        var tri = query.Tri ?? false;
        var total = query.Total ?? false;

        if (tri && total)
            return (false, "tri=true e total=true são mutuamente exclusivos.");

        if (tri && (query.Pmu?.Length ?? 0) != 1)
            return (false, "tri=true exige exatamente 1 pmu (id_name).");

        if (!tri && !total && string.IsNullOrWhiteSpace(query.Phase))
            return (false, "phase é obrigatório quando tri=false e total=false.");

        if (!tri && !total)
        {
            var phase = query.Phase?.Trim().ToUpperInvariant();
            if (phase is not ("A" or "B" or "C"))
                return (false, "phase deve ser A, B ou C.");
        }

        return (true, null);
    }

    // ========== Helpers ==========

    private static List<(DateTime ts, double val)> ComputePower1Phase(
        List<(DateTime ts, double v)> vMag,
        List<(DateTime ts, double v)> vAng,
        List<(DateTime ts, double v)> iMag,
        List<(DateTime ts, double v)> iAng,
        TimeSpan tol,
        string which)
    {
        vMag.Sort((a, b) => a.ts.CompareTo(b.ts));
        vAng.Sort((a, b) => a.ts.CompareTo(b.ts));
        iMag.Sort((a, b) => a.ts.CompareTo(b.ts));
        iAng.Sort((a, b) => a.ts.CompareTo(b.ts));

        int ivm = 0, iva = 0, iim = 0, iia = 0;
        const double Deg2Rad = Math.PI / 180.0;

        static void Adv(ref int idx, List<(DateTime ts, double v)> l, DateTime t, TimeSpan tol)
        {
            while (idx < l.Count && l[idx].ts < t && (t - l[idx].ts) > tol) idx++;
        }

        static bool Near(List<(DateTime ts, double v)> l, int idx, DateTime t, TimeSpan tol)
            => idx < l.Count && Math.Abs((l[idx].ts - t).TotalMilliseconds) <= tol.TotalMilliseconds;

        var outp = new List<(DateTime ts, double val)>();

        while (ivm < vMag.Count && iim < iMag.Count)
        {
            var t = vMag[ivm].ts;
            if (iMag[iim].ts > t) t = iMag[iim].ts;

            Adv(ref ivm, vMag, t, tol);
            Adv(ref iim, iMag, t, tol);
            if (ivm >= vMag.Count || iim >= iMag.Count) break;

            Adv(ref iva, vAng, t, tol);
            Adv(ref iia, iAng, t, tol);
            if (iva >= vAng.Count || iia >= iAng.Count) break;

            if (!Near(vMag, ivm, t, tol) || !Near(iMag, iim, t, tol) ||
                !Near(vAng, iva, t, tol) || !Near(iAng, iia, t, tol))
            {
                var min = vMag[ivm].ts < iMag[iim].ts ? vMag[ivm].ts : iMag[iim].ts;
                if (min == vMag[ivm].ts) ivm++; else iim++;
                continue;
            }

            var s = vMag[ivm].v * iMag[iim].v;
            var d = (vAng[iva].v - iAng[iia].v) * Deg2Rad;

            var val = (which == "active") ? (s * Math.Cos(d)) : (s * Math.Sin(d));
            outp.Add((t, val));

            ivm++; iim++; iva++; iia++;
        }

        return outp;
    }

    private static List<(DateTime ts, double val)> Sum3PhasePointwise(
        List<(DateTime ts, double val)> a,
        List<(DateTime ts, double val)> b,
        List<(DateTime ts, double val)> c,
        TimeSpan tol)
    {
        a.Sort((x, y) => x.ts.CompareTo(y.ts));
        b.Sort((x, y) => x.ts.CompareTo(y.ts));
        c.Sort((x, y) => x.ts.CompareTo(y.ts));

        int ia = 0, ib = 0, ic = 0;
        var outp = new List<(DateTime ts, double val)>();

        while (ia < a.Count && ib < b.Count && ic < c.Count)
        {
            var t = a[ia].ts;
            if (b[ib].ts > t) t = b[ib].ts;
            if (c[ic].ts > t) t = c[ic].ts;

            while (ia < a.Count && a[ia].ts < t && (t - a[ia].ts) > tol) ia++;
            while (ib < b.Count && b[ib].ts < t && (t - b[ib].ts) > tol) ib++;
            while (ic < c.Count && c[ic].ts < t && (t - c[ic].ts) > tol) ic++;

            if (ia >= a.Count || ib >= b.Count || ic >= c.Count) break;

            if (Math.Abs((a[ia].ts - t).TotalMilliseconds) > tol.TotalMilliseconds ||
                Math.Abs((b[ib].ts - t).TotalMilliseconds) > tol.TotalMilliseconds ||
                Math.Abs((c[ic].ts - t).TotalMilliseconds) > tol.TotalMilliseconds)
            {
                var min = a[ia].ts;
                if (b[ib].ts < min) min = b[ib].ts;
                if (c[ic].ts < min) min = c[ic].ts;

                if (min == a[ia].ts) ia++;
                else if (min == b[ib].ts) ib++;
                else ic++;
                continue;
            }

            outp.Add((t, a[ia].val + b[ib].val + c[ic].val));
            ia++; ib++; ic++;
        }

        return outp;
    }

    private static IEnumerable<(DateTime ts, double val)> TimeBucketDownsampleMinMax(
        IEnumerable<(DateTime ts, double val)> pts, int maxPoints)
    {
        var list = pts.OrderBy(p => p.ts).ToList();
        if (list.Count <= maxPoints) return list;

        int buckets = Math.Max(1, maxPoints / 2);
        var start = list.First().ts;
        var end = list.Last().ts;
        var span = (end - start).Ticks;
        if (span <= 0) return list.Take(maxPoints);

        long bucket = span / buckets;
        var result = new List<(DateTime, double)>(buckets * 2 + 2) { list.First() };

        for (int i = 0; i < buckets; i++)
        {
            var bStart = start.AddTicks(bucket * i);
            var bEnd = (i == buckets - 1) ? end : start.AddTicks(bucket * (i + 1));
            double? minVal = null, maxVal = null;
            DateTime minTs = default, maxTs = default;

            foreach (var p in list)
            {
                if (p.ts < bStart || p.ts >= bEnd) continue;
                if (minVal is null || p.val < minVal)
                {
                    minVal = p.val; minTs = p.ts;
                }
                if (maxVal is null || p.val > maxVal)
                {
                    maxVal = p.val; maxTs = p.ts;
                }
            }
            if (minVal is null) continue;

            if (minTs <= maxTs) { result.Add((minTs, minVal!.Value)); result.Add((maxTs, maxVal!.Value)); }
            else { result.Add((maxTs, maxVal!.Value)); result.Add((minTs, minVal!.Value)); }
        }

        result.Add(list.Last());
        return result;
    }
}
