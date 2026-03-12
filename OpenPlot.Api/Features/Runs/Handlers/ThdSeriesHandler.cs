using System.Data;
using System.Globalization;
using Dapper;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Data.Dtos;

namespace OpenPlot.Features.Runs.Handlers;

/// <summary>
/// Handler para Distorção Harmônica Total (THD) de tensão ou corrente.
/// Reusa ByRunQuery com parâmetro kind adicional.
/// </summary>
public sealed class ThdSeriesHandler
{
    private readonly IDbConnectionFactory _dbFactory;
    private readonly ITimeSeriesDownsampler _downsampler;
    private readonly IRunContextRepository _runRepository;
    private readonly IAnalysisCacheRepository _cacheRepository;
    private readonly IPlotMetaBuilder _metaBuilder;

    public ThdSeriesHandler(
        IRunContextRepository runRepository,
        IAnalysisCacheRepository cacheRepository,
        IDbConnectionFactory dbFactory,
        ITimeSeriesDownsampler downsampler,
        IPlotMetaBuilder metaBuilder)
    {
        _runRepository = runRepository ?? throw new ArgumentNullException(nameof(runRepository));
        _cacheRepository = cacheRepository ?? throw new ArgumentNullException(nameof(cacheRepository));
        _dbFactory = dbFactory ?? throw new ArgumentNullException(nameof(dbFactory));
        _downsampler = downsampler ?? throw new ArgumentNullException(nameof(downsampler));
        _metaBuilder = metaBuilder ?? throw new ArgumentNullException(nameof(metaBuilder));
    }

    public async Task<IResult> HandleAsync(
        ByRunQuery query,
        WindowQuery window,
        string kind,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        var validation = ValidateThdInput(query, kind);
        if (!validation.isValid)
            return Results.BadRequest(validation.errorMessage);

        var k = kind.Trim().ToLowerInvariant();
        var tri = query.Tri;
        var uphase = tri ? null : query.Phase?.Trim().ToUpperInvariant();
        var pmuName = query.Pmu?.Trim();
        var noDownsample = query.MaxPointsIsAll;
        var maxPts = query.ResolveMaxPoints(@default: 5000);

        // Construir lista de PMUs a filtrar
        var pmuList = new List<string>();
        if (!string.IsNullOrWhiteSpace(pmuName))
            pmuList.Add(pmuName);

        DateTime? fromUtc = window.FromUtc;
        DateTime? toUtc = window.ToUtc;
        if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
            return Results.BadRequest("from < to");

        var ctx = await _runRepository.ResolveAsync(query.RunId, fromUtc, toUtc, ct);
        if (ctx is null)
            return Results.NotFound("run_id não encontrado.");

        using var db = _dbFactory.Create();

        var phaseClause = tri
            ? "UPPER(s.phase::text) IN ('A','B','C')"
            : "UPPER(s.phase::text) = UPPER(@phase)";

        var qtyClause = k == "voltage"
            ? "LOWER(s.quantity::text) IN ('voltage','v')"
            : "LOWER(s.quantity::text) IN ('current','i')";

        var pmuFilter = pmuList.Count == 0
            ? "TRUE"
            : string.Join(" OR ", pmuList.Select((_, i) => $"LOWER(c.id_name) = LOWER(@pmu{i})"));


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
  WHERE {PHASE_CLAUSE}
    AND {QTY_CLAUSE}
    AND LOWER(s.component::text) IN ('thd')
    AND {PMU_FILTER}
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
ORDER BY s.signal_id, r.ts;";

        var sql = sqlTemplate
            .Replace("{PHASE_CLAUSE}", phaseClause)
            .Replace("{QTY_CLAUSE}", qtyClause)
            .Replace("{PMU_FILTER}", pmuFilter);

        var dyn = new DynamicParameters();
        dyn.Add("run_id", query.RunId);
        dyn.Add("phase", uphase);
        dyn.Add("from_utc", fromUtc);
        dyn.Add("to_utc", toUtc);

        for (int i = 0; i < pmuList.Count; i++)
            dyn.Add($"pmu{i}", pmuList[i]);

        var rows = (await db.QueryAsync<(
            int Signal_Id, int Pdc_Pmu_Id, string Phase, string Component,
            string Id_Name, string Pdc_Name, DateTime Ts, double Value
        )>(sql, dyn)).ToList();

        if (rows.Count == 0)
            return Results.NotFound("Nada encontrado para esse run_id/filtro no intervalo solicitado.");

        var series = rows
            .GroupBy(r => r.Signal_Id)
            .Select(g =>
            {
                var any = g.First();
                var downs = noDownsample 
                    ? g.Select(r => new Point(r.Ts, r.Value)).ToList()
                    : _downsampler.MinMax(
                        g.Select(r => new Point(r.Ts, r.Value)).ToList(),
                        maxPts);

                return new
                {
                    pmu = any.Id_Name,
                    pdc = any.Pdc_Name,
                    signal_id = any.Signal_Id,
                    pdc_pmu_id = any.Pdc_Pmu_Id,
                    meta = new
                    {
                        phase = any.Phase,
                        component = any.Component,
                        kind = k
                    },
                    points = downs.Select(p => new object[] { p.Ts, p.Val }).ToList()
                };
            })
            .ToList();

        var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
        var windowTo = toUtc ?? rows.Max(r => r.Ts);

        var meas = new MeasurementsQuery(
            Quantity: k,
            Component: "thd",
            PhaseMode: tri ? PhaseMode.ThreePhase : PhaseMode.Single,
            Phase: uphase,
            Unit: "%"
        );

        var plotMeta = _metaBuilder.Build(window, ctx, meas);

        var response = SeriesResponseBuilderExtensions
            .BuildSeriesResponse(query.RunId, windowFrom, windowTo, series, plotMeta)
            .WithModes(modes)
            .WithResolved(ctx.PdcName, series.Select(s => s.pmu).Distinct().Count())
            .WithTypeFields(new Dictionary<string, object?>
            {
                ["unit"] = "%",
                ["kind"] = k,
                ["tri"] = tri,
                ["phase"] = tri ? "ABC" : uphase
            })
            .Build();

        return Results.Ok(response);
    }

    private (bool isValid, string? errorMessage) ValidateThdInput(
        ByRunQuery query,
        string kind)
    {
        if (string.IsNullOrWhiteSpace(kind))
            return (false, "kind é obrigatório (voltage|current).");

        var k = kind.Trim().ToLowerInvariant();
        if (k is not ("voltage" or "current"))
            return (false, "kind deve ser 'voltage' ou 'current'.");

        var tri = query.Tri;
        if (!tri)
        {
            if (string.IsNullOrWhiteSpace(query.Phase))
                return (false, "phase é obrigatório (A|B|C) quando tri=false.");

            var phase = query.Phase.Trim().ToUpperInvariant();
            if (phase is not ("A" or "B" or "C"))
                return (false, "phase deve ser A, B ou C.");
        }
        else
        {
            if (string.IsNullOrWhiteSpace(query.Pmu))
                return (false, "para tri=true é obrigatório informar pmu (id_name da PMU).");
        }

        return (true, null);
    }
}
