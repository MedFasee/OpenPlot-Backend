using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Mvc;             // <- [FromServices], [FromQuery]
using Dapper;
using System.Data;
using OpenPlot.Data.Dtos;
using System.Numerics;
public static class RunsEndpoints
{
    public static IEndpointRouteBuilder MapRuns(this IEndpointRouteBuilder app)
    {
        var grp = app.MapGroup("")
                     .WithTags("Runs");

        grp.MapGet("/terminais/{nomeBusca}", async (
            string nomeBusca,                                 // rota
            [FromQuery] Guid? id,                             // query ?id=...
            [FromServices] IDbConnectionFactory dbf,          // serviços
            [FromServices] ILabelService labels,
            [FromServices] IPmuHierarchyService pmuHierarchy
        ) =>
        {
            using var db = dbf.Create();

            SearchRunFull? run = null;
            if (id is Guid gid && gid != Guid.Empty)
            {
                run = await db.QueryFirstOrDefaultAsync<SearchRunFull>(Data.Sql.SearchSql.GetRunById, new { id = gid });
                if (run is null) return Results.Json(new { status = 404, error = "search_run_not_found_by_id" }, statusCode: 404);
            }
            else
            {
                var rows = await db.QueryAsync<SearchRunFull>(Data.Sql.SearchSql.ListRecentDone);
                run = rows.FirstOrDefault(r => labels.BuildLabel(r.from_ts, r.to_ts, r.select_rate, r.source, r.terminal_id) == nomeBusca);
                if (run is null) return Results.Json(new { status = 404, error = "search_run_not_found_by_label" }, statusCode: 404);
            }

            const string pmusSql = @"
WITH run AS (SELECT id, signals, COALESCE(pmus_ok, pmus) AS pmus FROM openplot.search_runs WHERE id = @id),
src AS (
  SELECT CASE
           WHEN jsonb_typeof(signals) = 'array' AND jsonb_array_length(signals) > 0 THEN signals
           WHEN jsonb_typeof(pmus)    = 'array' AND jsonb_array_length(pmus)    > 0 THEN pmus
           ELSE '[]'::jsonb
         END AS arr
  FROM run
),
elems AS (
  SELECT jsonb_array_elements(arr) AS elem
  FROM src
),

pmus_from_string AS (
  SELECT DISTINCT p.pmu_id, p.id_name, p.full_name, p.volt_level, p.area, p.state, p.station
  FROM elems e
  JOIN openplot.pmu p ON p.id_name = btrim(e.elem::text, '""""')
  WHERE jsonb_typeof(e.elem) = 'string'
),

pmus_direct AS (
  SELECT DISTINCT p.pmu_id, p.id_name, p.full_name, p.volt_level, p.area, p.state, p.station
  FROM elems e
  JOIN LATERAL (
    SELECT NULLIF(TRIM(e.elem->>'pmu'), '')     AS key_pmu,
           NULLIF(TRIM(e.elem->>'id_name'), '') AS key_idname
  ) k ON TRUE
  JOIN openplot.pmu p ON p.id_name = COALESCE(k.key_pmu, k.key_idname)
  WHERE jsonb_typeof(e.elem) = 'object'
    AND COALESCE(k.key_pmu, k.key_idname) IS NOT NULL
),

pmus_by_pdcpmu AS (
  SELECT DISTINCT p.pmu_id, p.id_name, p.full_name, p.volt_level, p.area, p.state, p.station
  FROM elems e
  JOIN LATERAL (
    SELECT NULLIF(e.elem->>'pdc_pmu_id','')::int AS key_pdc_pmu_id
  ) k ON TRUE
  JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id = k.key_pdc_pmu_id
  JOIN openplot.pmu p       ON p.pmu_id       = ppm.pmu_id
  WHERE jsonb_typeof(e.elem) = 'object'
),

pmus_by_signal AS (
  SELECT DISTINCT p.pmu_id, p.id_name, p.full_name, p.volt_level, p.area, p.state, p.station
  FROM elems e
  JOIN LATERAL (
    SELECT NULLIF(e.elem->>'signal_id','')::int AS key_signal_id
  ) k ON TRUE
  JOIN openplot.signal  s   ON s.signal_id    = k.key_signal_id
  JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id = s.pdc_pmu_id
  JOIN openplot.pmu     p   ON p.pmu_id       = ppm.pmu_id
  WHERE jsonb_typeof(e.elem) = 'object'
),

pmus_by_point AS (
  SELECT DISTINCT p.pmu_id, p.id_name, p.full_name, p.volt_level, p.area, p.state, p.station
  FROM elems e
  JOIN LATERAL (
    SELECT NULLIF(e.elem->>'historian_point','')::int AS key_point
  ) k ON TRUE
  JOIN openplot.signal  s   ON s.historian_point = k.key_point
  JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id    = s.pdc_pmu_id
  JOIN openplot.pmu     p   ON p.pmu_id          = ppm.pmu_id
  WHERE jsonb_typeof(e.elem) = 'object'
),

-- 🔹 Consolidamos todas as PMUs envolvidas
pmus_union AS (
  SELECT DISTINCT pmu_id, id_name, full_name, volt_level, area, state, station
  FROM (
    SELECT * FROM pmus_from_string
    UNION ALL SELECT * FROM pmus_direct
    UNION ALL SELECT * FROM pmus_by_pdcpmu
    UNION ALL SELECT * FROM pmus_by_signal
    UNION ALL SELECT * FROM pmus_by_point
  ) u
),

-- 🔹 Agregamos sinais por PMU
signals_agg AS (
  SELECT
    pu.pmu_id,

    -- Flags por tipo, olhando para ""name""
    (MAX(CASE WHEN s.name ILIKE 'TENSAO%'   THEN 1 ELSE 0 END) > 0) AS has_tensao,
    (MAX(CASE WHEN s.name ILIKE 'CORRENTE%' THEN 1 ELSE 0 END) > 0) AS has_corrente,
    (MAX(CASE WHEN s.name ILIKE 'FREQUENCIA%' THEN 1 ELSE 0 END) > 0) AS has_freq,
    (MAX(CASE WHEN s.name ILIKE 'DFREQ%'    THEN 1 ELSE 0 END) > 0) AS has_dfreq,

    -- fases distintas, ignorando 'None' (phase é enum/phase_kind → convertemos pra text)
    ARRAY_REMOVE(ARRAY_AGG(DISTINCT s.phase::text), 'None') AS phases_raw,

    -- nomes crus como adicionais
    ARRAY[]::text[] AS adicionais_raw


  FROM pmus_union pu
  LEFT JOIN openplot.pdc_pmu ppm ON ppm.pmu_id = pu.pmu_id
  LEFT JOIN openplot.signal  s   ON s.pdc_pmu_id = ppm.pdc_pmu_id
  GROUP BY pu.pmu_id
),

-- 🔹 Construímos listas finais de grandezas, fases e adicionais
signals_final AS (
  SELECT
    pmu_id,

    ARRAY_REMOVE(ARRAY[
      CASE WHEN has_tensao   THEN 'Tensão'             END,
      CASE WHEN has_corrente THEN 'Corrente'           END,
      CASE WHEN has_freq     THEN 'Frequência'         END,
      CASE WHEN has_dfreq    THEN 'Var. de Frequência' END,
      CASE WHEN has_tensao AND has_corrente THEN 'Potência Ativa'   END,
      CASE WHEN has_tensao AND has_corrente THEN 'Potência Reativa' END
    ], NULL) AS grandezas,

    CASE
      WHEN phases_raw IS NULL OR CARDINALITY(phases_raw) = 0
        THEN ARRAY[]::text[]
      WHEN CARDINALITY(phases_raw) = 1 AND phases_raw[1] = 'A'
        THEN ARRAY['A']::text[]
      WHEN ARRAY['A','B','C']::text[] <@ phases_raw
        THEN ARRAY['A','B','C','Trifásico','Sequência Positiva']::text[]
      ELSE (
        SELECT ARRAY(
          SELECT UNNEST(phases_raw) p
          ORDER BY p
        )
      )
    END AS fases,

    COALESCE(adicionais_raw, ARRAY[]::text[]) AS adicionais

  FROM signals_agg
)

-- 🔹 SELECT final que o Dapper consome
SELECT
  pu.pmu_id,
  pu.id_name,
  pu.full_name,
  pu.volt_level,
  pu.area,
  pu.state,
  pu.station,
  COALESCE(sf.grandezas,  ARRAY[]::text[]) AS grandezas,
  COALESCE(sf.fases,      ARRAY[]::text[]) AS fases,
  COALESCE(sf.adicionais, ARRAY[]::text[]) AS adicionais
FROM pmus_union pu
LEFT JOIN signals_final sf ON sf.pmu_id = pu.pmu_id
ORDER BY pu.area       NULLS LAST,
         pu.state      NULLS LAST,
         pu.volt_level NULLS LAST,
         pu.station    NULLS LAST;;";

            var pmus = (await db.QueryAsync<PmuMeta>(pmusSql, new { id = run.id })).ToList();
            var hierarchy = pmuHierarchy.BuildHierarchy(pmus);

            var data = new
            {
                xml_file = run.source,
                total_terminais = pmus.Count,
                nome_busca = labels.BuildLabel(run.from_ts, run.to_ts, run.select_rate, run.source, run.terminal_id),
                terminais = hierarchy
            };
            return Results.Json(new { status = 200, data });
        });

        // -------------------------------
        // 2) /plots/voltage-phase/by-run
        // -------------------------------
        grp.MapGet("/plots/voltage-phase/by-run",
        async Task<IResult> (
            [AsParameters] ByRunQuery q,
            [FromServices] IDbConnectionFactory dbf,
            [FromQuery] DateTime? from,
            [FromQuery] DateTime? to
        ) =>
        {
            var tri = q.Tri;
            var pmuName = q.Pmu?.Trim();

            // Validação de parâmetros
            string? uphase = null;

            if (!tri)
            {
                // modo monofásico: phase é obrigatória
                if (string.IsNullOrWhiteSpace(q.Phase))
                    return Results.BadRequest("phase é obrigatório (A|B|C) quando tri=false.");

                uphase = q.Phase.Trim().ToUpperInvariant();
                if (!new[] { "A", "B", "C" }.Contains(uphase))
                    return Results.BadRequest("phase deve ser A, B ou C.");
            }
            else
            {
                // modo trifásico: exige PMU
                if (string.IsNullOrWhiteSpace(pmuName))
                    return Results.BadRequest("para tri=true é obrigatório informar pmu (id_name da PMU).");
            }

            var maxPts = Math.Max(q.MaxPoints, 100);
            var unit = (q.Unit ?? "raw").Trim().ToLowerInvariant();

            DateTime? fromUtc = from?.ToUniversalTime();
            DateTime? toUtc = to?.ToUniversalTime();
            if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
                return Results.BadRequest("from < to");

            using var db = dbf.Create();

            // Clausula de fase dinâmica
            var phaseClause = tri
                ? "UPPER(s.phase::text) IN ('A','B','C')"          // trifásico
                : "UPPER(s.phase::text) = UPPER(@phase)";          // uma fase só

            // Filtro de PMU (usado no modo tri, opcional no mono)
            var pmuFilter = !string.IsNullOrWhiteSpace(pmuName)
                ? "LOWER(pmu.id_name) = LOWER(@pmu)"
                : "TRUE";

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
         c.id_name, c.pdc_name, pmu.volt_level
  FROM ctx c
  JOIN openplot.pdc_pmu pp ON pp.pdc_id = c.pdc_id AND pp.pmu_id = c.pmu_id
  JOIN openplot.signal s   ON s.pdc_pmu_id = pp.pdc_pmu_id
  JOIN openplot.pmu pmu    ON pmu.pmu_id   = c.pmu_id
  WHERE {PHASE_CLAUSE}
    AND LOWER(s.quantity::text) IN ('voltage','v')
    AND LOWER(s.component::text) IN ('mag','magnitude','mod')
    AND {PMU_FILTER}
),
raw AS (
  SELECT m.signal_id, m.ts, m.value
  FROM openplot.measurements m
  WHERE m.ts >= (SELECT from_utc FROM win)
    AND m.ts <= (SELECT to_utc FROM win)
)
SELECT
  s.signal_id, s.pdc_pmu_id, s.phase, s.component,
  s.id_name, s.pdc_name, s.volt_level,
  r.ts, r.value
FROM sig s
JOIN raw r USING (signal_id)
ORDER BY s.signal_id, r.ts;";

            var sql = sqlTemplate
                .Replace("{PHASE_CLAUSE}", phaseClause)
                .Replace("{PMU_FILTER}", pmuFilter);

            var rows = (await db.QueryAsync<VoltRow>(sql, new
            {
                run_id = q.RunId,
                phase = uphase,     // ignorado se tri=true
                from_utc = fromUtc,
                to_utc = toUtc,
                pmu = pmuName     // usado quando informado
            })).ToList();

            if (rows.Count == 0)
                return Results.NotFound("Nada encontrado para esse run_id/filtro no intervalo solicitado.");

            var series = rows
                .GroupBy(r => r.Signal_Id)
                .Select(g =>
                {
                    var any = g.First();
                    double vb = 1.0;
                    if (unit == "pu" && any.Volt_Level.HasValue && any.Volt_Level.Value > 0)
                        vb = (any.Volt_Level.Value / Math.Sqrt(3.0));

                    var downs = TimeBucketDownsampleMinMax(
                        g.Select(r => (r.Ts, r.Value)), maxPts);

                    return new
                    {
                        pmu = any.Id_Name,
                        pdc = any.Pdc_Name,
                        signal_id = any.Signal_Id,
                        pdc_pmu_id = any.Pdc_Pmu_Id,
                        meta = new
                        {
                            phase = any.Phase,          // A/B/C
                            component = any.Component,
                            volt_level_kV = any.Volt_Level / 1000,
                        },
                        points = downs.Select(p =>
                            new object[] { p.ts, unit == "pu" ? p.val / vb : p.val })
                    };
                })
                .ToList();

            var first = rows.First();
            return Results.Ok(new
            {
                run_id = q.RunId,
                unit = unit,
                tri = tri,
                phase = tri ? "ABC" : uphase,
                resolved = new
                {
                    pdc = first.Pdc_Name,
                    pmu_count = series.Select(s => s.pmu).Distinct().Count()
                },
                window = new
                {
                    from = fromUtc ?? rows.Min(r => r.Ts),
                    to = toUtc ?? rows.Max(r => r.Ts)
                },
                series
            });
        });


        // -------------------------------
        // 3) /plots/current-phase/by-run (RAW em A)
        // -------------------------------
        grp.MapGet("/plots/current-phase/by-run",
        async Task<IResult> (
            [AsParameters] ByRunQuery q,
            [FromServices] IDbConnectionFactory dbf,
            [FromQuery] DateTime? from,
            [FromQuery] DateTime? to
        ) =>
        {
            var tri = q.Tri;
            var pmuName = q.Pmu?.Trim();

            // ---------------------------
            // Validação de parâmetros
            // ---------------------------
            string? uphase = null;

            if (!tri)
            {
                // modo monofásico: phase é obrigatória
                if (string.IsNullOrWhiteSpace(q.Phase))
                    return Results.BadRequest("phase é obrigatório (A|B|C) quando tri=false.");

                uphase = q.Phase.Trim().ToUpperInvariant();
                if (!new[] { "A", "B", "C" }.Contains(uphase))
                    return Results.BadRequest("phase deve ser A, B ou C.");
            }
            else
            {
                // modo trifásico: exige PMU
                if (string.IsNullOrWhiteSpace(pmuName))
                    return Results.BadRequest("para tri=true é obrigatório informar pmu (id_name da PMU).");
            }

            var maxPts = Math.Max(q.MaxPoints, 100);

            DateTime? fromUtc = from?.ToUniversalTime();
            DateTime? toUtc = to?.ToUniversalTime();
            if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
                return Results.BadRequest("from < to");

            using var db = dbf.Create();

            // Clausula de fase dinâmica (igual ao endpoint de tensão)
            var phaseClause = tri
                ? "UPPER(s.phase::text) IN ('A','B','C')"          // trifásico
                : "UPPER(s.phase::text) = UPPER(@phase)";          // uma fase só

            // Filtro de PMU (usado no modo tri, opcional no mono)
            var pmuFilter = !string.IsNullOrWhiteSpace(pmuName)
                ? "LOWER(pmu.id_name) = LOWER(@pmu)"
                : "TRUE";

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
    AND LOWER(s.quantity::text) IN ('current','i')
    AND LOWER(s.component::text) IN ('mag','magnitude','mod')
    AND {PMU_FILTER}
),
raw AS (
  SELECT m.signal_id, m.ts, m.value
  FROM openplot.measurements m
  WHERE m.ts >= (SELECT from_utc FROM win)
    AND m.ts <= (SELECT to_utc FROM win)
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
                .Replace("{PMU_FILTER}", pmuFilter);

            var rows = (await db.QueryAsync<(
                int Signal_Id, int Pdc_Pmu_Id, string Phase, string Component,
                string Id_Name, string Pdc_Name, DateTime Ts, double Value
            )>(sql, new
            {
                run_id = q.RunId,
                phase = uphase,      // ignorado se tri=true
                from_utc = fromUtc,
                to_utc = toUtc,
                pmu = pmuName      // usado quando informado
            })).ToList();

            if (rows.Count == 0)
                return Results.NotFound("Nada encontrado para esse run_id/filtro no intervalo solicitado.");

            var series = rows
                .GroupBy(r => r.Signal_Id)
                .Select(g =>
                {
                    var any = g.First();

                    var downs = TimeBucketDownsampleMinMax(
                        g.Select(r => (r.Ts, r.Value)), maxPts);

                    return new
                    {
                        pmu = any.Id_Name,
                        pdc = any.Pdc_Name,
                        signal_id = any.Signal_Id,
                        pdc_pmu_id = any.Pdc_Pmu_Id,
                        meta = new
                        {
                            phase = any.Phase,
                            component = any.Component
                        },
                        unit = "A", // corrente sempre em A no momento
                        points = downs.Select(p => new object[] { p.ts, p.val })
                    };
                })
                .ToList();

            var first = rows.First();
            var window = new
            {
                from = fromUtc ?? rows.Min(r => r.Ts),
                to = toUtc ?? rows.Max(r => r.Ts)
            };

            return Results.Ok(new
            {
                run_id = q.RunId,
                tri = tri,
                phase = tri ? "ABC" : uphase,
                unit = "raw",
                resolved = new
                {
                    pdc = first.Pdc_Name,
                    pmu_count = series.Select(s => s.pmu).Distinct().Count()
                },
                window,
                series
            });
        });


        // ---------------------------------------------
        // 4) /plots/seqpos/by-run  (tensão ou corrente)
        // ---------------------------------------------
        grp.MapGet("/plots/seqpos/by-run",
        async Task<IResult> (
            [AsParameters] SeqPosRunQuery q,
            [FromQuery] string[]? pmu,          // <--- múltiplas PMUs via ?pmu=...&pmu=...
            [FromQuery] string kind,            // "voltage" | "current"
            [FromServices] IDbConnectionFactory dbf,
            [FromQuery] DateTime? from,
            [FromQuery] DateTime? to
        ) =>
        {
            // =============================
            // kind obrigatório
            // =============================
            if (string.IsNullOrWhiteSpace(kind))
                return Results.BadRequest("kind é obrigatório (voltage|current).");

            var k = kind.Trim().ToLowerInvariant();
            if (k is not ("voltage" or "current"))
                return Results.BadRequest("kind deve ser 'voltage' ou 'current'.");

            // =============================
            // lista de PMUs (opcional)
            // =============================
            var pmuList = pmu?
                .Select(p => p.Trim())
                .Where(p => p.Length > 0)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList()
                ?? new List<string>();

            // =============================
            // unit
            // =============================
            var u = (q.Unit ?? "raw").Trim().ToLowerInvariant();
            if (u is not ("raw" or "pu"))
                return Results.BadRequest("unit deve ser 'raw' ou 'pu'.");

            var maxPts = Math.Max(q.MaxPoints, 100);

            // =============================
            // janela temporal
            // =============================
            DateTime? fromUtc = from?.ToUniversalTime();
            DateTime? toUtc = to?.ToUniversalTime();

            if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
                return Results.BadRequest("from < to");

            using var db = dbf.Create();

            // =======================================
            // SQL BASE – com placeholder de PMU_LIST
            // =======================================
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
  -- strings simples: ""SE_MG_Itajuba_UNIFEI""
  SELECT r.pdc_name, r.from_ts, r.to_ts, p.pmu_id, p.id_name
  FROM elems r
  JOIN openplot.pmu p ON p.id_name = btrim(r.elem::text, '""')
  WHERE jsonb_typeof(r.elem) = 'string'
  UNION ALL
  -- objetos com { ""pmu"": ""..."", ... } ou { ""id_name"": ""..."" }
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
  -- objetos com { ""pdc_pmu_id"": N }
  SELECT r.pdc_name, r.from_ts, r.to_ts, p.pmu_id, p.id_name
  FROM elems r
  JOIN LATERAL (SELECT NULLIF(r.elem->>'pdc_pmu_id','')::int AS key_pdc_pmu_id) k ON TRUE
  JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id = k.key_pdc_pmu_id
  JOIN openplot.pmu p ON p.pmu_id = ppm.pmu_id
  WHERE jsonb_typeof(r.elem) = 'object'
  UNION ALL
  -- objetos com { ""signal_id"": N }
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
         c.id_name, c.pdc_name, pmu.volt_level
  FROM ctx c
  JOIN openplot.pdc_pmu pp ON pp.pdc_id = c.pdc_id AND pp.pmu_id = c.pmu_id
  JOIN openplot.signal s   ON s.pdc_pmu_id = pp.pdc_pmu_id
  JOIN openplot.pmu pmu    ON pmu.pmu_id   = c.pmu_id
  WHERE
      ({PMU_FILTER})
    AND (
      (@kind = 'voltage' AND LOWER(s.quantity::text) IN ('voltage','v'))
      OR
      (@kind = 'current' AND LOWER(s.quantity::text) IN ('current','i'))
    )
    AND UPPER(s.phase::text) IN ('A','B','C')
    AND UPPER(s.component::text) IN ('MAG','ANG')
),
raw AS (
  SELECT m.signal_id, m.ts, m.value
  FROM openplot.measurements m
  WHERE m.ts >= (SELECT from_utc FROM win)
    AND m.ts <= (SELECT to_utc   FROM win)
)
SELECT
  s.signal_id, s.pdc_pmu_id, s.phase, s.component,
  s.id_name, s.pdc_name, s.volt_level,
  r.ts, r.value
FROM sig s
JOIN raw r USING (signal_id)
ORDER BY s.id_name, s.signal_id, r.ts;
";

            // ======================================================
            // Constrói PMU_FILTER dinamicamente
            // ======================================================
            string pmuFilter =
                pmuList.Count == 0
                ? "TRUE"
                : string.Join(" OR ", pmuList.Select((_, i) => $"LOWER(c.id_name) = LOWER(@pmu{i})"));

            var sql = sqlTemplate.Replace("{PMU_FILTER}", pmuFilter);

            // parâmetros dinâmicos
            var dyn = new DynamicParameters();
            dyn.Add("run_id", q.RunId);
            dyn.Add("kind", k);
            dyn.Add("from_utc", fromUtc);
            dyn.Add("to_utc", toUtc);

            for (int i = 0; i < pmuList.Count; i++)
                dyn.Add($"pmu{i}", pmuList[i]);

            // Executa consulta
            var rows = (await db.QueryAsync<SeqPosRow>(sql, dyn)).ToList();

            if (rows.Count == 0)
                return Results.NotFound("Nenhuma PMU encontrada para este run/kind.");

            // ======================================================
            // Processa PMU por PMU
            // ======================================================
            var series = new List<object>();

            foreach (var g in rows.GroupBy(r => r.Id_Name))
            {
                var sigRows = g.ToList();

                var vaMod = new List<(DateTime ts, double val)>();
                var vbMod = new List<(DateTime ts, double val)>();
                var vcMod = new List<(DateTime ts, double val)>();
                var vaAng = new List<(DateTime ts, double val)>();
                var vbAng = new List<(DateTime ts, double val)>();
                var vcAng = new List<(DateTime ts, double val)>();

                foreach (var r in sigRows)
                {
                    string ph = (r.Phase ?? "").ToUpperInvariant();
                    string cp = (r.Component ?? "").ToUpperInvariant();

                    if (ph == "A" && cp == "MAG") vaMod.Add((r.Ts, r.Value));
                    else if (ph == "A" && cp == "ANG") vaAng.Add((r.Ts, r.Value));
                    else if (ph == "B" && cp == "MAG") vbMod.Add((r.Ts, r.Value));
                    else if (ph == "B" && cp == "ANG") vbAng.Add((r.Ts, r.Value));
                    else if (ph == "C" && cp == "MAG") vcMod.Add((r.Ts, r.Value));
                    else if (ph == "C" && cp == "ANG") vcAng.Add((r.Ts, r.Value));
                }

                if (vaMod.Count == 0 || vbMod.Count == 0 || vcMod.Count == 0 ||
                    vaAng.Count == 0 || vbAng.Count == 0 || vcAng.Count == 0)
                    continue;

                // Ordena
                vaMod.Sort((a, b) => a.ts.CompareTo(b.ts));
                vbMod.Sort((a, b) => a.ts.CompareTo(b.ts));
                vcMod.Sort((a, b) => a.ts.CompareTo(b.ts));
                vaAng.Sort((a, b) => a.ts.CompareTo(b.ts));
                vbAng.Sort((a, b) => a.ts.CompareTo(b.ts));
                vcAng.Sort((a, b) => a.ts.CompareTo(b.ts));

                // Monta seq+
                var seq = ComputePositiveSequenceMagnitudeMedPlot(
                    vaMod, vbMod, vcMod,
                    vaAng, vbAng, vcAng);

                if (seq.Count == 0)
                    continue;

                var first = sigRows.First();

                // base pu
                double baseValue = 1.0;
                if (u == "pu" && k == "voltage")
                {
                    double lvl = q.VoltLevel ?? first.Volt_Level ?? 0;
                    if (lvl > 0)
                        baseValue = lvl / Math.Sqrt(3.0);
                }
                else if (u == "pu" && k == "current")
                {
                    // MedPlot usa ib=1A como base → pu == Ampère
                    baseValue = 1.0;
                }

                // downsample
                var downs = TimeBucketDownsampleMinMax(
                    seq.Select(p => (
                        p.ts,
                        u == "pu" ? p.mag / baseValue : p.mag
                    )),
                    maxPts);

                series.Add(new
                {
                    pmu = first.Id_Name,
                    pdc = first.Pdc_Name,
                    volt_level = q.VoltLevel ?? first.Volt_Level,
                    unit = u,
                    points = downs.Select(d => new object[] { d.ts, d.val })
                });
            }

            if (series.Count == 0)
                return Results.BadRequest("Nenhuma PMU pôde ser processada.");

            return Results.Ok(new
            {
                run_id = q.RunId,
                kind = k,
                unit = u,
                pmu_count = series.Count,
                window = new
                {
                    from = fromUtc ?? rows.Min(r => r.Ts),
                    to = toUtc ?? rows.Max(r => r.Ts)
                },
                series
            });
        });






        // -----------------------------------------
        // 5) /plots/frequency/by-run  (Frequência)
        // -----------------------------------------
        grp.MapGet("/plots/frequency/by-run",
        async Task<IResult> (
            [AsParameters] FreqRunQuery q,
            [FromServices] IDbConnectionFactory dbf,
            [FromQuery] DateTime? from,
            [FromQuery] DateTime? to
        ) =>
        {
            var pmuName = q.Pmu?.Trim();
            var maxPts = Math.Max(q.MaxPoints, 100);

            DateTime? fromUtc = from?.ToUniversalTime();
            DateTime? toUtc = to?.ToUniversalTime();
            if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
                return Results.BadRequest("from < to");

            using var db = dbf.Create();

            var pmuFilter = !string.IsNullOrWhiteSpace(pmuName)
                ? "LOWER(pmu.id_name) = LOWER(@pmu)"
                : "TRUE";

            const string sqlTemplate = @"
WITH run AS (
  SELECT id, source AS pdc_name, from_ts, to_ts, pmus, signals
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
  SELECT s.signal_id, s.pdc_pmu_id,
         c.id_name, c.pdc_name
  FROM ctx c
  JOIN openplot.pdc_pmu pp ON pp.pdc_id = c.pdc_id AND pp.pmu_id = c.pmu_id
  JOIN openplot.signal s   ON s.pdc_pmu_id = pp.pdc_pmu_id
  WHERE LOWER(s.quantity::text) = 'frequency'
    AND LOWER(s.component::text) = 'freq'
    AND {PMU_FILTER}
),
raw AS (
  SELECT m.signal_id, m.ts, m.value
  FROM openplot.measurements m
  WHERE m.ts >= (SELECT from_utc FROM win)
    AND m.ts <= (SELECT to_utc FROM win)
)
SELECT
  s.signal_id, s.pdc_pmu_id,
  s.id_name, s.pdc_name,
  r.ts, r.value
FROM sig s
JOIN raw r USING (signal_id)
ORDER BY s.signal_id, r.ts;
";

            var sql = sqlTemplate.Replace("{PMU_FILTER}", pmuFilter);

            var rows = (await db.QueryAsync<FreqRow>(sql, new
            {
                run_id = q.RunId,
                from_utc = fromUtc,
                to_utc = toUtc,
                pmu = pmuName
            })).ToList();

            if (rows.Count == 0)
                return Results.NotFound("Nenhuma frequência encontrada para esse run_id.");

            var series = rows
                .GroupBy(r => r.Signal_Id)
                .Select(g =>
                {
                    var any = g.First();

                    var downs = TimeBucketDownsampleMinMax(
                        g.Select(r => (r.Ts, r.Value)), maxPts);

                    return new
                    {
                        pmu = any.Id_Name,
                        pdc = any.Pdc_Name,
                        signal_id = any.Signal_Id,
                        pdc_pmu_id = any.Pdc_Pmu_Id,
                        unit = "Hz",
                        points = downs.Select(p => new object[] { p.ts, p.val })
                    };
                })
                .ToList();

            var first = rows.First();

            return Results.Ok(new
            {
                run_id = q.RunId,
                unit = "Hz",
                resolved = new
                {
                    pdc = first.Pdc_Name,
                    pmu_count = series.Select(s => s.pmu).Distinct().Count()
                },
                window = new
                {
                    from = fromUtc ?? rows.Min(r => r.Ts),
                    to = toUtc ?? rows.Max(r => r.Ts)
                },
                series
            });
        });
        // -----------------------------------------
        // 6) /plots/dfreq/by-run  (Derivada da frequência)
        // -----------------------------------------
        grp.MapGet("/plots/dfreq/by-run",
        async Task<IResult> (
            [AsParameters] FreqRunQuery q,
            [FromServices] IDbConnectionFactory dbf,
            [FromQuery] DateTime? from,
            [FromQuery] DateTime? to
        ) =>
        {
            var pmuName = q.Pmu?.Trim();
            var maxPts = Math.Max(q.MaxPoints, 100);

            DateTime? fromUtc = from?.ToUniversalTime();
            DateTime? toUtc = to?.ToUniversalTime();
            if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
                return Results.BadRequest("from < to");

            using var db = dbf.Create();

            var pmuFilter = !string.IsNullOrWhiteSpace(pmuName)
                ? "LOWER(pmu.id_name) = LOWER(@pmu)"
                : "TRUE";

            // SQL para dfreq
            const string sqlTemplate = @"
WITH run AS (
  SELECT id, source AS pdc_name, from_ts, to_ts, pmus, signals
  FROM openplot.search_runs
  WHERE id = @run_id::uuid
),
run_window AS (
  SELECT
    CASE WHEN pg_typeof(r.from_ts)::text = 'timestamp without time zone'
         THEN r.from_ts::timestamptz ELSE r.from_ts END AS from_utc,
    CASE WHEN pg_typeof(r.to_ts)::text = 'timestamp without time zone'
         THEN r.to_ts::timestamptz ELSE r.to_ts END AS to_utc,
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
  SELECT s.signal_id, s.pdc_pmu_id,
         c.id_name, c.pdc_name
  FROM ctx c
  JOIN openplot.pdc_pmu pp ON pp.pdc_id = c.pdc_id AND pp.pmu_id = c.pmu_id
  JOIN openplot.signal s   ON s.pdc_pmu_id = pp.pdc_pmu_id
  WHERE LOWER(s.quantity::text) = 'frequency'
    AND LOWER(s.component::text) = 'dfreq'
    AND {PMU_FILTER}
),
raw AS (
  SELECT m.signal_id, m.ts, m.value
  FROM openplot.measurements m
  WHERE m.ts >= (SELECT from_utc FROM win)
    AND m.ts <= (SELECT to_utc FROM win)
)
SELECT
  s.signal_id, s.pdc_pmu_id,
  s.id_name, s.pdc_name,
  r.ts, r.value
FROM sig s
JOIN raw r USING (signal_id)
ORDER BY s.signal_id, r.ts;
";

            var sql = sqlTemplate.Replace("{PMU_FILTER}", pmuFilter);

            var rows = (await db.QueryAsync<FreqRow>(sql, new
            {
                run_id = q.RunId,
                from_utc = fromUtc,
                to_utc = toUtc,
                pmu = pmuName
            })).ToList();

            if (rows.Count == 0)
                return Results.NotFound("Nenhuma dfreq encontrada para esse run_id.");

            var series = rows
                .GroupBy(r => r.Signal_Id)
                .Select(g =>
                {
                    var any = g.First();

                    var downs = TimeBucketDownsampleMinMax(
                        g.Select(r => (r.Ts, r.Value)), maxPts);

                    return new
                    {
                        pmu = any.Id_Name,
                        pdc = any.Pdc_Name,
                        signal_id = any.Signal_Id,
                        pdc_pmu_id = any.Pdc_Pmu_Id,
                        unit = "Hz/s",
                        points = downs.Select(p => new object[] { p.ts, p.val })
                    };
                })
                .ToList();

            var first = rows.First();

            return Results.Ok(new
            {
                run_id = q.RunId,
                unit = "Hz/s",
                resolved = new
                {
                    pdc = first.Pdc_Name,
                    pmu_count = series.Select(s => s.pmu).Distinct().Count()
                },
                window = new
                {
                    from = fromUtc ?? rows.Min(r => r.Ts),
                    to = toUtc ?? rows.Max(r => r.Ts)
                },
                series
            });
        });


        return app;
    }

    // --------- helpers ---------

    // Renomeado + sem MinBy/MaxBy (compatível com versões antigas)
    private static IEnumerable<(DateTime ts, double val)>
    TimeBucketDownsampleMinMax(IEnumerable<(DateTime ts, double val)> pts, int maxPoints)
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

    /// <summary>
    /// Calcula o módulo da sequência positiva (V1 ou I1) no estilo do MedPlot:
    /// - Usa Va, Vb, Vc (módulo e ângulo) trifásicos;
    /// - Faz alinhamento no tempo com tolerância de 3 ms entre as fases;
    /// - Não usa canal MISSING ainda (apenas pontos válidos).
    /// 
    /// As listas devem estar ORDENADAS por ts.
    /// </summary>
    private static List<(DateTime ts, double mag)> ComputePositiveSequenceMagnitudeMedPlot(
    List<(DateTime ts, double mag)> vaMod,
    List<(DateTime ts, double mag)> vbMod,
    List<(DateTime ts, double mag)> vcMod,
    List<(DateTime ts, double angDeg)> vaAng,
    List<(DateTime ts, double angDeg)> vbAng,
    List<(DateTime ts, double angDeg)> vcAng)
    {
        var result = new List<(DateTime ts, double mag)>();

        if (vaMod.Count == 0 || vbMod.Count == 0 || vcMod.Count == 0 ||
            vaAng.Count == 0 || vbAng.Count == 0 || vcAng.Count == 0)
            return result;

        var tolerance = TimeSpan.FromMilliseconds(3);

        int ia = 0, ib = 0, ic = 0;

        const double Deg2Rad = Math.PI / 180.0;
        Complex a = Complex.FromPolarCoordinates(1.0, 120.0 * Deg2Rad);
        Complex a2 = Complex.FromPolarCoordinates(1.0, 240.0 * Deg2Rad);

        while (ia < vaMod.Count && ib < vbMod.Count && ic < vcMod.Count)
        {
            var tA = vaMod[ia].ts;
            var tB = vbMod[ib].ts;
            var tC = vcMod[ic].ts;

            var maxTime = tA;
            if (tB > maxTime) maxTime = tB;
            if (tC > maxTime) maxTime = tC;

            while (ia < vaMod.Count &&
                   vaMod[ia].ts < maxTime &&
                   (maxTime - vaMod[ia].ts) > tolerance)
                ia++;

            while (ib < vbMod.Count &&
                   vbMod[ib].ts < maxTime &&
                   (maxTime - vbMod[ib].ts) > tolerance)
                ib++;

            while (ic < vcMod.Count &&
                   vcMod[ic].ts < maxTime &&
                   (maxTime - vcMod[ic].ts) > tolerance)
                ic++;

            if (ia >= vaMod.Count || ib >= vbMod.Count || ic >= vcMod.Count)
                break;

            tA = vaMod[ia].ts;
            tB = vbMod[ib].ts;
            tC = vcMod[ic].ts;

            if (Math.Abs((tA - maxTime).TotalMilliseconds) > 3 ||
                Math.Abs((tB - maxTime).TotalMilliseconds) > 3 ||
                Math.Abs((tC - maxTime).TotalMilliseconds) > 3)
            {
                var minTime = tA;
                if (tB < minTime) minTime = tB;
                if (tC < minTime) minTime = tC;

                if (minTime == tA && ia < vaMod.Count) ia++;
                else if (minTime == tB && ib < vbMod.Count) ib++;
                else if (minTime == tC && ic < vcMod.Count) ic++;

                continue;
            }

            // Aqui é onde o erro ocorria:
            double vaM = vaMod[ia].mag;
            double vbM = vbMod[ib].mag;
            double vcM = vcMod[ic].mag;

            double vaDeg = vaAng[ia].angDeg;
            double vbDeg = vbAng[ib].angDeg;
            double vcDeg = vcAng[ic].angDeg;

            double thA = vaDeg * Deg2Rad;
            double thB = vbDeg * Deg2Rad;
            double thC = vcDeg * Deg2Rad;

            Complex Va = Complex.FromPolarCoordinates(vaM, thA);
            Complex Vb = Complex.FromPolarCoordinates(vbM, thB);
            Complex Vc = Complex.FromPolarCoordinates(vcM, thC);

            Complex V1 = (Va + a * Vb + a2 * Vc) / 3.0;

            result.Add((maxTime, V1.Magnitude));

            ia++;
            ib++;
            ic++;
        }

        return result;
    }


}
