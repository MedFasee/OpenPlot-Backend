using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Mvc;             // <- [FromServices], [FromQuery]
using Dapper;
using System.Data;
using OpenPlot.Data.Dtos;
using System.Numerics;
using System.Globalization;
using Data.Sql;
using static ConfigEndpoints;
public static class RunsEndpoints
{
    public static IEndpointRouteBuilder MapRuns(this IEndpointRouteBuilder app)
    {
        var grp = app.MapGroup("")
                     .WithTags("Runs").RequireAuthorization();

        // GET /runs
        grp.MapGet("/runs", async (
        HttpContext http,
        [FromServices] IDbConnectionFactory dbf,
        [FromQuery] string? status,
        [FromServices] ITimeService time,
        [FromServices] ILabelService labels
    ) =>
            {
                var username =
                    http.User?.FindFirst("username")?.Value
                    ?? http.User?.Identity?.Name;

                if (string.IsNullOrWhiteSpace(username))
                    return Results.Unauthorized();

                using var db = dbf.Create();

                var rows = await db.QueryAsync<SearchRunRow>(
                    SearchSql.ListRuns,
                    new { status, username }
                );

            // ano -> mês -> dia -> itens
            var calendar = new Dictionary<string, Dictionary<string, Dictionary<string, List<SearchRunItem>>>>();

            foreach (var r in rows)
            {
                var label = labels.BuildLabel(r.from_ts, r.to_ts, r.select_rate, r.source, r.terminal_id);

                // agrupa pela data no fuso do Brasil
                var fromUtc = DateTime.SpecifyKind(r.from_ts, DateTimeKind.Utc);
                //var timeFilter = TimeZoneInfo.ConvertTimeFromUtc(fromUtc, time.BrazilTz);

                var y = fromUtc.Year.ToString("0000");
                var m = fromUtc.Month.ToString("00");
                var d = fromUtc.Day.ToString("00");

                if (!calendar.TryGetValue(y, out var months))
                    calendar[y] = months = new();

                if (!months.TryGetValue(m, out var days))
                    months[m] = days = new();

                if (!days.TryGetValue(d, out var items))
                    days[d] = items = new();

                items.Add(new SearchRunItem
                {
                    label = label,
                    status = r.status,
                    id = r.id.ToString()
                });
            }

            // agora data é só o calendário (sem lookup)
            var data = calendar;

            return Results.Json(new { status = 200, data });
        });


        grp.MapGet("/terminals/{nomeBusca}", async (
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
                    run = await db.QueryFirstOrDefaultAsync<SearchRunFull>(
                        Data.Sql.SearchSql.GetRunById,
                        new { id = gid }
                    );

                    if (run is null)
                        return Results.Json(new { status = 404, error = "search_run_not_found_by_id" }, statusCode: 404);
                }
                else
                {
                    var rows = await db.QueryAsync<SearchRunFull>(Data.Sql.SearchSql.ListRecentDone);
                    run = rows.FirstOrDefault(r =>
                        labels.BuildLabel(r.from_ts, r.to_ts, r.select_rate, r.source, r.terminal_id) == nomeBusca
                    );

                    if (run is null)
                        return Results.Json(new { status = 404, error = "search_run_not_found_by_label" }, statusCode: 404);
                }

                // -------------------------
                // NOVO: resolutionSearch (derivado de select_rate)
                // -------------------------
                var sr = run.select_rate; // int
                string resolutionSearch = $"{sr}";

                const string pmusSql = @"
    WITH run AS (
      SELECT id, signals, COALESCE(pmus_ok, pmus) AS pmus
      FROM openplot.search_runs
      WHERE id = @id
    ),
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
      JOIN openplot.pmu p ON p.id_name = btrim(e.elem::text, '""')
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
      JOIN LATERAL (SELECT NULLIF(e.elem->>'pdc_pmu_id','')::int AS key_pdc_pmu_id) k ON TRUE
      JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id = k.key_pdc_pmu_id
      JOIN openplot.pmu p       ON p.pmu_id       = ppm.pmu_id
      WHERE jsonb_typeof(e.elem) = 'object'
    ),

    pmus_by_signal AS (
      SELECT DISTINCT p.pmu_id, p.id_name, p.full_name, p.volt_level, p.area, p.state, p.station
      FROM elems e
      JOIN LATERAL (SELECT NULLIF(e.elem->>'signal_id','')::int AS key_signal_id) k ON TRUE
      JOIN openplot.signal  s   ON s.signal_id    = k.key_signal_id
      JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id = s.pdc_pmu_id
      JOIN openplot.pmu     p   ON p.pmu_id       = ppm.pmu_id
      WHERE jsonb_typeof(e.elem) = 'object'
    ),

    pmus_by_point AS (
      SELECT DISTINCT p.pmu_id, p.id_name, p.full_name, p.volt_level, p.area, p.state, p.station
      FROM elems e
      JOIN LATERAL (SELECT NULLIF(e.elem->>'historian_point','')::int AS key_point) k ON TRUE
      JOIN openplot.signal  s   ON s.historian_point = k.key_point
      JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id    = s.pdc_pmu_id
      JOIN openplot.pmu     p   ON p.pmu_id          = ppm.pmu_id
      WHERE jsonb_typeof(e.elem) = 'object'
    ),

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

    signals_agg AS (
      SELECT
        pu.pmu_id,

        (MAX(CASE WHEN LOWER(s.quantity::text) = 'voltage' THEN 1 ELSE 0 END) > 0) AS has_tensao,
        (MAX(CASE WHEN LOWER(s.quantity::text) = 'current' THEN 1 ELSE 0 END) > 0) AS has_corrente,

        (MAX(CASE WHEN LOWER(s.quantity::text) = 'frequency'
                   AND LOWER(s.component::text) = 'freq' THEN 1 ELSE 0 END) > 0) AS has_freq,

        (MAX(CASE WHEN LOWER(s.quantity::text) = 'frequency'
                   AND LOWER(s.component::text) = 'dfreq' THEN 1 ELSE 0 END) > 0) AS has_dfreq,

        ARRAY_REMOVE(ARRAY_AGG(DISTINCT s.phase::text), 'None') AS phases_raw,

        (MAX(CASE WHEN LOWER(s.component::text) = 'thd'
                   AND LOWER(s.quantity::text) = 'voltage' THEN 1 ELSE 0 END) > 0) AS has_thd_v,

        (MAX(CASE WHEN LOWER(s.component::text) = 'thd'
                   AND LOWER(s.quantity::text) = 'current' THEN 1 ELSE 0 END) > 0) AS has_thd_i,

        (MAX(CASE WHEN LOWER(s.quantity::text) = 'digital' THEN 1 ELSE 0 END) > 0) AS has_digital,

        ARRAY_REMOVE(
          ARRAY_AGG(DISTINCT CASE
            WHEN LOWER(s.component::text) = 'thd' THEN NULLIF(s.phase::text, 'None')
            ELSE NULL
          END),
          NULL
        ) AS thd_phases_raw

      FROM pmus_union pu
      LEFT JOIN openplot.pdc_pmu ppm ON ppm.pmu_id = pu.pmu_id
      LEFT JOIN openplot.signal  s   ON s.pdc_pmu_id = ppm.pdc_pmu_id
      GROUP BY pu.pmu_id
    ),

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
            THEN ARRAY['A','B','C','Trifásico','Sequência Positiva','Sequência Negativa', 'Sequência Zero', 'Desequilíbrio']::text[]
          ELSE (
            SELECT ARRAY(
              SELECT p
              FROM UNNEST(phases_raw) p
              ORDER BY p
            )
          )
        END AS fases,

        CASE
          WHEN (has_thd_v OR has_thd_i) AND (thd_phases_raw IS NULL OR CARDINALITY(thd_phases_raw) = 0)
            THEN ARRAY['Trifásico']::text[]
          WHEN thd_phases_raw IS NULL OR CARDINALITY(thd_phases_raw) = 0
            THEN ARRAY[]::text[]
          WHEN ARRAY['A','B','C']::text[] <@ thd_phases_raw
            THEN ARRAY['Trifásico','A','B','C']::text[]
          ELSE (
            SELECT ARRAY(
              SELECT p
              FROM UNNEST(thd_phases_raw) p
              ORDER BY CASE p
                WHEN 'Trifásico' THEN 0
                WHEN 'A' THEN 1
                WHEN 'B' THEN 2
                WHEN 'C' THEN 3
                ELSE 99
              END
            )
          )
        END AS thd_fases,

        has_thd_v,
        has_thd_i,
        has_digital

      FROM signals_agg
    )

    SELECT
      pu.pmu_id,
      pu.id_name,
      pu.full_name,
      pu.volt_level,
      pu.area,
      pu.state,
      pu.station,
      COALESCE(sf.grandezas, ARRAY[]::text[]) AS grandezas,
      COALESCE(sf.fases,     ARRAY[]::text[]) AS fases,
      COALESCE(sf.thd_fases, ARRAY[]::text[]) AS thd_fases,
      COALESCE(sf.has_thd_v, false)           AS has_thd_v,
      COALESCE(sf.has_thd_i, false)           AS has_thd_i,
      COALESCE(sf.has_digital, false)         AS has_digital
    FROM pmus_union pu
    LEFT JOIN signals_final sf ON sf.pmu_id = pu.pmu_id
    ORDER BY pu.area       NULLS LAST,
             pu.state      NULLS LAST,
             pu.volt_level NULLS LAST,
             pu.station    NULLS LAST;
    ";

                var pmuRows = (await db.QueryAsync<PmuMetaRow>(pmusSql, new { id = run.id })).ToList();

                var pmus = pmuRows.Select(r =>
                {
                    var adicionais = new List<PmuAdicional>();

                    var thdFases = (r.thd_fases != null && r.thd_fases.Count > 0)
                        ? r.thd_fases
                        : Array.Empty<string>();

                    if (r.has_thd_v)
                        adicionais.Add(new PmuAdicional
                        {
                            TipoMedida = "Medidas Analógicas",
                            Grandeza = "THD de Tensão",
                            Fase = thdFases
                        });

                    if (r.has_thd_i)
                        adicionais.Add(new PmuAdicional
                        {
                            TipoMedida = "Medidas Analógicas",
                            Grandeza = "THD de Corrente",
                            Fase = thdFases
                        });

                    if (r.has_digital)
                        adicionais.Add(new PmuAdicional
                        {
                            TipoMedida = "Medidas Digitais",
                            Grandeza = "Digital",
                            Fase = Array.Empty<string>() // digital não tem fase
                        });

                    return new PmuMeta
                    {
                        pmu_id = r.pmu_id,
                        id_name = r.id_name,
                        full_name = r.full_name,
                        volt_level = r.volt_level,
                        area = r.area,
                        state = r.state,
                        station = r.station,
                        Grandezas = r.grandezas ?? Array.Empty<string>(),
                        Fases = r.fases ?? Array.Empty<string>(),
                        Adicionais = adicionais
                    };
                }).ToList();

                var hierarchy = pmuHierarchy.BuildHierarchy(pmus);

                var fromUtc = (run.from_ts.Kind == DateTimeKind.Utc) ? run.from_ts : run.from_ts.ToUniversalTime();
                var toUtc = (run.to_ts.Kind == DateTimeKind.Utc) ? run.to_ts : run.to_ts.ToUniversalTime();

                var data = new
                {
                    xml_file = run.source,
                    total_terminais = pmus.Count,
                    nome_busca = labels.BuildLabel(run.from_ts, run.to_ts, run.select_rate, run.source, run.terminal_id),

                    // NOVO
                    resolutionSearch,

                    from = fromUtc.ToString("O"),
                    to = toUtc.ToString("O"),

                    terminais = hierarchy
                };

                return Results.Json(new { status = 200, data });
            });


        // -------------------------------
        // 2) /series/voltage/by-run
        // -------------------------------
        grp.MapGet("/series/voltage/by-run",
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

            var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
            var windowTo = toUtc ?? rows.Max(r => r.Ts);

            // DIA UTC DA CONSULTA
            var data = windowFrom
            .Date
            .ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

            var first = rows.First();
            return Results.Ok(new
            {
                run_id = q.RunId,
                data,
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
        // 3) /series/current/by-run (RAW em A)
        // -------------------------------
        grp.MapGet("/series/current/by-run",
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
            var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
            var windowTo = toUtc ?? rows.Max(r => r.Ts);

            // DIA UTC DA CONSULTA
            var data = windowFrom
            .Date
            .ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

            return Results.Ok(new
            {
                run_id = q.RunId,
                data,
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
        // 4) /series/seqpos/by-run  (tensão ou corrente)
        // ---------------------------------------------
        grp.MapGet("/series/seq/by-run",
        async Task<IResult> (
            [AsParameters] SeqPosRunQuery q,
            [FromQuery] string[]? pmu,          // múltiplas PMUs via ?pmu=...&pmu=...
            [FromQuery] string kind,            // "voltage" | "current"
            [FromQuery] string? seq,            // "pos" | "neg" | "zero"  (ou "seq+" | "seq-" | "seq0")
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
            // seq obrigatório (pos|neg|zero)
            // =============================
            if (string.IsNullOrWhiteSpace(seq))
                return Results.BadRequest("seq é obrigatório (pos|neg|zero) ou (seq+|seq-|seq0).");

            var seqNorm = seq.Trim().ToLowerInvariant();
            seqNorm = seqNorm switch
            {
                "pos" or "seq+" or "1" => "pos",
                "neg" or "seq-" or "2" => "neg",
                "zero" or "seq0" or "0" => "zero",
                _ => ""
            };

            if (seqNorm == "")
                return Results.BadRequest("seq inválida. Use pos|neg|zero (ou seq+|seq-|seq0).");

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

            var dyn = new DynamicParameters();
            dyn.Add("run_id", q.RunId);
            dyn.Add("kind", k);
            dyn.Add("from_utc", fromUtc);
            dyn.Add("to_utc", toUtc);

            for (int i = 0; i < pmuList.Count; i++)
                dyn.Add($"pmu{i}", pmuList[i]);

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

                vaMod.Sort((a, b) => a.ts.CompareTo(b.ts));
                vbMod.Sort((a, b) => a.ts.CompareTo(b.ts));
                vcMod.Sort((a, b) => a.ts.CompareTo(b.ts));
                vaAng.Sort((a, b) => a.ts.CompareTo(b.ts));
                vbAng.Sort((a, b) => a.ts.CompareTo(b.ts));
                vcAng.Sort((a, b) => a.ts.CompareTo(b.ts));

                // Monta sequência solicitada
                var seqSeries = ComputeSequenceMagnitudeMedPlot(
                    vaMod, vbMod, vcMod,
                    vaAng, vbAng, vcAng,
                    seqNorm);

                if (seqSeries.Count == 0)
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
                    baseValue = 1.0; // MedPlot: ib=1A
                }

                var downs = TimeBucketDownsampleMinMax(
                    seqSeries.Select(p => (
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
            var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
            var windowTo = toUtc ?? rows.Max(r => r.Ts);

            // DIA UTC DA CONSULTA
            var data = windowFrom
            .Date
            .ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

            return Results.Ok(new
            {
                run_id = q.RunId,
                data,
                kind = k,
                seq = seqNorm, // <-- opcional, mas útil
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



        // ---------------------------------------------
        // 5) /series/unbalance/by-run  (|seq-| / |seq+|)
        // ---------------------------------------------
        grp.MapGet("/series/unbalance/by-run",
        async Task<IResult> (
            [AsParameters] SeqPosRunQuery q,
            [FromQuery] string[]? pmu,
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
            // SQL (igual ao seu)
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

            string pmuFilter =
                pmuList.Count == 0
                ? "TRUE"
                : string.Join(" OR ", pmuList.Select((_, i) => $"LOWER(c.id_name) = LOWER(@pmu{i})"));

            var sql = sqlTemplate.Replace("{PMU_FILTER}", pmuFilter);

            var dyn = new DynamicParameters();
            dyn.Add("run_id", q.RunId);
            dyn.Add("kind", k);
            dyn.Add("from_utc", fromUtc);
            dyn.Add("to_utc", toUtc);

            for (int i = 0; i < pmuList.Count; i++)
                dyn.Add($"pmu{i}", pmuList[i]);

            var rows = (await db.QueryAsync<SeqPosRow>(sql, dyn)).ToList();

            if (rows.Count == 0)
                return Results.NotFound("Nenhuma PMU encontrada para este run/kind.");

            // ======================================================
            // Utilitário local: merge ponto a ponto com tolerância
            // ======================================================
            static List<(DateTime ts, double ratio)> RatioPointwise(
                List<(DateTime ts, double mag)> neg,
                List<(DateTime ts, double mag)> pos,
                TimeSpan tolerance)
            {
                var outp = new List<(DateTime ts, double ratio)>();
                int i = 0, j = 0;

                while (i < neg.Count && j < pos.Count)
                {
                    var tn = neg[i].ts;
                    var tp = pos[j].ts;

                    // alinha pelo maior timestamp (mesma lógica do seu compute)
                    var t = tn > tp ? tn : tp;

                    while (i < neg.Count && neg[i].ts < t && (t - neg[i].ts) > tolerance) i++;
                    while (j < pos.Count && pos[j].ts < t && (t - pos[j].ts) > tolerance) j++;

                    if (i >= neg.Count || j >= pos.Count) break;

                    tn = neg[i].ts;
                    tp = pos[j].ts;

                    if (Math.Abs((tn - t).TotalMilliseconds) > tolerance.TotalMilliseconds ||
                        Math.Abs((tp - t).TotalMilliseconds) > tolerance.TotalMilliseconds)
                    {
                        var minT = tn < tp ? tn : tp;
                        if (minT == tn) i++;
                        else j++;
                        continue;
                    }

                    var den = pos[j].mag;
                    if (den > 0)
                        outp.Add((t, neg[i].mag / den));

                    i++; j++;
                }

                return outp;
            }

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

                vaMod.Sort((a, b) => a.ts.CompareTo(b.ts));
                vbMod.Sort((a, b) => a.ts.CompareTo(b.ts));
                vcMod.Sort((a, b) => a.ts.CompareTo(b.ts));
                vaAng.Sort((a, b) => a.ts.CompareTo(b.ts));
                vbAng.Sort((a, b) => a.ts.CompareTo(b.ts));
                vcAng.Sort((a, b) => a.ts.CompareTo(b.ts));

                // calcula |seq+| e |seq-|
                var seqPos = ComputeSequenceMagnitudeMedPlot(vaMod, vbMod, vcMod, vaAng, vbAng, vcAng, "pos");
                var seqNeg = ComputeSequenceMagnitudeMedPlot(vaMod, vbMod, vcMod, vaAng, vbAng, vcAng, "neg");

                if (seqPos.Count == 0 || seqNeg.Count == 0)
                    {
                        // Opção B: não retorna série nula (front não quebra)
                        series.Add(new
                        {
                            pmu = sigRows.First().Id_Name,
                            pdc = sigRows.First().Pdc_Name,
                            volt_level = q.VoltLevel ?? sigRows.First().Volt_Level,
                            unit = "percent",
                            points = Array.Empty<object>() // <- NUNCA null
                        });
                        continue;
                    }

                    // base pu (mesma regra que você já usa)
                    var first = sigRows.First();
                    double baseValue = 1.0;

                    if (u == "pu" && k == "voltage")
                    {
                        double lvl = q.VoltLevel ?? first.Volt_Level ?? 0;
                        if (lvl > 0)
                            baseValue = lvl / Math.Sqrt(3.0);
                    }
                    else if (u == "pu" && k == "current")
                    {
                        baseValue = 1.0;
                    }

                    const double EPS = 1e-12;

                    // verifica se há algo diferente de zero
                    bool anyPos = seqPos.Any(p => Math.Abs(p.mag) > EPS);
                    bool anyNeg = seqNeg.Any(p => Math.Abs(p.mag) > EPS);

                    // caso 1: tudo zero → não há desequilíbrio definido
                    if (!anyPos && !anyNeg)
                    {
                        series.Add(new
                        {
                            pmu = first.Id_Name,
                            pdc = first.Pdc_Name,
                            volt_level = q.VoltLevel ?? first.Volt_Level,
                            unit = "percent",
                            points = Array.Empty<object>() // <- NUNCA null
                        });
                        continue;
                    }

                    // caso 3: seq+ toda zero → divisão indefinida → ignora PMU (mas devolve vazio p/ não quebrar front)
                    if (!anyPos)
                    {
                        series.Add(new
                        {
                            pmu = first.Id_Name,
                            pdc = first.Pdc_Name,
                            volt_level = q.VoltLevel ?? first.Volt_Level,
                            unit = "percent",
                            points = Array.Empty<object>() // <- NUNCA null
                        });
                        continue;
                    }

                    // caso normal → processa ratio completo
                    double Unit(double m) => (u == "pu") ? m / baseValue : m;

                    var ratio = RatioPointwise(
                        seqNeg.Select(p => (p.ts, Unit(p.mag))).ToList(),
                        seqPos.Select(p => (p.ts, Unit(p.mag))).ToList(),
                        TimeSpan.FromMilliseconds(3));

                    if (ratio.Count == 0)
                    {
                        series.Add(new
                        {
                            pmu = first.Id_Name,
                            pdc = first.Pdc_Name,
                            volt_level = q.VoltLevel ?? first.Volt_Level,
                            unit = "percent",
                            points = Array.Empty<object>() // <- NUNCA null
                        });
                        continue;
                    }

                    // downsample do ratio (mantém sua estratégia)
                    var downs = TimeBucketDownsampleMinMax(
                        ratio.Select(p => (p.ts, p.ratio)),
                        maxPts);

                    series.Add(new
                    {
                        pmu = first.Id_Name,
                        pdc = first.Pdc_Name,
                        volt_level = q.VoltLevel ?? first.Volt_Level,
                        unit = "percent", // ratio é adimensional; manter percent aqui casa com *100 no points
                        points = downs.Select(d => new object[]
                        {
            d.ts,
            d.val * 100.0 // percent
                        })
                    });

            }

            if (series.Count == 0)
                return Results.BadRequest("Nenhuma PMU pôde ser processada.");

            var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
            var windowTo = toUtc ?? rows.Max(r => r.Ts);

            // DIA UTC DA CONSULTA
            var data = windowFrom
                .Date
                .ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

            return Results.Ok(new
            {
                run_id = q.RunId,
                data,
                kind = k,
                metric = "unbalance", // |seq-| / |seq+| * 100
                pmu_count = series.Count,
                window = new
                {
                    from = windowFrom,
                    to = windowTo
                },
                series
            });


        });




        // -----------------------------------------
        // 6) /series/frequency/by-run  (Frequência)
        // -----------------------------------------
        grp.MapGet("/series/frequency/by-run",
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

            // FIX: alias correto é "c" (ctx c), não "pmu"
            var pmuFilter = !string.IsNullOrWhiteSpace(pmuName)
                ? "LOWER(c.id_name) = LOWER(@pmu)"
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

            var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
            var windowTo = toUtc ?? rows.Max(r => r.Ts);

            // DIA UTC DA CONSULTA
            var data = windowFrom
            .Date
            .ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

            return Results.Ok(new
            {
                run_id = q.RunId,
                data,
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
        // 7) /series/dfreq/by-run  (Derivada da frequência)
        // -----------------------------------------
        grp.MapGet("/series/dfreq/by-run",
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
  SELECT id, source AS pdc_name, from_ts, to_ts, COALESCE(pmus_ok, pmus) AS pmus, signals
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
            var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
            var windowTo = toUtc ?? rows.Max(r => r.Ts);

            // DIA UTC DA CONSULTA
            var data = windowFrom
            .Date
            .ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

            return Results.Ok(new
            {
                run_id = q.RunId,
                data,
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

        // -----------------------------------------------
        // X) /series/thd/by-run  (THD de tensão ou corrente)
        // -----------------------------------------------
        grp.MapGet("/series/thd/by-run",
        async Task<IResult> (
            [AsParameters] ByRunQuery q,
            [FromQuery] string kind,            // "voltage" | "current"
            [FromServices] IDbConnectionFactory dbf,
            [FromQuery] DateTime? from,
            [FromQuery] DateTime? to
        ) =>
        {
            var tri = q.Tri;
            var pmuName = q.Pmu?.Trim();

            // ---------------------------
            // Validação: kind obrigatório
            // ---------------------------
            if (string.IsNullOrWhiteSpace(kind))
                return Results.BadRequest("kind é obrigatório (voltage|current).");

            var k = kind.Trim().ToLowerInvariant();
            if (k is not ("voltage" or "current"))
                return Results.BadRequest("kind deve ser 'voltage' ou 'current'.");

            // ---------------------------
            // Validação de parâmetros (fase/tri/pmu)
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

            // Clausula de fase dinâmica
            var phaseClause = tri
                ? "UPPER(s.phase::text) IN ('A','B','C')"          // trifásico
                : "UPPER(s.phase::text) = UPPER(@phase)";          // uma fase só

            // Filtro de PMU (usado no modo tri, opcional no mono)
            var pmuFilter = !string.IsNullOrWhiteSpace(pmuName)
                ? "LOWER(pmu.id_name) = LOWER(@pmu)"
                : "TRUE";

            // Filtro de grandeza (kind)
            var qtyClause = k == "voltage"
                ? "LOWER(s.quantity::text) IN ('voltage','v')"
                : "LOWER(s.quantity::text) IN ('current','i')";

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
                .Replace("{PMU_FILTER}", pmuFilter)
                .Replace("{QTY_CLAUSE}", qtyClause);

            var rows = (await db.QueryAsync<(
                int Signal_Id, int Pdc_Pmu_Id, string Phase, string Component,
                string Id_Name, string Pdc_Name, DateTime Ts, double Value
            )>(sql, new
            {
                run_id = q.RunId,
                phase = uphase,      // ignorado se tri=true
                from_utc = fromUtc,
                to_utc = toUtc,
                pmu = pmuName
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
                            component = any.Component,
                            kind = k
                        },
                        unit = "%", // THD normalmente é percentual
                        points = downs.Select(p => new object[] { p.ts, p.val })
                    };
                })
                .ToList();

            var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
            var windowTo = toUtc ?? rows.Max(r => r.Ts);

            var data = windowFrom
                .Date
                .ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

            return Results.Ok(new
            {
                run_id = q.RunId,
                data,
                tri = tri,
                phase = tri ? "ABC" : uphase,
                kind = k,
                unit = "%",
                window = new { from = windowFrom, to = windowTo },
                resolved = new
                {
                    pdc = rows.First().Pdc_Name,
                    pmu_count = series.Select(s => s.pmu).Distinct().Count()
                },
                series
            });
        });

        // -----------------------------------------
        // X) /series/digital/by-run  (Digital)
        // -----------------------------------------
        grp.MapGet("/series/digital/by-run",
        async Task<IResult> (
            [AsParameters] DigitalRunQuery q,
            [FromQuery] string[]? pmu,
            [FromServices] IDbConnectionFactory dbf,
            [FromQuery] DateTime? from,
            [FromQuery] DateTime? to
        ) =>
        {
            // =============================
            // lista de PMUs (opcional)
            // =============================
            var pmuList = pmu?
                .Select(p => p.Trim())
                .Where(p => !string.IsNullOrWhiteSpace(p))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList()
                ?? new List<string>();

            var maxPts = Math.Max(q.MaxPoints, 100);

            // =============================
            // janela temporal
            // =============================
            DateTime? fromUtc = from?.ToUniversalTime();
            DateTime? toUtc = to?.ToUniversalTime();

            if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
                return Results.BadRequest("from < to");

            using var db = dbf.Create();

            // =============================
            // filtro dinâmico de PMU
            // (alias correto é 'c', igual freq)
            // =============================
            var pmuFilter = pmuList.Count > 0
                ? "LOWER(c.id_name) = ANY(@pmu_names)"
                : "TRUE";

            const string sqlTemplate = @"
WITH run AS (
  SELECT id, source AS pdc_name, from_ts, to_ts,
         COALESCE(pmus_ok, pmus) AS pmus, signals
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

  UNION ALL
  SELECT r.pdc_name, r.from_ts, r.to_ts, p.pmu_id, p.id_name
  FROM elems r
  JOIN LATERAL (SELECT NULLIF(r.elem->>'signal_id','')::int AS key_signal_id) k ON TRUE
  JOIN openplot.signal s ON s.signal_id = k.key_signal_id
  JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id = s.pdc_pmu_id
  JOIN openplot.pmu p ON p.pmu_id = ppm.pmu_id
),
pdc_ctx AS (
  SELECT w.pdc_name, w.from_ts, w.to_ts, pdc.pdc_id
  FROM src w
  JOIN openplot.pdc pdc ON LOWER(pdc.name) = LOWER(w.pdc_name)
),
ctx AS (
  SELECT pc.pdc_name, pc.from_ts, pc.to_ts,
         pid.id_name, pid.pmu_id, pc.pdc_id
  FROM pdc_ctx pc
  JOIN pmu_ids pid ON pid.pdc_name = pc.pdc_name
),
sig AS (
  SELECT s.signal_id, s.pdc_pmu_id,
         c.id_name, c.pdc_name
  FROM ctx c
  JOIN openplot.pdc_pmu pp ON pp.pdc_id = c.pdc_id AND pp.pmu_id = c.pmu_id
  JOIN openplot.signal s   ON s.pdc_pmu_id = pp.pdc_pmu_id
  WHERE LOWER(s.quantity::text) IN ('digital','dig')
    AND LOWER(s.component::text) IN ('dig','digital')
    AND {PMU_FILTER}
),
raw AS (
  SELECT m.signal_id, m.ts, m.value
  FROM openplot.measurements m
  WHERE m.ts >= (SELECT from_utc FROM win)
    AND m.ts <= (SELECT to_utc   FROM win)
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
                pmu_names = pmuList.Select(p => p.ToLowerInvariant()).ToArray()
            })).ToList();

            if (rows.Count == 0)
                return Results.NotFound("Nenhum digital encontrado para esse run_id.");

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
                        unit = "raw",
                        points = downs.Select(p => new object[] { p.ts, p.val })
                    };
                })
                .ToList();

            var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
            var windowTo = toUtc ?? rows.Max(r => r.Ts);

            var data = windowFrom
                .Date
                .ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

            return Results.Ok(new
            {
                run_id = q.RunId,
                data,
                unit = "raw",
                resolved = new
                {
                    pdc = rows.First().Pdc_Name,
                    pmu_count = series.Select(s => s.pmu).Distinct().Count()
                },
                window = new { from = windowFrom, to = windowTo },
                series
            });
        });




        // -----------------------------------------
        // 8) /series/power/by-run  (P ou Q)
        // -----------------------------------------
        grp.MapGet("/series/power/by-run",
        async Task<IResult> (
            [AsParameters] PowerPlotQuery q,
            [FromServices] IDbConnectionFactory dbf,
            [FromQuery] DateTime? from,
            [FromQuery] DateTime? to
        ) =>
        {
            // =============================
            // which (active|reactive)
            // =============================
            var which = (q.Which ?? "active").Trim().ToLowerInvariant();
            if (which is not ("active" or "reactive"))
                return Results.BadRequest("which deve ser 'active' ou 'reactive'.");

            // =============================
            // unit (raw|mw)
            // =============================
            var u = (q.Unit ?? "raw").Trim().ToLowerInvariant();
            if (u is not ("raw" or "mw"))
                return Results.BadRequest("unit deve ser 'raw' ou 'mw'.");

            var maxPts = Math.Max(q.MaxPoints, 100);

            // janela
            DateTime? fromUtc = from?.ToUniversalTime();
            DateTime? toUtc = to?.ToUniversalTime();
            if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
                return Results.BadRequest("from < to");

            // lista PMU
            var pmuList = (q.Pmu ?? Array.Empty<string>())
                .Select(s => s?.Trim())
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            // =============================
            // valida modo
            // =============================
            var tri = q.Tri ?? false;
            var total = q.Total ?? false; // <- aqui estoura


            string? phase = null;

            if (tri && total)
                return Results.BadRequest("tri=true e total=true são modos mutuamente exclusivos.");
            if (tri)
            {
                if (pmuList.Count != 1)
                    return Results.BadRequest("tri=true exige exatamente 1 pmu (id_name).");
            }
            else
            {
                if (!total)
                {
                    if (string.IsNullOrWhiteSpace(q.Phase))
                        return Results.BadRequest("phase é obrigatório (A|B|C) quando tri=false e total=false.");

                    phase = q.Phase.Trim().ToUpperInvariant();
                    if (phase is not ("A" or "B" or "C"))
                        return Results.BadRequest("phase deve ser A, B ou C.");
                }

                if (pmuList.Count == 0)
                    return Results.BadRequest("tri=false exige ao menos 1 pmu (id_name).");
            }

            // =============================
            // SQL: traz V/I MAG/ANG
            // =============================
            using var db = dbf.Create();

            string pmuFilter = pmuList.Count == 0
                ? "TRUE"
                : string.Join(" OR ", pmuList.Select((_, i) => $"LOWER(c.id_name) = LOWER(@pmu{i})"));

            // se tri=true OU total=true -> precisa A,B,C
            // senão (mono por fase) -> só a fase pedida
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
            dyn.Add("run_id", q.RunId);
            dyn.Add("from_utc", fromUtc);
            dyn.Add("to_utc", toUtc);
            dyn.Add("phase", phase);

            for (int i = 0; i < pmuList.Count; i++)
                dyn.Add($"pmu{i}", pmuList[i]);

            var rows = (await db.QueryAsync<PowerRow>(sql, dyn)).ToList();
            if (rows.Count == 0)
                return Results.NotFound("Nada encontrado para esse run/filtro no intervalo solicitado.");

            // =============================
            // helpers locais (P/Q por fase)
            // =============================
            static List<(DateTime ts, double val)> ComputePower1Phase(
                List<(DateTime ts, double val)> vMag,
                List<(DateTime ts, double val)> vAng,
                List<(DateTime ts, double val)> iMag,
                List<(DateTime ts, double val)> iAng,
                TimeSpan tol,
                string which
            )

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
                        // avança o menor timestamp dos módulos
                        var min = vMag[ivm].ts < iMag[iim].ts ? vMag[ivm].ts : iMag[iim].ts;
                        if (min == vMag[ivm].ts) ivm++; else iim++;
                        continue;
                    }

                    var s = vMag[ivm].val * iMag[iim].val;
                    var d = (vAng[iva].val - iAng[iia].val) * Deg2Rad;


                    var val = (which == "active") ? (s * Math.Cos(d)) : (s * Math.Sin(d));
                    outp.Add((t, val));

                    ivm++; iim++; iva++; iia++;
                }

                return outp;
            }

            static List<(DateTime ts, double val)> Sum3PhasePointwise(
                List<(DateTime ts, double val)> a,
                List<(DateTime ts, double val)> b,
                List<(DateTime ts, double val)> c,
                TimeSpan tol
            )
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

            // =============================
            // processamento
            // =============================
            var tol = TimeSpan.FromMilliseconds(3);
            var seriesOut = new List<object>();

            foreach (var pmuGroup in rows.GroupBy(r => r.Id_Name))
            {
                // coletor por chave
                static void Add(Dictionary<string, List<(DateTime ts, double v)>> d, string k, DateTime ts, double v)
                {
                    if (!d.TryGetValue(k, out var list)) d[k] = list = new();
                    list.Add((ts, v));
                }

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
                    Add(d, $"{qn}_{phs}_{cmp}", r.Ts, r.Value);
                }

                // função para pegar listas e validar
                bool Need(string key) => d.ContainsKey(key) && d[key].Count > 0;

                List<(DateTime ts, double val)> MakePhase(string phs)
                {
                    var vMagK = $"voltage_{phs}_MAG";
                    var vAngK = $"voltage_{phs}_ANG";
                    var iMagK = $"current_{phs}_MAG";
                    var iAngK = $"current_{phs}_ANG";

                    if (!Need(vMagK) || !Need(vAngK) || !Need(iMagK) || !Need(iAngK))
                        return new List<(DateTime ts, double val)>();

                    return ComputePower1Phase(
                        d[vMagK].Select(x => (x.ts, vMag: x.v)).ToList(),
                        d[vAngK].Select(x => (x.ts, vAng: x.v)).ToList(),
                        d[iMagK].Select(x => (x.ts, iMag: x.v)).ToList(),
                        d[iAngK].Select(x => (x.ts, iAng: x.v)).ToList(),
                        tol,
                        which
                    );
                }

                double scale =  1e-6; // Retorna sempre em MW
                var any = pmuGroup.First();

                if (tri)
                {
                    // 3 séries (A,B,C) para 1 PMU
                    foreach (var phs in new[] { "A", "B", "C" })
                    {
                        var pts = MakePhase(phs);
                        if (pts.Count == 0) continue;

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
                    // 1 série total = A+B+C
                    var aPts = MakePhase("A");
                    var bPts = MakePhase("B");
                    var cPts = MakePhase("C");
                    if (aPts.Count == 0 || bPts.Count == 0 || cPts.Count == 0) continue;

                    var sum = Sum3PhasePointwise(aPts, bPts, cPts, tol);
                    if (sum.Count == 0) continue;

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
                    // mono por fase (A|B|C)
                    var pts = MakePhase(phase!);
                    if (pts.Count == 0) continue;

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
            var data = windowFrom.Date.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

            return Results.Ok(new
            {
                run_id = q.RunId,
                data,
                which,
                unit = u,
                tri,
                total,
                phase,
                pmu_count = seriesOut.Count,
                window = new { from = windowFrom, to = windowTo },
                series = seriesOut
            });
        });

        // ---------------------------------------------
        // 4) /series/angle-diff/by-run  (Δângulo: PMU - referência)
        // ---------------------------------------------
        //
        // Regras:
        // - kind: voltage|current
        // - mode:
        //    - phase (A|B|C)      -> usa componente ANG da fase
        //    - seq (pos|neg|zero) -> calcula sequência (ANG) via MAG+ANG A/B/C e depois faz Δ
        // - precisa de ref (PMU referência)
        // - Δ = Wrap180(meas - ref)
        // - missing: igual MedPlot (se não há par, replica último válido; no início, procura o primeiro válido)
        //
        // Observação: usa alias correto "c" no filtro de PMU (igual você corrigiu em frequency/digital)

        grp.MapGet("/series/angle-diff/by-run",
        async Task<IResult> (
            [AsParameters] ByRunQuery q,
            [FromQuery] string kind,
            [FromQuery] string @ref,
            [FromQuery] string? phase,
            [FromQuery] string? seq,
            [FromQuery] string[]? pmu,
            [FromServices] IDbConnectionFactory dbf,
            [FromQuery] DateTime? from,
            [FromQuery] DateTime? to
        ) =>
{
    // =============================
    // valida kind
    // =============================
    if (string.IsNullOrWhiteSpace(kind))
        return Results.BadRequest("kind é obrigatório (voltage|current).");

    var k = kind.Trim().ToLowerInvariant();
    if (k != "voltage" && k != "current")
        return Results.BadRequest("kind deve ser 'voltage' ou 'current'.");

    // =============================
    // valida ref
    // =============================
    if (string.IsNullOrWhiteSpace(@ref))
        return Results.BadRequest("ref é obrigatório (id_name da PMU referência).");

    var refPmu = @ref.Trim();



    // =============================
    // valida modo (phase XOR seq)
    // =============================
    var hasPhase = !string.IsNullOrWhiteSpace(phase);
            var hasSeq = !string.IsNullOrWhiteSpace(seq);

            if (hasPhase == hasSeq)
                return Results.BadRequest("informe exatamente um dos parâmetros: phase (A|B|C) OU seq (pos|neg|zero).");

            string? uphase = null;
            string? seqNorm = null;

            if (hasPhase)
            {
                uphase = phase!.Trim().ToUpperInvariant();
                if (uphase is not ("A" or "B" or "C"))
                    return Results.BadRequest("phase deve ser A, B ou C.");
            }
            else
            {
                seqNorm = seq!.Trim().ToLowerInvariant();
                seqNorm = seqNorm switch
                {
                    "pos" or "seq+" or "1" => "pos",
                    "neg" or "seq-" or "2" => "neg",
                    "zero" or "seq0" or "0" => "zero",
                    _ => ""
                };
                if (seqNorm == "")
                    return Results.BadRequest("seq inválida. Use pos|neg|zero (ou seq+|seq-|seq0).");
            }

            // =============================
            // pmu list (alvos) opcional
            // =============================
            var pmuList = pmu?
                .Select(p => p.Trim())
                .Where(p => p.Length > 0)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList()
                ?? new List<string>();

            // sempre exclui a ref dos alvos (evita série Δ=0 desnecessária)
            pmuList = pmuList
                .Where(p => !p.Equals(refPmu, StringComparison.OrdinalIgnoreCase))
                .ToList();

            var maxPts = Math.Max(q.MaxPoints, 100);

            // =============================
            // janela temporal
            // =============================
            DateTime? fromUtc = from?.ToUniversalTime();
            DateTime? toUtc = to?.ToUniversalTime();
            if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
                return Results.BadRequest("from < to");

            using var db = dbf.Create();

            // ======================================================
            // SQL BASE
            // - modo phase: traz só ANG da fase
            // - modo seq: traz MAG+ANG de A/B/C (para calcular V/I seq no backend)
            // ======================================================
            var needMagAndAng = hasSeq;

            var componentClause = needMagAndAng
                ? "UPPER(s.component::text) IN ('MAG','ANG')"
                : "UPPER(s.component::text) IN ('ANG')";

            var phaseClause = hasSeq
                ? "UPPER(s.phase::text) IN ('A','B','C')"
                : "UPPER(s.phase::text) = UPPER(@phase)";

            // filtro de alvos (opcional). Se vazio: pega todos do run (inclui ref) e depois filtra em memória.
            // mas precisamos garantir que ref entre no resultado -> OR com ref sempre.
            // se pmuList vazio: mantém TRUE (vem tudo), ok.
            // se pmuList não vazio: (c.id_name IN pmuList) OR (c.id_name = ref)
            string pmuFilter;
            if (pmuList.Count == 0)
                pmuFilter = "TRUE";
            else
                pmuFilter = "(LOWER(c.id_name) = ANY(@pmu_names) OR LOWER(c.id_name) = LOWER(@pmu_ref))";

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
  WHERE ({PMU_FILTER})
    AND (
      (@kind = 'voltage' AND LOWER(s.quantity::text) IN ('voltage','v'))
      OR
      (@kind = 'current' AND LOWER(s.quantity::text) IN ('current','i'))
    )
    AND {PHASE_CLAUSE}
    AND {COMPONENT_CLAUSE}
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

            var sql = sqlTemplate
                .Replace("{PMU_FILTER}", pmuFilter)
                .Replace("{PHASE_CLAUSE}", phaseClause)
                .Replace("{COMPONENT_CLAUSE}", componentClause);

            // params
            var dyn = new DynamicParameters();
            dyn.Add("run_id", q.RunId);
            dyn.Add("kind", k);
            dyn.Add("from_utc", fromUtc);
            dyn.Add("to_utc", toUtc);
            dyn.Add("phase", uphase);
            dyn.Add("pmu_ref", refPmu.ToLowerInvariant());

            if (pmuList.Count > 0)
                dyn.Add("pmu_names", pmuList.Select(x => x.ToLowerInvariant()).ToArray());

            var rows = (await db.QueryAsync<SeqPosRow>(sql, dyn)).ToList();

            if (rows.Count == 0)
                return Results.NotFound("Nenhuma série encontrada para este run/filtros.");

            // ======================================================
            // helpers locais: Δ ângulo com missing estilo MedPlot
            // ======================================================
            static List<(DateTime ts, double difDeg)> ComputeAngleDiffMedPlot(
                List<(DateTime ts, double angDeg)> meas,
                List<(DateTime ts, double angDeg)> refe,
                TimeSpan tol)
            {
                meas.Sort((a, b) => a.ts.CompareTo(b.ts));
                refe.Sort((a, b) => a.ts.CompareTo(b.ts));

                int im = 0, ir = 0;
                var outp = new List<(DateTime ts, double difDeg)>();
                double difBack = 0.0;
                bool hasBack = false;

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
                        // missing -> replica último valor válido (se ainda não há, fica "sem ponto" até achar o primeiro par válido)
                        var minT = tm < tr ? tm : tr;
                        if (minT == tm) im++; else ir++;
                        continue;
                    }

                    var dif = Wrap180(meas[im].angDeg - refe[ir].angDeg);
                    outp.Add((t, dif));
                    difBack = dif;
                    hasBack = true;

                    im++; ir++;
                }

                // Se você quiser exatamente o "preenche com difBack" também quando só um lado falta,
                // isso deve ser feito num "timeline master". Aqui mantemos: só gera quando há par alinhado,
                // e o front vê gaps. (normalmente MedPlot replica na mesma linha; aqui não temos essa linha)
                // Se você realmente quer replicar SEM criar novos timestamps, mantenha como está.

                return outp;
            }

            var tol = TimeSpan.FromMilliseconds(3);

            // ======================================================
            // monta as séries da referência
            // ======================================================
            var refRows = rows
                .Where(r => (r.Id_Name ?? "").Equals(refPmu, StringComparison.OrdinalIgnoreCase))
                .ToList();

            if (refRows.Count == 0)
                return Results.BadRequest("PMU de referência não encontrada dentro do run/filtros.");

            List<(DateTime ts, double angDeg)> refAngSeries;

            if (hasPhase)
            {
                // fase: já veio só ANG
                refAngSeries = refRows
                    .Where(r => (r.Component ?? "").Equals("ANG", StringComparison.OrdinalIgnoreCase))
                    .Select(r => (r.Ts, r.Value))
                    .OrderBy(x => x.Ts)
                    .ToList();
            }
            else
            {
                // seq: precisa montar A/B/C MAG+ANG e calcular sequência
                var rvaMod = new List<(DateTime ts, double mag)>();
                var rvbMod = new List<(DateTime ts, double mag)>();
                var rvcMod = new List<(DateTime ts, double mag)>();
                var rvaAng = new List<(DateTime ts, double angDeg)>();
                var rvbAng = new List<(DateTime ts, double angDeg)>();
                var rvcAng = new List<(DateTime ts, double angDeg)>();

                foreach (var r in refRows)
                {
                    var ph = (r.Phase ?? "").ToUpperInvariant();
                    var cp = (r.Component ?? "").ToUpperInvariant();

                    if (ph == "A" && cp == "MAG") rvaMod.Add((r.Ts, r.Value));
                    else if (ph == "A" && cp == "ANG") rvaAng.Add((r.Ts, r.Value));
                    else if (ph == "B" && cp == "MAG") rvbMod.Add((r.Ts, r.Value));
                    else if (ph == "B" && cp == "ANG") rvbAng.Add((r.Ts, r.Value));
                    else if (ph == "C" && cp == "MAG") rvcMod.Add((r.Ts, r.Value));
                    else if (ph == "C" && cp == "ANG") rvcAng.Add((r.Ts, r.Value));
                }

                if (rvaMod.Count == 0 || rvbMod.Count == 0 || rvcMod.Count == 0 ||
                    rvaAng.Count == 0 || rvbAng.Count == 0 || rvcAng.Count == 0)
                    return Results.BadRequest("Referência não possui MAG/ANG trifásico completo (A/B/C) para calcular sequência.");

                rvaMod.Sort((a, b) => a.ts.CompareTo(b.ts));
                rvbMod.Sort((a, b) => a.ts.CompareTo(b.ts));
                rvcMod.Sort((a, b) => a.ts.CompareTo(b.ts));
                rvaAng.Sort((a, b) => a.ts.CompareTo(b.ts));
                rvbAng.Sort((a, b) => a.ts.CompareTo(b.ts));
                rvcAng.Sort((a, b) => a.ts.CompareTo(b.ts));

                refAngSeries = ComputeSequenceAngleMedPlot(rvaMod, rvbMod, rvcMod, rvaAng, rvbAng, rvcAng, seqNorm!);
            }

            if (refAngSeries.Count == 0)
                return Results.BadRequest("Não foi possível calcular série de referência (ângulo).");

            // ======================================================
            // processa cada PMU alvo e calcula Δ
            // ======================================================
            IEnumerable<IGrouping<string, SeqPosRow>> targetGroups;

            if (pmuList.Count > 0)
            {
                targetGroups = rows
                    .Where(r => pmuList.Contains(r.Id_Name ?? "", StringComparer.OrdinalIgnoreCase))
                    .GroupBy(r => r.Id_Name!);
            }
            else
            {
                // todas do run exceto ref
                targetGroups = rows
                    .Where(r => !(r.Id_Name ?? "").Equals(refPmu, StringComparison.OrdinalIgnoreCase))
                    .GroupBy(r => r.Id_Name!);
            }

            var series = new List<object>();

            foreach (var g in targetGroups)
            {
                var sigRows = g.ToList();
                var first = sigRows.First();

                List<(DateTime ts, double angDeg)> measAngSeries;

                if (hasPhase)
                {
                    measAngSeries = sigRows
                        .Where(r => (r.Component ?? "").Equals("ANG", StringComparison.OrdinalIgnoreCase))
                        .Select(r => (r.Ts, r.Value))
                        .OrderBy(x => x.Ts)
                        .ToList();
                }
                else
                {
                    var vaMod = new List<(DateTime ts, double mag)>();
                    var vbMod = new List<(DateTime ts, double mag)>();
                    var vcMod = new List<(DateTime ts, double mag)>();
                    var vaAng = new List<(DateTime ts, double angDeg)>();
                    var vbAng = new List<(DateTime ts, double angDeg)>();
                    var vcAng = new List<(DateTime ts, double angDeg)>();

                    foreach (var r in sigRows)
                    {
                        var ph = (r.Phase ?? "").ToUpperInvariant();
                        var cp = (r.Component ?? "").ToUpperInvariant();

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

                    vaMod.Sort((a, b) => a.ts.CompareTo(b.ts));
                    vbMod.Sort((a, b) => a.ts.CompareTo(b.ts));
                    vcMod.Sort((a, b) => a.ts.CompareTo(b.ts));
                    vaAng.Sort((a, b) => a.ts.CompareTo(b.ts));
                    vbAng.Sort((a, b) => a.ts.CompareTo(b.ts));
                    vcAng.Sort((a, b) => a.ts.CompareTo(b.ts));

                    measAngSeries = ComputeSequenceAngleMedPlot(vaMod, vbMod, vcMod, vaAng, vbAng, vcAng, seqNorm!);
                }

                if (measAngSeries.Count == 0)
                    continue;

                var dif = ComputeAngleDiffMedPlot(measAngSeries, refAngSeries, tol);
                if (dif.Count == 0)
                    continue;

                var downs = TimeBucketDownsampleMinMax(dif.Select(x => (x.ts, x.difDeg)), maxPts);

                series.Add(new
                {
                    pmu = first.Id_Name,
                    pdc = first.Pdc_Name,
                    reference = refPmu,
                    kind = k,
                    mode = hasPhase ? "phase" : "sequence",
                    phase = hasPhase ? uphase : null,
                    seq = hasSeq ? seqNorm : null,
                    unit = "deg",
                    points = downs.Select(p => new object[] { p.ts, p.val })
                });
            }

            if (series.Count == 0)
                return Results.BadRequest("Nenhuma PMU pôde ser processada (faltam sinais ou alinhamento falhou).");

            var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
            var windowTo = toUtc ?? rows.Max(r => r.Ts);

            var data = windowFrom
                .Date
                .ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

            return Results.Ok(new
            {
                run_id = q.RunId,
                data,
                kind = k,
                reference = refPmu,
                mode = hasPhase ? "phase" : "sequence",
                phase = hasPhase ? uphase : null,
                seq = hasSeq ? seqNorm : null,
                unit = "deg",
                pmu_count = series.Count,
                window = new { from = windowFrom, to = windowTo },
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
    private static List<(DateTime ts, double mag)> ComputeSequenceMagnitudeMedPlot(
    List<(DateTime ts, double mag)> vaMod,
    List<(DateTime ts, double mag)> vbMod,
    List<(DateTime ts, double mag)> vcMod,
    List<(DateTime ts, double angDeg)> vaAng,
    List<(DateTime ts, double angDeg)> vbAng,
    List<(DateTime ts, double angDeg)> vcAng,
    string seq) // 'pos', 'neg', 'zero'
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

            Complex Vseq = seq switch
            {
                "pos" => (Va + a * Vb + a2 * Vc) / 3.0, // sequência positiva
                "neg" => (Va + a2 * Vb + a * Vc) / 3.0, // sequência negativa
                "zero" => (Va + Vb + Vc) / 3.0, // sequência zero
                _ => throw new ArgumentException("seq deve ser: pos | neg | zero")
            };

            result.Add((maxTime, Vseq.Magnitude));

            ia++;
            ib++;
            ic++;
        }

        return result;
    }
    static double Wrap180(double difDeg)
    {
        if (difDeg > 180.0) return difDeg - 360.0;
        if (difDeg < -180.0) return difDeg + 360.0;
        return difDeg;
    }


    private static List<(DateTime ts, double angDeg)> ComputeSequenceAngleMedPlot(
        List<(DateTime ts, double mag)> vaMod,
        List<(DateTime ts, double mag)> vbMod,
        List<(DateTime ts, double mag)> vcMod,
        List<(DateTime ts, double angDeg)> vaAng,
        List<(DateTime ts, double angDeg)> vbAng,
        List<(DateTime ts, double angDeg)> vcAng,
        string seq // 'pos', 'neg', 'zero'
    )
    {
        var result = new List<(DateTime ts, double angDeg)>();

        if (vaMod.Count == 0 || vbMod.Count == 0 || vcMod.Count == 0 ||
            vaAng.Count == 0 || vbAng.Count == 0 || vcAng.Count == 0)
            return result;

        var tolerance = TimeSpan.FromMilliseconds(3);

        int ia = 0, ib = 0, ic = 0;

        const double Deg2Rad = Math.PI / 180.0;
        const double Rad2Deg = 180.0 / Math.PI;

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

            while (ia < vaMod.Count && vaMod[ia].ts < maxTime && (maxTime - vaMod[ia].ts) > tolerance) ia++;
            while (ib < vbMod.Count && vbMod[ib].ts < maxTime && (maxTime - vbMod[ib].ts) > tolerance) ib++;
            while (ic < vcMod.Count && vcMod[ic].ts < maxTime && (maxTime - vcMod[ic].ts) > tolerance) ic++;

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

            Complex Vseq = seq switch
            {
                "pos" => (Va + a * Vb + a2 * Vc) / 3.0,
                "neg" => (Va + a2 * Vb + a * Vc) / 3.0,
                "zero" => (Va + Vb + Vc) / 3.0,
                _ => throw new ArgumentException("seq deve ser: pos | neg | zero")
            };

            var ang = Vseq.Phase * Rad2Deg; // (-180, +180]
            result.Add((maxTime, ang));

            ia++; ib++; ic++;
        }

        return result;
    }



}
