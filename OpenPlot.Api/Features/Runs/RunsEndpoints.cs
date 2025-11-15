using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Mvc;             // <- [FromServices], [FromQuery]
using Dapper;
using System.Data;
using OpenPlot.Data.Dtos;

public static class RunsEndpoints
{
    public static IEndpointRouteBuilder MapRuns(this IEndpointRouteBuilder app)
    {
        app.MapGet("/terminais/{nomeBusca}", async (
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
WITH run AS (SELECT id, signals, pmus FROM openplot.search_runs WHERE id = @id),
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
        THEN ARRAY['A','B','C','Trifásico']::text[]
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

        return app;
    }
}
