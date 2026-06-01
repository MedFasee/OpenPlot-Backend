using System.Data;
using Data.Sql;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using OpenPlot.Data.Dtos;

public static class RunsTerminalEndpoints
{
    private const string RunNotFoundByIdError = "search_run_not_found_by_id";
    private const string RunNotFoundByLabelError = "search_run_not_found_by_label";

    private const string TerminalPmusSql = @"
    WITH run AS (
      SELECT id, source, signals, COALESCE(pmus_ok, pmus) AS pmus
      FROM openplot.search_runs
      WHERE id = @id
    ),
    src AS (
      SELECT CASE
               WHEN jsonb_typeof(signals) = 'array' AND jsonb_array_length(signals) > 0 THEN signals
               WHEN jsonb_typeof(pmus)    = 'array' AND jsonb_array_length(pmus)    > 0 THEN pmus
               ELSE '[]'::jsonb
             END AS arr,
             source
      FROM run
    ),
    elems AS (
      SELECT jsonb_array_elements(arr) AS elem, source
      FROM src
    ),

    pmus_from_string AS (
      SELECT DISTINCT p.pmu_id, p.id_name, p.full_name, p.volt_level, p.area, p.state, p.station, ppm.kind
      FROM elems e
      JOIN openplot.pmu p ON p.id_name = btrim(e.elem::text, '""')
      LEFT JOIN openplot.pdc pd ON pd.name = e.source
      LEFT JOIN openplot.pdc_pmu ppm ON ppm.pdc_id = pd.pdc_id AND ppm.pmu_id = p.pmu_id
      WHERE jsonb_typeof(e.elem) = 'string'
    ),

    pmus_direct AS (
      SELECT DISTINCT p.pmu_id, p.id_name, p.full_name, p.volt_level, p.area, p.state, p.station, ppm.kind
      FROM elems e
      JOIN LATERAL (
        SELECT NULLIF(TRIM(e.elem->>'pmu'), '')     AS key_pmu,
               NULLIF(TRIM(e.elem->>'id_name'), '') AS key_idname
      ) k ON TRUE
      JOIN openplot.pmu p ON p.id_name = COALESCE(k.key_pmu, k.key_idname)
      LEFT JOIN openplot.pdc pd ON pd.name = e.source
      LEFT JOIN openplot.pdc_pmu ppm ON ppm.pdc_id = pd.pdc_id AND ppm.pmu_id = p.pmu_id
      WHERE jsonb_typeof(e.elem) = 'object'
        AND COALESCE(k.key_pmu, k.key_idname) IS NOT NULL
    ),

    pmus_by_pdcpmu AS (
      SELECT DISTINCT p.pmu_id, p.id_name, p.full_name, p.volt_level, p.area, p.state, p.station, ppm.kind
      FROM elems e
      JOIN LATERAL (SELECT NULLIF(e.elem->>'pdc_pmu_id','')::int AS key_pdc_pmu_id) k ON TRUE
      JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id = k.key_pdc_pmu_id
      JOIN openplot.pmu p       ON p.pmu_id       = ppm.pmu_id
      WHERE jsonb_typeof(e.elem) = 'object'
    ),

    pmus_by_signal AS (
      SELECT DISTINCT p.pmu_id, p.id_name, p.full_name, p.volt_level, p.area, p.state, p.station, ppm.kind
      FROM elems e
      JOIN LATERAL (SELECT NULLIF(e.elem->>'signal_id','')::int AS key_signal_id) k ON TRUE
      JOIN openplot.signal  s   ON s.signal_id    = k.key_signal_id
      JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id = s.pdc_pmu_id
      JOIN openplot.pmu     p   ON p.pmu_id       = ppm.pmu_id
      WHERE jsonb_typeof(e.elem) = 'object'
    ),

    pmus_by_point AS (
      SELECT DISTINCT p.pmu_id, p.id_name, p.full_name, p.volt_level, p.area, p.state, p.station, ppm.kind
      FROM elems e
      JOIN LATERAL (SELECT NULLIF(e.elem->>'historian_point','')::int AS key_point) k ON TRUE
      JOIN openplot.signal  s   ON s.historian_point = k.key_point
      JOIN openplot.pdc_pmu ppm ON ppm.pdc_pmu_id    = s.pdc_pmu_id
      JOIN openplot.pmu     p   ON p.pmu_id          = ppm.pmu_id
      WHERE jsonb_typeof(e.elem) = 'object'
    ),

    pmus_union AS (
      SELECT
        pmu_id,
        id_name,
        full_name,
        volt_level,
        area,
        state,
        station,
        MAX(kind) AS kind
      FROM (
        SELECT * FROM pmus_from_string
        UNION ALL SELECT * FROM pmus_direct
        UNION ALL SELECT * FROM pmus_by_pdcpmu
        UNION ALL SELECT * FROM pmus_by_signal
        UNION ALL SELECT * FROM pmus_by_point
      ) u
      GROUP BY pmu_id, id_name, full_name, volt_level, area, state, station
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
      pu.kind,
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
             pu.station    NULLS LAST;";

    internal static async Task<SearchRunFull?> ResolveSearchRunAsync(
        IDbConnection db,
        Guid? id,
        string nomeBusca,
        ILabelService labels)
    {
        if (id is Guid validId && validId != Guid.Empty)
        {
            return await db.QueryFirstOrDefaultAsync<SearchRunFull>(
                SearchSql.GetRunById,
                new { id = validId });
        }

        var rows = await db.QueryAsync<SearchRunFull>(SearchSql.ListRecentDone);
        return rows.FirstOrDefault(row =>
            labels.BuildLabel(row.from_ts, row.to_ts, row.select_rate, row.source, row.terminal_id) == nomeBusca);
    }

    internal static object BuildSearchRunNotFoundError(Guid? id) => new
    {
        status = 404,
        error = id is Guid validId && validId != Guid.Empty
            ? RunNotFoundByIdError
            : RunNotFoundByLabelError
    };

    internal static async Task<List<PmuMeta>> LoadRunPmusAsync(IDbConnection db, Guid runId)
    {
        var pmuRows = (await db.QueryAsync<PmuMetaRow>(TerminalPmusSql, new { id = runId })).ToList();
        return pmuRows.Select(MapPmuMeta).ToList();
    }

    internal static object BuildTerminalsResponse(
        SearchRunFull run,
        IReadOnlyList<PmuMeta> pmus,
        object hierarchy,
        ILabelService labels)
    {
        var fromUtc = run.from_ts.Kind == DateTimeKind.Utc ? run.from_ts : run.from_ts.ToUniversalTime();
        var toUtc = run.to_ts.Kind == DateTimeKind.Utc ? run.to_ts : run.to_ts.ToUniversalTime();

        return new
        {
            xml_file = run.source,
            total_terminais = pmus.Count,
            nome_busca = labels.BuildLabel(run.from_ts, run.to_ts, run.select_rate, run.source, run.terminal_id),
            resolutionSearch = $"{run.select_rate}",
            from = fromUtc.ToString("O"),
            to = toUtc.ToString("O"),
            terminais = hierarchy
        };
    }

    public static RouteGroupBuilder MapRunsTerminals(this RouteGroupBuilder group)
    {
        group.MapGet("/terminals/{nomeBusca}", async (
            string nomeBusca,
            [FromQuery] Guid? id,
            [FromServices] IDbConnectionFactory dbf,
            [FromServices] ILabelService labels,
            [FromServices] IPmuHierarchyService pmuHierarchy
        ) =>
        {
            using var db = dbf.Create();

            var run = await ResolveSearchRunAsync(db, id, nomeBusca, labels);
            if (run is null)
                return Results.Json(BuildSearchRunNotFoundError(id), statusCode: 404);

            var pmus = await LoadRunPmusAsync(db, run.id);
            var hierarchy = pmuHierarchy.BuildHierarchy(pmus);
            var data = BuildTerminalsResponse(run, pmus, hierarchy, labels);

            return Results.Json(new { status = 200, data });
        });

        return group;
    }

    private static PmuMeta MapPmuMeta(PmuMetaRow row)
    {
        var adicionais = new List<PmuAdicional>();
        var thdFases = row.thd_fases is { Count: > 0 }
            ? row.thd_fases
            : Array.Empty<string>();

        if (row.has_thd_v)
        {
            adicionais.Add(new PmuAdicional
            {
                TipoMedida = "Medidas Analógicas",
                Grandeza = "THD de Tensão",
                Fase = thdFases
            });
        }

        if (row.has_thd_i)
        {
            adicionais.Add(new PmuAdicional
            {
                TipoMedida = "Medidas Analógicas",
                Grandeza = "THD de Corrente",
                Fase = thdFases
            });
        }

        if (row.has_digital)
        {
            adicionais.Add(new PmuAdicional
            {
                TipoMedida = "Medidas Digitais",
                Grandeza = "Digital",
                Fase = Array.Empty<string>()
            });
        }

        return new PmuMeta
        {
            pmu_id = row.pmu_id,
            id_name = row.id_name,
            full_name = row.full_name,
            volt_level = row.volt_level,
            area = row.area,
            state = row.state,
            station = row.station,
            kind = row.kind,
            Grandezas = row.grandezas ?? Array.Empty<string>(),
            Fases = row.fases ?? Array.Empty<string>(),
            Adicionais = adicionais
        };
    }
}
