using System.Data;
using System.Globalization;
using System.Numerics;
using System.Text;
using Dapper;
using Data.Sql;
using Microsoft.AspNetCore.Mvc;             // <- [FromServices], [FromQuery]
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Features.Ui;

using static ConfigEndpoints;

// DTO para teste de avarias
public class RunIngestaTest
{
    public Guid id { get; set; }
    public string? ids { get; set; }
}

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
                        id = r.id.ToString(),

                        shared = r.shared,
                        owner = r.owner
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
            THEN ARRAY['TrifÁSico','A','B','C']::text[]
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




        grp.MapGet("/series/voltage/by-run", async (
            [AsParameters] ByRunQuery q,
            [AsParameters] WindowQuery w,
            [FromQuery] string[]? pmu,
            [FromServices] VoltageSeriesHandler handler,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var ui = uiMenus.Build(UiMenuSet.Oscillations);
            return await handler.HandleAsync(q, w, ui, ct);
        });

        grp.MapGet("/series/current/by-run", async (
            [AsParameters] ByRunQuery q,
            [AsParameters] WindowQuery w,
            [FromQuery] string[]? pmu,
            [FromServices] CurrentSeriesHandler handler,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var modes = uiMenus.Build(UiMenuSet.Oscillations);
            return await handler.HandleAsync(q, w, pmu, modes, ct);
        });

        grp.MapGet("/series/seq/by-run", async (
            [AsParameters] SeqRunQuery q,
            [AsParameters] WindowQuery w,
            [FromQuery] string[]? pmu,
            [FromServices] SeqSeriesHandler handler,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var pmuList = (pmu ?? Array.Empty<string>())
                .Select(x => x.Trim())
                .Where(x => x.Length > 0)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            var req = new SeqRequest(
                Kind: (q.Kind ?? "").Trim().ToLowerInvariant() == "current" ? SeqKind.Current : SeqKind.Voltage,
                Seq: (q.Seq ?? "").Trim().ToLowerInvariant() switch
                {
                    "pos" or "seq+" or "1" => SeqType.Pos,
                    "neg" or "seq-" or "2" => SeqType.Neg,
                    "zero" or "seq0" or "0" => SeqType.Zero,
                    _ => throw new BadHttpRequestException("seq inválida (pos|neg|zero).")
                });

            var ui = uiMenus.Build(UiMenuSet.Oscillations);
            return await handler.HandleAsync(q, req, w, pmuList, ui, ct);
        });

        grp.MapGet("/series/unbalance/by-run", async (
            [AsParameters] UnbalanceRunQuery q,
            [AsParameters] WindowQuery w,
            [FromQuery] string[]? pmu,
            [FromServices] UnbalanceSeriesHandler handler,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var pmuList = (pmu ?? Array.Empty<string>())
                .Select(x => x.Trim())
                .Where(x => x.Length > 0)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            var req = new UnbalanceRequest(
                Kind: (q.Kind ?? "").Trim().ToLowerInvariant() == "current" ? SeqKind.Current : SeqKind.Voltage
            );

            var ui = uiMenus.Build(UiMenuSet.Oscillations);
            return await handler.HandleAsync(q, req, w, pmuList, ui, ct);
        });

        grp.MapGet("/series/frequency/by-run", async (
            [AsParameters] SimpleSeriesQuery q,
            [AsParameters] WindowQuery w,
            [FromQuery] string[]? pmu,
            [FromServices] SimpleSeriesHandler handler,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var pmuList = (pmu ?? Array.Empty<string>())
                .Select(x => x.Trim())
                .Where(x => x.Length > 0)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            var meas = new MeasurementsQuery(
                Quantity: "frequency",
                Component: "freq",
                PhaseMode: PhaseMode.Any,
                Phase: null,
                PmuNames: pmuList,
                Unit: "Hz"
            );

            var ui = uiMenus.Build(UiMenuSet.Oscillations | UiMenuSet.Events);
            return await handler.HandleAsync(q, w, meas, ui, ct);
        });

        grp.MapGet("/series/dfreq/by-run", async (
            [AsParameters] SimpleSeriesQuery q,
            [AsParameters] WindowQuery w,
            [FromQuery] string[]? pmu,
            [FromServices] SimpleSeriesHandler handler,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var pmuList = (pmu ?? Array.Empty<string>())
                .Select(x => x.Trim())
                .Where(x => x.Length > 0)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            var meas = new MeasurementsQuery(
                Quantity: "frequency",
                Component: "dfreq",
                PhaseMode: PhaseMode.Any,
                Phase: null,
                PmuNames: pmuList,
                Unit: "Hz/s"
            );

            var ui = uiMenus.Build(UiMenuSet.Oscillations);
            return await handler.HandleAsync(q, w, meas, ui, ct);
        });

        // -----------------------------------------------
        // /series/thd/by-run  (THD de tensão ou corrente)
        // -----------------------------------------------
        grp.MapGet("/series/thd/by-run",
        async (
            [AsParameters] ByRunQuery q,
            [AsParameters] WindowQuery w,
            [FromQuery] string kind,
            [FromServices] ThdSeriesHandler handler,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var modes = uiMenus.Build(UiMenuSet.Oscillations);
            return await handler.HandleAsync(q, w, kind, modes, ct);
        });

        // -----------------------------------------
        // X) /series/digital/by-run  (Digital)
        // -----------------------------------------
        grp.MapGet("/series/digital/by-run",
        async (
        [AsParameters] SimpleSeriesQuery q,
        [AsParameters] WindowQuery w,
        [FromQuery] string[]? pmu,
        [FromServices] SimpleSeriesHandler handler,
        [FromServices] IUiMenuService uiMenus,
        CancellationToken ct
    ) =>
        {
            var pmuList = (pmu ?? [])
                .Select(x => x.Trim())
                .Where(x => x.Length > 0)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            var meas = new MeasurementsQuery(
                Quantity: "digital",
                Component: "dig",
                PhaseMode: PhaseMode.Any,
                Phase: null,
                PmuNames: pmuList
            );

            var modes = uiMenus.Build(UiMenuSet.Oscillations);
            return await handler.HandleAsync(q, w, meas, modes, ct);
        });



        // -----------------------------------------
        // /series/power/by-run  (P ou Q)
        // -----------------------------------------
        grp.MapGet("/series/power/by-run",
        async (
            [AsParameters] PowerPlotQuery q,
            [AsParameters] WindowQuery w,
            [FromServices] PowerSeriesHandler handler,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var modes = uiMenus.Build(UiMenuSet.Oscillations);
            return await handler.HandleAsync(q, w, modes, ct);
        });

        // -----------------------------------------------
        // /series/angle-diff/by-run  (Δângulo: PMU - referência)
        // -----------------------------------------------
        // Calcula diferença de fase angular entre PMU de referência e PMUs de medição
        // Modos: phase (A|B|C) ou sequence (pos|neg|zero com cálculo de sequências)
        grp.MapGet("/series/angle-diff/by-run",
        async (
            [AsParameters] ByRunQuery q,
            [FromQuery] string kind,
            [FromQuery] string @ref,
            [FromQuery] string? phase,
            [FromQuery] string? seq,
            [FromQuery] string[]? pmu,
            [FromServices] AngleDiffSeriesHandler handler,
            [FromServices] IUiMenuService uiMenus,
            [FromQuery] DateTime? from,
            [FromQuery] DateTime? to,
            CancellationToken ct
        ) =>
        {
            var query = new AngleDiffQuery
            {
                RunId = q.RunId,
                MaxPoints = q.MaxPoints,
                Kind = kind,
                Reference = @ref,
                Phase = phase,
                Sequence = seq
            };

            var window = new WindowQuery(From: from, To: to);
            var modes = uiMenus.Build(UiMenuSet.Oscillations);

            return await handler.HandleAsync(query, window, pmu, modes, ct);
        });

// -----------------------------------------------
// Testes para avarias /series/angle-diff/by-run
// -----------------------------------------------
grp.MapGet("/series/angle-diff/teste", async (
            [FromServices] IDbConnectionFactory dbf,
            [FromServices] ILabelService labels,
            [FromServices] IPmuHierarchyService pmuHierarchy
    ) =>
            {
                using var db = dbf.Create();

                const string baseSql = @"
                  SELECT id, from_ts, to_ts, COALESCE(pmus_ok, pmus) AS pmus
                  FROM openplot.search_runs
                  WHERE id IN ({0})
                ";

                var ids = new[]
                {
                    // sag 3f
                    "c6ea4186-adb6-4698-8b4d-89935aaae6a8",

                    // swell 1f
                    "53dfbd1d-6a40-405e-abfd-5ed645662c7d",

                    // falta medida
                    "c200a6a7-6f11-4d03-b0a6-e2489878cf5d",

                    // thd 3f
                    "682509cb-4e9f-4291-86c2-5876817199c1",

                    // perda sinal 1f
                    "b8ff6681-abfd-4d06-9c06-ef6f24a3727d",

                    // cenário normal
                    "aef7987e-c750-429e-abfd-60e460b95cbb"
                };

                var inSql = string.Format(@"
      SELECT
        id,
        (SELECT STRING_AGG('''' || id, ',') FROM (VALUES {0}) AS x(id)) AS ids
      ", string.Join(",", ids.Select(id => $"('{id}')")));

                var runs = (await db.QueryAsync<RunIngestaTest>(inSql)).ToList();

                foreach (var run in runs)
                {
                    var idsList = run.ids.Split(',', StringSplitOptions.RemoveEmptyEntries);

                    var sb = new StringBuilder();
                    sb.AppendLine($"-- {run.id} ---------");

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
              THEN ARRAY['TrifÁSico','A','B','C']::text[]
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

                    sb.AppendLine($"terminais = {pmus.Count}");

                    foreach (var id in idsList)
                    {
                        var pmu = pmuRows.FirstOrDefault(r => r.id_name == id);

                        if (pmu != null)
                        {
                            sb.AppendLine($" - {id}: {pmu.full_name} ({pmu.area} {pmu.state})");
                        }
                        else
                        {
                            sb.AppendLine($" - {id}: não encontrado.");
                        }
                    }

                    var tmp = sb.ToString();
                    Console.WriteLine(tmp);
                }

                return Results.Text("OK");
            });


                return app;
            }
        }
