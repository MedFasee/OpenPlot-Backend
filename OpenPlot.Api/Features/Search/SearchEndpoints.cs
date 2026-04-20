using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc;           // <- necessário p/ [FromServices], [FromQuery]
using Dapper;
using System.Data;
using Data.Sql;
using System.Text.Json;
using OpenPlot.Data.Dtos;
using System.Security.Claims;

public static class SearchEndpoints
{
    public static IEndpointRouteBuilder MapSearch(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/search")
                     .WithTags("Search").RequireAuthorization();

        // POST /search (legado)
        group.MapPost("", async (
            SearchRequest req,                                    // body
            ClaimsPrincipal user,
            [FromServices] IDbConnectionFactory dbf               // serviço
        ) =>
        {
            if (req is null) return Results.BadRequest("JSON inválido");

            int ParseResolutionToSeconds(string res)
            {
                if (string.IsNullOrWhiteSpace(res)) return 0;
                var s = res.Trim().ToLowerInvariant();
                if (s.EndsWith("ms")) return Math.Max(1, int.Parse(s[..^2]) / 1000);
                if (s.EndsWith("s")) return int.Parse(s[..^1]);
                if (s.EndsWith("min")) return int.Parse(s[..^3]) * 60;
                if (s.EndsWith("m")) return int.Parse(s[..^1]) * 60;
                return int.Parse(s);
            }

            var id = Guid.NewGuid();
            var username = user.Identity?.Name ?? "unknown";

            using var db = dbf.Create();
            const string sql = @"
INSERT INTO openplot.search_runs
  (id, source, terminal_id, signals, from_ts, to_ts, select_rate, status, progress, message, username)
VALUES
  (@id, @source, @terminal_id, to_jsonb(@signals::json), @from, @to, @select_rate, 'queued', 0, 'Na fila', @username);";


            await db.ExecuteAsync(sql, new
            {
                id,
                source = req.Source,
                terminal_id = req.TerminalId,
                signals = JsonSerializer.Serialize(req.Signals),
                from = req.From.ToUniversalTime(),
                to = req.To.ToUniversalTime(),
                select_rate = ParseResolutionToSeconds(req.Resolution),
                username
            });

            return Results.Accepted($"/search/{id}", new { jobId = id });
        });

        group.MapPost("/share", async (
        HttpContext http,
        [FromServices] IDbConnectionFactory dbf,
        [FromBody] ShareRunRequest req
    ) =>
            {
                var username =
                    http.User?.FindFirst("username")?.Value
                    ?? http.User?.Identity?.Name;

                if (string.IsNullOrWhiteSpace(username))
                    return Results.Unauthorized();

                if (req.id == Guid.Empty)
                    return Results.BadRequest("id inválido");

                using var db = dbf.Create();

                var updated = await db.QuerySingleOrDefaultAsync(
                    SearchSql.SetRunShared,
                    new { id = req.id, shared = req.shared, username }
                );

                if (updated is null)
                    return Results.NotFound("run não encontrado (ou não pertence ao usuário).");

                return Results.Ok(new
                {
                    status = 200,
                    data = updated
                });
            });


        group.MapPost("/soft-delete", async (
        HttpContext http,
        [FromServices] IDbConnectionFactory dbf,
        [FromBody] SoftDeleteRun req
    ) =>
            {
                var username = http.User?.FindFirst("unique_name")?.Value
                               ?? http.User?.Identity?.Name;

                if (string.IsNullOrWhiteSpace(username))
                    return Results.Unauthorized();

                if (req.id == Guid.Empty)
                    return Results.BadRequest("id inválido");

                using var db = dbf.Create();

                var updated = await db.QuerySingleOrDefaultAsync(
                    SearchSql.SoftDeleteRun,
                    new { id = req.id, is_visible = req.is_visible, username }
                );

                if (updated is null)
                    return Results.NotFound("run não encontrado (ou não pertence ao usuário).");

                return Results.Ok(new { status = 200, data = updated });
            });


        // POST /search/all (multi-PMU)
        group.MapPost("/all", async (
            SearchReq req,                                       // body
            ClaimsPrincipal user,
            [FromServices] IDbConnectionFactory dbf,
            [FromServices] ILabelService labels
        ) =>
            {
                static int ParseRes(string s)
                {
                    if (string.IsNullOrWhiteSpace(s)) return 0;
                    s = s.Trim().ToLowerInvariant();
                    if (s.EndsWith("ms")) return Math.Max(1, int.Parse(s[..^2]) / 1000);
                    if (s.EndsWith("s")) return int.Parse(s[..^1]);
                    if (s.EndsWith("min")) return int.Parse(s[..^3]) * 60;
                    if (s.EndsWith("m")) return int.Parse(s[..^1]) * 60;
                    return int.Parse(s);
                }

                static bool WantsAllPmus(IReadOnlyCollection<string>? pmus) =>
                    pmus is not null && pmus.Any(p => string.Equals(p?.Trim(), "all", StringComparison.OrdinalIgnoreCase));

                // 1) valida source
                var source = req.Source?.Trim();
                if (string.IsNullOrWhiteSpace(source))
                    return Results.BadRequest("source é obrigatório");

                // 2) normaliza janela
                var fromUtc = req.From.Kind == DateTimeKind.Utc ? req.From : req.From.ToUniversalTime();
                var toUtc = req.To.Kind == DateTimeKind.Utc ? req.To : req.To.ToUniversalTime();
                var rate = ParseRes(req.Resolution ?? "0");

                using var db = dbf.Create();

                // 3) resolve PMUs (all ou lista explícita)
                List<string> pmusResolved;

                if (WantsAllPmus(req.Pmus))
                    {
                        const string sqlAllPmus = @"
                            SELECT
                                m.id_name
                            FROM openplot.pdc AS p
                            JOIN openplot.pdc_pmu AS pp
                              ON p.pdc_id = pp.pdc_id
                            JOIN openplot.pmu AS m
                              ON pp.pmu_id = m.pmu_id
                            WHERE p.name ILIKE @pdcName
                            ORDER BY m.area, m.state, m.volt_level, m.station, m.id_name;";

                        pmusResolved = (await db.QueryAsync<string>(sqlAllPmus, new { pdcName = source })).ToList();

                        if (pmusResolved.Count == 0)
                            return Results.BadRequest(new { error = "Nenhuma PMU encontrada para o PDC", source });
                    }
                else
                {
                    if (req.Pmus is null || req.Pmus.Count == 0)
                        return Results.BadRequest("pmus é obrigatório (ou use pmus=['all'])");

                    // remove lixo/duplicados
                    pmusResolved = req.Pmus
                        .Where(p => !string.IsNullOrWhiteSpace(p))
                        .Select(p => p.Trim())
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .ToList();

                    if (pmusResolved.Count == 0)
                        return Results.BadRequest("pmus é obrigatório (ou use pmus=['all'])");
                }

                // 4) cria run
                var label = labels.BuildLabel(fromUtc, toUtc, rate, source, null);
                var id = Guid.NewGuid();
                var username = user.Identity?.Name ?? "unknown";

                var affected = await db.ExecuteAsync(SearchSql.InsertRunBlind, new
                {
                    id,
                    source,
                    pmus = JsonSerializer.Serialize(pmusResolved),
                    from = fromUtc,
                    to = toUtc,
                    rate,
                    pmu_count = pmusResolved.Count,
                    label,
                    username
                });

                if (affected == 0)
                    return Results.BadRequest(new { error = "PDC não encontrado em openplot.pdc", source_tentado = source });

                return Results.Accepted($"/search/{id}", new { jobId = id, pmus = pmusResolved.Count, label });
            });


        // GET /search/{id}
        group.MapGet("/{id:guid}", async (
            Guid id,
            [FromServices] IDbConnectionFactory dbf
        ) =>
        {
            using var db = dbf.Create();
            const string sql = @"
SELECT id, status, terminal_id, progress, message, source, signals, from_ts, to_ts, created_at, started_at, finished_at
FROM openplot.search_runs WHERE id = @id";

            var row = await db.QuerySingleOrDefaultAsync(sql, new { id });
            return row is null ? Results.NotFound() : Results.Ok(row);
        });

        

        return app;
    }
}
