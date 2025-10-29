using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc;           // <- necessário p/ [FromServices], [FromQuery]
using Dapper;
using System.Data;
using Data.Sql;
using System.Text.Json;
using OpenPlot.Data.Dtos;

public static class SearchEndpoints
{
    public static IEndpointRouteBuilder MapSearch(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/search");

        // POST /search (legado)
        group.MapPost("/", async (
            SearchRequest req,                                    // body
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
            using var db = dbf.Create();
            const string sql = @"
INSERT INTO openplot.search_runs
  (id, source, terminal_id, signals, from_ts, to_ts, select_rate, status, progress, message)
VALUES
  (@id, @source, @terminal_id, to_jsonb(@signals::json), @from, @to, @select_rate, 'queued', 0, 'Na fila');";

            await db.ExecuteAsync(sql, new
            {
                id,
                source = req.Source,
                terminal_id = req.TerminalId,
                signals = JsonSerializer.Serialize(req.Signals),
                from = req.From.ToUniversalTime(),
                to = req.To.ToUniversalTime(),
                select_rate = ParseResolutionToSeconds(req.Resolution)
            });

            return Results.Accepted($"/search/{id}", new { jobId = id });
        });

        // POST /search/all (multi-PMU)
        group.MapPost("/all", async (
            SearchReq req,                                       // body
            [FromServices] IDbConnectionFactory dbf,
            [FromServices] ILabelService labels
        ) =>
        {
            if (string.IsNullOrWhiteSpace(req.Source) || req.Pmus is null || req.Pmus.Count == 0)
                return Results.BadRequest("source e pmus são obrigatórios");

            int ParseRes(string s)
            {
                if (string.IsNullOrWhiteSpace(s)) return 0;
                s = s.Trim().ToLowerInvariant();
                if (s.EndsWith("ms")) return Math.Max(1, int.Parse(s[..^2]) / 1000);
                if (s.EndsWith("s")) return int.Parse(s[..^1]);
                if (s.EndsWith("min")) return int.Parse(s[..^3]) * 60;
                if (s.EndsWith("m")) return int.Parse(s[..^1]) * 60;
                return int.Parse(s);
            }

            var fromUtc = req.From.Kind == DateTimeKind.Utc ? req.From : req.From.ToUniversalTime();
            var toUtc = req.To.Kind == DateTimeKind.Utc ? req.To : req.To.ToUniversalTime();
            var rate = ParseRes(req.Resolution ?? "0");
            var label = labels.BuildLabel(fromUtc, toUtc, rate, req.Source.Trim(), null);
            var id = Guid.NewGuid();

            using var db = dbf.Create();
            var affected = await db.ExecuteAsync(SearchSql.InsertRunBlind, new
            {
                id,
                source = req.Source.Trim(),
                pmus = JsonSerializer.Serialize(req.Pmus),
                from = fromUtc,
                to = toUtc,
                rate,
                pmu_count = req.Pmus.Count,
                label
            });

            if (affected == 0)
                return Results.BadRequest(new { error = "PDC não encontrado em openplot.pdc", source_tentado = req.Source });

            return Results.Accepted($"/search/{id}", new { jobId = id, pmus = req.Pmus.Count, label });
        });

        // GET /search/{id}
        group.MapGet("/{id:guid}", async (
            Guid id,
            [FromServices] IDbConnectionFactory dbf
        ) =>
        {
            using var db = dbf.Create();
            const string sql = @"
SELECT id, status, terminal_id, progress, message, source, signals, from_ts, to_ts, created_at
FROM openplot.search_runs WHERE id = @id";

            var row = await db.QuerySingleOrDefaultAsync(sql, new { id });
            return row is null ? Results.NotFound() : Results.Ok(row);
        });

        // GET /buscas-realizadas
        app.MapGet("/buscas-realizadas", async (
            [FromServices] IDbConnectionFactory dbf,
            [FromQuery] string? status,
            [FromServices] ITimeService time,
            [FromServices] ILabelService labels
        ) =>
        {
            using var db = dbf.Create();
            var rows = await db.QueryAsync<SearchRunRow>(SearchSql.ListRuns, new { status });

            var calendar = new Dictionary<string, Dictionary<string, Dictionary<string, List<string>>>>();
            var lookup = new Dictionary<string, string>();

            foreach (var r in rows)
            {
                var label = labels.BuildLabel(r.from_ts, r.to_ts, r.select_rate, r.source, r.terminal_id);
                var createdLocal = TimeZoneInfo.ConvertTimeFromUtc(
                    DateTime.SpecifyKind(r.created_at, DateTimeKind.Utc), time.BrazilTz);

                var y = createdLocal.Year.ToString("0000");
                var m = createdLocal.Month.ToString("00");
                var d = createdLocal.Day.ToString("00");

                if (!calendar.TryGetValue(y, out var months)) calendar[y] = months = new();
                if (!months.TryGetValue(m, out var days)) months[m] = days = new();
                if (!days.TryGetValue(d, out var labelsList)) days[d] = labelsList = new();

                labelsList.Add(label);
                if (!lookup.ContainsKey(label)) lookup[label] = r.id.ToString();
            }

            var data = new Dictionary<string, object>(calendar.ToDictionary(k => k.Key, v => (object)v.Value))
            {
                ["lookup"] = lookup
            };

            return Results.Json(new { status = 200, data });
        }).RequireAuthorization();

        return app;
    }
}
