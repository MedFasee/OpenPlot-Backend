using Dapper;
using Data.Sql;
using Microsoft.AspNetCore.Mvc;
using OpenPlot.Api.Services.Security;
using OpenPlot.Data.Dtos;
using static ConfigEndpoints;

public static class RunsListingEndpoints
{
    internal static Dictionary<string, Dictionary<string, Dictionary<string, List<SearchRunItem>>>> BuildRunsCalendar(
        IEnumerable<SearchRunRow> rows,
        ILabelService labels)
    {
        var calendar = new Dictionary<string, Dictionary<string, Dictionary<string, List<SearchRunItem>>>>();

        foreach (var row in rows)
        {
            var label = labels.BuildLabel(row.from_ts, row.to_ts, row.select_rate, row.source, row.terminal_id);
            var fromUtc = DateTime.SpecifyKind(row.from_ts, DateTimeKind.Utc);

            var year = fromUtc.Year.ToString("0000");
            var month = fromUtc.Month.ToString("00");
            var day = fromUtc.Day.ToString("00");

            if (!calendar.TryGetValue(year, out var months))
                calendar[year] = months = new();

            if (!months.TryGetValue(month, out var days))
                months[month] = days = new();

            if (!days.TryGetValue(day, out var items))
                days[day] = items = new();

            items.Add(RunsEndpoints.CreateSearchRunItem(row, label));
        }

        return calendar;
    }

    public static RouteGroupBuilder MapRunsListing(this RouteGroupBuilder group)
    {
        group.MapGet("/runs", async (
            HttpContext http,
            [FromServices] IUserContextAccessor userContextAccessor,
            [FromServices] IDbConnectionFactory dbf,
            [FromQuery] string? status,
            [FromServices] ITimeService time,
            [FromServices] ILabelService labels
        ) =>
        {
            var username = userContextAccessor.GetUsername(http);

            if (string.IsNullOrWhiteSpace(username))
                return Results.Unauthorized();

            using var db = dbf.Create();
            var rows = await db.QueryAsync<SearchRunRow>(
                SearchSql.ListRuns,
                new { status, username });

            var data = BuildRunsCalendar(rows, labels);
            return Results.Json(new { status = 200, data });
        });

        return group;
    }
}
