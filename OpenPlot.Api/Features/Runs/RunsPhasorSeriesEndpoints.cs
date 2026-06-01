using Microsoft.AspNetCore.Mvc;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Handlers;
using OpenPlot.Services.UI;

public static class RunsPhasorSeriesEndpoints
{
    public static RouteGroupBuilder MapRunsPhasorSeries(this RouteGroupBuilder group)
    {
        group.MapPost("/series/voltage/by-run", async (
            [FromBody] SeriesByRunRequest req,
            [FromServices] VoltageSeriesHandler handler,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var query = RunsEndpoints.BuildByRunQuery(req);
            var window = RunsSimpleSeriesEndpoints.BuildWindowQuery(req.From, req.To);
            var modes = RunsEndpoints.BuildOscillationModes(uiMenus);
            return await handler.HandleAsync(query, window, req.Pmu, modes, ct);
        });

        group.MapPost("/series/current/by-run", async (
            [FromBody] SeriesByRunRequest req,
            [FromServices] CurrentSeriesHandler handler,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var query = RunsEndpoints.BuildByRunQuery(req);
            var window = RunsSimpleSeriesEndpoints.BuildWindowQuery(req.From, req.To);
            var modes = RunsEndpoints.BuildOscillationModes(uiMenus);
            return await handler.HandleAsync(query, window, req.Pmu, modes, ct);
        });

        return group;
    }
}
