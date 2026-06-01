using Microsoft.AspNetCore.Mvc;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Handlers;
using OpenPlot.Services.UI;

public static class RunsAdvancedSeriesEndpoints
{
    public static RouteGroupBuilder MapRunsAdvancedSeries(this RouteGroupBuilder group)
    {
        group.MapPost("/series/thd/by-run", async (
            [FromBody] SeriesByRunRequest req,
            [FromServices] ThdSeriesHandler handler,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var query = RunsEndpoints.BuildThdByRunQuery(req);
            var window = RunsSimpleSeriesEndpoints.BuildWindowQuery(req.From, req.To);
            var modes = RunsEndpoints.BuildOscillationModes(uiMenus);
            return await handler.HandleAsync(query, window, req.Kind ?? "voltage", modes, ct);
        });

        group.MapPost("/series/power/by-run", async (
            [FromBody] PowerSeriesByRunRequest req,
            [FromServices] PowerSeriesHandler handler,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var query = RunsEndpoints.BuildPowerPlotQuery(req);
            var window = RunsSimpleSeriesEndpoints.BuildWindowQuery(req.From, req.To);
            var modes = RunsEndpoints.BuildOscillationModes(uiMenus);
            return await handler.HandleAsync(query, window, modes, ct);
        });

        group.MapPost("/series/angle-diff/by-run", async (
            [FromBody] AngleDiffSeriesByRunRequest req,
            [FromServices] AngleDiffSeriesHandler handler,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var query = RunsEndpoints.BuildAngleDiffQuery(req);
            var window = RunsSimpleSeriesEndpoints.BuildWindowQuery(req.From, req.To);
            var modes = RunsEndpoints.BuildOscillationModes(uiMenus);

            return await handler.HandleAsync(query, window, req.Pmu, modes, ct);
        });

        return group;
    }
}
