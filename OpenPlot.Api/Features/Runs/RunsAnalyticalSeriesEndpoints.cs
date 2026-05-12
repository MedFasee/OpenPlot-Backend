using Microsoft.AspNetCore.Mvc;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Handlers;
using OpenPlot.Services.UI;

public static class RunsAnalyticalSeriesEndpoints
{
    public static RouteGroupBuilder MapRunsAnalyticalSeries(this RouteGroupBuilder group)
    {
        group.MapPost("/series/seq/by-run", async (
            [FromBody] SeqSeriesByRunRequest req,
            [FromServices] SeqSeriesHandler handler,
            [FromServices] IPmuQueryHelper pmuHelper,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var query = RunsEndpoints.BuildSeqRunQuery(req);
            var window = RunsSimpleSeriesEndpoints.BuildWindowQuery(req.From, req.To);
            var pmuList = RunsSimpleSeriesEndpoints.BuildNormalizedPmuList(pmuHelper, req.Pmu);
            var request = RunsEndpoints.BuildSeqRequest(query);
            var modes = RunsEndpoints.BuildOscillationModes(uiMenus);

            return await handler.HandleAsync(query, request, window, pmuList, modes, ct);
        });

        group.MapPost("/series/unbalance/by-run", async (
            [FromBody] UnbalanceSeriesByRunRequest req,
            [FromServices] UnbalanceSeriesHandler handler,
            [FromServices] IPmuQueryHelper pmuHelper,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var query = RunsEndpoints.BuildUnbalanceRunQuery(req);
            var window = RunsSimpleSeriesEndpoints.BuildWindowQuery(req.From, req.To);
            var pmuList = RunsSimpleSeriesEndpoints.BuildNormalizedPmuList(pmuHelper, req.Pmu);
            var request = RunsEndpoints.BuildUnbalanceRequest(query);
            var modes = RunsEndpoints.BuildOscillationModes(uiMenus);

            return await handler.HandleAsync(query, request, window, pmuList, modes, ct);
        });

        return group;
    }
}
