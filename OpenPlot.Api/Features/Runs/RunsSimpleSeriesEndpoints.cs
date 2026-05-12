using Microsoft.AspNetCore.Mvc;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.UI;

public static class RunsSimpleSeriesEndpoints
{
    internal static WindowQuery BuildWindowQuery(DateTime? from, DateTime? to) =>
        new(from, to);

    internal static SimpleSeriesQuery BuildSimpleSeriesQuery(SeriesByRunRequest request) =>
        new()
        {
            RunId = request.RunId,
            MaxPoints = request.MaxPoints
        };

    internal static IReadOnlyList<string> BuildNormalizedPmuList(IPmuQueryHelper pmuHelper, string[]? pmuNames) =>
        pmuHelper.Normalize(pmuNames).ToList();

    internal static MeasurementsQuery BuildSimpleMeasurementsQuery(
        string quantity,
        string component,
        IReadOnlyList<string>? pmuNames,
        string? unit = null) =>
        new(
            Quantity: quantity,
            Component: component,
            PhaseMode: PhaseMode.Any,
            Phase: null,
            PmuNames: pmuNames,
            Unit: unit);

    internal static Dictionary<string, object?>? BuildOscillationModes(IUiMenuService uiMenus) =>
        uiMenus.Build(UiMenuSet.Oscillations);

    internal static Dictionary<string, object?>? BuildOscillationAndEventModes(IUiMenuService uiMenus) =>
        uiMenus.Build(UiMenuSet.Oscillations | UiMenuSet.Events);

    public static RouteGroupBuilder MapRunsSimpleSeries(this RouteGroupBuilder group)
    {
        group.MapPost("/series/frequency/by-run", async (
            [FromBody] SeriesByRunRequest req,
            [FromServices] SimpleSeriesHandler handler,
            [FromServices] IPmuQueryHelper pmuHelper,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var query = BuildSimpleSeriesQuery(req);
            var window = BuildWindowQuery(req.From, req.To);
            var pmuList = BuildNormalizedPmuList(pmuHelper, req.Pmu);
            var measurement = BuildSimpleMeasurementsQuery(
                quantity: "frequency",
                component: "freq",
                pmuNames: pmuList,
                unit: "Hz");
            var modes = BuildOscillationAndEventModes(uiMenus);

            return await handler.HandleAsync(query, window, measurement, modes, ct);
        });

        group.MapPost("/series/dfreq/by-run", async (
            [FromBody] SeriesByRunRequest req,
            [FromServices] SimpleSeriesHandler handler,
            [FromServices] IPmuQueryHelper pmuHelper,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var query = BuildSimpleSeriesQuery(req);
            var window = BuildWindowQuery(req.From, req.To);
            var pmuList = BuildNormalizedPmuList(pmuHelper, req.Pmu);
            var measurement = BuildSimpleMeasurementsQuery(
                quantity: "frequency",
                component: "dfreq",
                pmuNames: pmuList,
                unit: "Hz/s");
            var modes = BuildOscillationModes(uiMenus);

            return await handler.HandleAsync(query, window, measurement, modes, ct);
        });

        group.MapPost("/series/digital/by-run", async (
            [FromBody] SeriesByRunRequest req,
            [FromServices] SimpleSeriesHandler handler,
            [FromServices] IPmuQueryHelper pmuHelper,
            [FromServices] IUiMenuService uiMenus,
            CancellationToken ct
        ) =>
        {
            var query = BuildSimpleSeriesQuery(req);
            var window = BuildWindowQuery(req.From, req.To);
            var pmuList = BuildNormalizedPmuList(pmuHelper, req.Pmu);
            var measurement = BuildSimpleMeasurementsQuery(
                quantity: "digital",
                component: "dig",
                pmuNames: pmuList);
            var modes = BuildOscillationModes(uiMenus);

            return await handler.HandleAsync(query, window, measurement, modes, ct);
        });

        return group;
    }
}
