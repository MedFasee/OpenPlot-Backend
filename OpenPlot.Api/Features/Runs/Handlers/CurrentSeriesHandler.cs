using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Services.UI;

namespace OpenPlot.Features.Runs.Handlers;

public sealed class CurrentSeriesHandler
{
    private readonly IRunContextRepository _runs;
    private readonly IMeasurementsRepository _meas;
    private readonly IPlotMetaBuilder _meta;
    private readonly IPhasorRequestService _phasorRequest;
    private readonly ISeriesAssemblyService _seriesAssembly;
    private readonly ITimeSeriesDownsampler _down = new TimeBucketMinMaxDownsampler();
    private readonly IAnalysisCacheRepository _cacheRepo;
    private readonly IUiMenuService _uiMenus;
    private readonly ILogger<CurrentSeriesHandler> _logger;

    public CurrentSeriesHandler(
        IRunContextRepository runs,
        IMeasurementsRepository meas,
        IPlotMetaBuilder meta,
        IPhasorRequestService phasorRequest,
        ISeriesAssemblyService seriesAssembly,
        IAnalysisCacheRepository cacheRepo,
        IUiMenuService uiMenus,
        ILogger<CurrentSeriesHandler> logger)
    {
        _runs = runs;
        _meas = meas;
        _meta = meta;
        _phasorRequest = phasorRequest;
        _seriesAssembly = seriesAssembly;
        _cacheRepo = cacheRepo;
        _uiMenus = uiMenus;
        _logger = logger;
    }

    // Recebe modes já resolvidos no endpoint
    public async Task<IResult> HandleAsync(
        ByRunQuery q,
        WindowQuery w,
        string[]? pmu,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        static string PmuShort(string? idName)
        {
            if (string.IsNullOrWhiteSpace(idName)) return "";
            var s = idName.Trim();
            var i = s.IndexOf('|');
            return i >= 0 ? s[..i].Trim() : s;
        }

        var normalized = _phasorRequest.Resolve(q, pmu);
        if (!normalized.IsValid)
            return Results.BadRequest(normalized.Error);

        var selection = normalized.Selection!;
        var tri = selection.Tri;
        var pmuName = selection.TriPmuName;
        var uphase = selection.Phase;

        var noDownsample = q.MaxPointsIsAll;
        var maxPts = q.ResolveMaxPoints(@default: 5000);

        var fromUtc = w.FromUtc;
        var toUtc = w.ToUtc;
        if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
            return Results.BadRequest("from < to");

        var ctx = await _runs.ResolveAsync(q.RunId, fromUtc, toUtc, ct);
        if (ctx is null) return Results.NotFound("run_id não encontrado.");

        var pmuNames = selection.PmuNames;

        var meas = new MeasurementsQuery(
            Quantity: "current",
            Component: "mag",
            PhaseMode: tri ? PhaseMode.ThreePhase : PhaseMode.Single,
            Phase: uphase,
            PmuNames: tri
                ? new[] { pmuName }
                : (pmuNames.Length > 0 ? pmuNames : null),
            Unit: "A"
        );

        var frontWatch = Stopwatch.StartNew();
        _logger.LogInformation("[BYRUN][Current][FRONT][START] runId={RunId} maxPoints={MaxPoints}", q.RunId, noDownsample ? "all" : maxPts);

        var frontRows = await _meas.QueryPhasorAsync(ctx, meas, ct, noDownsample ? null : maxPts);

        frontWatch.Stop();
        _logger.LogInformation("[BYRUN][Current][FRONT][END] runId={RunId} elapsedMs={ElapsedMs} rows={Rows}", q.RunId, frontWatch.ElapsedMilliseconds, frontRows.Count);

        if (frontRows.Count == 0)
            return Results.NotFound("Nada encontrado para esse run/filtro no intervalo solicitado.");

        var windowFrom = fromUtc ?? frontRows.Min(r => r.Ts);
        var windowTo2 = toUtc ?? frontRows.Max(r => r.Ts);
        var cacheId = Guid.NewGuid();

        _ = Task.Run(async () =>
        {
            var bgWatch = Stopwatch.StartNew();
            var fullRowsCount = 0;
            var persisted = false;
            _logger.LogInformation("[BYRUN][Current][CACHE-BG][START] runId={RunId} cacheId={CacheId}", q.RunId, cacheId);

            try
            {
                var fullRows = await _meas.QueryPhasorAsync(ctx, meas, CancellationToken.None, null);
                fullRowsCount = fullRows.Count;
                if (fullRowsCount == 0)
                    return;

                var fullWindowFrom = fromUtc ?? fullRows.Min(r => r.Ts);
                var fullWindowTo = toUtc ?? fullRows.Max(r => r.Ts);

                var cacheSeriesFull = fullRows
                    .GroupBy(r => new
                    {
                        r.SignalId,
                        Phase = (r.Phase ?? "").Trim(),
                        Component = (r.Component ?? "").Trim(),
                        r.PdcPmuId,
                        r.IdName,
                        r.PdcName
                    })
                    .Select(g =>
                    {
                        var first = g.First();
                        return _seriesAssembly.BuildCacheSeries(
                            signalId: first.SignalId,
                            pdcPmuId: first.PdcPmuId,
                            idName: first.IdName,
                            pdcName: first.PdcName,
                            referenceTerminal: null,
                            unit: "A",
                            phase: first.Phase,
                            quantity: "current",
                            component: first.Component,
                            points: g.Select(x => (x.Ts, x.Value)));
                    })
                    .OrderBy(s => s.IdName, StringComparer.OrdinalIgnoreCase)
                    .ThenBy(s => s.Phase, StringComparer.OrdinalIgnoreCase)
                    .ThenBy(s => s.Component, StringComparer.OrdinalIgnoreCase)
                    .ToList();

                var cachePayloadFull = _seriesAssembly.BuildCachePayload(
                    fullWindowFrom,
                    fullWindowTo,
                    ctx.SelectRate ?? 0,
                    cacheSeriesFull);

                await _cacheRepo.SaveAsync(cacheId, q.RunId, cachePayloadFull, CancellationToken.None);
                persisted = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Falha ao persistir cache assíncrono de current/by-run. runId={RunId}", q.RunId);
            }
            finally
            {
                bgWatch.Stop();
                _logger.LogInformation("[BYRUN][Current][CACHE-BG][END] runId={RunId} cacheId={CacheId} elapsedMs={ElapsedMs} rows={Rows} persisted={Persisted}", q.RunId, cacheId, bgWatch.ElapsedMilliseconds, fullRowsCount, persisted);
            }
        });

        var series = frontRows
            .GroupBy(r => r.SignalId)
            .Select(g =>
            {
                var any = g.First();

                var points = _seriesAssembly.BuildPoints(
                    g.Select(x => (x.Ts, x.Value)),
                    true,
                    maxPts,
                    _down);

                return new
                {
                    pmu = PmuShort(any.IdName), // <<< até o primeiro '|'
                    pdc = any.PdcName,
                    signal_id = any.SignalId,
                    pdc_pmu_id = any.PdcPmuId,
                    meta = new
                    {
                        phase = (any.Phase ?? "").Trim().ToUpperInvariant(),
                        component = (any.Component ?? "").Trim().ToUpperInvariant()
                    },
                    points
                };
            })
            .ToList();

        var plotMeta = _meta.Build(w, ctx, meas);
        var selectedPmuCount = frontRows
            .Select(row => row.IdName)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Count();
        var resolvedModes = _uiMenus.RebuildForRun(
            modes,
            new UiMenuContext(
                WindowFromUtc: windowFrom,
                WindowToUtc: windowTo2,
                SelectRate: ctx.SelectRate,
                TotalSeriesCount: selectedPmuCount,
                ValidSeriesCount: selectedPmuCount,
                Quantity: "current",
                Component: "mag",
                Phase: tri ? "abc" : uphase?.Trim().ToLowerInvariant()));

        var response = SeriesResponseBuilderExtensions
            .BuildSeriesResponse(q.RunId, windowFrom, windowTo2, series, plotMeta)
            .WithModes(resolvedModes)
            .WithCacheId(cacheId)
            .WithResolved(ctx.PdcName, series.Select(s => s.pmu).Distinct(StringComparer.OrdinalIgnoreCase).Count())
            .WithTypeFields(new Dictionary<string, object?>
            {
                ["unit"] = "raw",
                ["tri"] = tri,
                ["phase"] = tri ? "ABC" : uphase
            })
            .Build();

        return Results.Ok(response);
    }
}