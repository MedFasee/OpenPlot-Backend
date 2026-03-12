using System.Globalization;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Calculations;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Features.Ui;

namespace OpenPlot.Features.Runs.Handlers;

public sealed class VoltageSeriesHandler
{
    private readonly IRunContextRepository _runs;
    private readonly IMeasurementsRepository _meas;
    private readonly IPlotMetaBuilder _meta;
    private readonly ITimeSeriesDownsampler _down = new TimeBucketMinMaxDownsampler();
    private readonly IAnalysisCacheRepository _cacheRepo;

    public VoltageSeriesHandler(IRunContextRepository runs, IMeasurementsRepository meas, IPlotMetaBuilder meta,IAnalysisCacheRepository cacheRepo)
    {
        _runs = runs;
        _meas = meas;
        _meta = meta;
        _cacheRepo = cacheRepo;
    }

    // Mantém compatibilidade (chamadas antigas)
    public Task<IResult> HandleAsync(ByRunQuery q, WindowQuery w, CancellationToken ct)
        => HandleAsync(q, w, modes: null, ct);

    // NOVO: recebe UI (já resolvida no endpoint)
    public async Task<IResult> HandleAsync(ByRunQuery q, WindowQuery w, Dictionary<string, object?>? modes, CancellationToken ct)
    {
        var tri = q.Tri;
        var pmuName = q.Pmu?.Trim();
        string? uphase = null;

        if (!tri)
        {
            if (string.IsNullOrWhiteSpace(q.Phase))
                return Results.BadRequest("phase é obrigatório (A|B|C) quando tri=false.");

            uphase = q.Phase.Trim().ToUpperInvariant();
            if (uphase is not ("A" or "B" or "C"))
                return Results.BadRequest("phase deve ser A, B ou C.");
        }
        else
        {
            if (string.IsNullOrWhiteSpace(pmuName))
                return Results.BadRequest("para tri=true é obrigatório informar pmu (id_name da PMU).");
        }

        var unit = (q.Unit ?? "raw").Trim().ToLowerInvariant();
        if (unit is not ("raw" or "pu"))
            return Results.BadRequest("unit deve ser 'raw' ou 'pu'.");

        var noDownsample = q.MaxPointsIsAll;
        var maxPts = q.ResolveMaxPoints(@default: 5000);

        var fromUtc = w.FromUtc;
        var toUtc = w.ToUtc;
        if (fromUtc.HasValue && toUtc.HasValue && fromUtc >= toUtc)
            return Results.BadRequest("from < to");

        var ctx = await _runs.ResolveAsync(q.RunId, fromUtc, toUtc, ct);
        if (ctx is null) return Results.NotFound("run_id não encontrado.");

        var pmuNames = q.Pmus?
            .Select(x => x?.Trim())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var meas = new MeasurementsQuery(
            Quantity: "voltage",
            Component: "mag",
            PhaseMode: tri ? PhaseMode.ThreePhase : PhaseMode.Single,
            Phase: uphase,
            PmuNames: tri
                ? (string.IsNullOrWhiteSpace(pmuName) ? null : new[] { pmuName })
                : (pmuNames is { Length: > 0 } ? pmuNames : null),
            Unit: unit
        );

        var rows = await _meas.QueryPhasorAsync(ctx, meas, ct);
        if (rows.Count == 0)
            return Results.NotFound("Nada encontrado para esse run/filtro no intervalo solicitado.");

        var windowFrom = fromUtc ?? rows.Min(r => r.Ts);
        var windowTo2 = toUtc ?? rows.Max(r => r.Ts);

        // ===== PROCESSAMENTO DE UNIDADE ANTES DO CACHE =====
        // Se unit == "pu", converte os valores agora (antes de armazenar em cache)
        var processedData = unit == "pu"
            ? rows.Select(r => (r, value: PerUnit.ToVoltagePu(r.Value, r.VoltLevel))).ToList()
            : rows.Select(r => (r, value: r.Value)).ToList();

        var cachePayload = new RowsCacheV2
        {
            From = windowFrom.ToUniversalTime(),
            To = windowTo2.ToUniversalTime(),
            SelectRate = (int)ctx.SelectRate,

            Series = processedData
                .GroupBy(x => new
                {
                    x.r.SignalId,    
                    Phase = (x.r.Phase ?? "").Trim(),
                    Component = (x.r.Component ?? "").Trim(),
                    x.r.PdcPmuId,
                    x.r.IdName,
                    x.r.PdcName
                })
                .Select(g =>
                {
                    var first = g.First();

                    return new RowsCacheSeries
                    {
                        SignalId = first.r.SignalId,
                        PdcPmuId = first.r.PdcPmuId,
                        IdName = first.r.IdName,
                        PdcName = first.r.PdcName,

                        Unit = unit,
                        Phase = first.r.Phase,
                        Quantity = "voltage",
                        Component = first.r.Component,

                        Points = g
                            .OrderBy(x => x.r.Ts)
                            .Select(x => new RowsCachePoint
                            {
                                Ts = x.r.Ts.ToUniversalTime(),
                                Value = x.value
                            })
                            .ToList()
                    };
                })
                .OrderBy(s => s.IdName, StringComparer.OrdinalIgnoreCase)
                .ThenBy(s => s.Phase, StringComparer.OrdinalIgnoreCase)
                .ThenBy(s => s.Component, StringComparer.OrdinalIgnoreCase)
                .ToList()
        };

        var cacheId = await _cacheRepo.SaveAsync(q.RunId, cachePayload, ct);

        // ===== DOWNSAMPLING DEPOIS (PARA VISUALIZAÇÃO) =====
        var series = processedData
            .GroupBy(x => x.r.SignalId)
            .Select(g =>
            {
                var any = g.First();

                // materializa série (já com valores processados)
                var raw = g.Select(x => new Point(x.r.Ts, x.value)).ToList();

                // downsample (ou não)
                var downs = noDownsample ? raw : _down.MinMax(raw, maxPts);

                // converte p/ points (sem pu conversion adicional)
                var points = downs.Select(p => new object[] { p.Ts, p.Val }).ToList();

                return new
                {
                    pmu = any.r.IdName,
                    pdc = any.r.PdcName,
                    signal_id = any.r.SignalId,
                    pdc_pmu_id = any.r.PdcPmuId,
                    meta = new
                    {
                        phase = (any.r.Phase ?? "").Trim().ToUpperInvariant(),
                        component = (any.r.Component ?? "").Trim().ToUpperInvariant(),
                        volt_level_kV = any.r.VoltLevel is null ? (double?)null : any.r.VoltLevel.Value / 1000.0
                    },
                    points
                };
            })
            .ToList();


        var data = windowFrom.Date.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);

        var plotMeta = _meta.Build(w, ctx, meas);

        var response = SeriesResponseBuilderExtensions
            .BuildSeriesResponse(q.RunId, windowFrom, windowTo2, series, plotMeta)
            .WithModes(modes)
            .WithCacheId(cacheId)
            .WithResolved(ctx.PdcName, series.Select(s => s.pmu).Distinct().Count())
            .WithTypeFields(new Dictionary<string, object?>
            {
                ["unit"] = unit,
                ["tri"] = tri,
                ["phase"] = tri ? "ABC" : uphase
            })
            .Build();

        return Results.Ok(response);
    }
}