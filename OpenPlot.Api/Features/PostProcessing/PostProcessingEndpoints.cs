using Microsoft.AspNetCore.Mvc;
using OpenPlot.Features.PostProcessing.Handlers;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Repositories;

public static class PostProcessingEndpoints
{
    public static IEndpointRouteBuilder MapPostProcessing(this IEndpointRouteBuilder app)
    {
        var grp = app.MapGroup("")
                     .WithTags("Post_Processing")
                     .RequireAuthorization();

        grp.MapGet("/dft", async (
            [FromQuery] Guid cache_id,
            [FromQuery] DateTime? from,
            [FromQuery] DateTime? to,
            [FromServices] IAnalysisCacheRepository cacheRepo,
            [FromServices] IDftMetaBuilder metaBuilder,
            CancellationToken ct = default
        ) =>
        {
            var payload = await cacheRepo.GetAsync<RowsCacheV2>(cache_id, ct);
            if (payload is null)
                return Results.NotFound("cache_id não encontrado.");

            var fromUtc = from?.ToUniversalTime();
            var toUtc = to?.ToUniversalTime();

            var dft = Dft.Compute(payload, fromUtc, toUtc);
            var plotMeta = metaBuilder.Build(payload, dft.FromUtc, dft.ToUtc);

            var series = dft.Specs.Select(kv => new
            {
                pmu = kv.Value.Pmu,
                component = kv.Value.Component,
                quantity = kv.Value.Quantity,
                phase = kv.Value.Phase,
                unit = kv.Value.Unit,
                meta = new { serie = kv.Key },

                sr = kv.Value.Sr,
                n = kv.Value.N,
                fMin = kv.Value.FMin,
                points = kv.Value.Points
                    .Select(p => new object[] { p.Hz, p.Mag })
                    .ToList()
            }).ToList();

            return Results.Ok(new
            {
                cache_id,
                meta = plotMeta,
                selectRate = payload.SelectRate,
                window = new { from = dft.FromUtc, to = dft.ToUtc },
                zoom = new
                {
                    fMin = dft.Zoom?.Position,
                    fMax = dft.Zoom?.Size
                },
                series
            });
        });

        grp.MapGet("/prony", async (
            [FromQuery] Guid cache_id,
            [FromQuery] int order,
            [FromQuery] DateTime? from,
            [FromQuery] DateTime? to,
            [FromQuery] bool include_points,
            [FromQuery] bool include_all_modes,
            [FromServices] IAnalysisCacheRepository cacheRepo,
            [FromServices] IPronyMetaBuilder metaBuilder,
            CancellationToken ct = default
        ) =>
        {
            var payload = await cacheRepo.GetAsync<RowsCacheV2>(cache_id, ct);
            if (payload is null)
                return Results.NotFound("cache_id não encontrado.");

            var fromUtc = from?.ToUniversalTime();
            var toUtc = to?.ToUniversalTime();

            try
            {
                var prony = Prony.Compute(payload, order, fromUtc, toUtc);
                var plotMeta = metaBuilder.Build(payload, prony.FromUtc, prony.ToUtc);

                var series = prony.Specs.Select(kv => new
                {
                    pmu = kv.Value.Pmu,
                    component = kv.Value.Component,
                    quantity = kv.Value.Quantity,
                    phase = kv.Value.Phase,
                    unit = kv.Value.Unit,
                    meta = new { serie = kv.Key },

                    sr = kv.Value.Sr,
                    n = kv.Value.N,
                    order = kv.Value.Order,
                    modes = kv.Value.Modes.Select(m => new
                    {
                        index = m.Index,
                        energy = m.Energy,
                        frequencyHz = m.FrequencyHz,
                        dampingPercent = m.DampingPercent,
                        amplitude = m.Amplitude,
                        phaseRad = m.PhaseRad,
                        real = m.Real,
                        imaginary = m.Imaginary
                    }).ToList(),
                    allModes = include_all_modes
                        ? kv.Value.AllModes.Select(m => new
                        {
                            index = m.Index,
                            energy = m.Energy,
                            frequencyHz = m.FrequencyHz,
                            dampingPercent = m.DampingPercent,
                            amplitude = m.Amplitude,
                            phaseRad = m.PhaseRad,
                            real = m.Real,
                            imaginary = m.Imaginary
                        }).ToList()
                        : null,
                    originalPoints = include_points
                        ? kv.Value.OriginalPoints.Select(p => new object[] { p.Ts, p.Value }).ToList()
                        : null,
                    estimatedPoints = include_points
                        ? kv.Value.EstimatedPoints.Select(p => new object[] { p.Ts, p.Value }).ToList()
                        : null
                }).ToList();

                return Results.Ok(new
                {
                    cache_id,
                    meta = plotMeta,
                    selectRate = payload.SelectRate,
                    window = new { from = prony.FromUtc, to = prony.ToUtc },
                    modeShapeCandidatesHz = prony.ModeShapeCandidatesHz.Select(candidate => new
                    {
                        index = candidate.Index,
                        frequencyHz = candidate.FrequencyHz,
                        vector = candidate.Vector.Select(point => new
                        {
                            series = point.Series,
                            pmu = point.Pmu,
                            phase = point.Phase,
                            component = point.Component,
                            quantity = point.Quantity,
                            unit = point.Unit,
                            amplitude = point.Amplitude,
                            phaseRad = point.PhaseRad
                        }).ToList()
                    }).ToList(),
                    series
                });
            }
            catch (ArgumentException ex)
            {
                return Results.BadRequest(ex.Message);
            }
            catch (InvalidOperationException ex)
            {
                return Results.BadRequest(ex.Message);
            }
        });

        return app;
    }
}
