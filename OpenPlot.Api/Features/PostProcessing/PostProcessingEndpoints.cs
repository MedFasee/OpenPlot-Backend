using Microsoft.AspNetCore.Mvc;
using OpenPlot.Features.PostProcessing.Handlers;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.BackgroundCache;

public static class PostProcessingEndpoints
{
    public static IEndpointRouteBuilder MapPostProcessing(
        this IEndpointRouteBuilder app)
    {
        var grp = app.MapGroup("")
            .WithTags("Post_Processing")
            .RequireAuthorization();

        grp.MapGet("/dft", async (
            [FromQuery] Guid cache_id,
            [FromQuery] DateTime? from,
            [FromQuery] DateTime? to,
            [FromServices] IAnalysisCacheRepository cacheRepo,
            [FromServices] IBackgroundCacheQueue cacheQueue,
            [FromServices] IDftMetaBuilder metaBuilder,
            CancellationToken ct = default
        ) =>
        {
            var payload =
                await cacheRepo.GetAsync<RowsCacheV2>(
                    cache_id,
                    ct);

            if (payload is null)
            {
                var pendingResult =
                    BuildCacheUnavailableResult(
                        cache_id,
                        cacheQueue);

                if (pendingResult is not null)
                    return pendingResult;

                return Results.NotFound(
                    "cache_id não encontrado.");
            }

            var fromUtc = from?.ToUniversalTime();
            var toUtc = to?.ToUniversalTime();

            var dft = Dft.Compute(
                payload,
                fromUtc,
                toUtc);

            var plotMeta = metaBuilder.Build(
                payload,
                dft.FromUtc,
                dft.ToUtc);

            var series = dft.Specs
                .Select(kv => new
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
                        .Select(p => new object[]
                        {
                            p.Hz,
                            p.Mag
                        })
                        .ToList()
                })
                .ToList();

            return Results.Ok(new
            {
                cache_id,
                meta = plotMeta,
                selectRate = payload.SelectRate,
                window = new
                {
                    from = dft.FromUtc,
                    to = dft.ToUtc
                },
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
            [FromServices] IBackgroundCacheQueue cacheQueue,
            [FromServices] IPronyMetaBuilder metaBuilder,
            CancellationToken ct = default
        ) =>
        {
            var payload =
                await cacheRepo.GetAsync<RowsCacheV2>(
                    cache_id,
                    ct);

            if (payload is null)
            {
                var pendingResult =
                    BuildCacheUnavailableResult(
                        cache_id,
                        cacheQueue);

                if (pendingResult is not null)
                    return pendingResult;

                return Results.NotFound(
                    "cache_id não encontrado.");
            }

            var fromUtc = from?.ToUniversalTime();
            var toUtc = to?.ToUniversalTime();

            try
            {
                var prony = Prony.Compute(
                    payload,
                    order,
                    fromUtc,
                    toUtc);

                var plotMeta = metaBuilder.Build(
                    payload,
                    prony.FromUtc,
                    prony.ToUtc);

                var series = prony.Specs
                    .Select(kv => new
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

                        modes = kv.Value.Modes
                            .Select(m => new
                            {
                                index = m.Index,
                                energy = m.Energy,
                                frequencyHz = m.FrequencyHz,
                                dampingPercent = m.DampingPercent,
                                amplitude = m.Amplitude,
                                phaseRad = m.PhaseRad,
                                real = m.Real,
                                imaginary = m.Imaginary
                            })
                            .ToList(),

                        allModes = include_all_modes
                            ? kv.Value.AllModes
                                .Select(m => new
                                {
                                    index = m.Index,
                                    energy = m.Energy,
                                    frequencyHz = m.FrequencyHz,
                                    dampingPercent = m.DampingPercent,
                                    amplitude = m.Amplitude,
                                    phaseRad = m.PhaseRad,
                                    real = m.Real,
                                    imaginary = m.Imaginary
                                })
                                .ToList()
                            : null,

                        originalPoints = include_points
                            ? kv.Value.OriginalPoints
                                .Select(p => new object[]
                                {
                                    p.Ts,
                                    p.Value
                                })
                                .ToList()
                            : null,

                        estimatedPoints = include_points
                            ? kv.Value.EstimatedPoints
                                .Select(p => new object[]
                                {
                                    p.Ts,
                                    p.Value
                                })
                                .ToList()
                            : null
                    })
                    .ToList();

                return Results.Ok(new
                {
                    cache_id,
                    meta = plotMeta,
                    selectRate = payload.SelectRate,
                    window = new
                    {
                        from = prony.FromUtc,
                        to = prony.ToUtc
                    },
                    modeShapeCandidatesHz =
                        prony.ModeShapeCandidatesHz,
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

        grp.MapGet("/cca", async (
        [FromQuery] Guid cache_id,
        [FromQuery] int model_order,
        [FromQuery] int block_rows,
        [FromQuery] int window_length_minutes,
        [FromQuery] int window_step_seconds,
        [FromQuery] double frequency_min_hz,
        [FromQuery] double frequency_max_hz,
        [FromQuery] DateTime? from,
        [FromQuery] DateTime? to,
        [FromQuery] bool include_all_modes,
        [FromServices] IAnalysisCacheRepository cacheRepo,
        [FromServices] ICcaMetaBuilder metaBuilder,
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
                    var cca = Cca.Compute(
                        payload,
                        model_order,
                        block_rows,
                        window_length_minutes,
                        window_step_seconds,
                        frequency_min_hz,
                        frequency_max_hz,
                        fromUtc,
                        toUtc);

                    var meta = metaBuilder.Build(payload, cca.FromUtc, cca.ToUtc);

                    return Results.Ok(new
                    {
                        cache_id,
                        meta,
                        selectRate = payload.SelectRate,
                        window = new { from = cca.FromUtc, to = cca.ToUtc },
                        parameters = new
                        {
                            modelOrder = cca.Parameters.ModelOrder,
                            blockRows = cca.Parameters.BlockRows,
                            windowLengthMinutes = cca.Parameters.WindowLengthMinutes,
                            windowStepSeconds = cca.Parameters.WindowStepSeconds,
                            frequencyMinHz = cca.Parameters.FrequencyMinHz,
                            frequencyMaxHz = cca.Parameters.FrequencyMaxHz
                        },
                        energySeries = cca.Windows.Select(windowItem => new
                        {
                            index = windowItem.Index,
                            from = windowItem.FromUtc,
                            to = windowItem.ToUtc,
                            frequencyHz = windowItem.Energy.FrequencyHz,
                            dampingPercent = windowItem.Energy.DampingPercent,
                            pseudoEnergy = windowItem.Energy.Score,
                            vector = windowItem.Energy.Vector.Select(point => new
                            {
                                series = point.Series,
                                pmu = point.Pmu,
                                amplitude = point.Amplitude,
                                phase = point.Phase,
                                phaseRad = point.PhaseRad,
                                component = point.Component,
                                quantity = point.Quantity,
                                unit = point.Unit
                            }).ToList()
                        }).ToList(),
                        idmSeries = cca.Windows.Select(windowItem => new
                        {
                            index = windowItem.Index,
                            from = windowItem.FromUtc,
                            to = windowItem.ToUtc,
                            frequencyHz = windowItem.Idm.FrequencyHz,
                            dampingPercent = windowItem.Idm.DampingPercent,
                            idm = windowItem.Idm.Score,
                            vector = windowItem.Idm.Vector.Select(point => new
                            {
                                series = point.Series,
                                pmu = point.Pmu,
                                amplitude = point.Amplitude,
                                phase = point.Phase,
                                phaseRad = point.PhaseRad,
                                component = point.Component,
                                quantity = point.Quantity,
                                unit = point.Unit
                            }).ToList()
                        }).ToList(),
                        windows = cca.Windows.Select(windowItem => new
                        {
                            index = windowItem.Index,
                            from = windowItem.FromUtc,
                            to = windowItem.ToUtc,
                            energy = new
                            {
                                index = windowItem.Energy.Index,
                                frequencyHz = windowItem.Energy.FrequencyHz,
                                dampingPercent = windowItem.Energy.DampingPercent,
                                pseudoEnergy = windowItem.Energy.Score
                            },
                            idm = new
                            {
                                index = windowItem.Idm.Index,
                                frequencyHz = windowItem.Idm.FrequencyHz,
                                dampingPercent = windowItem.Idm.DampingPercent,
                                idm = windowItem.Idm.Score
                            },
                            allModes = include_all_modes
                                ? windowItem.AllModes.Select(mode => new
                                {
                                    index = mode.Index,
                                    frequencyHz = mode.FrequencyHz,
                                    dampingPercent = mode.DampingPercent,
                                    pseudoEnergy = mode.PseudoEnergy,
                                    idm = mode.Idm,
                                    real = mode.Real,
                                    imaginary = mode.Imaginary
                                }).ToList()
                                : null
                        }).ToList()
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

    private static IResult? BuildCacheUnavailableResult(
        Guid cacheId,
        IBackgroundCacheQueue cacheQueue)
    {
        return cacheQueue.GetStatus(cacheId) switch
        {
            BackgroundCacheStatus.Pending
                or BackgroundCacheStatus.Running
                => Results.Json(
                    new
                    {
                        cache_id = cacheId,
                        status = "building",
                        message =
                            "O cache integral ainda está sendo preparado."
                    },
                    statusCode:
                        StatusCodes.Status202Accepted),

            BackgroundCacheStatus.Failed
                => Results.Json(
                    new
                    {
                        cache_id = cacheId,
                        status = "failed",
                        message =
                            "A construção do cache integral falhou."
                    },
                    statusCode:
                        StatusCodes.Status503ServiceUnavailable),

            _ => null
        };
    }
}
