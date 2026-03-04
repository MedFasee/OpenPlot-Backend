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
            [FromServices] IAnalysisCacheRepository cacheRepo,
            CancellationToken ct
        ) =>
        {
            var payload = await cacheRepo.GetAsync<RowsCacheV2>(cache_id, ct);
            if (payload is null)
                return Results.NotFound("cache_id não encontrado.");

            var dft = Dft.Compute(payload);

            var series = dft.Specs.Select(kv => new
            {
                name = kv.Key,
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
                selectRate = payload.SelectRate,
                window = new { from = payload.From, to = payload.To },
                series
            });
        });

        return app;
    }
}