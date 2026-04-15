using OpenPlot.Features.Runs.Handlers.Responses;

namespace OpenPlot.UnitTests.Features.Runs.Handlers;

public sealed class SeriesResponseBuilderTests
{
    [Fact]
    public void Build_WhenOptionalFieldsAreProvided_ReturnsExpectedStructure()
    {
        var runId = Guid.NewGuid();
        var from = new DateTime(2025, 4, 15, 10, 30, 0, DateTimeKind.Utc);
        var to = from.AddMinutes(1);
        var meta = new { title = "Title", x_label = "X", y_label = "Y" };
        var series = new[] { new { name = "Series1" } };
        var modes = new Dictionary<string, object?> { ["tri"] = true };

        var response = SeriesResponseBuilderExtensions
            .BuildSeriesResponse(runId, from, to, series, meta)
            .WithModes(modes)
            .WithCacheId("cache-123")
            .WithResolved("PDC-1", 2)
            .WithTypeField("unit", "Hz")
            .Build();

        var payload = Assert.IsType<Dictionary<string, object?>>(response);
        var resolved = payload["resolved"];
        var pdc = resolved?.GetType().GetProperty("pdc")?.GetValue(resolved);
        var pmuCount = resolved?.GetType().GetProperty("pmu_count")?.GetValue(resolved);
        var window = payload["window"];
        var windowFrom = window?.GetType().GetProperty("from")?.GetValue(window);
        var windowTo = window?.GetType().GetProperty("to")?.GetValue(window);

        Assert.Equal(runId, payload["run_id"]);
        Assert.Equal("15/04/2025", payload["data"]);
        Assert.Equal("cache-123", payload["cache_id"]);
        Assert.Equal("Hz", payload["unit"]);
        Assert.Same(modes, payload["modes"]);
        Assert.Equal("PDC-1", pdc);
        Assert.Equal(2, pmuCount);
        Assert.Equal(from, windowFrom);
        Assert.Equal(to, windowTo);
        Assert.Same(meta, payload["meta"]);
        Assert.Same(series, payload["series"]);
    }

    [Fact]
    public void Build_WhenOptionalFieldsAreOmitted_StillReturnsCorePayload()
    {
        var runId = Guid.NewGuid();
        var from = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var to = from.AddMinutes(5);

        var response = SeriesResponseBuilderExtensions
            .BuildSeriesResponse(runId, from, to, Array.Empty<object>(), new { title = "Title" })
            .WithResolved("PDC-1", 0)
            .Build();

        var payload = Assert.IsType<Dictionary<string, object?>>(response);

        Assert.Equal(runId, payload["run_id"]);
        Assert.Equal("01/01/2025", payload["data"]);
        Assert.DoesNotContain("modes", payload.Keys);
        Assert.DoesNotContain("cache_id", payload.Keys);
        Assert.Contains("resolved", payload.Keys);
        Assert.Contains("window", payload.Keys);
        Assert.Contains("meta", payload.Keys);
        Assert.Contains("series", payload.Keys);
    }
}
