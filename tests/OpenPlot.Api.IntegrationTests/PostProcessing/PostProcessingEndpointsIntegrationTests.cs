using System.Net;
using System.Text.Json;
using OpenPlot.Api.IntegrationTests.Infrastructure;
using OpenPlot.Features.Runs.Contracts;

namespace OpenPlot.Api.IntegrationTests.PostProcessing;

public sealed class PostProcessingEndpointsIntegrationTests(OpenPlotApiFactory factory) : IClassFixture<OpenPlotApiFactory>
{
    [Fact]
    public async Task GetDft_WhenCacheExists_ReturnsSpectrumPayload()
    {
        var cacheId = Guid.NewGuid();
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        factory.CacheRepository.Seed(cacheId, new RowsCacheV2
        {
            From = start,
            To = start.AddSeconds(3),
            SelectRate = 1,
            Series =
            [
                new RowsCacheSeries
                {
                    IdName = "PMU-1",
                    PdcName = "PDC-1",
                    Quantity = "frequency",
                    Component = "freq",
                    Unit = "Hz",
                    Points =
                    [
                        new RowsCachePoint { Ts = start, Value = 60 },
                        new RowsCachePoint { Ts = start.AddSeconds(1), Value = 60.2 },
                        new RowsCachePoint { Ts = start.AddSeconds(2), Value = 59.8 },
                        new RowsCachePoint { Ts = start.AddSeconds(3), Value = 60.1 }
                    ]
                }
            ]
        });

        using var client = factory.CreateClient();
        var response = await client.GetAsync($"/api/v1/dft?cache_id={cacheId:D}");

        response.EnsureSuccessStatusCode();

        using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var root = json.RootElement;

        Assert.Equal(cacheId.ToString(), root.GetProperty("cache_id").GetString());
        Assert.Equal(1, root.GetProperty("selectRate").GetInt32());
        Assert.True(root.GetProperty("series").GetArrayLength() > 0);
        Assert.Contains("Espectro de Freq.", root.GetProperty("meta").GetProperty("title").GetString() ?? string.Empty);
    }

    [Fact]
    public async Task GetDft_WhenCacheDoesNotExist_ReturnsNotFound()
    {
        using var client = factory.CreateClient();
        var response = await client.GetAsync($"/api/v1/dft?cache_id={Guid.NewGuid():D}");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task GetProny_WhenCacheExists_ReturnsModesPayload()
    {
        var cacheId = Guid.NewGuid();
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        factory.CacheRepository.Seed(cacheId, CreateOscillatoryPayload(start, sampleRate: 20, sampleCount: 40));

        using var client = factory.CreateClient();
        var response = await client.GetAsync($"/api/v1/prony?cache_id={cacheId:D}&order=4&include_points=true&include_all_modes=true");

        response.EnsureSuccessStatusCode();

        using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var root = json.RootElement;

        Assert.Equal(cacheId.ToString(), root.GetProperty("cache_id").GetString());
        Assert.Equal(20, root.GetProperty("selectRate").GetInt32());
        Assert.True(root.GetProperty("modeShapeCandidatesHz").GetArrayLength() > 0);
        Assert.True(root.GetProperty("series").GetArrayLength() > 0);

        var firstSeries = root.GetProperty("series")[0];
        Assert.Equal(4, firstSeries.GetProperty("order").GetInt32());
        Assert.True(firstSeries.GetProperty("modes").GetArrayLength() > 0);
        Assert.True(firstSeries.GetProperty("allModes").GetArrayLength() >= firstSeries.GetProperty("modes").GetArrayLength());
        Assert.True(firstSeries.GetProperty("originalPoints").GetArrayLength() > 0);
        Assert.True(firstSeries.GetProperty("estimatedPoints").GetArrayLength() > 0);
        Assert.Contains("Prony", root.GetProperty("meta").GetProperty("title").GetString() ?? string.Empty);
    }

    [Fact]
    public async Task GetProny_WhenCacheDoesNotExist_ReturnsNotFound()
    {
        using var client = factory.CreateClient();
        var response = await client.GetAsync($"/api/v1/prony?cache_id={Guid.NewGuid():D}&order=4&include_points=false&include_all_modes=false");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task GetProny_WhenOrderIsUnavailable_ReturnsBadRequest()
    {
        var cacheId = Guid.NewGuid();
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        factory.CacheRepository.Seed(cacheId, CreateOscillatoryPayload(start, sampleRate: 1, sampleCount: 4));

        using var client = factory.CreateClient();
        var response = await client.GetAsync($"/api/v1/prony?cache_id={cacheId:D}&order=4&include_points=false&include_all_modes=false");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);

        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("Ordem inválida para Prony", body, StringComparison.OrdinalIgnoreCase);
    }

    private static RowsCacheV2 CreateOscillatoryPayload(DateTime start, int sampleRate, int sampleCount)
    {
        var points = new List<RowsCachePoint>(sampleCount);
        for (var i = 0; i < sampleCount; i++)
        {
            var t = i / (double)sampleRate;
            points.Add(new RowsCachePoint
            {
                Ts = start.AddSeconds(t),
                Value = Math.Cos(2.0 * Math.PI * 1.0 * t)
            });
        }

        return new RowsCacheV2
        {
            From = start,
            To = points[^1].Ts,
            SelectRate = sampleRate,
            Series =
            [
                new RowsCacheSeries
                {
                    IdName = "PMU-1",
                    PdcName = "PDC-1",
                    Quantity = "frequency",
                    Component = "freq",
                    Unit = "Hz",
                    Points = points
                }
            ]
        };
    }
}
