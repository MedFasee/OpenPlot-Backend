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
}
