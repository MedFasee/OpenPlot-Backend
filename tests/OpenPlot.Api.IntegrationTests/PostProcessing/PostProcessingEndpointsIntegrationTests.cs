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
        Assert.Equal(0, root.GetProperty("modeShapeCandidatesHz").GetArrayLength());
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
    public async Task GetProny_WhenCacheHasAtLeastTwoValidSeries_ReturnsModeShapeCandidates()
    {
        var cacheId = Guid.NewGuid();
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        factory.CacheRepository.Seed(cacheId, CreateOscillatoryPayload(start, sampleRate: 20, sampleCount: 40, seriesCount: 2));

        using var client = factory.CreateClient();
        var response = await client.GetAsync($"/api/v1/prony?cache_id={cacheId:D}&order=4&include_points=true&include_all_modes=true");

        response.EnsureSuccessStatusCode();

        using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var root = json.RootElement;
        var firstSeries = root.GetProperty("series")[0];

        var expectedCandidates = firstSeries
            .GetProperty("allModes")
            .EnumerateArray()
            .Select(mode => mode.GetProperty("frequencyHz").GetDouble())
            .Where(freq => freq < 10.0 && freq > 1e-6)
            .OrderBy(freq => freq)
            .ToArray();

        var returnedCandidates = root
            .GetProperty("modeShapeCandidatesHz")
            .EnumerateArray()
            .Select(candidate => candidate.GetProperty("frequencyHz").GetDouble())
            .ToArray();

        var firstCandidate = root.GetProperty("modeShapeCandidatesHz")[0];
        var vector = firstCandidate.GetProperty("vector");

        Assert.NotEmpty(returnedCandidates);
        Assert.Equal(expectedCandidates, returnedCandidates);
        Assert.True(firstCandidate.TryGetProperty("index", out _));
        Assert.Equal(2, vector.GetArrayLength());
        Assert.False(string.IsNullOrWhiteSpace(vector[0].GetProperty("series").GetString()));
        Assert.False(string.IsNullOrWhiteSpace(vector[0].GetProperty("pmu").GetString()));
        Assert.True(vector[0].TryGetProperty("amplitude", out _));
        Assert.True(vector[0].TryGetProperty("phaseRad", out _));
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

        factory.CacheRepository.Seed(cacheId, CreateOscillatoryPayload(start, sampleRate: 1, sampleCount: 314));

        using var client = factory.CreateClient();
        var response = await client.GetAsync($"/api/v1/prony?cache_id={cacheId:D}&order=79&include_points=false&include_all_modes=false");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);

        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("Ordem inválida para Prony", body, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("78", body, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task GetCca_WhenCacheExists_ReturnsAmbientModesPayload()
    {
        var cacheId = Guid.NewGuid();
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        factory.CacheRepository.Seed(cacheId, CreateOscillatoryPayload(start, sampleRate: 1, sampleCount: 240, seriesCount: 2, frequencyHz: 0.35));

        using var client = factory.CreateClient();
        var response = await client.GetAsync($"/api/v1/cca?cache_id={cacheId:D}&model_order=4&block_rows=10&window_length_minutes=3&window_step_seconds=30&frequency_min_hz=0.3&frequency_max_hz=0.4&include_all_modes=true");

        response.EnsureSuccessStatusCode();

        using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var root = json.RootElement;

        Assert.Equal(cacheId.ToString(), root.GetProperty("cache_id").GetString());
        Assert.Equal(1, root.GetProperty("selectRate").GetInt32());
        Assert.True(root.GetProperty("energySeries").GetArrayLength() > 0);
        Assert.True(root.GetProperty("idmSeries").GetArrayLength() > 0);
        Assert.True(root.GetProperty("windows").GetArrayLength() > 0);
        Assert.Equal("CCA", root.GetProperty("meta").GetProperty("title").GetString()?.Split(' ')[0]);

        var firstWindow = root.GetProperty("windows")[0];
        Assert.True(firstWindow.GetProperty("allModes").GetArrayLength() > 0);

        var firstEnergy = root.GetProperty("energySeries")[0];
        Assert.True(firstEnergy.GetProperty("vector").GetArrayLength() == 2);
    }

    [Fact]
    public async Task GetCca_WhenCacheDoesNotExist_ReturnsNotFound()
    {
        using var client = factory.CreateClient();
        var response = await client.GetAsync($"/api/v1/cca?cache_id={Guid.NewGuid():D}&model_order=4&block_rows=10&window_length_minutes=3&window_step_seconds=30&frequency_min_hz=0.3&frequency_max_hz=0.4&include_all_modes=false");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task GetCca_WhenWindowLengthIsUnavailable_ReturnsBadRequest()
    {
        var cacheId = Guid.NewGuid();
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        factory.CacheRepository.Seed(cacheId, CreateOscillatoryPayload(start, sampleRate: 1, sampleCount: 120, seriesCount: 1, frequencyHz: 0.35));

        using var client = factory.CreateClient();
        var response = await client.GetAsync($"/api/v1/cca?cache_id={cacheId:D}&model_order=4&block_rows=10&window_length_minutes=3&window_step_seconds=30&frequency_min_hz=0.3&frequency_max_hz=0.4&include_all_modes=false");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);

        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("janela deslizante", body, StringComparison.OrdinalIgnoreCase);
    }

    private static RowsCacheV2 CreateOscillatoryPayload(DateTime start, int sampleRate, int sampleCount, int seriesCount = 1)
        => CreateOscillatoryPayload(start, sampleRate, sampleCount, seriesCount, frequencyHz: 1.0);

    private static RowsCacheV2 CreateOscillatoryPayload(DateTime start, int sampleRate, int sampleCount, int seriesCount, double frequencyHz)
    {
        var series = new List<RowsCacheSeries>(seriesCount);

        for (var seriesIndex = 0; seriesIndex < seriesCount; seriesIndex++)
        {
            var points = new List<RowsCachePoint>(sampleCount);
            for (var i = 0; i < sampleCount; i++)
            {
                var t = i / (double)sampleRate;
                points.Add(new RowsCachePoint
                {
                    Ts = start.AddSeconds(t),
                    Value = Math.Cos(2.0 * Math.PI * frequencyHz * t + (seriesIndex * Math.PI / 6.0))
                });
            }

            series.Add(new RowsCacheSeries
            {
                IdName = $"PMU-{seriesIndex + 1}",
                PdcName = "PDC-1",
                Quantity = "frequency",
                Component = "freq",
                Unit = "Hz",
                Phase = ((char)('A' + seriesIndex)).ToString(),
                Points = points
            });
        }

        return new RowsCacheV2
        {
            From = start,
            To = series[0].Points[^1].Ts,
            SelectRate = sampleRate,
            Series = series
        };
    }
}
