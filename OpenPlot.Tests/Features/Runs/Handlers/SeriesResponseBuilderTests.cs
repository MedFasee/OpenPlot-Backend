using Xunit;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Responses;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.Tests.Features.Runs.Handlers;

/// <summary>
/// Testes para SeriesResponseBuilder.
/// Valida construção de respostas padronizadas.
/// </summary>
public class SeriesResponseBuilderTests
{
    [Fact]
    public void SeriesResponseBuilder_BuildsCorrectStructure()
    {
        // Arrange
        var runId = Guid.NewGuid();
        var now = DateTime.UtcNow;
        var rows = new List<MeasurementRow>
        {
            new() { IdName = "PMU1", PdcName = "PDC1" },
            new() { IdName = "PMU2", PdcName = "PDC1" }
        };
        var series = new List<object> { new { name = "Series1" } };
        var meta = new PlotMetaDto("Title", "X", "Y");

        var builder = new SeriesResponseBuilder(
            runId,
            now,
            now.AddMinutes(1),
            series,
            rows,
            meta);

        // Act
        var response = builder.Build();

        // Assert
        Assert.NotNull(response);
        var responseType = response.GetType();
        
        // Verifica propriedades principais
        Assert.NotNull(responseType.GetProperty("run_id"));
        Assert.NotNull(responseType.GetProperty("data"));
        Assert.NotNull(responseType.GetProperty("resolved"));
        Assert.NotNull(responseType.GetProperty("window"));
        Assert.NotNull(responseType.GetProperty("meta"));
        Assert.NotNull(responseType.GetProperty("series"));
    }

    [Fact]
    public void SeriesResponseBuilder_WithCacheId_IncludesCacheIdInResponse()
    {
        // Arrange
        var runId = Guid.NewGuid();
        var now = DateTime.UtcNow;
        var rows = new List<MeasurementRow>
        {
            new() { IdName = "PMU1", PdcName = "PDC1" }
        };
        var series = new List<object>();
        var meta = new PlotMetaDto("Title", "X", "Y");

        var builder = new SeriesResponseBuilder(runId, now, now.AddMinutes(1), series, rows, meta)
            .WithCacheId("cache-123");

        // Act
        var response = builder.Build();

        // Assert
        Assert.NotNull(response);
        var cacheIdProp = response.GetType().GetProperty("cache_id");
        Assert.NotNull(cacheIdProp);
    }

    [Fact]
    public void SeriesResponseBuilder_WithModes_IncludesModesInResponse()
    {
        // Arrange
        var runId = Guid.NewGuid();
        var now = DateTime.UtcNow;
        var rows = new List<MeasurementRow>
        {
            new() { IdName = "PMU1", PdcName = "PDC1" }
        };
        var series = new List<object>();
        var meta = new PlotMetaDto("Title", "X", "Y");
        var modes = new Dictionary<string, object?> { { "mode1", "value1" } };

        var builder = new SeriesResponseBuilder(runId, now, now.AddMinutes(1), series, rows, meta)
            .WithModes(modes);

        // Act
        var response = builder.Build();

        // Assert
        Assert.NotNull(response);
        var modesProp = response.GetType().GetProperty("modes");
        Assert.NotNull(modesProp);
    }

    [Fact]
    public void SeriesResponseBuilder_CalculatesPmuCountCorrectly()
    {
        // Arrange
        var runId = Guid.NewGuid();
        var now = DateTime.UtcNow;
        var rows = new List<MeasurementRow>
        {
            new() { IdName = "PMU1", PdcName = "PDC1" },
            new() { IdName = "PMU1", PdcName = "PDC1" }, // Mesmo PMU
            new() { IdName = "PMU2", PdcName = "PDC1" },
            new() { IdName = "PMU3", PdcName = "PDC1" }
        };
        var series = new List<object>();
        var meta = new PlotMetaDto("Title", "X", "Y");

        var builder = new SeriesResponseBuilder(runId, now, now.AddMinutes(1), series, rows, meta);

        // Act
        var response = builder.Build();
        var resolvedProp = response.GetType().GetProperty("resolved")?.GetValue(response);
        var pmuCountProp = resolvedProp?.GetType().GetProperty("pmu_count")?.GetValue(resolvedProp);

        // Assert
        Assert.Equal(3, pmuCountProp); // PMU1, PMU2, PMU3
    }

    [Fact]
    public void SeriesResponseBuilder_FormatDateCorrectly()
    {
        // Arrange
        var runId = Guid.NewGuid();
        var specificDate = new DateTime(2025, 4, 15, 10, 30, 0, DateTimeKind.Utc);
        var rows = new List<MeasurementRow>
        {
            new() { IdName = "PMU1", PdcName = "PDC1" }
        };
        var series = new List<object>();
        var meta = new PlotMetaDto("Title", "X", "Y");

        var builder = new SeriesResponseBuilder(runId, specificDate, specificDate.AddMinutes(1), series, rows, meta);

        // Act
        var response = builder.Build();
        var dataProp = response.GetType().GetProperty("data")?.GetValue(response);

        // Assert
        Assert.Equal("15/04/2025", dataProp);
    }

    [Fact]
    public void SeriesErrorResponseBuilder_NotFound_ReturnsCorrectStatus()
    {
        // Act
        var result = SeriesErrorResponseBuilder.NotFound("Resource not found");

        // Assert
        Assert.IsAssignableFrom<NotFoundObjectResult>(result);
        var notFoundResult = result as NotFoundObjectResult;
        Assert.Equal(StatusCodes.Status404NotFound, notFoundResult?.StatusCode);
    }

    [Fact]
    public void SeriesErrorResponseBuilder_BadRequest_ReturnsCorrectStatus()
    {
        // Act
        var result = SeriesErrorResponseBuilder.BadRequest("Invalid parameter");

        // Assert
        Assert.IsAssignableFrom<BadHttpRequestResult>(result);
    }

    [Fact]
    public void SeriesErrorResponseBuilder_InternalError_ReturnsStatusCode500()
    {
        // Act
        var result = SeriesErrorResponseBuilder.InternalError("Internal server error");

        // Assert
        var statusResult = result as ObjectResult;
        Assert.NotNull(statusResult);
        Assert.Equal(StatusCodes.Status500InternalServerError, statusResult.StatusCode);
    }

    [Fact]
    public void SeriesErrorResponseBuilder_Timeout_ReturnsStatusCode408()
    {
        // Act
        var result = SeriesErrorResponseBuilder.Timeout();

        // Assert
        var statusResult = result as ObjectResult;
        Assert.NotNull(statusResult);
        Assert.Equal(StatusCodes.Status408RequestTimeout, statusResult.StatusCode);
    }
}
