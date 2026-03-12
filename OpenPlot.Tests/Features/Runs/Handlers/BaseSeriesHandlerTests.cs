using Moq;
using Xunit;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Abstractions;
using OpenPlot.Features.Runs.Handlers.Base;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Data.Dtos;

namespace OpenPlot.Tests.Features.Runs.Handlers;

/// <summary>
/// Testes para BaseSeriesHandler.
/// Valida o fluxo template method e comportamentos comuns.
/// </summary>
public class BaseSeriesHandlerTests
{
    /// <summary>
    /// Implementação concreta mínima para testes.
    /// </summary>
    private class TestSeriesHandler : BaseSeriesHandler<SimpleSeriesQuery>
    {
        public TestSeriesHandler(
            IRunContextRepository runRepository,
            IPlotMetaBuilder metaBuilder,
            ISeriesCacheService cacheService)
            : base(runRepository, metaBuilder, cacheService)
        {
        }

        protected override Task<IReadOnlyList<MeasurementRow>> QueryDataAsync(
            SimpleSeriesQuery query,
            RunContext runContext,
            WindowQuery window,
            CancellationToken ct)
        {
            // Retorna dados de teste
            var rows = new List<MeasurementRow>
            {
                new() { SignalId = 1, Ts = DateTime.UtcNow, Value = 100, IdName = "PMU1", PdcName = "PDC1" },
                new() { SignalId = 1, Ts = DateTime.UtcNow.AddSeconds(1), Value = 101, IdName = "PMU1", PdcName = "PDC1" }
            };
            return Task.FromResult<IReadOnlyList<MeasurementRow>>(rows);
        }

        protected override List<object> TransformData(
            IReadOnlyList<MeasurementRow> rows,
            int maxPoints,
            bool noDownsample)
        {
            return new List<object>
            {
                new { pmu = "PMU1", points = rows.Select(r => new object[] { r.Ts, r.Value }).ToList() }
            };
        }
    }

    private Mock<IRunContextRepository> _mockRunRepository = null!;
    private Mock<IPlotMetaBuilder> _mockMetaBuilder = null!;
    private Mock<ISeriesCacheService> _mockCacheService = null!;
    private BaseSeriesHandler<SimpleSeriesQuery> _handler = null!;

    public BaseSeriesHandlerTests()
    {
        _mockRunRepository = new Mock<IRunContextRepository>();
        _mockMetaBuilder = new Mock<IPlotMetaBuilder>();
        _mockCacheService = new Mock<ISeriesCacheService>();

        _handler = new TestSeriesHandler(
            _mockRunRepository.Object,
            _mockMetaBuilder.Object,
            _mockCacheService.Object);
    }

    [Fact]
    public async Task HandleAsync_WithValidQuery_ShouldReturnOkResult()
    {
        // Arrange
        var runId = Guid.NewGuid();
        var query = new SimpleSeriesQuery { RunId = runId, MaxPoints = "5000" };
        var window = new WindowQuery(null, null);
        var runContext = new RunContext { PdcName = "PDC1", SelectRate = 30 };

        _mockRunRepository
            .Setup(x => x.ResolveAsync(runId, null, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(runContext);

        _mockMetaBuilder
            .Setup(x => x.Build(It.IsAny<WindowQuery>(), It.IsAny<RunContext>(), It.IsAny<MeasurementsQuery>()))
            .Returns(new PlotMetaDto("Title", "X", "Y"));

        // Act
        var result = await _handler.HandleAsync(query, window, null, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.IsAssignableFrom<OkObjectResult>(result);
    }

    [Fact]
    public async Task HandleAsync_WithNullRunContext_ShouldReturnNotFound()
    {
        // Arrange
        var runId = Guid.NewGuid();
        var query = new SimpleSeriesQuery { RunId = runId };
        var window = new WindowQuery(null, null);

        _mockRunRepository
            .Setup(x => x.ResolveAsync(runId, null, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync((RunContext?)null);

        // Act
        var result = await _handler.HandleAsync(query, window, null, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.IsAssignableFrom<NotFoundObjectResult>(result);
    }

    [Fact]
    public void ValidateInput_WithEmptyRunId_ShouldReturnInvalid()
    {
        // Arrange
        var query = new SimpleSeriesQuery { RunId = Guid.Empty };
        var window = new WindowQuery(null, null);

        // Act
        var result = _handler.ValidateInput(query, window);

        // Assert
        Assert.False(result.isValid);
        Assert.NotEmpty(result.errorMessage!);
    }

    [Fact]
    public void ValidateInput_WithFromGreaterThanTo_ShouldReturnInvalid()
    {
        // Arrange
        var now = DateTime.UtcNow;
        var query = new SimpleSeriesQuery { RunId = Guid.NewGuid() };
        var window = new WindowQuery(now, now.AddMinutes(-1));

        // Act
        var result = _handler.ValidateInput(query, window);

        // Assert
        Assert.False(result.isValid);
    }

    [Fact]
    public void ValidateInput_WithValidParameters_ShouldReturnValid()
    {
        // Arrange
        var now = DateTime.UtcNow;
        var query = new SimpleSeriesQuery { RunId = Guid.NewGuid() };
        var window = new WindowQuery(now, now.AddMinutes(1));

        // Act
        var result = _handler.ValidateInput(query, window);

        // Assert
        Assert.True(result.isValid);
        Assert.Null(result.errorMessage);
    }

    [Fact]
    public void BuildPlotMeta_ShouldReturnDefaultMeta()
    {
        // Arrange
        var query = new SimpleSeriesQuery { RunId = Guid.NewGuid() };
        var runContext = new RunContext { PdcName = "PDC1" };

        // Act
        var meta = _handler.BuildPlotMeta(runContext, query);

        // Assert
        Assert.NotNull(meta);
        Assert.Equal("Série Temporal", meta.Title);
    }
}
