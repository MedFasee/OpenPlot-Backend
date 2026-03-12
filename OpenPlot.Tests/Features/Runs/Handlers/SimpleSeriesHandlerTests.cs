using Moq;
using Xunit;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.Tests.Features.Runs.Handlers;

/// <summary>
/// Testes para SimpleSeriesHandler refatorado.
/// Valida comportamento específico de séries simples (frequency, dfreq, digital).
/// </summary>
public class SimpleSeriesHandlerTests
{
    private readonly Mock<IRunContextRepository> _mockRunRepository;
    private readonly Mock<IMeasurementsRepository> _mockMeasRepository;
    private readonly Mock<ITimeSeriesDownsampler> _mockDownsampler;
    private readonly Mock<IPlotMetaBuilder> _mockMetaBuilder;
    private readonly Mock<IAnalysisCacheRepository> _mockCacheRepository;
    private readonly SimpleSeriesHandler _handler;

    public SimpleSeriesHandlerTests()
    {
        _mockRunRepository = new Mock<IRunContextRepository>();
        _mockMeasRepository = new Mock<IMeasurementsRepository>();
        _mockDownsampler = new Mock<ITimeSeriesDownsampler>();
        _mockMetaBuilder = new Mock<IPlotMetaBuilder>();
        _mockCacheRepository = new Mock<IAnalysisCacheRepository>();

        _handler = new SimpleSeriesHandler(
            _mockRunRepository.Object,
            _mockMeasRepository.Object,
            _mockDownsampler.Object,
            _mockMetaBuilder.Object,
            _mockCacheRepository.Object);
    }

    [Fact]
    public async Task HandleAsync_WithValidFrequencyQuery_ShouldReturnOkWithSeriesData()
    {
        // Arrange
        var runId = Guid.NewGuid();
        var now = DateTime.UtcNow;
        var query = new SimpleSeriesQuery { RunId = runId, MaxPoints = "5000" };
        var window = new WindowQuery(null, null);
        var measurements = new MeasurementsQuery(
            Quantity: "frequency",
            Component: "freq",
            PhaseMode: PhaseMode.Any,
            Phase: null,
            PmuNames: null,
            Unit: "Hz");

        var runContext = new RunContext
        {
            PdcName = "PDC_TEST",
            SelectRate = 30
        };

        var rows = new List<MeasurementRow>
        {
            new() { SignalId = 1, Ts = now, Value = 59.95, IdName = "PMU1", PdcName = "PDC_TEST", PdcPmuId = 1 },
            new() { SignalId = 1, Ts = now.AddSeconds(1), Value = 60.00, IdName = "PMU1", PdcName = "PDC_TEST", PdcPmuId = 1 },
            new() { SignalId = 1, Ts = now.AddSeconds(2), Value = 60.05, IdName = "PMU1", PdcName = "PDC_TEST", PdcPmuId = 1 }
        };

        _mockRunRepository
            .Setup(x => x.ResolveAsync(runId, null, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(runContext);

        _mockMeasRepository
            .Setup(x => x.QueryAsync(runContext, measurements, It.IsAny<CancellationToken>()))
            .ReturnsAsync(rows);

        _mockDownsampler
            .Setup(x => x.MinMax(It.IsAny<List<Point>>(), 5000))
            .Returns((List<Point> pts, int max) => pts);

        _mockCacheRepository
            .Setup(x => x.SaveAsync(runId, It.IsAny<RowsCacheV2>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync("cache-id-123");

        _mockMetaBuilder
            .Setup(x => x.Build(window, runContext, measurements))
            .Returns(new PlotMetaDto("Frequência", "Tempo", "Hz"));

        // Act
        var result = await _handler.HandleAsync(query, window, measurements, null, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.IsAssignableFrom<OkObjectResult>(result);

        var okResult = result as OkObjectResult;
        Assert.NotNull(okResult);
        var responseBody = okResult.Value;
        Assert.NotNull(responseBody);

        // Verificar estrutura básica da resposta
        var responseType = responseBody.GetType();
        Assert.NotNull(responseType.GetProperty("series"));
        Assert.NotNull(responseType.GetProperty("run_id"));
        Assert.NotNull(responseType.GetProperty("cache_id"));
    }

    [Fact]
    public async Task HandleAsync_WithEmptyQueryResult_ShouldReturnNotFound()
    {
        // Arrange
        var runId = Guid.NewGuid();
        var query = new SimpleSeriesQuery { RunId = runId };
        var window = new WindowQuery(null, null);
        var measurements = new MeasurementsQuery(
            Quantity: "frequency",
            Component: "freq",
            PhaseMode: PhaseMode.Any,
            Phase: null,
            PmuNames: null,
            Unit: "Hz");

        var runContext = new RunContext { PdcName = "PDC_TEST", SelectRate = 30 };

        _mockRunRepository
            .Setup(x => x.ResolveAsync(runId, null, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(runContext);

        _mockMeasRepository
            .Setup(x => x.QueryAsync(runContext, measurements, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<MeasurementRow>());

        // Act
        var result = await _handler.HandleAsync(query, window, measurements, null, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.IsAssignableFrom<NotFoundObjectResult>(result);
    }

    [Fact]
    public async Task HandleAsync_WithDownsampling_ShouldApplyDownsampler()
    {
        // Arrange
        var runId = Guid.NewGuid();
        var now = DateTime.UtcNow;
        var query = new SimpleSeriesQuery { RunId = runId, MaxPoints = "1000" };
        var window = new WindowQuery(null, null);
        var measurements = new MeasurementsQuery(
            Quantity: "frequency",
            Component: "freq",
            PhaseMode: PhaseMode.Any,
            Phase: null,
            PmuNames: null,
            Unit: "Hz");

        var runContext = new RunContext { PdcName = "PDC_TEST", SelectRate = 30 };
        var rows = new List<MeasurementRow>
        {
            new() { SignalId = 1, Ts = now, Value = 59.95, IdName = "PMU1", PdcName = "PDC_TEST", PdcPmuId = 1 }
        };

        // Simular downsampling que reduz pontos
        var downsampledPoints = new List<Point>
        {
            new(now, 59.95)
        };

        _mockRunRepository
            .Setup(x => x.ResolveAsync(runId, null, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(runContext);

        _mockMeasRepository
            .Setup(x => x.QueryAsync(runContext, measurements, It.IsAny<CancellationToken>()))
            .ReturnsAsync(rows);

        _mockDownsampler
            .Setup(x => x.MinMax(It.IsAny<List<Point>>(), 1000))
            .Returns(downsampledPoints);

        _mockCacheRepository
            .Setup(x => x.SaveAsync(runId, It.IsAny<RowsCacheV2>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync("cache-123");

        _mockMetaBuilder
            .Setup(x => x.Build(window, runContext, measurements))
            .Returns(new PlotMetaDto("Test", "X", "Y"));

        // Act
        var result = await _handler.HandleAsync(query, window, measurements, null, CancellationToken.None);

        // Assert
        _mockDownsampler.Verify(
            x => x.MinMax(It.IsAny<List<Point>>(), 1000),
            Times.Once,
            "Downsampler deve ser chamado com maxPoints = 1000");
    }

    [Fact]
    public async Task HandleAsync_WithMaxPointsAll_ShouldNotCallDownsampler()
    {
        // Arrange
        var runId = Guid.NewGuid();
        var now = DateTime.UtcNow;
        var query = new SimpleSeriesQuery { RunId = runId, MaxPoints = "all" };
        var window = new WindowQuery(null, null);
        var measurements = new MeasurementsQuery(
            Quantity: "frequency",
            Component: "freq",
            PhaseMode: PhaseMode.Any,
            Phase: null,
            PmuNames: null,
            Unit: "Hz");

        var runContext = new RunContext { PdcName = "PDC_TEST", SelectRate = 30 };
        var rows = new List<MeasurementRow>
        {
            new() { SignalId = 1, Ts = now, Value = 59.95, IdName = "PMU1", PdcName = "PDC_TEST", PdcPmuId = 1 }
        };

        _mockRunRepository
            .Setup(x => x.ResolveAsync(runId, null, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(runContext);

        _mockMeasRepository
            .Setup(x => x.QueryAsync(runContext, measurements, It.IsAny<CancellationToken>()))
            .ReturnsAsync(rows);

        _mockCacheRepository
            .Setup(x => x.SaveAsync(runId, It.IsAny<RowsCacheV2>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync("cache-123");

        _mockMetaBuilder
            .Setup(x => x.Build(window, runContext, measurements))
            .Returns(new PlotMetaDto("Test", "X", "Y"));

        // Act
        var result = await _handler.HandleAsync(query, window, measurements, null, CancellationToken.None);

        // Assert
        _mockDownsampler.Verify(
            x => x.MinMax(It.IsAny<List<Point>>(), It.IsAny<int>()),
            Times.Never,
            "Downsampler não deve ser chamado quando MaxPointsIsAll=true");
    }

    [Fact]
    public async Task HandleAsync_ShouldCacheResults()
    {
        // Arrange
        var runId = Guid.NewGuid();
        var now = DateTime.UtcNow;
        var query = new SimpleSeriesQuery { RunId = runId, MaxPoints = "5000" };
        var window = new WindowQuery(null, null);
        var measurements = new MeasurementsQuery(
            Quantity: "frequency",
            Component: "freq",
            PhaseMode: PhaseMode.Any,
            Phase: null,
            PmuNames: null,
            Unit: "Hz");

        var runContext = new RunContext { PdcName = "PDC_TEST", SelectRate = 30 };
        var rows = new List<MeasurementRow>
        {
            new() { SignalId = 1, Ts = now, Value = 59.95, IdName = "PMU1", PdcName = "PDC_TEST", PdcPmuId = 1 }
        };

        _mockRunRepository
            .Setup(x => x.ResolveAsync(runId, null, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(runContext);

        _mockMeasRepository
            .Setup(x => x.QueryAsync(runContext, measurements, It.IsAny<CancellationToken>()))
            .ReturnsAsync(rows);

        _mockDownsampler
            .Setup(x => x.MinMax(It.IsAny<List<Point>>(), 5000))
            .Returns((List<Point> pts, int max) => pts);

        _mockCacheRepository
            .Setup(x => x.SaveAsync(runId, It.IsAny<RowsCacheV2>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync("cache-456");

        _mockMetaBuilder
            .Setup(x => x.Build(window, runContext, measurements))
            .Returns(new PlotMetaDto("Test", "X", "Y"));

        // Act
        var result = await _handler.HandleAsync(query, window, measurements, null, CancellationToken.None);

        // Assert
        _mockCacheRepository.Verify(
            x => x.SaveAsync(runId, It.IsAny<RowsCacheV2>(), It.IsAny<CancellationToken>()),
            Times.Once,
            "Cache deve ser salvo uma vez");
    }
}
