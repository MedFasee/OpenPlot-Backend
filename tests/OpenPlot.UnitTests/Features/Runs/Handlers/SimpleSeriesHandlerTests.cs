using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Moq;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.BackgroundCache;
using OpenPlot.Services.UI;
using OpenPlot.UnitTests.Infrastructure;

namespace OpenPlot.UnitTests.Features.Runs.Handlers;

public sealed class SimpleSeriesHandlerTests
{
    private readonly Mock<IRunContextRepository> _runRepository = new();
    private readonly Mock<IMeasurementsRepository> _measurementsRepository = new();
    private readonly Mock<ITimeSeriesDownsampler> _downsampler = new();
    private readonly Mock<IPlotMetaBuilder> _metaBuilder = new();
    private readonly Mock<IUiMenuService> _uiMenus = new();
    private readonly Mock<IBackgroundCacheQueue> _backgroundCacheQueue = new();
    private readonly Mock<IBackgroundCacheCoordinator> _backgroundCacheCoordinator = new();
    private readonly Mock<IHttpContextAccessor> _httpContextAccessor = new();
    private readonly Mock<ILogger<SimpleSeriesHandler>> _logger = new();
    private readonly SeriesAssemblyService _seriesAssembly = new();
    private readonly SimpleSeriesHandler _sut;

    public SimpleSeriesHandlerTests()
    {
        _uiMenus
            .Setup(uiMenus => uiMenus.RebuildForRun(It.IsAny<Dictionary<string, object?>?>(), It.IsAny<UiMenuContext>()))
            .Returns((Dictionary<string, object?>? modes, UiMenuContext _) => modes);

        _backgroundCacheCoordinator
            .Setup(coordinator => coordinator.ReserveOrGetAsync(It.IsAny<CacheWorkKey>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new CacheReservation(Guid.NewGuid(), IsOwner: true));

        _backgroundCacheQueue
            .Setup(queue => queue.ScheduleAfterResponse(It.IsAny<HttpContext?>(), It.IsAny<BackgroundCacheWorkItem>()))
            .Returns(true);

        _sut = new SimpleSeriesHandler(
            _runRepository.Object,
            _measurementsRepository.Object,
            _downsampler.Object,
            _metaBuilder.Object,
            _seriesAssembly,
            _uiMenus.Object,
            _backgroundCacheQueue.Object,
            _backgroundCacheCoordinator.Object,
            _httpContextAccessor.Object,
            _logger.Object);
    }

    [Fact]
    public async Task HandleAsync_WhenMaxPointsIsNumeric_DoesNotDownsamplePreviewInMemory()
    {
        var runId = Guid.NewGuid();
        var window = new WindowQuery(null, null);
        var measurement = CreateMeasurementsQuery();
        var runContext = CreateRunContext(runId);
        var rows = CreateRows();
        _runRepository
            .Setup(repository => repository.ResolveAsync(runId, null, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(runContext);
        _measurementsRepository
            .Setup(repository => repository.QueryAsync(runContext, measurement, It.IsAny<CancellationToken>(), It.IsAny<int?>()))
            .ReturnsAsync(rows);
        _metaBuilder
            .Setup(builder => builder.Build(window, runContext, measurement))
            .Returns(new PlotMetaDto("Test", "X", "Y"));

        var result = await _sut.HandleAsync(
            new SimpleSeriesQuery { RunId = runId, MaxPoints = "1000" },
            window,
            measurement,
            modes: null,
            CancellationToken.None);

        ResultAssertions.HasStatusCode(result, StatusCodes.Status200OK);
        _downsampler.Verify(downsampler => downsampler.MinMax(It.IsAny<IReadOnlyList<Point>>(), It.IsAny<int>()), Times.Never);
    }

    [Fact]
    public async Task HandleAsync_WhenMaxPointsIsAll_DoesNotCallDownsampler()
    {
        var runId = Guid.NewGuid();
        var window = new WindowQuery(null, null);
        var measurement = CreateMeasurementsQuery();
        var runContext = CreateRunContext(runId);
        var rows = CreateRows();

        _runRepository
            .Setup(repository => repository.ResolveAsync(runId, null, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(runContext);
        _measurementsRepository
            .Setup(repository => repository.QueryAsync(runContext, measurement, It.IsAny<CancellationToken>(), It.IsAny<int?>()))
            .ReturnsAsync(rows);
        _metaBuilder
            .Setup(builder => builder.Build(window, runContext, measurement))
            .Returns(new PlotMetaDto("Test", "X", "Y"));

        var result = await _sut.HandleAsync(
            new SimpleSeriesQuery { RunId = runId, MaxPoints = "all" },
            window,
            measurement,
            modes: null,
            CancellationToken.None);

        ResultAssertions.HasStatusCode(result, StatusCodes.Status200OK);
        _downsampler.Verify(downsampler => downsampler.MinMax(It.IsAny<IReadOnlyList<Point>>(), It.IsAny<int>()), Times.Never);
    }

    [Fact]
    public async Task HandleAsync_WhenMeasurementsExist_SchedulesBackgroundCacheWorkThroughUnifiedQueue()
    {
        var runId = Guid.NewGuid();
        var window = new WindowQuery(null, null);
        var measurement = CreateMeasurementsQuery();
        var runContext = CreateRunContext(runId);
        var rows = CreateRows();

        _runRepository
            .Setup(repository => repository.ResolveAsync(runId, null, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(runContext);
        _measurementsRepository
            .Setup(repository => repository.QueryAsync(runContext, measurement, It.IsAny<CancellationToken>(), It.IsAny<int?>()))
            .ReturnsAsync(rows);
        _downsampler
            .Setup(downsampler => downsampler.MinMax(It.IsAny<IReadOnlyList<Point>>(), 5000))
            .Returns(rows.Select(row => new Point(row.Ts, row.Value)).ToArray());
        _metaBuilder
            .Setup(builder => builder.Build(window, runContext, measurement))
            .Returns(new PlotMetaDto("Test", "X", "Y"));

        var result = await _sut.HandleAsync(
            new SimpleSeriesQuery { RunId = runId, MaxPoints = "5000" },
            window,
            measurement,
            modes: null,
            CancellationToken.None);

        ResultAssertions.HasStatusCode(result, StatusCodes.Status200OK);

        _backgroundCacheCoordinator.Verify(
            coordinator => coordinator.ReserveOrGetAsync(
                It.Is<CacheWorkKey>(key => key.Type == "SIMPLE" && key.RunId == runId),
                It.IsAny<CancellationToken>()),
            Times.Once);

        _backgroundCacheQueue.Verify(
            queue => queue.ScheduleAfterResponse(It.IsAny<HttpContext?>(), It.IsAny<BackgroundCacheWorkItem>()),
            Times.Once);
    }

    private static MeasurementsQuery CreateMeasurementsQuery() =>
        new(
            Quantity: "frequency",
            Component: "freq",
            PhaseMode: PhaseMode.Any,
            Phase: null,
            PmuNames: null,
            Unit: "Hz");

    private static RunContext CreateRunContext(Guid runId) =>
        new(
            runId,
            "PDC-TEST",
            new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            new DateTime(2025, 1, 1, 0, 1, 0, DateTimeKind.Utc),
            1,
            ["PMU1"],
            30);

    private static IReadOnlyList<MeasurementRow> CreateRows()
    {
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        return
        [
            new MeasurementRow(1, 1, "PMU1", "PDC-TEST", start, 59.95),
            new MeasurementRow(1, 1, "PMU1", "PDC-TEST", start.AddSeconds(1), 60.00),
            new MeasurementRow(1, 1, "PMU1", "PDC-TEST", start.AddSeconds(2), 60.05)
        ];
    }
}
