using Microsoft.AspNetCore.Http;
using Moq;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Abstractions;
using OpenPlot.Features.Runs.Handlers.Base;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.UnitTests.Infrastructure;

namespace OpenPlot.UnitTests.Features.Runs.Handlers;

public sealed class BaseSeriesHandlerTests
{
    private sealed class TestSeriesHandler(
        IRunContextRepository runRepository,
        IPlotMetaBuilder metaBuilder,
        ISeriesCacheService cacheService) : BaseSeriesHandler<SimpleSeriesQuery>(runRepository, metaBuilder, cacheService)
    {
        public IReadOnlyList<MeasurementRow> RowsToReturn { get; set; } = [];

        public (bool isValid, string? errorMessage) Validate(SimpleSeriesQuery query, WindowQuery window) =>
            ValidateInput(query, window);

        public PlotMetaDto BuildDefaultPlotMeta(RunContext runContext, SimpleSeriesQuery query, WindowQuery window) =>
            BuildPlotMeta(runContext, query, window);

        protected override Task<IReadOnlyList<MeasurementRow>> QueryDataAsync(
            SimpleSeriesQuery query,
            RunContext runContext,
            WindowQuery window,
            CancellationToken ct) =>
            Task.FromResult(RowsToReturn);

        protected override List<object> TransformData(
            IReadOnlyList<MeasurementRow> rows,
            int maxPoints,
            bool noDownsample) =>
            [new { count = rows.Count, maxPoints, noDownsample }];
    }

    private readonly Mock<IRunContextRepository> _runRepository = new();
    private readonly Mock<IPlotMetaBuilder> _metaBuilder = new();
    private readonly Mock<ISeriesCacheService> _cacheService = new();
    private readonly TestSeriesHandler _sut;

    public BaseSeriesHandlerTests()
    {
        _sut = new TestSeriesHandler(_runRepository.Object, _metaBuilder.Object, _cacheService.Object);
    }

    [Fact]
    public async Task HandleAsync_WhenRunExistsAndRowsAreReturned_ReturnsOk()
    {
        var runId = Guid.NewGuid();
        var window = new WindowQuery(null, null);
        var runContext = CreateRunContext(runId);
        _sut.RowsToReturn = CreateRows();

        _runRepository
            .Setup(repository => repository.ResolveAsync(runId, null, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(runContext);

        var result = await _sut.HandleAsync(new SimpleSeriesQuery { RunId = runId }, window, modes: null, CancellationToken.None);

        ResultAssertions.HasStatusCode(result, StatusCodes.Status200OK);
    }

    [Fact]
    public async Task HandleAsync_WhenRunDoesNotExist_ReturnsNotFound()
    {
        var runId = Guid.NewGuid();

        _runRepository
            .Setup(repository => repository.ResolveAsync(runId, null, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync((RunContext?)null);

        var result = await _sut.HandleAsync(new SimpleSeriesQuery { RunId = runId }, new WindowQuery(null, null), modes: null, CancellationToken.None);

        ResultAssertions.HasStatusCode(result, StatusCodes.Status404NotFound);
    }

    [Fact]
    public void Validate_WhenRunIdIsEmpty_ReturnsInvalidResult()
    {
        var validation = _sut.Validate(new SimpleSeriesQuery { RunId = Guid.Empty }, new WindowQuery(null, null));

        Assert.False(validation.isValid);
        Assert.Equal("run_id é obrigatório.", validation.errorMessage);
    }

    [Fact]
    public void Validate_WhenFromIsGreaterThanOrEqualToTo_ReturnsInvalidResult()
    {
        var now = DateTime.UtcNow;

        var validation = _sut.Validate(
            new SimpleSeriesQuery { RunId = Guid.NewGuid() },
            new WindowQuery(now, now.AddMinutes(-1)));

        Assert.False(validation.isValid);
        Assert.Equal("from deve ser menor que to.", validation.errorMessage);
    }

    [Fact]
    public void BuildPlotMeta_WhenBaseImplementationIsUsed_ReturnsDefaultMetadata()
    {
        var meta = _sut.BuildDefaultPlotMeta(
            CreateRunContext(Guid.NewGuid()),
            new SimpleSeriesQuery { RunId = Guid.NewGuid() },
            new WindowQuery(null, null));

        Assert.Equal("Série Temporal", meta.Title);
        Assert.Equal("Tempo", meta.XLabel);
        Assert.Equal("Valor", meta.YLabel);
    }

    private static RunContext CreateRunContext(Guid runId) =>
        new(
            runId,
            "PDC-1",
            new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            new DateTime(2025, 1, 1, 0, 1, 0, DateTimeKind.Utc),
            1,
            ["PMU-1"],
            60);

    private static IReadOnlyList<MeasurementRow> CreateRows()
    {
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        return
        [
            new MeasurementRow(1, 10, "PMU-1", "PDC-1", start, 59.9),
            new MeasurementRow(1, 10, "PMU-1", "PDC-1", start.AddSeconds(1), 60.1)
        ];
    }
}
