using OpenPlot.Core.TimeSeries;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Abstractions;
using OpenPlot.Features.Runs.Handlers.Base;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.Features.Runs.Handlers;

/// <summary>
/// Handler para séries simples (frequência, dfreq, digital, etc).
/// Características: não requerem cálculos complexos, apenas passthrough dos valores.
/// </summary>
public sealed class SimpleSeriesHandler : BaseSeriesHandler<SimpleSeriesQuery>
{
    private readonly IMeasurementsRepository _measRepository;
    private readonly ITimeSeriesDownsampler _downsampler;
    private readonly ISeriesAssemblyService _seriesAssembly;
    private MeasurementsQuery? _currentMeasurement; // Armazenado durante execução

    public SimpleSeriesHandler(
        IRunContextRepository runRepository,
        IMeasurementsRepository measRepository,
        ITimeSeriesDownsampler downsampler,
        IPlotMetaBuilder metaBuilder,
        ISeriesAssemblyService seriesAssembly,
        IAnalysisCacheRepository cacheRepository)
        : base(runRepository, metaBuilder, ConvertCacheRepo(cacheRepository))
    {
        _measRepository = measRepository ?? throw new ArgumentNullException(nameof(measRepository));
        _downsampler = downsampler ?? throw new ArgumentNullException(nameof(downsampler));
        _seriesAssembly = seriesAssembly ?? throw new ArgumentNullException(nameof(seriesAssembly));
    }

    /// <summary>
    /// Sobrecarga para compatibilidade com código antigo.
    /// </summary>
    public Task<IResult> HandleAsync(
        SimpleSeriesQuery q,
        WindowQuery w,
        MeasurementsQuery meas,
        CancellationToken ct)
        => HandleAsync(q, w, meas, modes: null, ct);

    /// <summary>
    /// Sobrecarga que permite especificar MeasurementsQuery customizada.
    /// </summary>
    public async Task<IResult> HandleAsync(
        SimpleSeriesQuery q,
        WindowQuery w,
        MeasurementsQuery meas,
        Dictionary<string, object?>? modes,
        CancellationToken ct)
    {
        _currentMeasurement = meas;
        return await base.HandleAsync(q, w, modes, ct);
    }

    protected override async Task<IReadOnlyList<MeasurementRow>> QueryDataAsync(
        SimpleSeriesQuery query,
        RunContext runContext,
        WindowQuery window,
        CancellationToken ct)
    {
        if (_currentMeasurement is null)
        {
            throw new InvalidOperationException("MeasurementsQuery não foi configurada.");
        }

        return await _measRepository.QueryAsync(runContext, _currentMeasurement, ct);
    }

    protected override List<object> TransformData(
        IReadOnlyList<MeasurementRow> rows,
        int maxPoints,
        bool noDownsample)
    {
        return rows
            .GroupBy(r => r.SignalId)
            .Select(g =>
            {
                var first = g.First();
                var points = _seriesAssembly.BuildPoints(
                    g.Select(x => (x.Ts, x.Value)),
                    noDownsample,
                    maxPoints,
                    _downsampler);

                return new SeriesDto(
                    Pdc: first.PdcName,
                    Pmu: first.IdName,
                    SignalId: first.SignalId,
                    PdcPmuId: first.PdcPmuId,
                    Unit: _currentMeasurement?.Unit ?? "raw",
                    Meta: null,
                    Points: points
                );
            })
            .Cast<object>()
            .ToList();
    }

    protected override RowsCacheV2? BuildCachePayload(
        IReadOnlyList<MeasurementRow> rows,
        DateTime windowFrom,
        DateTime windowTo,
        RunContext runContext)
    {
        var cacheSeries = rows
            .GroupBy(r => r.SignalId)
            .Select(g =>
            {
                var first = g.First();
                return _seriesAssembly.BuildCacheSeries(
                    signalId: first.SignalId,
                    pdcPmuId: first.PdcPmuId,
                    idName: first.IdName,
                    pdcName: first.PdcName,
                    unit: _currentMeasurement?.Unit,
                    phase: null,
                    quantity: _currentMeasurement?.Quantity,
                    component: _currentMeasurement?.Component,
                    points: g.Select(x => (x.Ts, x.Value)));
            })
            .OrderBy(s => s.IdName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        return _seriesAssembly.BuildCachePayload(
            windowFrom,
            windowTo,
            (int)runContext.SelectRate,
            cacheSeries);
    }

    protected override PlotMetaDto BuildPlotMeta(
        RunContext runContext,
        SimpleSeriesQuery query)
    {
        if (_currentMeasurement is null)
        {
            return base.BuildPlotMeta(runContext, query);
        }

        var yLabel = _currentMeasurement.Unit ?? "raw";
        var title = $"{_currentMeasurement.Quantity?.ToUpperInvariant()} - {_currentMeasurement.Component?.ToUpperInvariant()}";

        return new PlotMetaDto(
            Title: title,
            XLabel: "Tempo",
            YLabel: yLabel
        );
    }

    protected override string GetEmptyDataMessage() =>
        "Nada encontrado para esse run/filtro.";

    /// <summary>
    /// Conversor adaptador para ISeriesCacheService.
    /// </summary>
    private static ISeriesCacheService ConvertCacheRepo(IAnalysisCacheRepository repo)
    {
        return new CacheServiceAdapter(repo);
    }

    /// <summary>
    /// Adaptador para converter IAnalysisCacheRepository em ISeriesCacheService.
    /// </summary>
    private sealed class CacheServiceAdapter : ISeriesCacheService
    {
        private readonly IAnalysisCacheRepository _innerRepo;

        public CacheServiceAdapter(IAnalysisCacheRepository innerRepo)
        {
            _innerRepo = innerRepo;
        }

        public async Task<object?> SaveAsync(Guid runId, RowsCacheV2 payload, CancellationToken ct)
        {
            return await _innerRepo.SaveAsync(runId, payload, ct);
        }
    }
}