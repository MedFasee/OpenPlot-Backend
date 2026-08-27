using Microsoft.Extensions.Logging;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Data.Dtos;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers.Abstractions;
using OpenPlot.Features.Runs.Handlers.Base;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.UI;

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
        IAnalysisCacheRepository cacheRepository,
        IUiMenuService uiMenus,
        ILogger<SimpleSeriesHandler> logger)
        : base(runRepository, metaBuilder, ConvertCacheRepo(cacheRepository), uiMenus, logger)
    {
        _measRepository = measRepository ?? throw new ArgumentNullException(nameof(measRepository));
        _downsampler = downsampler ?? throw new ArgumentNullException(nameof(downsampler));
        _seriesAssembly = seriesAssembly ?? throw new ArgumentNullException(nameof(seriesAssembly));
    }

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
        CancellationToken ct,
        int? maxPoints)
    {
        _logger.LogInformation(
        "[SIMPLE] quantity={Quantity} component={Component} phaseMode={PhaseMode} phase={Phase} pmus={Pmus}",
        _currentMeasurement.Quantity,
        _currentMeasurement.Component,
        _currentMeasurement.PhaseMode,
        _currentMeasurement.Phase,
        _currentMeasurement.PmuNames is null
            ? "<null>"
            : string.Join(",", _currentMeasurement.PmuNames));
        if (_currentMeasurement is null)
        {
            throw new InvalidOperationException("MeasurementsQuery não foi configurada.");
        }

        return await _measRepository.QueryAsync(runContext, _currentMeasurement, ct, maxPoints);
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
                    true,
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
                    referenceTerminal: null,
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
            runContext.SelectRate ?? 0,
            cacheSeries);
    }

    protected override UiMenuContext BuildUiMenuContext(
        RunContext runContext,
        DateTime windowFrom,
        DateTime windowTo,
        IReadOnlyList<MeasurementRow> rows)
    {
        var groupedRows = rows
            .GroupBy(row => row.IdName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var pmuCount = groupedRows.Count;
        var availablePointCount = groupedRows.Count == 0
            ? 0
            : groupedRows.Min(group => group.Count());

        return new UiMenuContext(
            WindowFromUtc: windowFrom,
            WindowToUtc: windowTo,
            SelectRate: runContext.SelectRate,
            TotalSeriesCount: pmuCount,
            ValidSeriesCount: pmuCount,
            AvailablePointCount: availablePointCount,
            Quantity: _currentMeasurement?.Quantity,
            Component: _currentMeasurement?.Component,
            Phase: _currentMeasurement?.Phase);
    }

    protected override PlotMetaDto BuildPlotMeta(
        RunContext runContext,
        SimpleSeriesQuery query,
        WindowQuery window)
    {
        if (_currentMeasurement is null)
        {
            return base.BuildPlotMeta(runContext, query, window);
        }

        return _metaBuilder.Build(window, runContext, _currentMeasurement);
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

        public async Task<object?> SaveAsync(Guid cacheId, Guid runId, RowsCacheV2 payload, CancellationToken ct)
        {
            return await _innerRepo.SaveAsync(cacheId, runId, payload, ct);
        }
    }
}
