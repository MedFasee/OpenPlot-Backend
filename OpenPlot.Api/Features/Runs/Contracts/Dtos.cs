using System.Text.Json;

namespace OpenPlot.Features.Runs.Contracts;

public sealed record SeriesPoint(DateTime Ts, double Val);


public sealed record PlotMetaDto(
    string Title,
    string XLabel,
    string YLabel
);


public sealed record SeriesDto(
    string Pdc,
    string Pmu,
    int SignalId,
    int PdcPmuId,
    string Unit,
    object? Meta,
    IReadOnlyList<object[]> Points
);

public sealed record SeriesResponseDto(
    Guid RunId,
    string Data,
    object Resolved,
    object Window,
    PlotMetaDto? Meta,
    IReadOnlyList<SeriesDto> Series
);



public sealed class RowsCacheV2
{
    public DateTime From { get; init; }
    public DateTime To { get; init; }
    public int SelectRate { get; init; }
    public List<RowsCacheSeries> Series { get; init; } = new();
}

public sealed class RowsCacheSeries
{
    public int SignalId { get; init; }
    public int PdcPmuId { get; init; }
    public string IdName { get; init; } = default!;
    public string PdcName { get; init; } = default!;
    public string? Phase { get; init; }
    public string? Component { get; init; }

    public List<RowsCachePoint> Points { get; init; } = new();
}

public sealed class RowsCachePoint
{
    public DateTime Ts { get; init; }
    public double Value { get; init; }
}