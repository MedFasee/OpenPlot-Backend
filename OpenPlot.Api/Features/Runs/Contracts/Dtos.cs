using System.Text.Json;
using System.Text.Json.Serialization;

namespace OpenPlot.Features.Runs.Contracts;


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
    public string? ReferenceTerminal { get; init; }
    public string? Phase { get; init; }
    public string? Component { get; init; }
    public string? Quantity { get; init; }
    public string? Unit { get; init; }

    [JsonPropertyOrder(99)] public List<RowsCachePoint> Points { get; init; } = new();
}

public sealed class RowsCachePoint
{
    public DateTime Ts { get; init; }
    public double Value { get; init; }
}