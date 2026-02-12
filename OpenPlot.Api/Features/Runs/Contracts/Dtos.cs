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
    PlotMetaDto? Meta,               // <-- AQUI
    IReadOnlyList<SeriesDto> Series
);

