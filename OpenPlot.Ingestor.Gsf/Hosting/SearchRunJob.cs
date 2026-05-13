using System;

namespace OpenPlot.Ingestor.Gsf.Hosting;

internal sealed class SearchRunJob
{
    public Guid Id { get; init; }
    public string Source { get; init; } = "";
    public string? TerminalId { get; init; }
    public string SignalsJson { get; init; } = "";
    public string? PmusJson { get; init; }
    public DateTime From { get; init; }
    public DateTime To { get; init; }
    public int SelectRate { get; init; }
}
