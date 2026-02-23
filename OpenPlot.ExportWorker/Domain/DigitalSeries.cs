namespace OpenPlot.ExportWorker.Domain;

public sealed record DigitalSeries(
    int Index,
    string Name,
    bool[] Values
);