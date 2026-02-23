namespace OpenPlot.ExportWorker.Domain;

public sealed record AnalogSeries(
    int Index,
    string Name,
    string Unit,
    double[] Values
);