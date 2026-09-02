namespace OpenPlot.ExportWorker.Domain;

public sealed record AnalogSeries(
    int Index,
    string Name,
    string Phase,
    string Unit,
    double[] Values
);
