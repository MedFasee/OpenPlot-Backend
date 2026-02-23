namespace OpenPlot.ExportWorker.Domain;

public sealed record PmuComtrade(
    string PmuDisplayName,
    string PmuFileSafeName,
    DateTimeOffset StartUtc,
    int SampleRate,
    IReadOnlyList<AnalogSeries> Analogs,
    IReadOnlyList<DigitalSeries> Digitals
);