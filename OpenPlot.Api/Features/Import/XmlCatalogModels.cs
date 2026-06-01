namespace OpenPlot.Features.Import;

internal sealed record ParsedCatalogFile(
    string FilePath,
    ParsedPdc Pdc,
    IReadOnlyList<ParsedPmu> Pmus,
    IReadOnlyList<string> Notes);

internal sealed record ParsedPdc(
    string Name,
    string Kind,
    string Address,
    int Fps,
    string UserName,
    string Password,
    string DatabaseName);

internal sealed record ParsedPmu(
    string IdName,
    string FullName,
    int VoltLevel,
    int? IdNumber,
    string Area,
    string State,
    string Station,
    double? Latitude,
    double? Longitude,
    IReadOnlyList<ParsedSignal> Signals);

internal sealed record ParsedSignal(
    string Name,
    string Quantity,
    string Phase,
    string Component,
    int HistorianPoint,
    string? NotInsertedNote);
