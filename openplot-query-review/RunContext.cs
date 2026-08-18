namespace OpenPlot.ExportWorker.Domain;

public sealed record RunContext(
    Guid RunId,
    string PdcName,
    string Label,
    DateTimeOffset FromUtc,
    DateTimeOffset ToUtc,
    int SelectRate,
    string[] RunPmus,        // COALESCE(pmus_ok, pmus) parseado
    string[]? PmuFilter      // opcional (se quiser limitar)
);