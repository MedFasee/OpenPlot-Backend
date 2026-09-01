namespace OpenPlot.Features.Runs.Repositories;

// Normalizacao compartilhada por chaves de coalescencia de query e de
// metadata cache: a ordem das PMUs nunca deve alterar a chave resultante.
internal static class MeasurementKeyNormalization
{
    public static string NormalizePmuKey(IReadOnlyList<string> pmuNames) =>
        string.Join(
            '\u001F',
            pmuNames
                .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                .Select(x => x.Trim().ToUpperInvariant()));
}
