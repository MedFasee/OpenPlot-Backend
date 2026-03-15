using OpenPlot.Data.Dtos;

namespace OpenPlot.Features.Runs.Handlers;

public sealed record PhasorRequestSelection(
    bool Tri,
    string? Phase,
    string TriPmuName,
    string[] PmuNames
);

public interface IPhasorRequestService
{
    (bool IsValid, string? Error, PhasorRequestSelection? Selection) Resolve(ByRunQuery q, string[]? pmuFromEndpoint);
}

public sealed class PhasorRequestService : IPhasorRequestService
{
    private readonly IPmuQueryHelper _pmuHelper;

    public PhasorRequestService(IPmuQueryHelper pmuHelper)
    {
        _pmuHelper = pmuHelper;
    }

    public (bool IsValid, string? Error, PhasorRequestSelection? Selection) Resolve(ByRunQuery q, string[]? pmuFromEndpoint)
    {
        var tri = q.Tri;

        var pmuName = q.Pmu?.Trim();
        if (tri && string.IsNullOrWhiteSpace(pmuName) && pmuFromEndpoint is { Length: > 0 })
            pmuName = pmuFromEndpoint[0]?.Trim();

        string? uphase = null;

        if (!tri)
        {
            if (string.IsNullOrWhiteSpace(q.Phase))
                return (false, "phase é obrigatório (A|B|C) quando tri=false.", null);

            uphase = q.Phase.Trim().ToUpperInvariant();
            if (uphase is not ("A" or "B" or "C"))
                return (false, "phase deve ser A, B ou C.", null);
        }
        else
        {
            if (string.IsNullOrWhiteSpace(pmuName))
                return (false, "para tri=true é obrigatório informar pmu (id_name da PMU).", null);
        }

        var pmuNames = _pmuHelper.Normalize(pmuFromEndpoint, q.Pmus);

        return (true, null, new PhasorRequestSelection(
            Tri: tri,
            Phase: uphase,
            TriPmuName: pmuName!,
            PmuNames: pmuNames));
    }
}
