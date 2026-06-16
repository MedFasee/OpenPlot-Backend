using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.Features.PostProcessing.Handlers;

public sealed record CcaPlotMeta(
    string Title,
    string XLabel,
    string FrequencyYLabel,
    string DampingYLabel,
    string EnergyYLabel,
    string IdmYLabel);

public interface ICcaMetaBuilder
{
    CcaPlotMeta Build(RowsCacheV2 payload, DateTime? fromUtc = null, DateTime? toUtc = null);
}

public sealed class CcaMetaBuilder : ICcaMetaBuilder
{
    private readonly IPlotMetaBuilder _plotMetaBuilder;

    public CcaMetaBuilder(IPlotMetaBuilder plotMetaBuilder)
    {
        _plotMetaBuilder = plotMetaBuilder;
    }

    public CcaPlotMeta Build(RowsCacheV2 payload, DateTime? fromUtc = null, DateTime? toUtc = null)
    {
        if (payload.Series is null || payload.Series.Count == 0)
        {
            return new CcaPlotMeta(
                Title: "CCA",
                XLabel: "Tempo (UTC)",
                FrequencyYLabel: "Frequência (Hz)",
                DampingYLabel: "Amortecimento (%)",
                EnergyYLabel: "Pseudoenergia",
                IdmYLabel: "IDM");
        }

        var first = payload.Series[0];
        var pmuNames = payload.Series
            .Select(series => series.IdName)
            .Where(name => !string.IsNullOrWhiteSpace(name))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        var measurementsQuery = BuildMeasurementsQuery(payload, first, pmuNames);
        var context = new RunContext(
            RunId: Guid.Empty,
            PdcName: first.PdcName,
            FromUtc: fromUtc ?? payload.From,
            ToUtc: toUtc ?? payload.To,
            PdcId: 0,
            PmuNames: pmuNames,
            SelectRate: payload.SelectRate);

        var window = new WindowQuery(fromUtc ?? payload.From, toUtc ?? payload.To);
        var baseMeta = _plotMetaBuilder.Build(window, context, measurementsQuery);

        return new CcaPlotMeta(
            Title: BuildCcaTitle(baseMeta.Title),
            XLabel: baseMeta.XLabel,
            FrequencyYLabel: "Frequência (Hz)",
            DampingYLabel: "Amortecimento (%)",
            EnergyYLabel: "Pseudoenergia",
            IdmYLabel: "IDM");
    }

    private static MeasurementsQuery BuildMeasurementsQuery(
        RowsCacheV2 payload,
        RowsCacheSeries first,
        IReadOnlyList<string> pmuNames)
    {
        var quantity = NormalizeQuantity(first.Quantity);
        var component = NormalizeComponent(first.Component);
        var phase = NormalizePhase(first.Phase);
        var phaseMode = InferPhaseMode(payload, first, component, phase);

        return new MeasurementsQuery(
            Quantity: quantity,
            Component: component,
            PhaseMode: phaseMode,
            Phase: phase,
            PmuNames: pmuNames.Count == 0 ? null : pmuNames,
            Unit: first.Unit,
            ReferenceTerminal: first.ReferenceTerminal);
    }

    private static string NormalizeQuantity(string? quantity)
    {
        var value = (quantity ?? string.Empty).Trim().ToLowerInvariant();
        return value switch
        {
            "active" => "p_active",
            "reactive" => "p_reactive",
            _ => value
        };
    }

    private static string NormalizeComponent(string? component)
    {
        var value = (component ?? string.Empty).Trim().ToLowerInvariant();
        return value switch
        {
            "seq" => "mag",
            "angle_diff_phase" or "angle_diff_sequence" => "angle",
            _ => value
        };
    }

    private static string? NormalizePhase(string? phase)
    {
        if (string.IsNullOrWhiteSpace(phase))
            return null;

        var value = phase.Trim().ToLowerInvariant();
        return value switch
        {
            "pos" or "neg" or "zero" => null,
            _ => phase.Trim().ToUpperInvariant()
        };
    }

    private static PhaseMode InferPhaseMode(
        RowsCacheV2 payload,
        RowsCacheSeries first,
        string component,
        string? phase)
    {
        var rawComponent = (first.Component ?? string.Empty).Trim().ToLowerInvariant();
        var rawPhase = (first.Phase ?? string.Empty).Trim().ToLowerInvariant();
        var distinctPmus = payload.Series
            .Select(series => series.IdName)
            .Where(name => !string.IsNullOrWhiteSpace(name))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Count();
        var phasesForFirstPmu = payload.Series
            .Where(series => string.Equals(series.IdName, first.IdName, StringComparison.OrdinalIgnoreCase))
            .Select(series => (series.Phase ?? string.Empty).Trim().ToUpperInvariant())
            .Where(name => name.Length > 0)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (rawComponent == "ratio")
            return PhaseMode.Deseq;

        if (rawComponent == "seq" || rawComponent == "angle_diff_sequence")
        {
            return rawPhase switch
            {
                "pos" => PhaseMode.SeqPos,
                "neg" => PhaseMode.SeqNeg,
                "zero" => PhaseMode.SeqZero,
                _ => PhaseMode.Any
            };
        }

        if (rawComponent == "angle_diff_phase")
            return PhaseMode.Single;

        if (phase is "A" or "B" or "C")
            return PhaseMode.Single;

        var hasAbc = phasesForFirstPmu.Contains("A") && phasesForFirstPmu.Contains("B") && phasesForFirstPmu.Contains("C");
        if (distinctPmus == 1 && hasAbc)
            return PhaseMode.ThreePhase;

        return PhaseMode.Any;
    }

    private static string BuildCcaTitle(string baseTitle)
    {
        var article = UsesFeminineArticle(baseTitle) ? "da" : "do";
        return $"CCA {article} {baseTitle}";
    }

    private static bool UsesFeminineArticle(string title)
    {
        if (string.IsNullOrWhiteSpace(title))
            return false;

        return title.StartsWith("Diferença", StringComparison.OrdinalIgnoreCase)
            || title.StartsWith("Frequência", StringComparison.OrdinalIgnoreCase)
            || title.StartsWith("Potência", StringComparison.OrdinalIgnoreCase)
            || title.StartsWith("Variação", StringComparison.OrdinalIgnoreCase)
            || title.StartsWith("Distorção", StringComparison.OrdinalIgnoreCase)
            || title.StartsWith("Sinal", StringComparison.OrdinalIgnoreCase);
    }
}
