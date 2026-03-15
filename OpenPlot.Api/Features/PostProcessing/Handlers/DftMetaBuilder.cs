using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.Features.PostProcessing.Handlers;

public interface IDftMetaBuilder
{
    PlotMetaDto Build(RowsCacheV2 payload);
}

public sealed class DftMetaBuilder : IDftMetaBuilder
{
    private readonly IPlotMetaBuilder _plotMetaBuilder;

    public DftMetaBuilder(IPlotMetaBuilder plotMetaBuilder)
    {
        _plotMetaBuilder = plotMetaBuilder;
    }

    public PlotMetaDto Build(RowsCacheV2 payload)
    {
        if (payload.Series is null || payload.Series.Count == 0)
            return new PlotMetaDto("Espectro de Freq.", "Tempo (UTC)", "Frequência (Hz)");

        var first = payload.Series[0];
        var pmuNames = payload.Series
            .Select(s => s.IdName)
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        var meas = BuildMeasurementsQuery(payload, first, pmuNames);
        var ctx = new RunContext(
            RunId: Guid.Empty,
            PdcName: first.PdcName,
            FromUtc: payload.From,
            ToUtc: payload.To,
            PdcId: 0,
            PmuNames: pmuNames,
            SelectRate: payload.SelectRate);

        var window = new WindowQuery(From: payload.From, To: payload.To);
        var baseMeta = _plotMetaBuilder.Build(window, ctx, meas);

        return new PlotMetaDto(
            Title: BuildDftTitle(baseMeta.Title),
            XLabel: baseMeta.XLabel,
            YLabel: "Frequência (Hz)");
    }

    private static MeasurementsQuery BuildMeasurementsQuery(
        RowsCacheV2 payload,
        RowsCacheSeries first,
        IReadOnlyList<string> pmuNames)
    {
        var quantity = NormalizeQuantity(first.Quantity);
        var component = NormalizeComponent(first.Component);
        var phase = NormalizePhase(first.Phase);
        var unit = first.Unit;
        var phaseMode = InferPhaseMode(payload, first, component, phase);

        return new MeasurementsQuery(
            Quantity: quantity,
            Component: component,
            PhaseMode: phaseMode,
            Phase: phase,
            PmuNames: pmuNames.Count == 0 ? null : pmuNames,
            Unit: unit,
            ReferenceTerminal: first.ReferenceTerminal);
    }

    private static string NormalizeQuantity(string? quantity)
    {
        var q = (quantity ?? string.Empty).Trim().ToLowerInvariant();
        return q switch
        {
            "active" => "p_active",
            "reactive" => "p_reactive",
            _ => q
        };
    }

    private static string NormalizeComponent(string? component)
    {
        var c = (component ?? string.Empty).Trim().ToLowerInvariant();
        return c switch
        {
            "seq" => "mag",
            "angle_diff_phase" or "angle_diff_sequence" => "angle",
            _ => c
        };
    }

    private static string? NormalizePhase(string? phase)
    {
        if (string.IsNullOrWhiteSpace(phase)) return null;

        var p = phase.Trim().ToLowerInvariant();
        return p switch
        {
            "pos" => null,
            "neg" => null,
            "zero" => null,
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
            .Select(s => s.IdName)
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Count();
        var phasesForFirstPmu = payload.Series
            .Where(s => string.Equals(s.IdName, first.IdName, StringComparison.OrdinalIgnoreCase))
            .Select(s => (s.Phase ?? string.Empty).Trim().ToUpperInvariant())
            .Where(s => s.Length > 0)
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

    private static string BuildDftTitle(string baseTitle)
    {
        var article = UsesFeminineArticle(baseTitle) ? "da" : "do";
        return $"Espectro de Freq. {article} {baseTitle}";
    }

    private static bool UsesFeminineArticle(string title)
    {
        if (string.IsNullOrWhiteSpace(title)) return false;

        return title.StartsWith("Diferença", StringComparison.OrdinalIgnoreCase)
            || title.StartsWith("Frequência", StringComparison.OrdinalIgnoreCase)
            || title.StartsWith("Potência", StringComparison.OrdinalIgnoreCase)
            || title.StartsWith("Variação", StringComparison.OrdinalIgnoreCase)
            || title.StartsWith("Distorção", StringComparison.OrdinalIgnoreCase)
            || title.StartsWith("Sinal", StringComparison.OrdinalIgnoreCase);
    }
}
