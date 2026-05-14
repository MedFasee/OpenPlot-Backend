using System;
using System.Collections.Generic;
using System.Linq;
using OpenPlot.Features.Runs.Contracts;

namespace OpenPlot.Services.UI;

[Flags]
public enum UiMenuSet
{
    None = 0,
    Oscillations = 1,
    Events = 2
}

public sealed record FeatureFlags(
    bool EnablesDFT,
    bool EnablesProny,
    bool EnablesCCA,
    bool EnablesEventsAnalyzer
);

public sealed record UiMenuContext(
    DateTime WindowFromUtc,
    DateTime WindowToUtc,
    int? SelectRate,
    int? EffectivePointCount = null,
    int? TotalSeriesCount = null,
    int? ValidSeriesCount = null)
{
    public static UiMenuContext FromCache(RowsCacheV2 payload)
    {
        ArgumentNullException.ThrowIfNull(payload);

        var totalSeriesCount = payload.Series?.Count ?? 0;
        var validSeriesCount = payload.Series?.Count(s => s.Points.Count >= 2) ?? 0;

        return new UiMenuContext(
            WindowFromUtc: payload.From,
            WindowToUtc: payload.To,
            SelectRate: payload.SelectRate,
            EffectivePointCount: ComputeUniformPointCount(payload.From, payload.To, payload.SelectRate),
            TotalSeriesCount: totalSeriesCount,
            ValidSeriesCount: validSeriesCount);
    }

    public int ResolveEffectivePointCount()
    {
        if (EffectivePointCount is > 0)
            return EffectivePointCount.Value;

        return ComputeUniformPointCount(WindowFromUtc, WindowToUtc, SelectRate);
    }

    private static int ComputeUniformPointCount(DateTime fromUtc, DateTime toUtc, int? selectRate)
    {
        if (selectRate is null || selectRate <= 0)
            return 0;

        var duration = toUtc - fromUtc;
        if (duration < TimeSpan.Zero)
            return 0;

        return (int)Math.Floor(duration.TotalSeconds * selectRate.Value) + 1;
    }
}

public interface IUiMenuService
{
    // Retorno simples (pra /runs): dicionário com defaults
    Dictionary<string, object?>? Build(UiMenuSet set);

    Dictionary<string, object?>? Build(UiMenuSet set, UiMenuContext context);

    Dictionary<string, object?>? RebuildForRun(Dictionary<string, object?>? modes, UiMenuContext context);

    // Opcional (pra /runs inferir automatico quando você quiser)
    UiMenuSet ResolveFromPlot(string quantity, string? component, string? kind);
}

public sealed class UiMenuService : IUiMenuService
{
    private readonly FeatureFlags _flags;
    public UiMenuService(FeatureFlags flags) => _flags = flags;

    public Dictionary<string, object?>? Build(UiMenuSet set)
        => BuildInternal(set, null);

    public Dictionary<string, object?>? Build(UiMenuSet set, UiMenuContext context)
        => BuildInternal(set, context);

    public Dictionary<string, object?>? RebuildForRun(Dictionary<string, object?>? modes, UiMenuContext context)
    {
        var set = InferSet(modes);
        return set == UiMenuSet.None
            ? null
            : BuildInternal(set, context);
    }

    private Dictionary<string, object?>? BuildInternal(UiMenuSet set, UiMenuContext? context)
    {
        if (set == UiMenuSet.None) return null;

        var modes = new Dictionary<string, object?>();

        if (set.HasFlag(UiMenuSet.Oscillations))
        {
            var osc = BuildOscillations(context);
            if (osc.Count > 0) modes["oscillations"] = osc;
        }

        if (set.HasFlag(UiMenuSet.Events) && _flags.EnablesEventsAnalyzer)
        {
            var evt = BuildEvents();
            if (evt.Count > 0) modes["events"] = evt;
        }

        return modes.Count == 0 ? null : modes;
    }

    private Dictionary<string, object?> BuildOscillations(UiMenuContext? context)
    {
        var oscillations = new Dictionary<string, object?>();

        var transient = new Dictionary<string, object?>();
        transient["DFT"] = BuildDftSettings(_flags.EnablesDFT);
        transient["Prony"] = BuildPronySettings(context);

        var environment = new Dictionary<string, object?>();
        environment["DFT"] = BuildDftSettings(_flags.EnablesDFT);
        if (_flags.EnablesCCA)
        {
            environment["CVA"] = new Dictionary<string, object?>
            {
                ["Ordem do modelo"] = 8,
                ["N° de linhas por bloco"] = 20,
                ["Tam. da janela (min.)"] = 10,
                ["Passo da janela (s)"] = 60,
                ["Freq. mínima (Hz)"] = 0.3,
                ["Freq. máxima (Hz)"] = 0.4
            };
        }

        if (transient.Count > 0) oscillations["Transitório"] = transient;
        if (environment.Count > 0) oscillations["Ambiente"] = environment;

        return oscillations;
    }

    private static Dictionary<string, object?> BuildDftSettings(bool enabled) => new()
    {
        ["enabled"] = enabled
    };

    private Dictionary<string, object?> BuildPronySettings(UiMenuContext? context)
    {
        if (context is null)
        {
            return new Dictionary<string, object?>
            {
                ["enabled"] = _flags.EnablesProny,
                ["Ordem"] = 300
            };
        }

        var order = ComputePronyDefaultOrder(context);
        var enabled = _flags.EnablesProny && IsPronyEnabled(context, order);

        return new Dictionary<string, object?>
        {
            ["enabled"] = enabled,
            ["Ordem"] = order
        };
    }

    private static int ComputePronyDefaultOrder(UiMenuContext context)
    {
        var sampleCount = context.ResolveEffectivePointCount();
        if (sampleCount <= 0)
            return 300;
        if (sampleCount <= 1)
            return 1;

        var sampleSpan = sampleCount - 1;
        return sampleSpan > 4
            ? Math.Min(sampleSpan / 4, 300)
            : Math.Max(sampleSpan, 1);
    }

    private static bool IsPronyEnabled(UiMenuContext context, int order)
    {
        if (context.SelectRate is null || context.SelectRate <= 0)
            return false;

        var sampleCount = context.ResolveEffectivePointCount();
        if (sampleCount < 4)
            return false;

        if ((double)sampleCount / context.SelectRate.Value > 60.0)
            return false;

        if (context.TotalSeriesCount is > 25)
            return false;

        var validSeriesCount = context.ValidSeriesCount ?? context.TotalSeriesCount ?? 0;
        if (validSeriesCount <= 0)
            return false;

        if (order <= 0 || order >= sampleCount || order > 300)
            return false;

        return validSeriesCount * (sampleCount - order) >= order;
    }

    private static UiMenuSet InferSet(Dictionary<string, object?>? modes)
    {
        if (modes is null || modes.Count == 0)
            return UiMenuSet.None;

        var set = UiMenuSet.None;
        if (modes.ContainsKey("oscillations")) set |= UiMenuSet.Oscillations;
        if (modes.ContainsKey("events")) set |= UiMenuSet.Events;

        return set;
    }

    private Dictionary<string, object?> BuildEvents()
    {
        var settings = new Dictionary<string, object?>
        {
            ["Filtro Passa-Baixa"] = new Dictionary<string, object?>
            {
                ["Ordem do filtro"] = 20
            },

            ["Filtro Passa-Faixa"] = new Dictionary<string, object?>
            {
                ["Ordem do filtro do ruído"] = 20,
                ["Ordem do filtro de oscilações"] = 300
            },

            ["Filtro + Taxa de Variação"] = new Dictionary<string, object?>
            {
                ["Ordem do filtro"] = 20,
                ["Intervalo de amostras"] = 60
            },

            ["Filtro de Kalman"] = new Dictionary<string, object?>
            {
                ["selected"] = true,
                ["Desvio padrão da medida"] = 0.005,
                ["Característica dinâmica do evento"] = 0.01,
                ["Limite de aceleração angular"] = 1
            }
        };

        return new Dictionary<string, object?>
        {
            ["(ícone de raio)"] = new Dictionary<string, object?>
            {
                ["Pré-processar"] = true,
                ["Configurações"] = settings
            }
        };
    }

    public UiMenuSet ResolveFromPlot(string quantity, string? component, string? kind)
    {
        var q = (quantity ?? "").Trim().ToLowerInvariant();
        var c = (component ?? "").Trim().ToLowerInvariant();

        if (q == "digital") return UiMenuSet.None;
        if (c == "thd") return UiMenuSet.None;

        if (q == "frequency" && c == "freq")
            return UiMenuSet.Oscillations | UiMenuSet.Events;

        return UiMenuSet.Oscillations;
    }
}