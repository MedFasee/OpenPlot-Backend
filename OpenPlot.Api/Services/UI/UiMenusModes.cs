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
    int? ValidSeriesCount = null,
    int? AvailablePointCount = null)
{
    public static UiMenuContext FromCache(RowsCacheV2 payload)
    {
        ArgumentNullException.ThrowIfNull(payload);

        var totalSeriesCount = payload.Series?.Count ?? 0;
        var validSeriesCount = payload.Series?.Count(s => s.Points.Count >= 2) ?? 0;
        var availablePointCount = payload.Series is { Count: > 0 }
            ? payload.Series
                .Where(s => s.Points.Count > 0)
                .Select(s => s.Points.Count)
                .DefaultIfEmpty(0)
                .Min()
            : 0;

        return new UiMenuContext(
            WindowFromUtc: payload.From,
            WindowToUtc: payload.To,
            SelectRate: payload.SelectRate,
            EffectivePointCount: ComputeUniformPointCount(payload.From, payload.To, payload.SelectRate),
            TotalSeriesCount: totalSeriesCount,
            ValidSeriesCount: validSeriesCount,
            AvailablePointCount: availablePointCount);
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
    private const int DefaultCcaModelOrder = 8;
    private const int DefaultCcaBlockRows = 20;
    private const int DefaultCcaWindowMinutes = 10;
    private const int DefaultCcaWindowStepSeconds = 60;
    private const double DefaultCcaFrequencyMinHz = 0.3;
    private const double DefaultCcaFrequencyMaxHz = 0.4;

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
            environment["CCA"] = BuildCcaSettings(context);
        }

        if (transient.Count > 0) oscillations["Transitório"] = transient;
        if (environment.Count > 0) oscillations["Ambiente"] = environment;

        return oscillations;
    }

    private static Dictionary<string, object?> BuildDftSettings(bool enabled) => new()
    {
        ["enabled"] = enabled
    };

    private Dictionary<string, object?> BuildCcaSettings(UiMenuContext? context)
    {
        var enabled = _flags.EnablesCCA && (context is null || IsCcaEnabled(context));

        return new Dictionary<string, object?>
        {
            ["enabled"] = enabled,
            ["Ordem do modelo"] = DefaultCcaModelOrder,
            ["N° de linhas por bloco"] = DefaultCcaBlockRows,
            ["Tam. da janela (min.)"] = DefaultCcaWindowMinutes,
            ["Passo da janela (s)"] = DefaultCcaWindowStepSeconds,
            ["Freq. mínima (Hz)"] = DefaultCcaFrequencyMinHz,
            ["Freq. máxima (Hz)"] = DefaultCcaFrequencyMaxHz
        };
    }

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

        return Math.Min(ComputeMaxAllowedPronyOrder(sampleCount), 300);
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

        var maxAllowedOrder = Math.Min(ComputeMaxAllowedPronyOrder(sampleCount), 300);
        if (order <= 0 || order > maxAllowedOrder)
            return false;

        return validSeriesCount * (sampleCount - order) >= order;
    }

    private static int ComputeMaxAllowedPronyOrder(int sampleCount) =>
        Math.Max(1, sampleCount / 4);

    private static bool IsCcaEnabled(UiMenuContext context)
    {
        if (context.SelectRate is null || context.SelectRate <= 0)
            return false;

        var validSeriesCount = context.ValidSeriesCount ?? context.TotalSeriesCount ?? 0;
        if (validSeriesCount <= 0)
            return false;

        var availablePointCount = context.AvailablePointCount ?? context.ResolveEffectivePointCount();
        if (availablePointCount <= 0)
            return false;

        var windowPointCount = DefaultCcaWindowMinutes * 60 * context.SelectRate.Value;

        if (windowPointCount <= 2 * DefaultCcaBlockRows)
            return false;

        return windowPointCount <= availablePointCount;
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