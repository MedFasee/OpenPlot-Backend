using System;
using System.Collections.Generic;

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

public interface IUiMenuService
{
    // Retorno simples (pra /runs): dicionário com defaults
    Dictionary<string, object?>? Build(UiMenuSet set);

    // Opcional (pra /runs inferir automatico quando você quiser)
    UiMenuSet ResolveFromPlot(string quantity, string? component, string? kind);
}

public sealed class UiMenuService : IUiMenuService
{
    private readonly FeatureFlags _flags;
    public UiMenuService(FeatureFlags flags) => _flags = flags;

    public Dictionary<string, object?>? Build(UiMenuSet set)
    {
        if (set == UiMenuSet.None) return null;

        var modes = new Dictionary<string, object?>();

        if (set.HasFlag(UiMenuSet.Oscillations))
        {
            var osc = BuildOscillations();
            if (osc.Count > 0) modes["oscillations"] = osc;
        }

        if (set.HasFlag(UiMenuSet.Events) && _flags.EnablesEventsAnalyzer)
        {
            var evt = BuildEvents();
            if (evt.Count > 0) modes["events"] = evt;
        }

        return modes.Count == 0 ? null : modes;
    }

    private Dictionary<string, object?> BuildOscillations()
    {
        var oscillations = new Dictionary<string, object?>();

        var transient = new Dictionary<string, object?>();
        if (_flags.EnablesDFT) transient["DFT"] = true;
        if (_flags.EnablesProny) transient["Prony"] = new Dictionary<string, object?> { ["Ordem"] = 300 };

        var environment = new Dictionary<string, object?>();
        if (_flags.EnablesDFT) environment["DFT"] = true;
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