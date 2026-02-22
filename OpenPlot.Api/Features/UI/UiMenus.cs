namespace OpenPlot.Features.Ui;

[Flags]
public enum UiMenuSet
{
    None = 0,
    Oscillations = 1,
    Events = 2
}

public enum UiNodeType { Group, SplitButton, MenuItem, TextBox, Label, Separator }

public sealed record UiNode(
    string Id,
    string Text,
    UiNodeType Type,
    bool Visible = true,
    bool Enabled = true,
    string? DefaultValue = null,
    bool? Checked = null,
    string? Feature = null,
    IReadOnlyList<UiNode>? Children = null
);

public sealed record UiCatalog(IReadOnlyList<UiNode> Menus);

public sealed record FeatureFlags(
    bool EnablesDFT,
    bool EnablesProny,
    bool EnablesCCA,
    bool EnablesEventsAnalyzer
);

public interface IUiMenuService
{
    UiCatalog? Build(UiMenuSet set);
    UiMenuSet ResolveFromRequest(string quantity, string? component, string? kind);
}

public sealed class UiMenuService : IUiMenuService
{
    private readonly FeatureFlags _flags;
    private static readonly Lazy<UiCatalog> _full = new(BuildFullCatalog);

    public UiMenuService(FeatureFlags flags) => _flags = flags;

    // =========================================================
    // REGRA DE HABILITAÇÃO (o que você descreveu)
    // =========================================================
    public UiMenuSet ResolveFromRequest(string quantity, string? component, string? kind)
    {
        var q = (quantity ?? "").Trim().ToLowerInvariant();
        var c = (component ?? "").Trim().ToLowerInvariant();
        var k = (kind ?? "").Trim().ToLowerInvariant();

        // Digital -> nada
        if (q == "digital") return UiMenuSet.None;

        // THD -> nada (você modela via endpoint /thd e component='thd')
        if (c == "thd") return UiMenuSet.None;

        // Frequência (apenas freq) -> Oscilações + Eventos
        if (q == "frequency" && c == "freq") return UiMenuSet.Oscillations | UiMenuSet.Events;

        // dfreq (derivada) entra em "todas as outras grandezas" -> só oscilações
        if (q == "frequency" && c == "dfreq") return UiMenuSet.Oscillations;

        // THD endpoint usa quantity=voltage/current + component=thd, então já cai acima.
        // Restante -> oscilações
        return UiMenuSet.Oscillations;
    }

    // =========================================================
    // BUILD: árvore completa + filtro por set e feature flags
    // =========================================================
    public UiCatalog? Build(UiMenuSet set)
    {
        if (set == UiMenuSet.None) return null;

        bool FeatureOn(string? f) => f switch
        {
            null => true,
            "enablesDFT" => _flags.EnablesDFT,
            "enablesProny" => _flags.EnablesProny,
            "enablesCCA" => _flags.EnablesCCA,
            "enablesEventsAnalyzer" => _flags.EnablesEventsAnalyzer,
            _ => true
        };

        UiNode? Prune(UiNode n)
        {
            if (!FeatureOn(n.Feature)) return null;

            var kids = n.Children?
                .Select(Prune)
                .Where(x => x is not null)
                .Cast<UiNode>()
                .ToArray();

            return n with { Children = kids };
        }

        var menus = _full.Value.Menus
            .Select(Prune)
            .Where(x => x is not null)
            .Cast<UiNode>()
            .Where(m => m.Id switch
            {
                "oscillations" => set.HasFlag(UiMenuSet.Oscillations),
                "events" => set.HasFlag(UiMenuSet.Events),
                _ => true
            })
            .ToArray();

        return new UiCatalog(menus);
    }

    // =========================================================
    // ÁRVORE COMPLETA (espelho do Designer que você me mandou)
    // =========================================================
    private static UiCatalog BuildFullCatalog()
    {
        UiNode Oscillations() => new(
            Id: "oscillations",
            Text: "Análise de Oscilações",
            Type: UiNodeType.Group,
            Children: new[]
            {
                new UiNode("transient", "Transitório", UiNodeType.SplitButton, Children: new[]
                {
                    new UiNode("transient.dft", "DFT", UiNodeType.MenuItem, Feature: "enablesDFT", Children: new[]
                    {
                        new UiNode("transient.dft.calc", "Calcular", UiNodeType.MenuItem),
                    }),
                    new UiNode("transient.prony", "Prony", UiNodeType.MenuItem, Feature: "enablesProny", Children: new[]
                    {
                        new UiNode("transient.prony.order.lbl", "Ordem", UiNodeType.Label),
                        new UiNode("transient.prony.order", "Ordem", UiNodeType.TextBox),
                        new UiNode("transient.prony.calc", "Calcular", UiNodeType.MenuItem),
                    }),
                }),

                new UiNode("environment", "Ambiente", UiNodeType.SplitButton, Children: new[]
                {
                    new UiNode("environment.dft", "DFT", UiNodeType.MenuItem, Feature: "enablesDFT", Children: new[]
                    {
                        new UiNode("environment.dft.calc", "Calcular", UiNodeType.MenuItem),
                    }),

                    new UiNode("environment.cva", "CVA", UiNodeType.MenuItem, Feature: "enablesCCA", Children: new[]
                    {
                        new UiNode("environment.cva.modelOrder.lbl", "Ordem do modelo", UiNodeType.Label),
                        new UiNode("environment.cva.modelOrder", "Ordem do modelo", UiNodeType.TextBox),

                        new UiNode("environment.cva.linesPerBlock.lbl", "Nº de linhas por bloco", UiNodeType.Label),
                        new UiNode("environment.cva.linesPerBlock", "Nº de linhas por bloco", UiNodeType.TextBox),

                        new UiNode("environment.cva.windowMin.lbl", "Tam. da janela (min.)", UiNodeType.Label),
                        new UiNode("environment.cva.windowMin", "Tam. da janela (min.)", UiNodeType.TextBox),

                        new UiNode("environment.cva.stepSec.lbl", "Passo da janela (s)", UiNodeType.Label),
                        new UiNode("environment.cva.stepSec", "Passo da janela (s)", UiNodeType.TextBox),

                        new UiNode("environment.cva.fmin.lbl", "Freq. mínima (Hz)", UiNodeType.Label),
                        new UiNode("environment.cva.fmin", "Freq. mínima (Hz)", UiNodeType.TextBox),

                        new UiNode("environment.cva.fmax.lbl", "Freq. máxima (Hz)", UiNodeType.Label),
                        new UiNode("environment.cva.fmax", "Freq. máxima (Hz)", UiNodeType.TextBox),

                        new UiNode("environment.cva.calc", "Calcular", UiNodeType.MenuItem),
                    }),
                }),
            }
        );

        UiNode Events() => new(
            Id: "events",
            Text: "Análise de Eventos",
            Type: UiNodeType.Group,
            Feature: "enablesEventsAnalyzer",
            Children: new[]
            {
                new UiNode("events.lightning", "(ícone raio)", UiNodeType.SplitButton, Children: new[]
                {
                    new UiNode("events.preprocess", "Pré-Processar", UiNodeType.MenuItem),
                    new UiNode("events.settings", "Configurações", UiNodeType.MenuItem, Children: new[]
                    {
                        new UiNode("events.settings.lp", "Filtro Passa Baixa", UiNodeType.MenuItem, Children: new[]
                        {
                            new UiNode("events.settings.lp.order.lbl", "Ordem do filtro:", UiNodeType.Label, Visible:false),
                            new UiNode("events.settings.lp.order", "Ordem do filtro", UiNodeType.TextBox, DefaultValue:"20", Visible:false),
                        }),

                        new UiNode("events.settings.bp", "Filtro Passa Faixa", UiNodeType.MenuItem, Children: new[]
                        {
                            new UiNode("events.settings.bp.noise.lbl", "Ordem do filtro do ruído:", UiNodeType.Label, Visible:false),
                            new UiNode("events.settings.bp.noise", "Ordem do filtro do ruído", UiNodeType.TextBox, DefaultValue:"20", Visible:false),
                            new UiNode("events.settings.bp.osc.lbl", "Ordem do filtro de oscilações:", UiNodeType.Label, Visible:false),
                            new UiNode("events.settings.bp.osc", "Ordem do filtro de oscilações", UiNodeType.TextBox, DefaultValue:"300", Visible:false),
                        }),

                        new UiNode("events.settings.deriv", "Filtro + Taxa de Variação", UiNodeType.MenuItem, Children: new[]
                        {
                            new UiNode("events.settings.deriv.order.lbl", "Ordem do filtro:", UiNodeType.Label, Visible:false),
                            new UiNode("events.settings.deriv.order", "Ordem do filtro", UiNodeType.TextBox, DefaultValue:"20", Visible:false),
                            new UiNode("events.settings.deriv.n.lbl", "Intervalo de amostras:", UiNodeType.Label, Visible:false),
                            new UiNode("events.settings.deriv.n", "Intervalo de amostras", UiNodeType.TextBox, DefaultValue:"60", Visible:false),
                        }),

                        new UiNode("events.settings.kalman", "Filtro de Kalman", UiNodeType.MenuItem, Checked:true, Children: new[]
                        {
                            new UiNode("events.settings.kalman.sigma.lbl", "Desvio padrão da medida:", UiNodeType.Label),
                            new UiNode("events.settings.kalman.sigma", "σ medida", UiNodeType.TextBox),
                            new UiNode("events.settings.kalman.eventDyn.lbl", "Característica dinâmica do evento:", UiNodeType.Label),
                            new UiNode("events.settings.kalman.eventDyn", "Dinâmica do evento", UiNodeType.TextBox),
                            new UiNode("events.settings.kalman.accel.lbl", "Limite de aceleração angular:", UiNodeType.Label),
                            new UiNode("events.settings.kalman.accel", "Limite aceleração angular", UiNodeType.TextBox),
                        }),
                    }),
                }),
            }
        );

        return new UiCatalog(new[] { Oscillations(), Events() });
    }
}