using System.Globalization;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.Features.Runs.Contracts;


public interface IPlotMetaBuilder
{
    PlotMetaDto Build(WindowQuery w, RunContext ctx, MeasurementsQuery meas);
}

public sealed class PlotMetaBuilder : IPlotMetaBuilder
{
    public PlotMetaDto Build(WindowQuery w, RunContext ctx, MeasurementsQuery meas)
    {
        var title = BuildTitle(ctx, meas);
        var xLabel = BuildXLabel(w, ctx);
        var yLabel = BuildYLabel(meas);

        return new PlotMetaDto(title, xLabel, yLabel);
    }

    private static string BuildXLabel(WindowQuery w, RunContext ctx)
    {
        var from = w.FromUtc ?? ctx.FromUtc;
        var to = w.ToUtc ?? ctx.ToUtc;

        if (from.Date == to.Date)
        {
            var diaStr = from.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);
            return $"Tempo (UTC) - Dia {diaStr}";
        }

        return "Tempo (UTC)";
    }

    private static string BuildYLabel(MeasurementsQuery meas)
    {
        var quantity = Norm(meas.Quantity);
        var component = Norm(meas.Component);
        var unit = Norm(meas.Unit);
        var phaseMode = InferPhaseMode(meas);


        if (quantity == "voltage" && component != "thd" && phaseMode != "deseq" && unit != "%")
        {
            if (component == "angle") return "Diferença Angular (Graus)";
            return unit == "pu" ? "Tensão (pu)" : "Tensão (V)";
        }

        if (quantity == "current" && component != "thd" && phaseMode != "deseq")
        {
            if (component == "angle") return "Diferença Angular (Graus)";
            return "Corrente (A)";
        }
        // dfreq vem como Quantity="frequency" + Component="dfreq"
        if (component == "dfreq")
            return "Var. de Frequência (Hz/s)";

        // frequency normal
        if (quantity == "frequency" || component == "freq")
            return "Frequência (Hz)";

        if (meas.PhaseMode == PhaseMode.Deseq || component is "unbalance" or "ratio")
            return "Desequilíbrio (%)";

        if (quantity == "p_active") return "Potência Ativa (MW)";
        if (quantity == "p_reactive") return "Potência Reativa (MVAr)";
        if (quantity == "frequency") return "Frequência (Hz)";
        if (quantity == "dfreq") return "Var. de Frequência (Hz/s)";
        if (quantity == "digital") return "Nível";
        if (component == "thd") return "Distorção Harmônica (%)";

        return quantity switch
        {
            "voltage" => "Tensão",
            "current" => "Corrente",
            "p_active" => "Potência Ativa",
            "p_reactive" => "Potência Reativa",
            "frequency" => "Frequência",
            "dfreq" => "Var. de Frequência",
            "digital" => "Digital",
            _ => "Grandeza"
        };
    }

    private static string BuildTitle(RunContext ctx, MeasurementsQuery meas)
    {
        var quantity = Norm(meas.Quantity);
        var component = Norm(meas.Component);
        var unit = Norm(meas.Unit);

        // No back: PhaseMode é enum; aqui eu converto para rótulos do front.
        var pmu0 = meas.PmuNames?.FirstOrDefault();

        var labelGrandeza = quantity switch
        {
            "voltage" => "Tensão",
            "current" => "Corrente",
            "p_active" => "Potência Ativa",
            "p_reactive" => "Potência Reativa",
            "frequency" => "Frequência",
            "dfreq" => "Variação de Frequência",
            "digital" => "Sinal Digital",
            _ => "Grandeza"
        };

        var labelComp = component switch
        {
            "mag" => "Módulo",
            "angle" => "Ângulo",
            "thd" => "THD",
            _ => null
        };

        var labelDom = GetDomainLabel(meas); // fase / sequência / etc.
        var resSuffix = BuildResolutionSuffix(ctx);

        // -------------------------
        // Casos sem fase (iguais ao front)
        // -------------------------
        // dfreq vem como Quantity="frequency" + Component="dfreq" 
        if (component == "dfreq")
            return "Variação de Frequência" + resSuffix;

        // frequency "normal"
        if (quantity == "frequency" || component == "freq")
            return "Frequência" + resSuffix;

        if (quantity == "p_active") return "Potência Ativa" + resSuffix;
        if (quantity == "p_reactive") return "Potência Reativa" + resSuffix;
        if (quantity == "digital") return "Sinal Digital" + resSuffix;

        // -------------------------
        // THD (igual ao front)
        // -------------------------
        if (component == "thd")
            return $"Distorção de {labelGrandeza} Harmônica Total" + resSuffix;

        // -------------------------
        // Desequilíbrio (igual ao front)
        // -------------------------
        if (meas.PhaseMode == PhaseMode.Deseq || component is "unbalance" or "ratio")
            return $"Desequilíbrio de {labelGrandeza}" + resSuffix;

        // -------------------------
        // Ângulo (igual ao front)
        // -------------------------
        if (component == "angle")
        {
            var refT = string.IsNullOrWhiteSpace(meas.ReferenceTerminal)
                ? ""
                : $" Ref.: {meas.ReferenceTerminal.Trim()}";

            return $"Diferença Angular da {labelGrandeza}{refT}" + resSuffix;
        }

        // -------------------------
        // Trifásico "por terminal" (padrão que você quer)
        // -> "Módulo da Tensão - <PMU> - 100 fasores/s"
        // -------------------------
        if (meas.PhaseMode == PhaseMode.ThreePhase)
        {
            var left = "Módulo da " + labelGrandeza; // no exemplo do front é sempre "Módulo"
            if (!string.IsNullOrWhiteSpace(pmu0))
                return $"{left} - {pmu0}{resSuffix}";
            return left + resSuffix;
        }

        // -------------------------
        // Sequências (padrão que você quer)
        // -> "Módulo da Tensão - Sequência Positiva - 100 fasores/s"
        // -------------------------
        if (meas.PhaseMode is PhaseMode.SeqPos or PhaseMode.SeqNeg or PhaseMode.SeqZero)
        {
            var left = "Módulo da " + labelGrandeza;

            var seqLabel = meas.PhaseMode switch
            {
                PhaseMode.SeqPos => "Sequência Positiva",
                PhaseMode.SeqNeg => "Sequência Negativa",
                _ => "Sequência Zero"
            };

            return $"{left} - {seqLabel}{resSuffix}";
        }

        // -------------------------
        // Base (igual ao front)
        // -------------------------
        string baseTitle;
        if (!string.IsNullOrWhiteSpace(labelComp) && labelGrandeza != "Grandeza")
            baseTitle = $"{labelComp} da {labelGrandeza}";
        else if (labelGrandeza != "Grandeza")
            baseTitle = labelGrandeza;
        else
            baseTitle = "Gráfico";

        if (!string.IsNullOrWhiteSpace(labelDom))
            baseTitle += $" - {labelDom}";

        return baseTitle + resSuffix;
    }

    // Domínio (fase ou sequência) no mesmo espírito do getDomainLabel do front
    private static string GetDomainLabel(MeasurementsQuery meas)
    {
        var ph = (meas.Phase ?? "").Trim().ToUpperInvariant();

        return meas.PhaseMode switch
        {
            PhaseMode.Single when ph is "A" or "B" or "C" => $"Fase {ph}",
            PhaseMode.SeqPos => "Sequência Positiva",
            PhaseMode.SeqNeg => "Sequência Negativa",
            PhaseMode.SeqZero => "Sequência Zero",
            _ => ""
        };
    }



    private static string BuildResolutionSuffix(RunContext ctx)
    {
        var sr = ctx.SelectRate;
        if (sr == 1) return $" - {sr} fasor/s";
        if (sr > 1) return $" - {sr} fasores/s";
        return "";
    }

    private static string Norm(string? s) => (s ?? "").Trim().ToLowerInvariant();

    private static string InferPhaseMode(MeasurementsQuery meas)
    {
        var component = Norm(meas.Component);
        if (component == "unbalance") return "deseq";
        return "";
    }
}
