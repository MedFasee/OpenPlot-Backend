using System;
using System.Globalization;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Repositories;

public interface IPlotMetaBuilder
{
    PlotMetaDto Build(SimpleSeriesQuery q, WindowQuery w, RunContext ctx, MeasurementsQuery meas);
}

public sealed class PlotMetaBuilder : IPlotMetaBuilder
{
    public PlotMetaDto Build(SimpleSeriesQuery q, WindowQuery w, RunContext ctx, MeasurementsQuery meas)
    {
        var title = BuildTitle(w, ctx, meas);
        var xLabel = BuildXLabel(ctx);
        var yLabel = BuildYLabel(meas);

        return new PlotMetaDto(title, xLabel, yLabel);
    }

    private static string BuildXLabel(RunContext ctx)
    {
        var from = ctx.FromUtc;
        var to = ctx.ToUtc;

        if (from.Date == to.Date)
        {
            var diaStr = from.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture);
            return $"Tempo (UTC) - Dia {diaStr}";
        }

        return "Tempo (UTC)";
    }

    private static string BuildYLabel(MeasurementsQuery meas)
    {
        var quantity = (meas.Quantity ?? "").Trim().ToLowerInvariant();
        var component = (meas.Component ?? "").Trim().ToLowerInvariant();
        var unit = (meas.Unit ?? "").Trim().ToLowerInvariant();
        var phaseMode = meas.PhaseMode;

        // helpers que NÃO dependem de membros específicos do enum:
        static bool IsDeseq(object? pm) =>
            pm is not null && pm.ToString()!.Equals("Deseq", StringComparison.OrdinalIgnoreCase);

        if (quantity == "frequency") return "Frequência (Hz)";
        if (quantity == "dfreq") return "Var. de Frequência (Hz/s)";
        if (quantity == "p_active") return "Potência Ativa (MW)";
        if (quantity == "p_reactive") return "Potência Reativa (MVAr)";
        if (quantity == "digital") return "Nível";

        if (component == "thd") return "Distorção Harmônica (%)";
        if (IsDeseq(phaseMode)) return "Desequilíbrio (%)";

        if (quantity == "voltage" && component != "thd" && !IsDeseq(phaseMode))
        {
            if (component == "angle") return "Diferença Angular (Graus)";
            return unit == "pu" ? "Tensão (pu)" : "Tensão (V)";
        }

        if (quantity == "current" && component != "thd" && !IsDeseq(phaseMode))
        {
            if (component == "angle") return "Diferença Angular (Graus)";
            return "Corrente (A)";
        }

        return "Valor";
    }

    private static string BuildTitle(WindowQuery w, RunContext ctx, MeasurementsQuery meas)
    {
        var quantity = (meas.Quantity ?? "").Trim().ToLowerInvariant();
        var component = (meas.Component ?? "").Trim().ToLowerInvariant();
        var phaseMode = meas.PhaseMode;

        static bool IsDeseq(object? pm) =>
            pm is not null && pm.ToString()!.Equals("Deseq", StringComparison.OrdinalIgnoreCase);

        var resSuffix = BuildResolutionSuffix(ctx);


        if (quantity == "frequency") return "Frequência" + resSuffix;
        if (quantity == "dfreq") return "Variação de Frequência" + resSuffix;
        if (quantity == "p_active") return "Potência Ativa" + resSuffix;
        if (quantity == "p_reactive") return "Potência Reativa" + resSuffix;
        if (quantity == "digital") return "Sinal Digital" + resSuffix;

        if (component == "thd")
        {
            var baseQ = quantity switch
            {
                "voltage" => "Tensão",
                "current" => "Corrente",
                _ => "Grandeza"
            };
            return $"Distorção de {baseQ} Harmônica Total" + resSuffix;
        }

        if (IsDeseq(phaseMode))
        {
            var grandeza = quantity switch
            {
                "voltage" => "Tensão",
                "current" => "Corrente",
                _ => "Grandeza"
            };
            return $"Desequilíbrio de {grandeza}" + resSuffix;
        }

        if (component == "angle")
        {
            var baseQ = quantity switch
            {
                "voltage" => "Tensão",
                "current" => "Corrente",
                _ => "Grandeza"
            };
            return $"Diferença Angular da {baseQ}" + resSuffix;
        }

        var labelGrandeza = quantity switch
        {
            "voltage" => "Tensão",
            "current" => "Corrente",
            "frequency" => "Frequência",
            "dfreq" => "Var. de Frequência",
            "p_active" => "Potência Ativa",
            "p_reactive" => "Potência Reativa",
            "digital" => "Digital",
            _ => "Grandeza"
        };

        var labelComp = component switch
        {
            "mag" => "Módulo",
            "angle" => "Ângulo",
            "thd" => "THD",
            _ => ""
        };

        var baseTitle =
            !string.IsNullOrWhiteSpace(labelComp) ? $"{labelComp} da {labelGrandeza}" : labelGrandeza;

        return baseTitle + resSuffix;
    }

    private static string BuildResolutionSuffix(RunContext ctx)
    {
        // vem do banco: search_runs.select_rate
        var sr = ctx.SelectRate;

        if (sr == 1) return $" - {sr} fasor/s";

        if (sr > 1) return $" - {sr} fasores/s";

        else return "";

    }

}
