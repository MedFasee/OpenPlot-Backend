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

        if (quantity == "voltage" && component != "thd" && phaseMode != "deseq")
        {
            if (component == "angle") return "Diferença Angular (Graus)";
            return unit == "pu" ? "Tensão (pu)" : "Tensão (V)";
        }

        if (quantity == "current" && component != "thd" && phaseMode != "deseq")
        {
            if (component == "angle") return "Diferença Angular (Graus)";
            return "Corrente (A)";
        }

        if (quantity == "p_active") return "Potência Ativa (MW)";
        if (quantity == "p_reactive") return "Potência Reativa (MVAr)";
        if (quantity == "frequency") return "Frequência (Hz)";
        if (quantity == "dfreq") return "Var. de Frequência (Hz/s)";
        if (quantity == "digital") return "Nível";
        if (component == "thd") return "Distorção Harmônica (%)";
        if (quantity == "unbalance") return "Desequilíbrio (%)";

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
        var resSuffix = BuildResolutionSuffix(ctx);

        if (quantity == "frequency") return "Frequência" + resSuffix;
        if (quantity == "dfreq") return "Variação de Frequência" + resSuffix;
        if (quantity == "digital") return "Sinal Digital" + resSuffix;

        if (component == "thd") return "Distorção Harmônica Total" + resSuffix;
        if (component == "angle") return "Ângulo" + resSuffix;

        if (InferPhaseMode(meas) == "deseq")
            return "Desequilíbrio (|seq-|/|seq+|)" + resSuffix;

        if (quantity == "voltage") return "Tensão" + resSuffix;
        if (quantity == "current") return "Corrente" + resSuffix;
        if (quantity == "p_active") return "Potência Ativa" + resSuffix;
        if (quantity == "p_reactive") return "Potência Reativa" + resSuffix;

        return "Gráfico" + resSuffix;
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
