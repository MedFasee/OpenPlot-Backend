namespace OpenPlot.Features.Runs.Calculations;

public static class PerUnit
{
    /// <summary>
    /// Converte valor em V (fase-neutro) para pu usando base de linha-linha (kV) do banco (volt_level).
    /// MedPlot: base = Vbase = Vll / sqrt(3).
    /// </summary>
    public static double ToVoltagePu(double valueVolts, double? voltLevel)
    {
        if (voltLevel is null || voltLevel <= 0) return valueVolts; // fallback seguro
        var vbase = voltLevel.Value / Math.Sqrt(3.0);
        if (vbase <= 0) return valueVolts;
        return valueVolts / vbase;
    }

    public static IReadOnlyList<(DateTime ts, double val)> ToVoltagePu(
        IEnumerable<(DateTime ts, double val)> points,
        double? voltLevel)
    {

        // função pura: não toca em DB, não lê clock etc.
        return points.Select(p => (p.ts, ToVoltagePu(p.val, voltLevel))).ToList();
    }
}
