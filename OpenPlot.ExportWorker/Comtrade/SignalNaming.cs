using OpenPlot.ExportWorker.Domain;

namespace OpenPlot.ExportWorker.Comtrade;

public static class SignalNaming
{
    public static bool IsDigital(MeasurementRow r)
    {
        var q = NormalizeQuantity(r.Quantity);
        var c = NormalizeComponent(r.Component);
        return q is "digital" or "d" || c is "DIGITAL" or "DIG";
    }

    public static bool IsThd(MeasurementRow r)
        => NormalizeComponent(r.Component) == "THD";

    public static string MapAnalogName(MeasurementRow r)
    {
        var q = NormalizeQuantity(r.Quantity);
        var c = NormalizeComponent(r.Component);
        var ph = NormalizePhase(r.Phase);

        // Frequência e delta de frequência não possuem fase física A/B/C.
        // São diferenciados explicitamente pelo nome do canal.
        if (q is "frequency" or "freq")
        {
            return c switch
            {
                "DFREQ" => "VdFreq",
                _       => "VFreq"
            };
        }

        var prefix = q switch
        {
            "voltage" or "v" => "V",
            "current" or "i" => "I",
            _ => "X"
        };

        var phasePart = IsPhysicalPhase(ph)
            ? ph.ToLowerInvariant()
            : "";

        if (c == "THD")
            return $"{prefix}{phasePart} THD";

        if (c == "MAG")
            return $"{prefix}{phasePart} Mag RMS";

        if (c == "ANG")
            return $"{prefix}{phasePart} Phi";

        return $"{prefix}{phasePart}{(string.IsNullOrWhiteSpace(c) ? "VAL" : c)}";
    }

    /// <summary>
    /// Mapeia a fase física do catálogo para o campo ph do canal analógico COMTRADE.
    ///
    /// MAG: A/B/C -> Am/Bm/Cm
    /// ANG: A/B/C -> Aa/Ba/Ca
    /// THD: A/B/C -> A/B/C
    /// FREQ/DFREQ: não possuem fase física; usa-se '+' conforme a convenção
    /// adotada pelos arquivos COMTRADE de referência do OpenPlot.
    /// </summary>
    public static string MapAnalogPhase(MeasurementRow r)
    {
        var q = NormalizeQuantity(r.Quantity);
        var c = NormalizeComponent(r.Component);
        var ph = NormalizePhase(r.Phase);

        if (q is "frequency" or "freq")
            return "+";

        if (!IsPhysicalPhase(ph))
            return "";

        return c switch
        {
            "MAG" => $"{ph}m",
            "ANG" => $"{ph}a",
            "THD" => ph,
            _     => ph
        };
    }

    public static string MapAnalogUnit(MeasurementRow r)
    {
        if (IsThd(r))
            return "%";

        var q = NormalizeQuantity(r.Quantity);
        var c = NormalizeComponent(r.Component);

        // A tabela measurements armazena frequency_hz e delta_freq_hz.
        if (q is "frequency" or "freq")
            return "Hz";

        if (c == "ANG")
            return "DEG";

        return q switch
        {
            "voltage" or "v" => "V",
            "current" or "i" => "A",
            _ => "?"
        };
    }

    /// <summary>
    /// Ordem semântica dos canais analógicos no CFG:
    /// magnitude A/B/C, ângulo A/B/C, THD A/B/C, frequência e delta frequência.
    /// Tensão precede corrente quando ambas estiverem presentes.
    /// </summary>
    public static int GetAnalogSortOrder(MeasurementRow r)
    {
        var q = NormalizeQuantity(r.Quantity);
        var c = NormalizeComponent(r.Component);
        var ph = NormalizePhase(r.Phase);

        var quantityOrder = q switch
        {
            "voltage" or "v" => 0,
            "current" or "i" => 1,
            "frequency" or "freq" => 2,
            _ => 9
        };

        var componentOrder = c switch
        {
            "MAG"   => 0,
            "ANG"   => 1,
            "THD"   => 2,
            "FREQ"  => 0,
            "DFREQ" => 1,
            _       => 9
        };

        var phaseOrder = ph switch
        {
            "A" => 0,
            "B" => 1,
            "C" => 2,
            _ => 9
        };

        return quantityOrder * 100 + componentOrder * 10 + phaseOrder;
    }

    public static string MapDigitalName(MeasurementRow r)
    {
        var name = (r.SignalName ?? "").Trim();
        return string.IsNullOrEmpty(name) ? $"D{r.SignalId}" : name;
    }

    private static string NormalizeQuantity(string? value)
        => (value ?? "").Trim().ToLowerInvariant();

    private static string NormalizeComponent(string? value)
        => (value ?? "").Trim().ToUpperInvariant();

    private static string NormalizePhase(string? value)
        => (value ?? "").Trim().ToUpperInvariant();

    private static bool IsPhysicalPhase(string phase)
        => phase is "A" or "B" or "C";
}
