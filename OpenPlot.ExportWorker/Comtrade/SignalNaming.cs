using OpenPlot.ExportWorker.Domain;

namespace OpenPlot.ExportWorker.Comtrade;

public static class SignalNaming
{
    public static bool IsDigital(MeasurementRow r)
    {
        var q = (r.Quantity ?? "").Trim().ToLowerInvariant();
        var c = (r.Component ?? "").Trim().ToUpperInvariant();
        return q is "digital" or "d" || c is "DIGITAL" or "DIG";
    }

    public static bool IsThd(MeasurementRow r)
    {
        var c = (r.Component ?? "").Trim().ToLowerInvariant();
        return c == "thd";
    }

    public static string MapAnalogName(MeasurementRow r)
    {
        var q = (r.Quantity ?? "").Trim().ToLowerInvariant();
        var c = (r.Component ?? "").Trim().ToUpperInvariant();
        var ph = (r.Phase ?? "").Trim().ToUpperInvariant();

        // THD: "Va THD" / "Ia THD"
        if (IsThd(r))
        {
            var prefix = q switch
            {
                "voltage" or "v" => "V",
                "current" or "i" => "I",
                _ => "X"
            };

            var phasePart = (ph is "A" or "B" or "C") ? ph.ToLowerInvariant() : "";
            return $"{prefix}{phasePart} THD";
        }

        if (q is "frequency" or "freq") return "FREQ";

        var basePrefix = q switch
        {
            "voltage" or "v" => "V",
            "current" or "i" => "I",
            _ => "X"
        };

        var phaseOk = (ph is "A" or "B" or "C") ? ph : "";

        if (c is "MAG") return $"{basePrefix}{phaseOk}MAG";
        if (c is "ANG") return $"{basePrefix}{phaseOk}ANG";

        return $"{basePrefix}{phaseOk}{(string.IsNullOrWhiteSpace(c) ? "VAL" : c)}";
    }

    public static string MapAnalogUnit(MeasurementRow r)
    {
        // Para replicar seu exemplo (unidade vazia), troque para: return "";
        if (IsThd(r)) return "%";

        var q = (r.Quantity ?? "").Trim().ToLowerInvariant();
        var c = (r.Component ?? "").Trim().ToUpperInvariant();

        if (q is "frequency" or "freq") return "Hz";
        if (c is "ANG") return "DEG";

        return q switch
        {
            "voltage" or "v" => "V",
            "current" or "i" => "A",
            _ => "?"
        };
    }

    public static string MapDigitalName(MeasurementRow r)
    {
        // MVP: sem metadata extra, usa nome estável por signal_id.
        // Quando você tiver acrônimo/descrição, a gente troca aqui para "VO1|TRIGGER_OSC".
        return $"D{r.SignalId}";
    }
}