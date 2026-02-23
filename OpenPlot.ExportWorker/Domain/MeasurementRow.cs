namespace OpenPlot.ExportWorker.Domain;

public sealed class MeasurementRow
{
    public int SignalId { get; set; }
    public int PdcPmuId { get; set; }

    public string IdName { get; set; } = "";   // PMU
    public string PdcName { get; set; } = "";  // PDC (source)

    public string Quantity { get; set; } = ""; // voltage/current/frequency...
    public string Component { get; set; } = ""; // MAG/ANG/FREQ/DFREQ...
    public string? Phase { get; set; }         // A/B/C/...
    public string? Unit { get; set; }          // sempre null aqui (sem s.unit)

    public DateTimeOffset Ts { get; set; }
    public double Value { get; set; }
}