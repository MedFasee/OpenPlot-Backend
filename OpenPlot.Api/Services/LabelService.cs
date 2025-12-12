using System;

public interface ILabelService
{
    string BuildLabel(DateTime fromUtc, DateTime toUtc, int selectRate, string source, string? terminalId);
}

public sealed class LabelService : ILabelService
{
    private readonly ITimeService _time;
    public LabelService(ITimeService time) => _time = time;

    public string BuildLabel(DateTime fromUtc, DateTime toUtc, int selectRate, string source, string? terminalId)
    {
 
        var label = $"{fromUtc:yyyyMMdd}_{fromUtc:HHmmss}_{toUtc:HHmmss}_{selectRate}_{source}";
        return label;
    }
}
