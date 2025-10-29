using System;

public interface ITimeService
{
    TimeZoneInfo BrazilTz { get; }
    (DateTime FromLocal, DateTime ToLocal) ToBrazil(DateTime fromUtc, DateTime toUtc);
}

public sealed class TimeService : ITimeService
{
    private readonly TimeZoneInfo _tz;
    public TimeZoneInfo BrazilTz => _tz;

    public TimeService()
    {
        try { _tz = TimeZoneInfo.FindSystemTimeZoneById("America/Sao_Paulo"); }
        catch { _tz = TimeZoneInfo.FindSystemTimeZoneById("E. South America Standard Time"); }
    }

    public (DateTime, DateTime) ToBrazil(DateTime fromUtc, DateTime toUtc)
    {
        var f = TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(fromUtc, DateTimeKind.Utc), _tz);
        var t = TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(toUtc, DateTimeKind.Utc), _tz);
        return (f, t);
    }
}
