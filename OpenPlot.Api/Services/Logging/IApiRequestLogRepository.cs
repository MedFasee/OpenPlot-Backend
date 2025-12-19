namespace OpenPlot.Api.Services.Logging;

public interface IApiRequestLogRepository
{
    Task InsertAsync(ApiRequestLogEntry entry, CancellationToken ct = default);
}
