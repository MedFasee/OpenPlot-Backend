using System.Diagnostics;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using OpenPlot.Api.Services.Security;

namespace OpenPlot.Api.Services.Logging;

public sealed class RequestLoggingMiddleware
{
    private static readonly HashSet<string> SensitiveQueryKeys = new(StringComparer.OrdinalIgnoreCase)
    {
        "token",
        "access_token",
        "id_token",
        "refresh_token",
        "code",
        "password",
        "secret",
        "client_secret",
        "api_key",
        "apikey"
    };

    private readonly RequestDelegate _next;
    private readonly ILogger<RequestLoggingMiddleware> _logger;

    public RequestLoggingMiddleware(
        RequestDelegate next,
        ILogger<RequestLoggingMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task Invoke(
        HttpContext context,
        IApiRequestLogRepository logRepo,
        IUserContextAccessor userContextAccessor,
        IConfiguration configuration)
    {
        var enabled = configuration.GetValue("RequestLogging:Enabled", false);
        if (!enabled)
        {
            await _next(context);
            return;
        }

        var sw = Stopwatch.StartNew();
        var request = context.Request;

        var correlationId = request.Headers["X-Correlation-ID"].FirstOrDefault();
        if (string.IsNullOrWhiteSpace(correlationId) || correlationId.Length > 128)
            correlationId = Guid.NewGuid().ToString("N");

        context.Items["CorrelationId"] = correlationId;
        context.Response.Headers["X-Correlation-ID"] = correlationId;

        var userContext = userContextAccessor.GetCurrent(context);
        var remoteIp = context.Connection.RemoteIpAddress?.ToString();
        var userAgent = request.Headers["User-Agent"].FirstOrDefault();

        var includeRequestBody = configuration.GetValue("RequestLogging:IncludeRequestBody", false);
        var maxBodyLength = Math.Clamp(
            configuration.GetValue("RequestLogging:MaxRequestBodyBytes", 10 * 1024),
            0,
            64 * 1024);

        string? requestBodyForLog = null;
        if (includeRequestBody
            && maxBodyLength > 0
            && HasBody(request)
            && !IsSensitivePath(request.Path))
        {
            request.EnableBuffering();

            using var reader = new StreamReader(
                request.Body,
                Encoding.UTF8,
                detectEncodingFromByteOrderMarks: false,
                bufferSize: 1024,
                leaveOpen: true);

            var buffer = new char[maxBodyLength + 1];
            var read = await reader.ReadBlockAsync(buffer, 0, buffer.Length);
            request.Body.Position = 0;

            requestBodyForLog = new string(buffer, 0, Math.Min(read, maxBodyLength));
            if (read > maxBodyLength)
                requestBodyForLog += "...(truncated)";
        }

        var protocol = request.Protocol;
        var queryString = SanitizeQueryString(request);
        var contentType = request.ContentType;
        var contentLength = request.ContentLength;

        try
        {
            await _next(context);
            sw.Stop();

            var statusCode = context.Response.StatusCode;

            _logger.LogInformation(
                "HTTP {Method} {Path} -> {StatusCode} in {ElapsedMs} ms | User={User} | UserId={UserId} | IP={IP} | CorrelationId={CorrelationId} | UA={UserAgent}",
                request.Method,
                request.Path,
                statusCode,
                sw.ElapsedMilliseconds,
                userContext.UserName ?? "<anonymous>",
                userContext.UserId ?? "<none>",
                remoteIp,
                correlationId,
                userAgent);

            var entry = new ApiRequestLogEntry
            {
                TimestampUtc = DateTime.UtcNow,
                Method = request.Method,
                Path = request.Path,
                StatusCode = statusCode,
                ElapsedMs = (int)Math.Min(sw.ElapsedMilliseconds, int.MaxValue),
                UserName = userContext.UserName,
                UserId = userContext.UserId,
                Ip = remoteIp,
                CorrelationId = correlationId,
                UserAgent = userAgent,
                Protocol = protocol,
                ContentType = contentType,
                ContentLength = contentLength,
                RequestBody = requestBodyForLog,
                QueryString = queryString
            };

            try
            {
                await logRepo.InsertAsync(entry, context.RequestAborted);
            }
            catch (OperationCanceledException)
            {
                // Request foi cancelado pelo cliente; ignora erro ao salvar log.
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Falha ao gravar log de request em openplot.api_request_log.");
            }
        }
        catch (Exception ex)
        {
            sw.Stop();

            _logger.LogError(
                ex,
                "HTTP {Method} {Path} threw exception after {ElapsedMs} ms | User={User} | UserId={UserId} | IP={IP} | CorrelationId={CorrelationId}",
                request.Method,
                request.Path,
                sw.ElapsedMilliseconds,
                userContext.UserName ?? "<anonymous>",
                userContext.UserId ?? "<none>",
                remoteIp,
                correlationId);

            throw;
        }
    }

    private static bool HasBody(HttpRequest request) =>
        (request.ContentLength ?? 0) > 0
        || request.Headers.ContainsKey("Transfer-Encoding");

    private static bool IsSensitivePath(PathString path) =>
        path.StartsWithSegments("/api/v1/auth", StringComparison.OrdinalIgnoreCase)
        || path.StartsWithSegments("/api/v1/sso", StringComparison.OrdinalIgnoreCase);

    private static string? SanitizeQueryString(HttpRequest request)
    {
        if (!request.QueryString.HasValue || request.Query.Count == 0)
            return null;

        var items = new List<string>();

        foreach (var pair in request.Query)
        {
            var key = pair.Key;
            var sensitive = SensitiveQueryKeys.Contains(key);

            foreach (var value in pair.Value)
            {
                var safeValue = sensitive ? "[REDACTED]" : value ?? string.Empty;
                items.Add($"{Uri.EscapeDataString(key)}={Uri.EscapeDataString(safeValue)}");
            }
        }

        return items.Count == 0 ? null : "?" + string.Join("&", items);
    }
}
