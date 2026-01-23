using System.Diagnostics;
using System.IO;
using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using OpenPlot.Auth.Web.Session;

namespace OpenPlot.Api.Services.Logging;

public class RequestLoggingMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<RequestLoggingMiddleware> _logger;

    public RequestLoggingMiddleware(
        RequestDelegate next,
        ILogger<RequestLoggingMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task Invoke(HttpContext context, IApiRequestLogRepository logRepo)
    {
        var sw = Stopwatch.StartNew();
        var request = context.Request;

        var correlationId = request.Headers["X-Correlation-ID"].FirstOrDefault()
                            ?? Guid.NewGuid().ToString("N");

        context.Items["CorrelationId"] = correlationId;
        context.Response.Headers["X-Correlation-ID"] = correlationId;

        string? userName = null;
        string? userId = null;

        // ==========================================
        // 1) Usuário via Claims (JWT)
        // ==========================================
        if (context.User?.Identity?.IsAuthenticated == true)
        {
            userName =
                context.User.FindFirst("username")?.Value
                ?? context.User.Identity?.Name
                ?? context.User.FindFirst(ClaimTypes.Name)?.Value
                ?? context.User.FindFirst(ClaimTypes.Email)?.Value;

            userId =
                context.User.FindFirst("sub")?.Value
                ?? context.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
        }

        // ==========================================
        // 2) Fallback via SessionUserService
        // ==========================================
        if (userName is null)
        {
            var sessionUserSvc = context.RequestServices.GetService<ISessionUserService>();
            var login = sessionUserSvc?.GetCurrentUser();

            if (login is not null)
            {
                userId = login.Sub ?? login.Username;
                userName = login.Username;
            }
        }

        var remoteIp = context.Connection.RemoteIpAddress?.ToString();
        var userAgent = request.Headers["User-Agent"].FirstOrDefault();

        // ==========================================
        // Captura de body do REQUEST
        // ==========================================
        string? requestBodyForLog = null;

        var isLoginEndpoint = request.Path.StartsWithSegments("/api/v1/auth/login",
                              StringComparison.OrdinalIgnoreCase);

        // Tem body? (Content-Length > 0 ou Transfer-Encoding chunked)
        var hasBody =
            (request.ContentLength ?? 0) > 0 ||
            string.Equals(request.Headers["Transfer-Encoding"], "chunked",
                          StringComparison.OrdinalIgnoreCase);

        if (hasBody && !isLoginEndpoint)
        {
            request.EnableBuffering();   // permite ler sem consumir o stream

            const int maxBodyLength = 10 * 1024; // 10 kB

            using var reader = new StreamReader(
                request.Body,
                Encoding.UTF8,
                detectEncodingFromByteOrderMarks: false,
                bufferSize: 1024,
                leaveOpen: true);

            var body = await reader.ReadToEndAsync();

            requestBodyForLog = body.Length > maxBodyLength
                ? body.Substring(0, maxBodyLength) + "...(truncated)"
                : body;

            request.Body.Position = 0; // volta pro começo pro resto do pipeline
        }

        // Linha completa da requisição 
        var protocol = request.Protocol; 
        var queryString = request.QueryString.Value;

        // Tipo e tamanho DO REQUEST (o que o usuário mandou)
        var contentType = request.ContentType;
        var contentLength = request.ContentLength;

        try
        {
            await _next(context);
            sw.Stop();

            var statusCode = context.Response.StatusCode;

            _logger.LogInformation(
                "HTTP {Method} {Path} -> {StatusCode} in {ElapsedMs} ms | " +
                "User={User} | UserId={UserId} | IP={IP} | CorrelationId={CorrelationId} | UA={UserAgent}",
                request.Method,
                request.Path,
                statusCode,
                sw.ElapsedMilliseconds,
                userName ?? "<anonymous>",
                userId ?? "<none>",
                remoteIp,
                correlationId,
                userAgent
            );

            var entry = new ApiRequestLogEntry
            {
                TimestampUtc = DateTime.UtcNow,
                Method = request.Method,
                Path = request.Path,
                StatusCode = statusCode,
                ElapsedMs = (int)sw.ElapsedMilliseconds,
                UserName = userName,
                UserId = userId,
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
                await logRepo.InsertAsync(entry);
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
                "HTTP {Method} {Path} threw exception after {ElapsedMs} ms | " +
                "User={User} | UserId={UserId} | IP={IP} | CorrelationId={CorrelationId} | UA={UserAgent}",
                request.Method,
                request.Path,
                sw.ElapsedMilliseconds,
                userName ?? "<anonymous>",
                userId ?? "<none>",
                remoteIp,
                correlationId,
                userAgent
            );

            // se quiser, pode também gravar na tabela aqui usando requestBodyForLog

            throw;
        }
    }
}