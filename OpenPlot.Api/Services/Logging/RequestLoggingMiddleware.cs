using System.Diagnostics;
using System.Security.Claims;
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

    public async Task Invoke(HttpContext context)
    {
        var sw = Stopwatch.StartNew();
        var request = context.Request;

        var correlationId = request.Headers["X-Correlation-ID"].FirstOrDefault()
                            ?? Guid.NewGuid().ToString("N");

        context.Items["CorrelationId"] = correlationId;
        context.Response.Headers["X-Correlation-ID"] = correlationId;

        string? userName = null;
        string? userId = null;

        // =========================================================
        // (1) Via Claims (JWT)
        // =========================================================
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

        // =========================================================
        // (2) Via SessionUserService (LoginResponse)
        // =========================================================
        if (userName is null)
        {
            var sessionUserSvc = context.RequestServices.GetService<ISessionUserService>();

            var login = sessionUserSvc?.GetCurrentUser();  // LoginResponse

            if (login is not null)
            {
                userId = login.Sub ?? login.Username;
                userName = login.DisplayName ?? login.Username;
            }
        }

        // =========================================================
        var remoteIp = context.Connection.RemoteIpAddress?.ToString();
        var userAgent = request.Headers["User-Agent"].FirstOrDefault();
       

        try
        {
            await _next(context);
            sw.Stop();

            var statusCode = context.Response.StatusCode;

            _logger.LogInformation(
                "HTTP {Method} {Path} -> {StatusCode} in {ElapsedMs} ms | " +
                "User={User} | UserId={UserId} | IP={IP} | CorrelationId={CorrelationId}" +
                " | UA={UserAgent}",
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
        }
        catch (Exception ex)
        {
            sw.Stop();

            _logger.LogError(
                ex,
                "HTTP {Method} {Path} threw exception after {ElapsedMs} ms | " +
                "User={User} | UserId={UserId} | IP={IP} | CorrelationId={CorrelationId}" +
                " | UA={UserAgent}",
                request.Method,
                request.Path,
                sw.ElapsedMilliseconds,
                userName ?? "<anonymous>",
                userId ?? "<none>",
                remoteIp,
                correlationId,
                userAgent
            );

            throw;
        }
    }
}
