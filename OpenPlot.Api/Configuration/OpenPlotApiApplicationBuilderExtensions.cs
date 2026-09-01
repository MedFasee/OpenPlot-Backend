using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using OpenPlot.Api.Services.Logging;
using OpenPlot.Features.Auth;
using OpenPlot.Features.Import;
using OpenPlot.Features.PostProcessing.Handlers;
using OpenPlot.Features.Sso;

namespace OpenPlot.Api.Configuration;

internal static class OpenPlotApiApplicationBuilderExtensions
{
    internal static WebApplication UseOpenPlotApiPipeline(this WebApplication app)
    {
        app.UseCors(OpenPlotApiServiceCollectionExtensions.CorsPolicyName);
        app.UseSession();
        app.UseAuthentication();
        app.UseMiddleware<RequestLoggingMiddleware>();
        app.UseAuthorization();

        if (app.Environment.IsDevelopment() || app.Configuration.GetValue<bool>("Swagger:Enabled"))
        {
            app.UseSwagger();
            app.UseSwaggerUI();
        }

        return app;
    }

    internal static WebApplication MapOpenPlotApiEndpoints(this WebApplication app)
    {
        app.MapHealthChecks("/health").AllowAnonymous();

        var apiV1 = app.MapGroup("/api/v1");

        apiV1.MapAuth();
        apiV1.MapSso();
        apiV1.MapConfig();
        apiV1.MapExport();
        apiV1.MapSearch();
        apiV1.MapRuns();
        apiV1.MapPostProcessing();
        apiV1.MapImport();

        return app;
    }
}
