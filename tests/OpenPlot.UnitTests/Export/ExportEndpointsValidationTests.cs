using System.Text.Json;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Metadata;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace OpenPlot.UnitTests.Export;

public sealed class ExportEndpointsValidationTests
{
    [Theory]
    [InlineData("done")]
    [InlineData("DONE")]
    public void CanConvertSearchRun_WhenStatusIsDone_ReturnsTrue(string status)
    {
        Assert.True(ExportEndpoints.CanConvertSearchRun(status));
    }

    [Theory]
    [InlineData("queued")]
    [InlineData("running")]
    [InlineData("failed")]
    [InlineData(null)]
    public void CanConvertSearchRun_WhenStatusIsNotDone_ReturnsFalse(string? status)
    {
        Assert.False(ExportEndpoints.CanConvertSearchRun(status));
    }

    [Fact]
    public void BuildIncompleteRunError_ReturnsExpectedMessageAndStatus()
    {
        var payload = ExportEndpoints.BuildIncompleteRunError("running");
        var json = JsonSerializer.SerializeToDocument(payload);

        Assert.Equal(
            "A consulta não está concluída. Só é possível converter consultas completas/íntegras.",
            json.RootElement.GetProperty("error").GetString());
        Assert.Equal("running", json.RootElement.GetProperty("status").GetString());
    }

    [Fact]
    public void MapExport_RegistersExpectedRoutes()
    {
        var app = CreateApp();

        var routes = GetExportEndpoints(app)
            .Select(endpoint => endpoint.RoutePattern.RawText)
            .ToArray();

        Assert.Contains("/export/", routes, StringComparer.Ordinal);
        Assert.Contains("/export/{format}/{id:guid}", routes, StringComparer.Ordinal);
        Assert.Contains("/export/{format}/{id:guid}/file", routes, StringComparer.Ordinal);
    }

    [Fact]
    public void MapExport_RequiresAuthorizationOnAllRoutes()
    {
        var app = CreateApp();

        var endpoints = GetExportEndpoints(app);

        Assert.NotEmpty(endpoints);
        Assert.All(endpoints, endpoint =>
            Assert.Contains(endpoint.Metadata, metadata => metadata is IAuthorizeData));
    }

    [Fact]
    public void MapExport_AssignsExportTagToAllRoutes()
    {
        var app = CreateApp();

        var endpoints = GetExportEndpoints(app);

        Assert.NotEmpty(endpoints);
        Assert.All(endpoints, endpoint =>
        {
            var tagsMetadata = endpoint.Metadata.OfType<ITagsMetadata>().SingleOrDefault();
            Assert.NotNull(tagsMetadata);
            Assert.Contains("Export", tagsMetadata!.Tags, StringComparer.Ordinal);
        });
    }

    private static WebApplication CreateApp()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddAuthorization();

        var app = builder.Build();
        app.MapExport();
        return app;
    }

    private static RouteEndpoint[] GetExportEndpoints(WebApplication app) =>
        ((IEndpointRouteBuilder)app).DataSources
            .SelectMany(dataSource => dataSource.Endpoints)
            .OfType<RouteEndpoint>()
            .Where(endpoint => endpoint.RoutePattern.RawText?.StartsWith("/export", StringComparison.Ordinal) == true)
            .ToArray();
}
