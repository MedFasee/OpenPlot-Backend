using OpenTelemetry.Exporter;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace OpenPlot.Api.Configuration;

internal static class OpenTelemetryConfigurationExtensions
{
    internal static WebApplicationBuilder ConfigureOpenTelemetry(this WebApplicationBuilder builder)
    {
        var enabled = builder.Configuration.GetValue("OTEL_ENABLED", false);
        if (!enabled)
            return builder;

        var serviceName = builder.Configuration["OTEL_SERVICE_NAME"] ?? builder.Environment.ApplicationName;
        var serviceVersion = builder.Configuration["OTEL_SERVICE_VERSION"] ?? "1.0.0";

        var endpointRaw = builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"] ?? "http://localhost:4317";
        if (!Uri.TryCreate(endpointRaw, UriKind.Absolute, out var endpoint))
            throw new InvalidOperationException($"Invalid OTLP endpoint: '{endpointRaw}'");

        var protocolRaw = builder.Configuration["OTEL_EXPORTER_OTLP_PROTOCOL"];
        var protocol = ParseProtocol(protocolRaw);

        var resourceBuilder = ResourceBuilder.CreateDefault()
            .AddService(serviceName: serviceName, serviceVersion: serviceVersion);

        builder.Services
            .AddOpenTelemetry()
            .WithMetrics(metricsBuilder =>
            {
                metricsBuilder
                    .SetResourceBuilder(resourceBuilder)
                    .AddMeter(serviceName)
                    .AddAspNetCoreInstrumentation()
                    .AddHttpClientInstrumentation()
                    .AddRuntimeInstrumentation()
                    .AddOtlpExporter(exporter =>
                    {
                        exporter.Endpoint = endpoint;
                        exporter.Protocol = protocol;
                    });
            })
            .WithTracing(tracingBuilder =>
            {
                tracingBuilder
                    .SetResourceBuilder(resourceBuilder)
                    .AddAspNetCoreInstrumentation(options =>
                    {
                        options.EnrichWithHttpRequest = (activity, request) =>
                        {
                            var userAgent = request.Headers.UserAgent.ToString();
                            if (!string.IsNullOrWhiteSpace(userAgent))
                                activity?.SetTag("http.user_agent", userAgent);
                        };

                        options.EnrichWithHttpResponse = (activity, response) =>
                        {
                            activity?.SetTag("http.status_code", response.StatusCode);
                        };

                        options.EnrichWithException = (activity, exception) =>
                        {
                            activity?.SetTag("exception.type", exception.GetType().FullName);
                        };

                        options.Filter = context =>
                        {
                            var path = context.Request.Path.Value ?? string.Empty;
                            return !path.StartsWith("/api/Configuration", StringComparison.OrdinalIgnoreCase);
                        };
                    })
                    .AddHttpClientInstrumentation()
                    .AddOtlpExporter(exporter =>
                    {
                        exporter.Endpoint = endpoint;
                        exporter.Protocol = protocol;
                    });
            });

        builder.Logging.AddOpenTelemetry(loggingOptions =>
        {
            loggingOptions
                .SetResourceBuilder(resourceBuilder)
                .AddOtlpExporter(exporter =>
                {
                    exporter.Endpoint = endpoint;
                    exporter.Protocol = protocol;
                });

            loggingOptions.IncludeFormattedMessage = true;
            loggingOptions.IncludeScopes = true;
            loggingOptions.ParseStateValues = true;
        });

        return builder;
    }

    private static OtlpExportProtocol ParseProtocol(string? protocolRaw)
    {
        return protocolRaw?.Trim().ToLowerInvariant() switch
        {
            "http/protobuf" => OtlpExportProtocol.HttpProtobuf,
            "httpprotobuf" => OtlpExportProtocol.HttpProtobuf,
            _ => OtlpExportProtocol.Grpc
        };
    }
}
