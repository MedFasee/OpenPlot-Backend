using System.Data;
using System.Linq;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using Microsoft.OpenApi.Models;
using OpenPlot.Api.Services.Security;
using OpenPlot.Api.Services.Logging;
using OpenPlot.Auth.Infrastructure.Auth;
using OpenPlot.Auth.Infrastructure.Auth.Options;
using OpenPlot.Auth.Services;
using OpenPlot.Auth.Web.Session;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.Auth;
using OpenPlot.Features.Export;
using OpenPlot.Features.Import;
using OpenPlot.Features.PostProcessing.Handlers;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.UI;

namespace OpenPlot.Api.Configuration;

internal static class OpenPlotApiServiceCollectionExtensions
{
    internal const string DevCorsPolicyName = "DevCors";

    internal static WebApplicationBuilder ConfigureOpenPlotWebHost(this WebApplicationBuilder builder)
    {
        builder.WebHost.UseUrls("http://0.0.0.0:7011");
        builder.WebHost.ConfigureKestrel(options =>
        {
            options.Limits.RequestHeadersTimeout = TimeSpan.FromMinutes(5);
            options.Limits.KeepAliveTimeout = TimeSpan.FromMinutes(5);
        });

        return builder;
    }

    internal static WebApplicationBuilder AddOpenPlotApiServices(this WebApplicationBuilder builder)
    {
        builder.Services.AddOpenPlotCors();
        builder.Services.AddOpenPlotDataAccess(builder.Configuration);
        builder.Services.AddOpenPlotLogging();
        builder.Services.AddOpenPlotDomainServices();
        builder.Services.AddOpenPlotSessionServices();
        builder.Services.AddOpenPlotAuthentication(builder.Configuration);
        builder.Services.AddOpenPlotJsonSerialization();
        builder.Services.AddOpenPlotSwagger();

        return builder;
    }

    private static IServiceCollection AddOpenPlotCors(this IServiceCollection services)
    {
        services.AddCors(options =>
        {
            options.AddPolicy(DevCorsPolicyName, policy =>
            {
                policy
                    .SetIsOriginAllowed(_ => true)
                    .AllowAnyHeader()
                    .AllowAnyMethod()
                    .AllowCredentials();
            });
        });

        return services;
    }

    private static IServiceCollection AddOpenPlotDataAccess(this IServiceCollection services, IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString("Db")
            ?? "Host=localhost;Database=postgres;Username=postgres;Password=postgres";

        services.AddScoped<IDbConnectionFactory>(_ => new NpgsqlConnectionFactory(connectionString));
        services.AddScoped<IApiRequestLogRepository, ApiRequestLogRepository>();
        services.AddScoped<IRunContextRepository, RunContextRepository>();
        services.AddScoped<IMeasurementsRepository, MeasurementsRepository>();
        services.AddScoped<IAnalysisCacheRepository, AnalysisCacheRepository>();

        return services;
    }

    private static IServiceCollection AddOpenPlotLogging(this IServiceCollection services)
    {
        services.AddHttpContextAccessor();
        services.AddDistributedMemoryCache();

        return services;
    }

    private static IServiceCollection AddOpenPlotDomainServices(this IServiceCollection services)
    {
        services.AddSingleton<ITimeService, TimeService>();
        services.AddSingleton<ILabelService, LabelService>();
        services.AddSingleton<IPmuHierarchyService, PmuHierarchyService>();
        services.AddSingleton<IPlotMetaBuilder, PlotMetaBuilder>();
        services.AddSingleton<ITimeSeriesDownsampler, TimeBucketMinMaxDownsampler>();
        services.AddScoped<IUserContextAccessor, UserContextAccessor>();
        services.AddSingleton(new FeatureFlags(
            EnablesDFT: true,
            EnablesProny: true,
            EnablesCCA: true,
            EnablesEventsAnalyzer: true));
        services.AddSingleton<IUiMenuService, UiMenuService>();

        services.AddScoped<SimpleSeriesHandler>();
        services.AddScoped<VoltageSeriesHandler>();
        services.AddScoped<CurrentSeriesHandler>();
        services.AddScoped<SeqSeriesHandler>();
        services.AddScoped<UnbalanceSeriesHandler>();
        services.AddScoped<ThdSeriesHandler>();
        services.AddScoped<PowerSeriesHandler>();
        services.AddScoped<AngleDiffSeriesHandler>();
        services.AddScoped<IPhasorRequestService, PhasorRequestService>();
        services.AddScoped<IPmuQueryHelper, PmuQueryHelper>();
        services.AddScoped<ISeriesAssemblyService, SeriesAssemblyService>();
        services.AddScoped<IDftMetaBuilder, DftMetaBuilder>();
        services.AddScoped<IPronyMetaBuilder, PronyMetaBuilder>();
        services.AddSingleton<IExportArtifactStore, DiskExportArtifactStore>();
        services.AddScoped<IExportFileService, ExportFileService>();
        services.AddScoped<IXmlImportService, XmlImportService>();

        return services;
    }

    private static IServiceCollection AddOpenPlotSessionServices(this IServiceCollection services)
    {
        services.AddSession(options =>
        {
            options.Cookie.Name = "openplot.sid";
            options.IdleTimeout = TimeSpan.FromHours(24);
            options.Cookie.HttpOnly = true;
            options.Cookie.SameSite = SameSiteMode.None;
            options.Cookie.SecurePolicy = CookieSecurePolicy.Always;
        });

        services.AddScoped<ISessionUserService, SessionUserService>();

        return services;
    }

    private static IServiceCollection AddOpenPlotAuthentication(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddSingleton<IUserStore, JsonUserStore>();
        services.AddScoped<IAuthService>(serviceProvider =>
        {
            var authOptions = serviceProvider.GetRequiredService<IOptions<AuthOptions>>().Value;
            var userStore = serviceProvider.GetRequiredService<IUserStore>();
            return authOptions.UseMock
                ? new MockAuthService(userStore)
                : new RealAuthService();
        });

        services.Configure<AuthEndpoints.JwtOptions>(configuration.GetSection("Jwt"));

        var jwtOptions = configuration.GetSection("Jwt").Get<AuthEndpoints.JwtOptions>() ?? new();
        var cookieName = jwtOptions.CookieName ?? "AuthToken";

        services
            .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
            .AddJwtBearer(options =>
            {
                options.TokenValidationParameters = new TokenValidationParameters
                {
                    ValidateIssuer = true,
                    ValidIssuer = jwtOptions.Issuer,
                    ValidateAudience = true,
                    ValidAudience = jwtOptions.Audience,
                    ValidateIssuerSigningKey = true,
                    IssuerSigningKey = new SymmetricSecurityKey(
                        Encoding.UTF8.GetBytes(jwtOptions.SigningKey ?? "dev-key")),
                    ValidateLifetime = true,
                    ClockSkew = TimeSpan.FromMinutes(2)
                };

                options.Events = new JwtBearerEvents
                {
                    OnMessageReceived = context =>
                    {
                        var authorizationHeader = context.Request.Headers["Authorization"].FirstOrDefault();
                        if (!string.IsNullOrEmpty(authorizationHeader) && authorizationHeader.StartsWith("Bearer "))
                        {
                            context.Token = authorizationHeader[7..];
                            return Task.CompletedTask;
                        }

                        if (context.Request.Cookies.TryGetValue(cookieName, out var cookieToken)
                            && !string.IsNullOrWhiteSpace(cookieToken))
                        {
                            context.Token = cookieToken;
                        }

                        return Task.CompletedTask;
                    }
                };
            });

        services.AddAuthorization();

        return services;
    }

    private static IServiceCollection AddOpenPlotJsonSerialization(this IServiceCollection services)
    {
        services.ConfigureHttpJsonOptions(options =>
        {
            options.SerializerOptions.PropertyNameCaseInsensitive = true;
            options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
        });

        return services;
    }

    private static IServiceCollection AddOpenPlotSwagger(this IServiceCollection services)
    {
        services.AddEndpointsApiExplorer();
        services.AddSwaggerGen(options =>
        {
            options.SwaggerDoc("v1", new OpenApiInfo { Title = "OpenPlot API", Version = "v1" });

            var bearerScheme = new OpenApiSecurityScheme
            {
                Name = "Authorization",
                Type = SecuritySchemeType.Http,
                Scheme = "bearer",
                In = ParameterLocation.Header
            };

            options.AddSecurityDefinition("Bearer", bearerScheme);
            options.AddSecurityRequirement(new OpenApiSecurityRequirement
            {
                { bearerScheme, new List<string>() }
            });
            options.CustomSchemaIds(type => type.FullName?.Replace("+", "."));
        });

        return services;
    }
}
