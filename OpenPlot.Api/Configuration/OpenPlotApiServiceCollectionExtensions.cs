using System.Data;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using Microsoft.OpenApi.Models;
using OpenPlot.Api.Services.Logging;
using OpenPlot.Api.Services.Security;
using OpenPlot.Auth.Infrastructure.Auth;
using OpenPlot.Auth.Infrastructure.Auth.Models;
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
using OpenPlot.Features.Runs.Services;
using OpenPlot.Features.Sso;
using OpenPlot.Features.Sso.Repositories;
using OpenPlot.Features.Sso.Services;
using OpenPlot.Services.BackgroundCache;
using OpenPlot.Services.UI;

namespace OpenPlot.Api.Configuration;

internal static class OpenPlotApiServiceCollectionExtensions
{
    internal const string CorsPolicyName = "OpenPlotCors";

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
        builder.Services.AddOpenPlotCors(builder.Configuration, builder.Environment);
        builder.Services.AddOpenPlotDataAccess(builder.Configuration);
        builder.Services.AddOpenPlotLogging();
        builder.Services.AddOpenPlotDomainServices();
        builder.Services.AddOpenPlotSessionServices();
        builder.Services.AddOpenPlotAuthentication(builder.Configuration);
        builder.Services.AddOpenPlotJsonSerialization();
        builder.Services.AddOpenPlotSwagger();
        builder.Services.AddOpenPlotBackgroundCache();
        builder.Services.AddHealthChecks();

        return builder;
    }

    private static IServiceCollection AddOpenPlotCors(
        this IServiceCollection services,
        IConfiguration configuration,
        IWebHostEnvironment environment)
    {
        var allowAnyOrigin = configuration.GetValue("Cors:AllowAnyOrigin", false);
        var allowCredentials = configuration.GetValue("Cors:AllowCredentials", true);
        var allowedOrigins = configuration
            .GetSection("Cors:AllowedOrigins")
            .Get<string[]>()
            ?.Where(origin => !string.IsNullOrWhiteSpace(origin) && origin != "*")
            .Select(origin => origin.Trim().TrimEnd('/'))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray()
            ?? [];

        if (environment.IsDevelopment() && !allowAnyOrigin && allowedOrigins.Length == 0)
        {
            allowedOrigins =
            [
                "http://localhost:5173",
                "http://localhost:4173"
            ];
        }

        if (allowAnyOrigin && allowCredentials)
        {
            throw new InvalidOperationException(
                "Cors:AllowAnyOrigin=true não pode ser combinado com Cors:AllowCredentials=true.");
        }

        services.AddCors(options =>
        {
            options.AddPolicy(CorsPolicyName, policy =>
            {
                policy.AllowAnyHeader().AllowAnyMethod();

                if (allowAnyOrigin)
                {
                    policy.AllowAnyOrigin();
                }
                else if (allowedOrigins.Length > 0)
                {
                    policy.WithOrigins(allowedOrigins);
                }
                else
                {
                    // Produção sem origem configurada: nega CORS em vez de abrir por padrão.
                    policy.SetIsOriginAllowed(_ => false);
                }

                if (allowCredentials)
                    policy.AllowCredentials();
            });
        });

        return services;
    }

    private static IServiceCollection AddOpenPlotDataAccess(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString("Db");
        if (string.IsNullOrWhiteSpace(connectionString))
            connectionString = Environment.GetEnvironmentVariable("OPENPLOT_DB_CONNECTION");

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                "Defina ConnectionStrings:Db ou OPENPLOT_DB_CONNECTION.");
        }

        services.AddScoped<IDbConnectionFactory>(_ => new NpgsqlConnectionFactory(connectionString));
        services.AddMemoryCache();
        services.AddSingleton<IQueryExecutionCoordinator, QueryExecutionCoordinator>();
        services.AddSingleton<IMeasurementMetadataCache, MeasurementMetadataCache>();
        services.AddSingleton<ISamplingExecutionPolicy, SamplingExecutionPolicy>();
        services.Configure<MeasurementQuerySchedulerOptions>(
            configuration.GetSection("OpenPlot:Measurements:QueryScheduler"));
        services.AddSingleton<IMeasurementQueryScheduler, MeasurementQueryScheduler>();
        services.AddScoped<IApiRequestLogRepository, ApiRequestLogRepository>();
        services.AddScoped<IRunContextRepository, RunContextRepository>();
        services.AddScoped<IMeasurementsRepository, MeasurementsRepository>();
        services.AddScoped<IAnalysisCacheRepository, AnalysisCacheRepository>();
        services.AddScoped<ISsoAuthRepository, SsoAuthRepository>();

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
        services.AddSingleton<IOpenPlotLoginTokenService, OpenPlotLoginTokenService>();
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
        services.AddScoped<ICcaMetaBuilder, CcaMetaBuilder>();
        services.AddSingleton<IExportArtifactStore, DiskExportArtifactStore>();
        services.AddScoped<IExportFileService, ExportFileService>();
        services.AddScoped<IXmlImportService, XmlImportService>();

        // Mantém integralmente o warm-up/background cache existente na B2.
        services.AddSingleton<MeasurementsWarmUpService>();
        services.AddSingleton<IMeasurementsWarmUpQueue>(serviceProvider =>
            serviceProvider.GetRequiredService<MeasurementsWarmUpService>());
        services.AddHostedService(serviceProvider =>
            serviceProvider.GetRequiredService<MeasurementsWarmUpService>());

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

    private static IServiceCollection AddOpenPlotAuthentication(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        const string localScheme = "OpenPlotLocalJwt";
        const string dynamicScheme = "OpenPlotDynamicJwt";

        var providerOptions = configuration.GetSection("AuthProvider").Get<AuthProviderOptions>() ?? new();
        if (!providerOptions.IsSupported)
        {
            throw new InvalidOperationException(
                $"AuthProvider:Provider inválido: '{providerOptions.Provider}'. Use 'OpenPlot' ou 'Ons'.");
        }

        services.Configure<AuthOptions>(configuration.GetSection("Auth"));
        services.Configure<AuthProviderOptions>(configuration.GetSection("AuthProvider"));
        services.Configure<UserStoreOptions>(configuration.GetSection("Auth:UserStore"));
        services.Configure<SsoOptions>(configuration.GetSection("Sso"));
        services.Configure<List<SsoClientOptions>>(configuration.GetSection("SsoClients"));
        services.Configure<AuthEndpoints.JwtOptions>(configuration.GetSection("Jwt"));

        services.AddSingleton<IUserStore, JsonUserStore>();
        services.AddSingleton<ISsoClientRegistry, SsoClientRegistry>();
        services.AddScoped<ISsoIdentityService, SsoIdentityService>();
        services.AddScoped<ISsoRequestValidator, SsoRequestValidator>();
        services.AddScoped<IAuthService>(serviceProvider =>
        {
            var authOptions = serviceProvider.GetRequiredService<IOptions<AuthOptions>>().Value;
            if (!authOptions.UseMock)
                return new RealAuthService();

            var userStore = serviceProvider.GetRequiredService<IUserStore>();
            return new MockAuthService(userStore);
        });

        var jwtOptions = configuration.GetSection("Jwt").Get<AuthEndpoints.JwtOptions>() ?? new();
        var cookieName = string.IsNullOrWhiteSpace(jwtOptions.CookieName)
            ? "AuthToken"
            : jwtOptions.CookieName;

        if (providerOptions.IsOpenPlot && string.IsNullOrWhiteSpace(jwtOptions.SigningKey))
        {
            throw new InvalidOperationException(
                "Jwt:SigningKey é obrigatório quando AuthProvider:Provider=OpenPlot. Configure-o por secret/variável de ambiente.");
        }

        var externalProviders = configuration
            .GetSection("ExternalJwtProviders")
            .Get<List<ExternalJwtProviderOptions>>()
            ?? [];

        var enabledExternalProviders = externalProviders
            .Where(provider => provider.Enabled && !string.IsNullOrWhiteSpace(provider.AuthenticationScheme))
            .ToList();

        var duplicateSchemes = enabledExternalProviders
            .GroupBy(provider => provider.AuthenticationScheme, StringComparer.OrdinalIgnoreCase)
            .Where(group => group.Count() > 1)
            .Select(group => group.Key)
            .ToArray();

        if (duplicateSchemes.Length > 0)
        {
            throw new InvalidOperationException(
                $"AuthenticationScheme duplicado em ExternalJwtProviders: {string.Join(", ", duplicateSchemes)}.");
        }

        var issuerToScheme = enabledExternalProviders
            .SelectMany(provider => provider.ResolveIssuerCandidates()
                .Select(issuer => (Issuer: NormalizeIssuer(issuer), provider.AuthenticationScheme)))
            .Where(item => !string.IsNullOrWhiteSpace(item.Issuer))
            .GroupBy(item => item.Issuer, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(
                grouping => grouping.Key,
                grouping => grouping.First().AuthenticationScheme,
                StringComparer.OrdinalIgnoreCase);

        var authenticationBuilder = services
            .AddAuthentication(options =>
            {
                options.DefaultAuthenticateScheme = dynamicScheme;
                options.DefaultChallengeScheme = dynamicScheme;
                options.DefaultScheme = dynamicScheme;
            })
            .AddPolicyScheme(dynamicScheme, dynamicScheme, options =>
            {
                options.ForwardDefaultSelector = context =>
                {
                    var authorizationHeader = context.Request.Headers["Authorization"].FirstOrDefault();
                    if (!string.IsNullOrWhiteSpace(authorizationHeader)
                        && authorizationHeader.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
                    {
                        var token = authorizationHeader[7..].Trim();
                        if (!string.IsNullOrWhiteSpace(token))
                        {
                            try
                            {
                                var jwt = new JwtSecurityTokenHandler().ReadJwtToken(token);
                                var issuer = NormalizeIssuer(jwt.Issuer);
                                if (issuerToScheme.TryGetValue(issuer, out var mappedScheme))
                                    return mappedScheme;
                            }
                            catch
                            {
                                // Token malformado segue para o esquema local e falhará na validação.
                            }
                        }
                    }

                    return localScheme;
                };
            })
            .AddJwtBearer(localScheme, options =>
            {
                options.MapInboundClaims = false;
                options.TokenValidationParameters = new TokenValidationParameters
                {
                    ValidateIssuer = true,
                    ValidIssuer = jwtOptions.Issuer,
                    ValidateAudience = true,
                    ValidAudience = jwtOptions.Audience,
                    ValidateIssuerSigningKey = true,
                    IssuerSigningKey = string.IsNullOrWhiteSpace(jwtOptions.SigningKey)
                        ? null
                        : new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwtOptions.SigningKey)),
                    ValidateLifetime = true,
                    ClockSkew = TimeSpan.FromMinutes(2)
                };

                options.Events = new JwtBearerEvents
                {
                    OnMessageReceived = context =>
                    {
                        var authorizationHeader = context.Request.Headers["Authorization"].FirstOrDefault();
                        if (!string.IsNullOrWhiteSpace(authorizationHeader)
                            && authorizationHeader.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
                        {
                            context.Token = authorizationHeader[7..].Trim();
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

        foreach (var provider in enabledExternalProviders)
        {
            authenticationBuilder.AddJwtBearer(provider.AuthenticationScheme, options =>
            {
                options.MapInboundClaims = false;
                options.Authority = provider.Authority;

                if (!string.IsNullOrWhiteSpace(provider.MetadataAddress))
                    options.MetadataAddress = provider.MetadataAddress;

                options.RequireHttpsMetadata = provider.RequireHttpsMetadata;
                options.SaveToken = provider.SaveToken;
                options.IncludeErrorDetails = provider.IncludeErrorDetails;

                options.TokenValidationParameters = new TokenValidationParameters
                {
                    ValidateIssuer = true,
                    ValidateAudience = true,
                    ValidateIssuerSigningKey = true,
                    ValidateLifetime = true,
                    ClockSkew = TimeSpan.FromMinutes(Math.Max(0, provider.ClockSkewMinutes))
                };

                if (!string.IsNullOrWhiteSpace(provider.Audience))
                    options.Audience = provider.Audience;

                if (provider.ValidAudiences.Count > 0)
                    options.TokenValidationParameters.ValidAudiences = provider.ValidAudiences;

                if (!string.IsNullOrWhiteSpace(provider.ValidIssuer))
                    options.TokenValidationParameters.ValidIssuer = provider.ValidIssuer;

                if (provider.ValidIssuers.Count > 0)
                    options.TokenValidationParameters.ValidIssuers = provider.ValidIssuers;

                if (!string.IsNullOrWhiteSpace(provider.NameClaimType))
                    options.TokenValidationParameters.NameClaimType = provider.NameClaimType;

                if (!string.IsNullOrWhiteSpace(provider.RoleClaimType))
                    options.TokenValidationParameters.RoleClaimType = provider.RoleClaimType;
            });
        }

        services.AddAuthorization();
        return services;
    }

    private static string NormalizeIssuer(string? issuer) =>
        (issuer ?? string.Empty).Trim().TrimEnd('/');

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
                BearerFormat = "JWT",
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
