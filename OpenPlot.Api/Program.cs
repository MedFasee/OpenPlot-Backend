using System.Data;
using System.Text;
using System.Text.Json;
// Evitar starvation
using System.Threading;
using MathNet.Numerics;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using Microsoft.OpenApi.Models;
using OpenPlot.Api.Services.Logging;
using OpenPlot.Auth.Infrastructure.Auth;
using OpenPlot.Auth.Infrastructure.Auth.Options;
using OpenPlot.Auth.Services;
using OpenPlot.Auth.Web.Session;
using OpenPlot.Core.TimeSeries;
using OpenPlot.Features.Auth;
// ==== OpenPlot usings ====
using OpenPlot.Features.Import;
using OpenPlot.Features.PostProcessing.Handlers;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Runs.Handlers;
using OpenPlot.Features.Runs.Repositories;
using OpenPlot.Services.UI;
using Serilog;
using Serilog.Events;

ThreadPool.GetMinThreads(out var worker, out var io);
ThreadPool.SetMinThreads(
    workerThreads: Math.Max(worker, 200),
    completionPortThreads: Math.Max(io, 200)
);

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .MinimumLevel.Override("Microsoft", LogEventLevel.Fatal)
    .Enrich.FromLogContext().WriteTo.Console()
    .WriteTo.File("logs/api-.log", 
    rollingInterval: RollingInterval.Day).CreateLogger();




var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog(); // substitui o logger padrão pelo Serilog

// ======================================================================
// API ouvindo na LAN
// ======================================================================
builder.WebHost.UseUrls("http://0.0.0.0:7011");

// ======================================================================
// CORS — versão que funciona na LAN (dev-friendly)
// Aceita QUALQUER origem, mas sem wildcard '*' (compatível com cookies)
// ======================================================================
builder.Services.AddCors(options =>
{
    options.AddPolicy("DevCors", policy =>
    {
        policy
            .SetIsOriginAllowed(_ => true)  // <-- QUALQUER origem aceita
            .AllowAnyHeader()
            .AllowAnyMethod()
            .AllowCredentials();
    });
});

// ======================================================================
// Conexão com banco
// ======================================================================
var cs = builder.Configuration.GetConnectionString("Db")
         ?? "Host=localhost;Database=postgres;Username=postgres;Password=postgres";

builder.Services.AddScoped<IDbConnectionFactory>(_ => new NpgsqlConnectionFactory(cs));

// Repositório de logs de request (Postgres / openplot.api_request_log)
builder.Services.AddScoped<IApiRequestLogRepository, ApiRequestLogRepository>();

// ======================================================================
// Serviços internos
// ======================================================================
builder.Services.AddSingleton<ITimeService, TimeService>();
builder.Services.AddSingleton<ILabelService, LabelService>();
builder.Services.AddSingleton<IPmuHierarchyService, PmuHierarchyService>();
builder.Services.AddSingleton<IPlotMetaBuilder, PlotMetaBuilder>();
builder.Services.AddScoped<IRunContextRepository, RunContextRepository>();
builder.Services.AddSingleton<ITimeSeriesDownsampler, TimeBucketMinMaxDownsampler>();
builder.Services.AddScoped<SimpleSeriesHandler>();
builder.Services.AddScoped<IRunContextRepository, RunContextRepository>();
builder.Services.AddScoped<IPlotMetaBuilder, PlotMetaBuilder>();
builder.Services.AddScoped<VoltageSeriesHandler>();
builder.Services.AddScoped<CurrentSeriesHandler>();
builder.Services.AddScoped<SeqSeriesHandler>();
builder.Services.AddScoped<UnbalanceSeriesHandler>();
builder.Services.AddScoped<IDftMetaBuilder, DftMetaBuilder>();

// UI Analise
builder.Services.AddSingleton(new FeatureFlags(
    EnablesDFT: true,
    EnablesProny: true,
    EnablesCCA: true,
    EnablesEventsAnalyzer: true
));
builder.Services.AddSingleton<IUiMenuService, UiMenuService>();

// Repositories
builder.Services.AddScoped<IRunContextRepository, RunContextRepository>();
builder.Services.AddScoped<IMeasurementsRepository, MeasurementsRepository>();
builder.Services.AddScoped<IAnalysisCacheRepository, AnalysisCacheRepository>();


// Handlers
builder.Services.AddScoped<SimpleSeriesHandler>();
builder.Services.AddScoped<VoltageSeriesHandler>();
builder.Services.AddScoped<CurrentSeriesHandler>();
builder.Services.AddScoped<IPhasorRequestService, PhasorRequestService>();
builder.Services.AddScoped<IPmuQueryHelper, PmuQueryHelper>();
builder.Services.AddScoped<ISeriesAssemblyService, SeriesAssemblyService>();
builder.Services.AddScoped<SeqSeriesHandler>();
builder.Services.AddScoped<UnbalanceSeriesHandler>();
builder.Services.AddScoped<ThdSeriesHandler>();
builder.Services.AddScoped<PowerSeriesHandler>();
builder.Services.AddScoped<AngleDiffSeriesHandler>();

// Meta
builder.Services.AddSingleton<IPlotMetaBuilder, PlotMetaBuilder>();


// ======================================================================
// Session
// ======================================================================
builder.Services.AddHttpContextAccessor();
builder.Services.AddDistributedMemoryCache();

builder.Services.AddSession(o =>
{
    o.Cookie.Name = "openplot.sid";
    o.IdleTimeout = TimeSpan.FromHours(24);
    o.Cookie.HttpOnly = true;
    o.Cookie.SameSite = SameSiteMode.None;
    o.Cookie.SecurePolicy = CookieSecurePolicy.Always; 
});


// 👍 Registro que estava faltando e gerava erro: “session UNKNOWN”
builder.Services.AddScoped<ISessionUserService, SessionUserService>();

// ======================================================================
// Auth / JWT
// ======================================================================
builder.Services.AddSingleton<IUserStore, JsonUserStore>();

builder.Services.AddScoped<IAuthService>(sp =>
{
    var opt = sp.GetRequiredService<IOptions<AuthOptions>>().Value;
    var store = sp.GetRequiredService<IUserStore>();
    return opt.UseMock ? new MockAuthService(store) : new RealAuthService();
});

builder.Services.Configure<AuthEndpoints.JwtOptions>(
    builder.Configuration.GetSection("Jwt")
);

var jwt = builder.Configuration.GetSection("Jwt")
    .Get<AuthEndpoints.JwtOptions>() ?? new();

var jwtCookie = jwt.CookieName ?? "AuthToken";

builder.Services
    .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(o =>
    {
        o.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuer = true,
            ValidIssuer = jwt.Issuer,
            ValidateAudience = true,
            ValidAudience = jwt.Audience,
            ValidateIssuerSigningKey = true,
            IssuerSigningKey = new SymmetricSecurityKey(
                Encoding.UTF8.GetBytes(jwt.SigningKey ?? "dev-key")
            ),
            ValidateLifetime = true,
            ClockSkew = TimeSpan.FromMinutes(2)
        };

        o.Events = new JwtBearerEvents
        {
            OnMessageReceived = ctx =>
            {
                // Authorization: Bearer ...
                var authHeader = ctx.Request.Headers["Authorization"].FirstOrDefault();
                if (!string.IsNullOrEmpty(authHeader) &&
                    authHeader.StartsWith("Bearer "))
                {
                    ctx.Token = authHeader.Substring(7);
                    return Task.CompletedTask;
                }

                // Token via cookie
                if (ctx.Request.Cookies.TryGetValue(jwtCookie, out var cookieToken) &&
                    !string.IsNullOrWhiteSpace(cookieToken))
                {
                    ctx.Token = cookieToken;
                }

                return Task.CompletedTask;
            }
        };
    });

builder.Services.AddAuthorization();

// ======================================================================
// JSON Serialization Configuration
// ======================================================================
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNameCaseInsensitive = true;
    options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
});

// ======================================================================
// Swagger
// ======================================================================
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new OpenApiInfo { Title = "OpenPlot API", Version = "v1" });

    var bearer = new OpenApiSecurityScheme
    {
        Name = "Authorization",
        Type = SecuritySchemeType.Http,
        Scheme = "bearer",
        In = ParameterLocation.Header
    };

    c.AddSecurityDefinition("Bearer", bearer);
    c.AddSecurityRequirement(new OpenApiSecurityRequirement
    {
        { bearer, new List<string>() }
    });

    c.CustomSchemaIds(t => t.FullName?.Replace("+", "."));
});

// ======================================================================
// PIPELINE
// ======================================================================
var app = builder.Build();

// CORS sempre no topo
app.UseCors("DevCors");

// 1) Sessão precisa ser carregada antes de qualquer coisa que use SessionUserService
app.UseSession();

// 2) Autenticação (se tiver JWT/cookie, ele já pode usar a sessão também)
app.UseAuthentication();

// 3) Agora o middleware de logging já enxerga HttpContext.User e Session
app.UseMiddleware<RequestLoggingMiddleware>();

// 4) Autorização
app.UseAuthorization();

// 5) Swagger etc.
app.UseSwagger();
app.UseSwaggerUI();

// ======================================================================
// ENDPOINTS
// ======================================================================
var apiV1 = app.MapGroup("/api/v1");
//app.MapGet("/api/v1/", () => Results.Ok(new { ok = true, now = DateTime.UtcNow }));

apiV1.MapAuth();
apiV1.MapConfig();
apiV1.MapSearch();
apiV1.MapRuns();
apiV1.MapPostProcessing();
apiV1.MapImport();

app.Run();
