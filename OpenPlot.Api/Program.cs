using System.Text;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using Microsoft.OpenApi.Models;
using System.Data;
using Microsoft.Extensions.Options;

// ==== OpenPlot usings ====
using OpenPlot.Features.Import;
using OpenPlot.Features.Auth;
using OpenPlot.Auth.Infrastructure.Auth;
using OpenPlot.Auth.Infrastructure.Auth.Options;
using OpenPlot.Auth.Services;
using OpenPlot.Auth.Web.Session;

var builder = WebApplication.CreateBuilder(args);

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

// ======================================================================
// Serviços internos
// ======================================================================
builder.Services.AddSingleton<ITimeService, TimeService>();
builder.Services.AddSingleton<ILabelService, LabelService>();
builder.Services.AddSingleton<IPmuHierarchyService, PmuHierarchyService>();

// ======================================================================
// Session
// ======================================================================
builder.Services.AddHttpContextAccessor();
builder.Services.AddDistributedMemoryCache();

builder.Services.AddSession(o =>
{
    o.Cookie.Name = "openplot.sid";
    o.IdleTimeout = TimeSpan.FromMinutes(60);
    o.Cookie.HttpOnly = true;
    o.Cookie.SameSite = SameSiteMode.None;
    o.Cookie.SecurePolicy = CookieSecurePolicy.None;
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

app.UseAuthentication();
app.UseAuthorization();

app.UseSwagger();
app.UseSwaggerUI();

app.UseSession();

// ======================================================================
// ENDPOINTS
// ======================================================================
var apiV1 = app.MapGroup("/api/v1");
//app.MapGet("/api/v1/", () => Results.Ok(new { ok = true, now = DateTime.UtcNow }));

apiV1.MapAuth();
apiV1.MapConfig();
apiV1.MapSearch();
apiV1.MapRuns();
apiV1.MapImport();

app.Run();
