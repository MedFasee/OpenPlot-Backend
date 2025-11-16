using System.Text;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using Microsoft.OpenApi.Models;

// ==== OpenPlot usings ====
using OpenPlot.Features.Import;
using OpenPlot.Features.Auth;                        // MapAuthEndpoints + JwtOptions
using OpenPlot.Auth.Contracts.Requests;              // LoginRequest
using OpenPlot.Auth.Contracts.Responses;             // LoginResponse
using OpenPlot.Auth.Infrastructure.Auth;             // IUserStore, JsonUserStore
using OpenPlot.Auth.Infrastructure.Auth.Options;     // AuthOptions, UserStoreOptions
using OpenPlot.Auth.Services;                        // IAuthService, MockAuthService, RealAuthService
using OpenPlot.Auth.Web.Session;                     // ISessionUserService, SessionUserService, SessionOptionsEx

// Outros (DB/serviços utilitários)
using System.Data;
using Microsoft.Extensions.Options;

// ==========================================
// CONFIGURAÇÃO
// ==========================================
var builder = WebApplication.CreateBuilder(args);

// ---------- Options ----------
builder.Services.Configure<AuthOptions>(builder.Configuration.GetSection("Auth"));
builder.Services.Configure<UserStoreOptions>(builder.Configuration.GetSection("Auth:UserStore"));
builder.Services.Configure<SessionOptionsEx>(builder.Configuration.GetSection("Session"));

// ---------- CORS ----------
var origins = builder.Configuration.GetSection("Cors:AllowedOrigins").Get<string[]>()
              ?? new[] { "http://localhost:5173", "http://127.17.0.2:5173", "http://127.17.0.1:5173" };

builder.Services.AddCors(opt =>
{
    opt.AddPolicy("frontend", p => p
        .WithOrigins(origins)
        .AllowAnyHeader()
        .AllowAnyMethod()
        .AllowCredentials());
});

// ---------- Conexão BD ----------
var cs = builder.Configuration.GetConnectionString("Db")
         ?? "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=postgres";
builder.Services.AddScoped<IDbConnectionFactory>(_ => new NpgsqlConnectionFactory(cs));

// ---------- Serviços utilitários ----------
builder.Services.AddSingleton<ITimeService, TimeService>();
builder.Services.AddSingleton<ILabelService, LabelService>();
builder.Services.AddSingleton<IPmuHierarchyService, PmuHierarchyService>();

// ---------- HttpContext + Session ----------
builder.Services.AddHttpContextAccessor();
builder.Services.AddDistributedMemoryCache();

builder.Services.AddSession(o =>
{
    var s = builder.Configuration.GetSection("Session").Get<SessionOptionsEx>() ?? new SessionOptionsEx();
    o.Cookie.Name = s.CookieName;
    o.IdleTimeout = TimeSpan.FromMinutes(s.IdleTimeoutMinutes);
    o.Cookie.HttpOnly = true;
    o.Cookie.SameSite = SameSiteMode.None;
    o.Cookie.SecurePolicy = CookieSecurePolicy.Always; // HTTPS recomendado
});

// ---------- SessionUserService ----------
builder.Services.AddScoped<ISessionUserService, SessionUserService>();

// ---------- Auth: Mock ↔ Real ----------
builder.Services.AddSingleton<IUserStore, JsonUserStore>(); // sempre disponível

builder.Services.AddScoped<IAuthService>(sp =>
{
    var authOpt = sp.GetRequiredService<IOptions<AuthOptions>>().Value;
    var store = sp.GetRequiredService<IUserStore>();

    if (authOpt.UseMock)
        return new MockAuthService(store);

    return new RealAuthService(); // produção (Keycloak)
});

// ---------- JWT ----------
builder.Services.Configure<AuthEndpoints.JwtOptions>(builder.Configuration.GetSection("Jwt"));
var jwt = builder.Configuration.GetSection("Jwt").Get<AuthEndpoints.JwtOptions>() ?? new();

// nome do cookie onde vamos guardar o token
var jwtCookieName = jwt.CookieName ?? "AuthToken";

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
            IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwt.SigningKey ?? "dev-key")),
            ValidateLifetime = true,
            ClockSkew = TimeSpan.FromMinutes(2)
        };

        // PONTO-CHAVE: ler token também do cookie
        o.Events = new JwtBearerEvents
        {
            OnMessageReceived = context =>
            {
                // Se já vier Authorization: Bearer ..., respeita isso
                var authHeader = context.Request.Headers["Authorization"].FirstOrDefault();
                if (!string.IsNullOrEmpty(authHeader) && authHeader.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
                {
                    context.Token = authHeader.Substring("Bearer ".Length).Trim();
                    return Task.CompletedTask;
                }

                // Senão, tenta pegar do cookie
                if (context.Request.Cookies.TryGetValue(jwtCookieName, out var cookieToken) &&
                    !string.IsNullOrWhiteSpace(cookieToken))
                {
                    context.Token = cookieToken;
                }

                return Task.CompletedTask;
            }
        };
    });

builder.Services.AddAuthorization();

// ---------- Swagger ----------
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new OpenApiInfo { Title = "OpenPlot API", Version = "v1" });

    var bearer = new OpenApiSecurityScheme
    {
        Name = "Authorization",
        Type = SecuritySchemeType.Http,
        Scheme = "bearer",
        BearerFormat = "JWT",
        In = ParameterLocation.Header,
        Description = "Informe: Bearer {token}"
    };
    c.AddSecurityDefinition("Bearer", bearer);
    c.AddSecurityRequirement(new OpenApiSecurityRequirement { { bearer, new List<string>() } });

    // Evita conflitos de schema
    c.CustomSchemaIds(t => t.FullName?.Replace("+", "."));
    c.ResolveConflictingActions(apiDescriptions => apiDescriptions.First());
});

builder.Logging.ClearProviders();
builder.Logging.AddConsole();

// ==========================================
// PIPELINE
// ==========================================
var app = builder.Build();

app.UseCors("frontend");

if (app.Environment.IsDevelopment())
{
    app.UseDeveloperExceptionPage();
}

app.UseSwagger();
app.UseSwaggerUI();

app.UseSession();
app.UseAuthentication();
app.UseAuthorization();

// ==========================================
// ENDPOINTS
// ==========================================
app.MapGet("/", () => Results.Ok(new { ok = true, now = DateTime.UtcNow }));

app.MapAuth();
app.MapConfig();
app.MapSearch();
app.MapRuns();
app.MapImport();


app.Run();
