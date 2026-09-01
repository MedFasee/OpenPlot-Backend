using System.Collections.Concurrent;
using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenPlot.Api.Services.Logging;
using OpenPlot.Auth.Contracts.Requests;
using OpenPlot.Auth.Contracts.Responses;
using OpenPlot.Auth.Services;
using OpenPlot.Features.Runs.Contracts;
using OpenPlot.Features.Sso.Repositories;

namespace OpenPlot.Api.IntegrationTests.Infrastructure;

public sealed class OpenPlotApiFactory : WebApplicationFactory<Program>
{
    public TestAnalysisCacheRepository CacheRepository { get; } = new();
    public TestSsoAuthRepository SsoRepository { get; } = new();

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Development");
        builder.UseSetting("AuthProvider:Provider", "OpenPlot");
        builder.UseSetting("Jwt:SigningKey", "integration-test-signing-key-openplot-only");
        builder.UseSetting("ConnectionStrings:Db", "Host=localhost;Port=5432;Database=openplot_tests;Username=test;Password=test");
        builder.UseSetting("SsoClients:0:ClientId", "multiinfeed");
        builder.UseSetting("SsoClients:0:Secret", "integration-test-secret");
        builder.UseSetting("SsoClients:0:TechnicalUsername", "MIF_robo");

        builder.ConfigureServices(services =>
        {
            services.RemoveAll<IAuthService>();
            services.RemoveAll<IAnalysisCacheRepository>();
            services.RemoveAll<IApiRequestLogRepository>();
            services.RemoveAll<ISsoAuthRepository>();

            services.AddSingleton<IAuthService, FakeAuthService>();
            services.AddSingleton<IAnalysisCacheRepository>(CacheRepository);
            services.AddSingleton<IApiRequestLogRepository, NoOpApiRequestLogRepository>();
            services.AddSingleton<ISsoAuthRepository>(SsoRepository);

            services.AddAuthentication(options =>
            {
                options.DefaultAuthenticateScheme = TestAuthHandler.SchemeName;
                options.DefaultChallengeScheme = TestAuthHandler.SchemeName;
                options.DefaultScheme = TestAuthHandler.SchemeName;
            }).AddScheme<AuthenticationSchemeOptions, TestAuthHandler>(
                TestAuthHandler.SchemeName,
                _ => { });

            services.AddAuthorization();
            services.PostConfigure<SessionOptions>(options =>
            {
                options.Cookie.SecurePolicy = CookieSecurePolicy.None;
            });
        });
    }

    public sealed class TestAnalysisCacheRepository : IAnalysisCacheRepository
    {
        private readonly ConcurrentDictionary<Guid, object> _items = new();

        public void Seed(Guid cacheId, object payload) => _items[cacheId] = payload;

        public Task<Guid> SaveAsync(Guid jobId, object payload, CancellationToken ct)
        {
            var id = Guid.NewGuid();
            _items[id] = payload;
            return Task.FromResult(id);
        }

        public Task<Guid> SaveAsync(Guid cacheId, Guid jobId, object payload, CancellationToken ct)
        {
            _items[cacheId] = payload;
            return Task.FromResult(cacheId);
        }

        public Task<T?> GetAsync<T>(Guid cacheId, CancellationToken ct)
        {
            if (_items.TryGetValue(cacheId, out var payload) && payload is T typed)
                return Task.FromResult<T?>(typed);

            return Task.FromResult<T?>(default);
        }
    }

    public sealed class TestSsoAuthRepository : ISsoAuthRepository
    {
        private readonly ConcurrentDictionary<string, DateTime> _nonces = new(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, ConsumedSsoLoginToken> _tokens = new(StringComparer.OrdinalIgnoreCase);

        public void SeedLoginToken(string token, string consultaId, string originClient, DateTime? createdAtUtc = null, DateTime? expiresAtUtc = null)
        {
            var created = createdAtUtc ?? DateTime.UtcNow;
            _tokens[token] = new ConsumedSsoLoginToken
            {
                Id = Guid.NewGuid(),
                ConsultaId = consultaId,
                OriginClient = originClient,
                CreatedAtUtc = created,
                ExpiresAtUtc = expiresAtUtc ?? created.AddMinutes(5)
            };
        }

        public Task<bool> TryRegisterNonceAsync(string clientId, string nonce, DateTime createdAtUtc, DateTime expiresAtUtc, CancellationToken ct = default)
            => Task.FromResult(_nonces.TryAdd($"{clientId}:{nonce}", expiresAtUtc));

        public Task CreateLoginTokenAsync(string token, string consultaId, string originClient, DateTime createdAtUtc, DateTime expiresAtUtc, CancellationToken ct = default)
        {
            SeedLoginToken(token, consultaId, originClient, createdAtUtc, expiresAtUtc);
            return Task.CompletedTask;
        }

        public Task<ConsumedSsoLoginToken?> ConsumeLoginTokenAsync(string token, CancellationToken ct = default)
        {
            if (!_tokens.TryRemove(token, out var consumedToken))
                return Task.FromResult<ConsumedSsoLoginToken?>(null);

            if (consumedToken.ExpiresAtUtc <= DateTime.UtcNow)
                return Task.FromResult<ConsumedSsoLoginToken?>(null);

            return Task.FromResult<ConsumedSsoLoginToken?>(consumedToken);
        }
    }

    private sealed class FakeAuthService : IAuthService
    {
        public Task<(bool ok, LoginResponse? resp, string? error)> AuthenticateAsync(LoginRequest request, CancellationToken ct = default)
        {
            var response = new LoginResponse
            {
                Sub = "user-1",
                Username = request.Username,
                PreferredUsername = request.Username,
                Roles = ["reader"],
                SessionId = "session-1",
                Email = $"{request.Username}@openplot.test"
            };

            return Task.FromResult<(bool ok, LoginResponse? resp, string? error)>((true, response, null));
        }
    }

    private sealed class NoOpApiRequestLogRepository : IApiRequestLogRepository
    {
        public Task InsertAsync(ApiRequestLogEntry entry, CancellationToken ct = default) => Task.CompletedTask;
    }

    private sealed class TestAuthHandler : AuthenticationHandler<AuthenticationSchemeOptions>
    {
        public const string SchemeName = "Test";

        public TestAuthHandler(
            IOptionsMonitor<AuthenticationSchemeOptions> options,
            ILoggerFactory logger,
            UrlEncoder encoder)
            : base(options, logger, encoder)
        {
        }

        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            var identity = new ClaimsIdentity(
            [
                new Claim("username", "integration-user"),
                new Claim(ClaimTypes.Name, "integration-user"),
                new Claim(ClaimTypes.NameIdentifier, "integration-user-id")
            ], SchemeName);

            var principal = new ClaimsPrincipal(identity);
            var ticket = new AuthenticationTicket(principal, SchemeName);
            return Task.FromResult(AuthenticateResult.Success(ticket));
        }
    }
}
