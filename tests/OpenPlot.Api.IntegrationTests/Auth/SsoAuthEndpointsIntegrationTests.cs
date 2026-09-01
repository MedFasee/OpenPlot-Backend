using System.IdentityModel.Tokens.Jwt;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Mvc.Testing;
using OpenPlot.Api.IntegrationTests.Infrastructure;
using OpenPlot.Features.Sso.Repositories;

namespace OpenPlot.Api.IntegrationTests.Auth;

public sealed class SsoAuthEndpointsIntegrationTests(OpenPlotApiFactory factory) : IClassFixture<OpenPlotApiFactory>
{
    private static WebApplicationFactory<Program> CreateOnsFactory(OpenPlotApiFactory baseFactory)
    {
        return baseFactory.WithWebHostBuilder(builder =>
        {
            builder.UseSetting("AuthProvider:Provider", "Ons");
        });
    }

    [Fact]
    public async Task ConsumeSsoToken_WhenTokenIsValid_ReturnsLoginEnvelopeWithSsoClaims()
    {
        using var onsFactory = CreateOnsFactory(factory);
        var consultaId = $"consulta-{Guid.NewGuid():N}";
        var token = $"token-{Guid.NewGuid():N}";

        using (var scope = onsFactory.Services.CreateScope())
        {
            var repository = scope.ServiceProvider.GetRequiredService<ISsoAuthRepository>();
            ((OpenPlotApiFactory.TestSsoAuthRepository)repository).SeedLoginToken(token, consultaId, "multiinfeed");
        }

        using var client = onsFactory.CreateClient();
        var response = await client.PostAsJsonAsync("/api/v1/sso/consumir-token", new { token });

        response.EnsureSuccessStatusCode();

        using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var root = json.RootElement;
        var data = root.GetProperty("data");
        var login = data.GetProperty("login");
        var jwt = login.GetProperty("token").GetString();

        Assert.Equal(200, root.GetProperty("status").GetInt32());
        Assert.Equal(consultaId, data.GetProperty("consultaId").GetString());
        Assert.Equal("multiinfeed", data.GetProperty("originClient").GetString());
        Assert.Equal($"/processar-busca?consultaId={consultaId}", data.GetProperty("redirectPath").GetString());
        Assert.Equal("MIF_robo", login.GetProperty("usuario").GetProperty("nome").GetString());
        Assert.False(string.IsNullOrWhiteSpace(jwt));

        var parsedToken = new JwtSecurityTokenHandler().ReadJwtToken(jwt);
        Assert.Equal("sso", parsedToken.Claims.Single(claim => claim.Type == "auth_flow").Value);
        Assert.Equal("multiinfeed", parsedToken.Claims.Single(claim => claim.Type == "origin_client").Value);
        Assert.Equal(consultaId, parsedToken.Claims.Single(claim => claim.Type == "consulta_id").Value);
        Assert.Equal("MIF_robo", parsedToken.Claims.Single(claim => claim.Type == JwtRegisteredClaimNames.UniqueName).Value);
    }

    [Fact]
    public async Task Logout_AfterSsoLogin_ClearsSessionAndReturnsOk()
    {
        using var onsFactory = CreateOnsFactory(factory);
        var token = $"token-{Guid.NewGuid():N}";

        using (var scope = onsFactory.Services.CreateScope())
        {
            var repository = scope.ServiceProvider.GetRequiredService<ISsoAuthRepository>();
            ((OpenPlotApiFactory.TestSsoAuthRepository)repository).SeedLoginToken(token, "consulta-logout", "multiinfeed");
        }

        using var client = onsFactory.CreateClient(new WebApplicationFactoryClientOptions
        {
            HandleCookies = true
        });

        var loginResponse = await client.PostAsJsonAsync("/api/v1/sso/consumir-token", new { token });
        loginResponse.EnsureSuccessStatusCode();

        var logoutResponse = await client.PostAsync("/api/v1/auth/logout", content: null);

        Assert.Equal(HttpStatusCode.OK, logoutResponse.StatusCode);

        using var json = JsonDocument.Parse(await logoutResponse.Content.ReadAsStringAsync());
        Assert.Equal("Sessão encerrada", json.RootElement.GetProperty("message").GetString());
    }
}
