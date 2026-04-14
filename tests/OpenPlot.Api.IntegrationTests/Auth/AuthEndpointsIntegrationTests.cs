using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using OpenPlot.Api.IntegrationTests.Infrastructure;
using OpenPlot.Auth.Contracts.Requests;

namespace OpenPlot.Api.IntegrationTests.Auth;

public sealed class AuthEndpointsIntegrationTests(OpenPlotApiFactory factory) : IClassFixture<OpenPlotApiFactory>
{
    [Fact]
    public async Task Login_ReturnsTokenEnvelope()
    {
        using var client = factory.CreateClient();

        var response = await client.PostAsJsonAsync("/api/v1/auth/login", new LoginRequest
        {
            Username = "alice",
            Password = "secret"
        });

        response.EnsureSuccessStatusCode();

        using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        var root = json.RootElement;

        Assert.Equal(200, root.GetProperty("status").GetInt32());
        Assert.Equal("alice", root.GetProperty("data").GetProperty("usuario").GetProperty("nome").GetString());
        Assert.False(string.IsNullOrWhiteSpace(root.GetProperty("data").GetProperty("token").GetString()));
    }

    [Fact]
    public async Task Logout_AfterLogin_ClearsSessionAndReturnsOk()
    {
        using var client = factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            HandleCookies = true
        });

        var loginResponse = await client.PostAsJsonAsync("/api/v1/auth/login", new LoginRequest
        {
            Username = "bob",
            Password = "secret"
        });

        loginResponse.EnsureSuccessStatusCode();

        var logoutResponse = await client.PostAsync("/api/v1/auth/logout", content: null);

        Assert.Equal(HttpStatusCode.OK, logoutResponse.StatusCode);

        using var json = JsonDocument.Parse(await logoutResponse.Content.ReadAsStringAsync());
        Assert.Equal("Sessão encerrada", json.RootElement.GetProperty("message").GetString());
    }
}
