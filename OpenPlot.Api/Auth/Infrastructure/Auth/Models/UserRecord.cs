namespace OpenPlot.Auth.Infrastructure.Auth.Models;

public sealed record UserRecord(
    string sub,
    string username,
    string password,                 // MOCK: texto puro — em prod usar hash!
    string preferred_username,
    string email,
    string[] roles,
    Dictionary<string, string> claims
);

public sealed class UserStoreOptions
{
    public string UsersPath { get; set; } = "auth/users.json";
}

