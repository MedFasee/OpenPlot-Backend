using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using OpenPlot.Auth.Contracts.Responses;

namespace OpenPlot.Features.Auth;

public interface IOpenPlotLoginTokenService
{
    string CreateJwt(LoginResponse response);
    LoginEnvelope CreateEnvelope(LoginResponse response, string jwt);
}

public sealed class OpenPlotLoginTokenService : IOpenPlotLoginTokenService
{
    private readonly IOptions<AuthEndpoints.JwtOptions> _jwtOptions;

    public OpenPlotLoginTokenService(IOptions<AuthEndpoints.JwtOptions> jwtOptions)
    {
        _jwtOptions = jwtOptions;
    }

    public string CreateJwt(LoginResponse response)
    {
        var now = DateTime.UtcNow;
        var jwtOptions = _jwtOptions.Value;

        var claims = new List<Claim>
        {
            new(JwtRegisteredClaimNames.Sub, response.Sub),
            new(JwtRegisteredClaimNames.UniqueName, response.Username),
            new("preferred_username", response.PreferredUsername ?? response.Username),
            new(JwtRegisteredClaimNames.Email, response.Email ?? string.Empty),
            new("sid", response.SessionId ?? Guid.NewGuid().ToString("N"))
        };

        if (response.Roles is not null)
            claims.AddRange(response.Roles.Select(static role => new Claim(ClaimTypes.Role, role)));

        if (response.Claims is not null)
        {
            var existingTypes = new HashSet<string>(claims.Select(static claim => claim.Type), StringComparer.OrdinalIgnoreCase);
            foreach (var claim in response.Claims)
            {
                if (string.IsNullOrWhiteSpace(claim.Key)
                    || string.IsNullOrWhiteSpace(claim.Value)
                    || existingTypes.Contains(claim.Key))
                {
                    continue;
                }

                claims.Add(new Claim(claim.Key, claim.Value));
                existingTypes.Add(claim.Key);
            }
        }

        var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwtOptions.SigningKey));
        var credentials = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);

        var token = new JwtSecurityToken(
            issuer: jwtOptions.Issuer,
            audience: jwtOptions.Audience,
            claims: claims,
            notBefore: now,
            expires: now.AddHours(jwtOptions.ExpirationHours),
            signingCredentials: credentials);

        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    public LoginEnvelope CreateEnvelope(LoginResponse response, string jwt)
        => new()
        {
            Token = jwt,
            Usuario = new UsuarioDto
            {
                Nome = response.Username,
                Email = response.Email ?? $"{response.Username}@medplot.com",
                NomePref = response.PreferredUsername ?? response.Username,
                Role = MapRole(response.Roles)
            }
        };

    private static string MapRole(IReadOnlyCollection<string>? roles)
    {
        if (roles?.Contains("admin", StringComparer.OrdinalIgnoreCase) == true)
            return "admin";

        if (roles?.Contains("editor", StringComparer.OrdinalIgnoreCase) == true)
            return "editor";

        return "reader";
    }
}
