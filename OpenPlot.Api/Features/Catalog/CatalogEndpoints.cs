using Microsoft.AspNetCore.Routing;   // IEndpointRouteBuilder
using Microsoft.AspNetCore.Builder;   // MapGroup/RouteGroupBuilder (se usar)
using Dapper;
using System.Data;
using Data.Sql;


public static class CatalogEndpoints
{
    public static IEndpointRouteBuilder MapCatalog(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/catalog").RequireAuthorization();

        // GET /catalog/arquivos  (mantendo rota antiga /arquivos, reexporte se quiser)
        group.MapGet("/arquivos", async (IDbConnectionFactory dbf) =>
        {
            using var db = dbf.Create();
            var nomes = (await db.QueryAsync<string>(PdcSql.ListPdcNames)).ToArray();
            return Results.Json(new { status = 200, data = new { arquivos = nomes } });
        });

        return app;
    }
}
