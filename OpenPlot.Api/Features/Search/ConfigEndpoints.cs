using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc;           // <- necessário p/ [FromServices], [FromQuery]
using Dapper;
using System.Data;
using Data.Sql;
using System.Text.Json;
using OpenPlot.Data.Dtos;

// DTO simples para mapear o SELECT
file sealed class PdcRow
{
    public string name { get; init; } = "";
    public int fps { get; init; }
}


public static class ConfigEndpoints
{
    public static void MapConfig(this IEndpointRouteBuilder app)
    {
        // GET /api/v1/arquivos
        app.MapGet("arquivos", async (
            [FromServices] IDbConnectionFactory dbf   // usa o serviço de DB já existente
        ) =>
        {
            using var db = dbf.Create();

            const string sql = @"
                SELECT name, fps
                FROM openplot.pdc
                ORDER BY pdc_id ASC;";

            var rows = await db.QueryAsync<PdcRow>(sql);

            var arquivos = rows.Select(r => new
            {
                nome = $"{r.name}",          // ajuste aqui se quiser outra convenção
                resolucao_maxima = r.fps
            });

            return Results.Json(new
            {
                status = 200,
                data = new { arquivos }
            });
        })
        .WithName("GetArquivos")
        .WithTags("Config");
    }
}
