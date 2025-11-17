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

file sealed class PmuRow
{
    public string pdc_name { get; init; } = "";
    public int pmu_id { get; init; }
    public string id_name { get; init; } = "";
    public string? full_name { get; init; }
    public int? volt_level { get; init; }
    public string? area { get; init; }
    public string? state { get; init; }
    public string? station { get; init; }
    public double? lat { get; init; }
    public double? lon { get; init; }
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

        // ============================================================
        // GET /xml/{pdcName}/terminais
        // ============================================================
        app.MapGet("xml/{pdcName}/terminais", async (
            [FromRoute] string pdcName,
            [FromServices] IDbConnectionFactory dbf
        ) =>
        {
            using var db = dbf.Create();

            const string sql = @"
SELECT
    p.name          AS pdc_name,
    m.pmu_id,
    m.id_name,
    m.full_name,
    m.volt_level,
    m.area,
    m.state,
    m.station,
    m.lat,
    m.lon
FROM openplot.pdc AS p
JOIN openplot.pdc_pmu AS pp
  ON p.pdc_id = pp.pdc_id
JOIN openplot.pmu AS m
  ON pp.pmu_id = m.pmu_id
WHERE p.name ILIKE @pdcName
ORDER BY m.area, m.state, m.volt_level, m.station, m.id_name;";

            var pmus = (await db.QueryAsync<PmuRow>(sql, new { pdcName })).ToList();

            if (pmus.Count == 0)
                return Results.NotFound(new
                {
                    status = 404,
                    mensagem = $"Nenhuma PMU encontrada para o PDC '{pdcName}'."
                });

            string XmlFileName = $"{pmus.First().pdc_name}.xml";

            // ---------------------------------------
            // Mapeia árvore: área -> estado -> tensão -> estação -> terminais
            // ---------------------------------------
            var gruposPorArea = pmus
                .GroupBy(p => p.area)
                .Select(gArea => new
                {
                    area = gArea.Key,
                    estados = gArea
                        .GroupBy(p => p.state)
                        .Select(gEstado => new
                        {
                            nome = gEstado.Key,
                            tensoes = gEstado
                                .GroupBy(p => p.volt_level ?? 0)
                                .Select(gTensao => new
                                {
                                    valor = (gTensao.Key / 1000),
                                    estacoes = gTensao
                                        .GroupBy(p => p.station)
                                        .Select(gEst => new
                                        {
                                            estacao = gEst.Key,
                                            nome = gEst.First().id_name,
                                            terminais = gEst.Select(t =>
                                            {
                                                var id = $"{t.id_name}";
                                                return new
                                                {
                                                    id,
                                                    terminal = t.full_name,
                                                    nome = t.full_name ?? t.id_name,
                                                    estacao = t.station,
                                                    estado = t.state,
                                                    area = t.area,
                                                    tensao = (decimal)Math.Round((t.volt_level ?? 0) / 1000.0, 2),
                                                    lat = t.lat,
                                                    lon = t.lon
                                                };
                                            })
                                        })
                                })
                        })
                });

            var response = new
            {
                status = 200,
                data = new
                {
                    xml_file = XmlFileName,
                    total_terminais = pmus.Count,
                    terminais = gruposPorArea
                }
            };

            return Results.Json(response);
        })
        .WithName("GetPdcTerminais")
        .WithTags("Config");
    }
}