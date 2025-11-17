using System;
using System.Collections.Generic;
using System.Linq;

public interface IPmuHierarchyService
{
    object BuildHierarchy(IReadOnlyList<PmuMeta> pmus);
}

public sealed class PmuHierarchyService : IPmuHierarchyService
{
    public object BuildHierarchy(IReadOnlyList<PmuMeta> pmus)
    {
        return pmus
            .GroupBy(p => p.area ?? "NA")
            .OrderBy(g => g.Key)
            .Select(gArea => new
            {
                area = gArea.Key,
                estados = gArea
                    .GroupBy(p => p.state ?? "NA")
                    .OrderBy(g => g.Key)
                    .Select(gState => new
                    {
                        nome = gState.Key,
                        tensoes = gState
                            .GroupBy(p => p.volt_level ?? 0)
                            .OrderBy(gVolt => gVolt.Key)
                            .Select(gVolt => new
                            {
                                // valor em kV
                                valor = Math.Round(gVolt.Key / 1000.0, 2),

                                // aqui ainda vem uma lista “flat” de PMUs/terminais,
                                // o front vai agrupar em terminais/estações
                                estacoes = gVolt
                                    .OrderBy(p => p.station ?? p.full_name ?? p.id_name)
                                    .Select(p => new
                                    {
                                        id = p.id_name,
                                        nome = p.full_name ?? p.id_name,
                                        tensao = Math.Round(gVolt.Key / 1000.0, 2),  // kV
                                        area = gArea.Key,
                                        estado = gState.Key,
                                        estacao = p.station,
                                        Grandezas = p.Grandezas ?? Array.Empty<string>(),
                                        Fases = p.Fases ?? Array.Empty<string>(),
                                        adicionais = p.Adicionais ?? Array.Empty<string>(),
                                    })
                                    .ToList()
                            })
                            .ToList()
                    })
                    .ToList()
            })
            .ToList();
    }

}

public sealed class PmuMeta
{
    public int pmu_id { get; set; }
    public string id_name { get; set; } = "";
    public string? full_name { get; set; }
    public int? volt_level { get; set; }  // volts
    public string? area { get; set; }
    public string? state { get; set; }
    public string? station { get; set; }

    // Novas propriedades (para habilitação de possibilidades no front)
    public IReadOnlyList<string>? Grandezas { get; set; }
    public IReadOnlyList<string>? Fases { get; set; }
    public IReadOnlyList<string>? Adicionais { get; set; }
}
