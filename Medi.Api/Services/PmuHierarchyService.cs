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
            .Select(gArea => new {
                area = gArea.Key,
                estados = gArea
                    .GroupBy(p => p.state ?? "NA")
                    .OrderBy(g => g.Key)
                    .Select(gState => new {
                        nome = gState.Key,
                        tensoes = gState
                            .GroupBy(p => p.volt_level ?? 0)
                            .OrderBy(g => g.Key)
                            .Select(gVolt => new {
                                valor = Math.Round((gVolt.Key) / 1000.0, 2),
                                estacoes = gVolt
                                    .OrderBy(p => p.station ?? p.full_name ?? p.id_name)
                                    .Select(p => new {
                                        id = p.id_name,
                                        nome = p.full_name ?? p.id_name,
                                        tensao = Math.Round((gVolt.Key) / 1000.0, 2),
                                        area = gArea.Key,
                                        estado = gState.Key,
                                        estacao = p.station
                                    }).ToList()
                            }).ToList()
                    }).ToList()
            }).ToList();
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
}
