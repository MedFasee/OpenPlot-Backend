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
                                valor = Math.Round(gVolt.Key / 1000.0, 2),
                                estacoes = gVolt
                                    .OrderBy(p => p.station ?? p.full_name ?? p.id_name)
                                    .Select(p => new
                                    {
                                        id = p.id_name,
                                        nome = p.full_name ?? p.id_name,
                                        tipo = p.kind,
                                        tensao = Math.Round(gVolt.Key / 1000.0, 2),
                                        area = gArea.Key,
                                        estado = gState.Key,
                                        estacao = p.station,
                                        grandezas = p.Grandezas ?? Array.Empty<string>(),
                                        fases = p.Fases ?? Array.Empty<string>(),
                                        adicionais = p.Adicionais ?? Array.Empty<PmuAdicional>(),
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
    public int? volt_level { get; set; }
    public string? area { get; set; }
    public string? state { get; set; }
    public string? station { get; set; }
    public string? kind { get; set; }
    public string? tipo { get; set; }

    public IReadOnlyList<string>? Grandezas { get; set; }
    public IReadOnlyList<string>? Fases { get; set; }

    public IReadOnlyList<PmuAdicional>? Adicionais { get; set; }
}


public sealed class PmuAdicional
{
    public string TipoMedida { get; set; } = "";          // "Medidas Analógicas" | "Medidas Digitais"
    public string Grandeza { get; set; } = "";            // "THD de Tensão" | "THD de Corrente" | "Digital"
    public IReadOnlyList<string> Fase { get; set; } = Array.Empty<string>(); // ["A","B","C","Trifásico"] ou [] p/ digital
}

public sealed class PmuMetaRow
{
    public int pmu_id { get; set; }
    public string id_name { get; set; } = "";
    public string? full_name { get; set; }
    public int? volt_level { get; set; }
    public string? area { get; set; }
    public string? state { get; set; }
    public string? station { get; set; }
    public string? kind { get; set; }

    public IReadOnlyList<string>? grandezas { get; set; }
    public IReadOnlyList<string>? fases { get; set; }

    public IReadOnlyList<string>? thd_fases { get; set; }
    public bool has_thd_v { get; set; }
    public bool has_thd_i { get; set; }
    public bool has_digital { get; set; }
}
