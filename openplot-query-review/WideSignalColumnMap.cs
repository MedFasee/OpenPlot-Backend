using System;
using System.Collections.Generic;

namespace OpenPlot.Features.Runs.Repositories;

internal static class WideSignalColumnMap
{
    private static readonly HashSet<string> SupportedColumns = new(StringComparer.OrdinalIgnoreCase)
    {
        "va_mod_v",
        "va_ang_deg",
        "vb_mod_v",
        "vb_ang_deg",
        "vc_mod_v",
        "vc_ang_deg",
        "ia_mod_a",
        "ia_ang_deg",
        "ib_mod_a",
        "ib_ang_deg",
        "ic_mod_a",
        "ic_ang_deg",
        "cthd_a_pct",
        "cthd_b_pct",
        "cthd_c_pct",
        "vthd_a_pct",
        "vthd_b_pct",
        "vthd_c_pct",
        "frequency_hz",
        "delta_freq_hz",
        "cfds"
    };

    /// <summary>
    /// Projeta o valor narrow (m.value) a partir de measurements_wide sem interpolar coluna de usuário.
    /// </summary>
    public static string BuildValueCaseSql(string signalAlias, string wideAlias)
    {
        return $@"
CASE
  WHEN LOWER({signalAlias}.quantity::text) IN ('voltage','v') AND UPPER({signalAlias}.phase::text) = 'A' AND UPPER({signalAlias}.component::text) = 'MAG' THEN {wideAlias}.va_mod_v
  WHEN LOWER({signalAlias}.quantity::text) IN ('voltage','v') AND UPPER({signalAlias}.phase::text) = 'A' AND UPPER({signalAlias}.component::text) = 'ANG' THEN {wideAlias}.va_ang_deg
  WHEN LOWER({signalAlias}.quantity::text) IN ('voltage','v') AND UPPER({signalAlias}.phase::text) = 'B' AND UPPER({signalAlias}.component::text) = 'MAG' THEN {wideAlias}.vb_mod_v
  WHEN LOWER({signalAlias}.quantity::text) IN ('voltage','v') AND UPPER({signalAlias}.phase::text) = 'B' AND UPPER({signalAlias}.component::text) = 'ANG' THEN {wideAlias}.vb_ang_deg
  WHEN LOWER({signalAlias}.quantity::text) IN ('voltage','v') AND UPPER({signalAlias}.phase::text) = 'C' AND UPPER({signalAlias}.component::text) = 'MAG' THEN {wideAlias}.vc_mod_v
  WHEN LOWER({signalAlias}.quantity::text) IN ('voltage','v') AND UPPER({signalAlias}.phase::text) = 'C' AND UPPER({signalAlias}.component::text) = 'ANG' THEN {wideAlias}.vc_ang_deg

  WHEN LOWER({signalAlias}.quantity::text) IN ('current','i') AND UPPER({signalAlias}.phase::text) = 'A' AND UPPER({signalAlias}.component::text) = 'MAG' THEN {wideAlias}.ia_mod_a
  WHEN LOWER({signalAlias}.quantity::text) IN ('current','i') AND UPPER({signalAlias}.phase::text) = 'A' AND UPPER({signalAlias}.component::text) = 'ANG' THEN {wideAlias}.ia_ang_deg
  WHEN LOWER({signalAlias}.quantity::text) IN ('current','i') AND UPPER({signalAlias}.phase::text) = 'B' AND UPPER({signalAlias}.component::text) = 'MAG' THEN {wideAlias}.ib_mod_a
  WHEN LOWER({signalAlias}.quantity::text) IN ('current','i') AND UPPER({signalAlias}.phase::text) = 'B' AND UPPER({signalAlias}.component::text) = 'ANG' THEN {wideAlias}.ib_ang_deg
  WHEN LOWER({signalAlias}.quantity::text) IN ('current','i') AND UPPER({signalAlias}.phase::text) = 'C' AND UPPER({signalAlias}.component::text) = 'MAG' THEN {wideAlias}.ic_mod_a
  WHEN LOWER({signalAlias}.quantity::text) IN ('current','i') AND UPPER({signalAlias}.phase::text) = 'C' AND UPPER({signalAlias}.component::text) = 'ANG' THEN {wideAlias}.ic_ang_deg

  WHEN LOWER({signalAlias}.quantity::text) IN ('current','i') AND UPPER({signalAlias}.phase::text) = 'A' AND UPPER({signalAlias}.component::text) = 'THD' THEN {wideAlias}.cthd_a_pct
  WHEN LOWER({signalAlias}.quantity::text) IN ('current','i') AND UPPER({signalAlias}.phase::text) = 'B' AND UPPER({signalAlias}.component::text) = 'THD' THEN {wideAlias}.cthd_b_pct
  WHEN LOWER({signalAlias}.quantity::text) IN ('current','i') AND UPPER({signalAlias}.phase::text) = 'C' AND UPPER({signalAlias}.component::text) = 'THD' THEN {wideAlias}.cthd_c_pct

  WHEN LOWER({signalAlias}.quantity::text) IN ('voltage','v') AND UPPER({signalAlias}.phase::text) = 'A' AND UPPER({signalAlias}.component::text) = 'THD' THEN {wideAlias}.vthd_a_pct
  WHEN LOWER({signalAlias}.quantity::text) IN ('voltage','v') AND UPPER({signalAlias}.phase::text) = 'B' AND UPPER({signalAlias}.component::text) = 'THD' THEN {wideAlias}.vthd_b_pct
  WHEN LOWER({signalAlias}.quantity::text) IN ('voltage','v') AND UPPER({signalAlias}.phase::text) = 'C' AND UPPER({signalAlias}.component::text) = 'THD' THEN {wideAlias}.vthd_c_pct

  WHEN LOWER({signalAlias}.quantity::text) IN ('frequency','freq') AND UPPER({signalAlias}.component::text) = 'FREQ' THEN {wideAlias}.frequency_hz
  WHEN LOWER({signalAlias}.quantity::text) IN ('frequency','freq') AND UPPER({signalAlias}.component::text) = 'DFREQ' THEN {wideAlias}.delta_freq_hz

  WHEN LOWER({signalAlias}.quantity::text) IN ('digital','d')
       AND UPPER({signalAlias}.component::text) = 'DIG'
       AND UPPER(COALESCE({signalAlias}.name, '')) = 'CFDS'
    THEN {wideAlias}.cfds

  ELSE NULL
END";
    }

    public static string BuildNotNullGuardSql(string signalAlias, string wideAlias)
    {
        return $@"({BuildValueCaseSql(signalAlias, wideAlias)}) IS NOT NULL";
    }

    public static bool IsSupportedColumn(string columnName)
    {
        if (string.IsNullOrWhiteSpace(columnName))
            return false;

        return SupportedColumns.Contains(columnName.Trim());
    }
}
