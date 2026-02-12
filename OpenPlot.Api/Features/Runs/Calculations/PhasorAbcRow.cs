using System;
using System.Collections.Generic;
using System.Linq;
using OpenPlot.Features.Runs.Repositories;

namespace OpenPlot.Features.Runs.Calculations;

public static class PhasorAbcExtractor
{
    public sealed record AbcMagAng(
        List<(DateTime ts, double mag)> VaMag,
        List<(DateTime ts, double mag)> VbMag,
        List<(DateTime ts, double mag)> VcMag,
        List<(DateTime ts, double angDeg)> VaAng,
        List<(DateTime ts, double angDeg)> VbAng,
        List<(DateTime ts, double angDeg)> VcAng
    );

    public static AbcMagAng Extract(IReadOnlyList<PhasorAbcRow> rows)
    {
        var vaMag = new List<(DateTime ts, double mag)>();
        var vbMag = new List<(DateTime ts, double mag)>();
        var vcMag = new List<(DateTime ts, double mag)>();
        var vaAng = new List<(DateTime ts, double angDeg)>();
        var vbAng = new List<(DateTime ts, double angDeg)>();
        var vcAng = new List<(DateTime ts, double angDeg)>();

        foreach (var r in rows)
        {
            var ph = (r.Phase ?? "").Trim().ToUpperInvariant();
            var cp = (r.Component ?? "").Trim().ToUpperInvariant();

            if (ph == "A" && cp == "MAG") vaMag.Add((r.Ts, r.Value));
            else if (ph == "B" && cp == "MAG") vbMag.Add((r.Ts, r.Value));
            else if (ph == "C" && cp == "MAG") vcMag.Add((r.Ts, r.Value));
            else if (ph == "A" && cp == "ANG") vaAng.Add((r.Ts, r.Value));
            else if (ph == "B" && cp == "ANG") vbAng.Add((r.Ts, r.Value));
            else if (ph == "C" && cp == "ANG") vcAng.Add((r.Ts, r.Value));
        }

        vaMag.Sort((a, b) => a.ts.CompareTo(b.ts));
        vbMag.Sort((a, b) => a.ts.CompareTo(b.ts));
        vcMag.Sort((a, b) => a.ts.CompareTo(b.ts));
        vaAng.Sort((a, b) => a.ts.CompareTo(b.ts));
        vbAng.Sort((a, b) => a.ts.CompareTo(b.ts));
        vcAng.Sort((a, b) => a.ts.CompareTo(b.ts));

        return new AbcMagAng(vaMag, vbMag, vcMag, vaAng, vbAng, vcAng);
    }
}
