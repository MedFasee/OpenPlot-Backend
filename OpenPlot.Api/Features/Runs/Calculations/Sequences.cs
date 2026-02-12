using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;

namespace OpenPlot.Features.Runs.Calculations;

public static class Sequences
{
    /// <summary>
    /// MedPlot-style: calcula magnitude da sequência (pos/neg/zero)
    /// com alinhamento por timestamp (tolerância 3ms).
    /// Listas DEVEM estar ordenadas por ts.
    /// </summary>
    public static IReadOnlyList<(DateTime ts, double mag)> ComputeSequenceMagnitudeMedPlot(
        List<(DateTime ts, double mag)> vaMod,
        List<(DateTime ts, double mag)> vbMod,
        List<(DateTime ts, double mag)> vcMod,
        List<(DateTime ts, double angDeg)> vaAng,
        List<(DateTime ts, double angDeg)> vbAng,
        List<(DateTime ts, double angDeg)> vcAng,
        string seq) // "pos"|"neg"|"zero"
    {
        var result = new List<(DateTime ts, double mag)>();

        if (vaMod.Count == 0 || vbMod.Count == 0 || vcMod.Count == 0 ||
            vaAng.Count == 0 || vbAng.Count == 0 || vcAng.Count == 0)
            return result;

        var tolerance = TimeSpan.FromMilliseconds(3);

        int ia = 0, ib = 0, ic = 0;

        const double Deg2Rad = Math.PI / 180.0;
        Complex a = Complex.FromPolarCoordinates(1.0, 120.0 * Deg2Rad);
        Complex a2 = Complex.FromPolarCoordinates(1.0, 240.0 * Deg2Rad);

        while (ia < vaMod.Count && ib < vbMod.Count && ic < vcMod.Count)
        {
            var tA = vaMod[ia].ts;
            var tB = vbMod[ib].ts;
            var tC = vcMod[ic].ts;

            var maxTime = tA;
            if (tB > maxTime) maxTime = tB;
            if (tC > maxTime) maxTime = tC;

            while (ia < vaMod.Count &&
                   vaMod[ia].ts < maxTime &&
                   (maxTime - vaMod[ia].ts) > tolerance)
                ia++;

            while (ib < vbMod.Count &&
                   vbMod[ib].ts < maxTime &&
                   (maxTime - vbMod[ib].ts) > tolerance)
                ib++;

            while (ic < vcMod.Count &&
                   vcMod[ic].ts < maxTime &&
                   (maxTime - vcMod[ic].ts) > tolerance)
                ic++;

            if (ia >= vaMod.Count || ib >= vbMod.Count || ic >= vcMod.Count)
                break;

            tA = vaMod[ia].ts;
            tB = vbMod[ib].ts;
            tC = vcMod[ic].ts;

            if (Math.Abs((tA - maxTime).TotalMilliseconds) > 3 ||
                Math.Abs((tB - maxTime).TotalMilliseconds) > 3 ||
                Math.Abs((tC - maxTime).TotalMilliseconds) > 3)
            {
                var minTime = tA;
                if (tB < minTime) minTime = tB;
                if (tC < minTime) minTime = tC;

                if (minTime == tA && ia < vaMod.Count) ia++;
                else if (minTime == tB && ib < vbMod.Count) ib++;
                else if (minTime == tC && ic < vcMod.Count) ic++;

                continue;
            }

            double vaM = vaMod[ia].mag;
            double vbM = vbMod[ib].mag;
            double vcM = vcMod[ic].mag;

            double vaDeg = vaAng[ia].angDeg;
            double vbDeg = vbAng[ib].angDeg;
            double vcDeg = vcAng[ic].angDeg;

            double thA = vaDeg * Deg2Rad;
            double thB = vbDeg * Deg2Rad;
            double thC = vcDeg * Deg2Rad;

            Complex Va = Complex.FromPolarCoordinates(vaM, thA);
            Complex Vb = Complex.FromPolarCoordinates(vbM, thB);
            Complex Vc = Complex.FromPolarCoordinates(vcM, thC);

            Complex Vseq = seq switch
            {
                "pos" => (Va + a * Vb + a2 * Vc) / 3.0,
                "neg" => (Va + a2 * Vb + a * Vc) / 3.0,
                "zero" => (Va + Vb + Vc) / 3.0,
                _ => throw new ArgumentException("seq deve ser: pos | neg | zero")
            };

            result.Add((maxTime, Vseq.Magnitude));

            ia++; ib++; ic++;
        }

        return result;
    }

    public static IReadOnlyList<(DateTime ts, double angDeg)> ComputeSequenceAngleMedPlot(
        List<(DateTime ts, double mag)> vaMod,
        List<(DateTime ts, double mag)> vbMod,
        List<(DateTime ts, double mag)> vcMod,
        List<(DateTime ts, double angDeg)> vaAng,
        List<(DateTime ts, double angDeg)> vbAng,
        List<(DateTime ts, double angDeg)> vcAng,
        string seq // "pos"|"neg"|"zero"
    )
    {
        var result = new List<(DateTime ts, double angDeg)>();

        if (vaMod.Count == 0 || vbMod.Count == 0 || vcMod.Count == 0 ||
            vaAng.Count == 0 || vbAng.Count == 0 || vcAng.Count == 0)
            return result;

        var tolerance = TimeSpan.FromMilliseconds(3);

        int ia = 0, ib = 0, ic = 0;

        const double Deg2Rad = Math.PI / 180.0;
        const double Rad2Deg = 180.0 / Math.PI;

        Complex a = Complex.FromPolarCoordinates(1.0, 120.0 * Deg2Rad);
        Complex a2 = Complex.FromPolarCoordinates(1.0, 240.0 * Deg2Rad);

        while (ia < vaMod.Count && ib < vbMod.Count && ic < vcMod.Count)
        {
            var tA = vaMod[ia].ts;
            var tB = vbMod[ib].ts;
            var tC = vcMod[ic].ts;

            var maxTime = tA;
            if (tB > maxTime) maxTime = tB;
            if (tC > maxTime) maxTime = tC;

            while (ia < vaMod.Count && vaMod[ia].ts < maxTime && (maxTime - vaMod[ia].ts) > tolerance) ia++;
            while (ib < vbMod.Count && vbMod[ib].ts < maxTime && (maxTime - vbMod[ib].ts) > tolerance) ib++;
            while (ic < vcMod.Count && vcMod[ic].ts < maxTime && (maxTime - vcMod[ic].ts) > tolerance) ic++;

            if (ia >= vaMod.Count || ib >= vbMod.Count || ic >= vcMod.Count)
                break;

            tA = vaMod[ia].ts;
            tB = vbMod[ib].ts;
            tC = vcMod[ic].ts;

            if (Math.Abs((tA - maxTime).TotalMilliseconds) > 3 ||
                Math.Abs((tB - maxTime).TotalMilliseconds) > 3 ||
                Math.Abs((tC - maxTime).TotalMilliseconds) > 3)
            {
                var minTime = tA;
                if (tB < minTime) minTime = tB;
                if (tC < minTime) minTime = tC;

                if (minTime == tA && ia < vaMod.Count) ia++;
                else if (minTime == tB && ib < vbMod.Count) ib++;
                else if (minTime == tC && ic < vcMod.Count) ic++;

                continue;
            }

            double vaM = vaMod[ia].mag;
            double vbM = vbMod[ib].mag;
            double vcM = vcMod[ic].mag;

            double vaDeg = vaAng[ia].angDeg;
            double vbDeg = vbAng[ib].angDeg;
            double vcDeg = vcAng[ic].angDeg;

            double thA = vaDeg * Deg2Rad;
            double thB = vbDeg * Deg2Rad;
            double thC = vcDeg * Deg2Rad;

            Complex Va = Complex.FromPolarCoordinates(vaM, thA);
            Complex Vb = Complex.FromPolarCoordinates(vbM, thB);
            Complex Vc = Complex.FromPolarCoordinates(vcM, thC);

            Complex Vseq = seq switch
            {
                "pos" => (Va + a * Vb + a2 * Vc) / 3.0,
                "neg" => (Va + a2 * Vb + a * Vc) / 3.0,
                "zero" => (Va + Vb + Vc) / 3.0,
                _ => throw new ArgumentException("seq deve ser: pos | neg | zero")
            };

            var ang = Vseq.Phase * Rad2Deg; // (-180, +180]
            result.Add((maxTime, ang));

            ia++; ib++; ic++;
        }

        return result;
    }

    public static double Wrap180(double difDeg)
    {
        if (difDeg > 180.0) return difDeg - 360.0;
        if (difDeg < -180.0) return difDeg + 360.0;
        return difDeg;
    }
}
