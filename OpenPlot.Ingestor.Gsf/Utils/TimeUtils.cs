
using System;
using System.Collections.Generic;
using System.Text;

namespace OpenPlot.Ingestor.Gsf.Utils
{
    public static class TimeUtils
    {
        private static readonly DateTime OA_START = new DateTime(1899, 12, 30);
        private static readonly DateTime UNIX_START = new DateTime(1970, 1, 1);
        public static readonly double OA_MILLISECOND = 1.0 / 86400000;

        public static IEnumerable<DateTime> EachDay(DateTime start, DateTime finish)
        {
            for (var day = start.Date; day.Date <= finish.Date; day = day.AddDays(1))
                yield return day;
        }

        public static long Soc(DateTime time)
        {
            return (time.Ticks - 621355968000000000) / 10000000;
        }

        public static DateTime FromSoc(long soc, int fracSoc, int socMax)
        {
            return UNIX_START.AddSeconds(soc).AddMilliseconds(1000 * fracSoc / socMax);
        }

        public static double OaDate(DateTime time)
        {
            return (time.Date - OA_START).TotalDays + (time - time.Date).TotalSeconds / 86400.0;
        }

        public static double SocDiff(long startSoc, int startFracSoc, long endSoc, int endFracSoc, int socMax)
        {
            var secDiff = endSoc - startSoc;
            var fracDiff = 1000.0 * (endFracSoc - startFracSoc) / socMax;
            return secDiff / (86400.0) + fracDiff / (86400000.0);
        }

        public static DateTime FromOA(double oaDate)
        {
            return OA_START.AddDays(oaDate);
        }
    }
}
