using System;
using System.Collections.Generic;

namespace OpenPlot.Ingestor.Gsf.Repository
{
    public static class Resolutions
    {
        public static List<int> GetAllResolutions()
        {
            List<int> taxa = new List<int>
            {
                1,
                10,
                25,
                30,
                50,
                60,
                100,
                120,
                200,
                240
            };
            return taxa;
        }

        public static TimeSpan GetInterval(int resolution)
        {
            double auxValue = 1000;
            switch (resolution)
            {
                case 1:
                    return new TimeSpan(TimeSpan.TicksPerMillisecond * 1000);
                case 10:
                    return new TimeSpan((long)(TimeSpan.TicksPerMillisecond * (auxValue / 10)));
                case 25:
                    return new TimeSpan((long)(TimeSpan.TicksPerMillisecond * (auxValue / 25)));
                case 30:
                    return new TimeSpan((long)(TimeSpan.TicksPerMillisecond * (auxValue / 30)));
                case 50:
                    return new TimeSpan((long)(TimeSpan.TicksPerMillisecond * (auxValue / 50)));
                case 60:
                    return new TimeSpan((long)(TimeSpan.TicksPerMillisecond * (auxValue / 60)));
                case 100:
                    return new TimeSpan((long)(TimeSpan.TicksPerMillisecond * (auxValue / 100)));
                case 120:
                    return new TimeSpan((long)(TimeSpan.TicksPerMillisecond * (auxValue / 120)));
                case 200:
                    return new TimeSpan((long)(TimeSpan.TicksPerMillisecond * (auxValue / 200)));
                case 240:
                    return new TimeSpan((long)(TimeSpan.TicksPerMillisecond * (auxValue / 240)));
                default:
                    throw new Exception("Unknown resolution");
            }
        }
    }
}