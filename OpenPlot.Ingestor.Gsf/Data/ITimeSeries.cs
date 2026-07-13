using System;
using System.Collections.Generic;
using System.Text;

namespace OpenPlot.Ingestor.Gsf.Data
{
    public interface ITimeSeries
    {
        int Count { get; }

        void Add(double timestamp, double reading, int quality = 29);
        double Timestamp(int position);
        double Reading(int position);
        int Quality(int position);

        double[] GetReadings();
        double[] GetTimestamps();
        int[] GetQualities();
    }
}
