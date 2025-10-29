using Medi.Ingestor.Gsf.Data;
using System;
using System.Collections.Generic;

namespace Medi.Ingestor.Gsf.Data
{
    public class TimeSeries : ITimeSeries
    {

        private List<double> Readings { get; set; }
        private List<double> Timestamps { get; set; }
        public int Count => Readings.Count;

        public TimeSeries()
        {
            Readings = new List<double>(3700);
            Timestamps = new List<double>(3700);
        }

        public TimeSeries(List<double> timestamp, List<double> readings)
        {
            if (readings == null)
                throw new ArgumentNullException(nameof(readings), "The reading vector can't be null!");
            if (timestamp == null)
                throw new ArgumentNullException(nameof(timestamp), "The timestamp vector can't be null!");
            if (readings.Count != timestamp.Count)
                throw new ArgumentException("The readings and timestamp arrays must have same length!");
            Readings = readings;
            Timestamps = timestamp;
        }

        public void Add(double timestamp, double reading)
        {
            Timestamps.Add(timestamp);
            Readings.Add(reading);
        }

        public double[] GetReadings()
        {
            double[] result = null;
            if (Readings != null)
                result = Readings.ToArray();

            return result;
        }

        public double[] GetTimestamps()
        {
            double[] result = null;
            if (Timestamps != null)
                result = Timestamps.ToArray();

            return result;
        }

        public double Timestamp(int position)
        {
            return Timestamps[position];
        }

        public double Reading(int position)
        {
            return Readings[position];
        }

    }
}
