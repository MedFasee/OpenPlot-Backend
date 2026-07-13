using OpenPlot.Ingestor.Gsf.Data;
using System;
using System.Collections.Generic;
using System.Linq;

namespace OpenPlot.Ingestor.Gsf.Data
{
    public class TimeSeries : ITimeSeries
    {

        private List<double> Readings { get; set; }
        private List<double> Timestamps { get; set; }
        private List<int> Qualities { get; set; }
        public int Count => Readings.Count;

        public TimeSeries()
        {
            Readings = new List<double>(3700);
            Timestamps = new List<double>(3700);
            Qualities = new List<int>(3700);
        }

        public TimeSeries(List<double> timestamp, List<double> readings, List<int>? qualities = null)
        {
            if (readings == null)
                throw new ArgumentNullException(nameof(readings), "The reading vector can't be null!");
            if (timestamp == null)
                throw new ArgumentNullException(nameof(timestamp), "The timestamp vector can't be null!");
            if (readings.Count != timestamp.Count)
                throw new ArgumentException("The readings and timestamp arrays must have same length!");

            Readings = readings;
            Timestamps = timestamp;

            if (qualities is not null)
            {
                if (qualities.Count != readings.Count)
                    throw new ArgumentException("The qualities array must have same length as readings!");
                Qualities = qualities;
            }
            else
            {
                Qualities = Enumerable.Repeat(29, readings.Count).ToList();
            }
        }

        public void Add(double timestamp, double reading, int quality = 29)
        {
            Timestamps.Add(timestamp);
            Readings.Add(reading);
            Qualities.Add(quality);
        }

        public double[] GetReadings()
        {
            return Readings.ToArray();
        }

        public double[] GetTimestamps()
        {
            return Timestamps.ToArray();
        }

        public int[] GetQualities()
        {
            return Qualities.ToArray();
        }

        public double Timestamp(int position)
        {
            return Timestamps[position];
        }

        public double Reading(int position)
        {
            return Readings[position];
        }

        public int Quality(int position)
        {
            return Qualities[position];
        }

    }
}
