using System;
using System.Collections.Generic;
using System.Linq;
using SnapDB.Snap;
using SnapDB.Snap.Filters;
using SnapDB.Snap.Services;
using SnapDB.Snap.Services.Reader;
using Newtonsoft.Json;
using OpenPlot.Ingestor.Gsf.Snap;

namespace OpenPlot.Ingestor.Gsf.Repository
{
    public class HistorianDataFetcher
    {
        public class MeasurementData
        {
            public ulong HistorianID { get; set; }
            public DateTime Time { get; set; }
            public float Value { get; set; }
            public int Quality { get; set; }

            public MeasurementData(ulong id, DateTime time, float value, int quality)
            {
                HistorianID = id;
                Time = time;
                Value = value;
                Quality = quality == 0 ? 29 : quality;
            }
        }

        public class TimeSeriesDataWrapper
        {
            public List<MeasurementData> TimeSeriesDataPoints { get; set; }
        }

        public IEnumerable<MeasurementData> FetchHistorianData(string historianServer, string instanceName, DateTime startTime, DateTime stopTime, int dataRate, int equipmentRate, string measurementIDs = null, TimeSpan interval = default(TimeSpan))
        {
            int DefaultHistorianPort = 38402;

            if (string.IsNullOrEmpty(historianServer))
                throw new ArgumentNullException(nameof(historianServer), "Missing historian server parameter");

            if (string.IsNullOrEmpty(instanceName))
                throw new ArgumentNullException(nameof(instanceName), "Missing historian instance name parameter");

            if (startTime > stopTime)
                throw new ArgumentException("Invalid time range specified", nameof(startTime));

            string[] parts = historianServer.Split(':');
            string hostName = parts[0];
            int port;

            if (parts.Length < 2 || !int.TryParse(parts[1], out port))
                port = DefaultHistorianPort;

            using (SnapClient client = SnapClient.Connect(hostName, port))
            using (ClientDatabaseBase<HistorianKey, HistorianValue> reader = client.GetDatabase<HistorianKey, HistorianValue>(instanceName))
            {
                SeekFilterBase<HistorianKey> timeFilter;

                ulong intervalTicks = (ulong)interval.Ticks;

                if (dataRate != equipmentRate)
                    timeFilter = TimestampSeekFilter.CreateFromIntervalData<HistorianKey>((ulong)startTime.Ticks, (ulong)stopTime.Ticks - intervalTicks, intervalTicks, (ulong)TimeSpan.TicksPerMillisecond * 2);
                else
                    timeFilter = TimestampSeekFilter.CreateFromRange<HistorianKey>((ulong)startTime.Ticks, (ulong)stopTime.Ticks);

                MatchFilterBase<HistorianKey, HistorianValue> pointFilter = null;
                HistorianKey key = new HistorianKey();
                HistorianValue value = new HistorianValue();

                if (!string.IsNullOrEmpty(measurementIDs))
                    pointFilter = PointIDMatchFilter.CreateFromList<HistorianKey, HistorianValue>(measurementIDs.Split(',').Select(ulong.Parse));

                TreeStream<HistorianKey, HistorianValue> stream = reader.Read(SortedTreeEngineReaderOptions.Default, timeFilter, pointFilter);

                while (stream.Read(key, value))
                {
                    int qualityFlags = (int)value.Value3;
                    yield return new MeasurementData(key.PointID, key.TimestampAsDate, value.AsSingle, (int)qualityFlags);
                }
            }
        }

        public string FetchHistorianDataAsString(string historianServer, string instanceName, DateTime startTime, DateTime stopTime, int dataRate, int equipmentRate, string measurementIDs = null, TimeSpan interval = default(TimeSpan))
        {
            IEnumerable<MeasurementData> historianData = FetchHistorianData(historianServer, "PPA", startTime, stopTime, dataRate, equipmentRate, measurementIDs, interval);

            var dataWrapper = new TimeSeriesDataWrapper { TimeSeriesDataPoints = historianData.ToList() };

            JsonSerializerSettings settings = new JsonSerializerSettings
            {
                DateFormatString = "yyyy-MM-dd HH:mm:ss.fff",
                Formatting = Formatting.None
            };

            string jsonData = JsonConvert.SerializeObject(dataWrapper, settings);
            jsonData = jsonData.Replace("\r\n", "").Replace("\"Time\":\"", "\"Time\":\" ");
            return jsonData;
        }
    }
}