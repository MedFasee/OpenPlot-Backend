using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using OpenPlot.Ingestor.Gsf.Snap;
using SnapDB.Snap.Filters;
using SnapDB.Snap.Services.Net;
using SnapDB.Snap.Services.Reader;

namespace OpenPlot.Ingestor.Gsf.Repository
{
    public class HistorianDataFetcher
    {
        private const int DefaultHistorianPort = 38402;
        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            PropertyNamingPolicy = null
        };

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
            public List<MeasurementData> TimeSeriesDataPoints { get; set; } = new();
        }

        public IEnumerable<MeasurementData> FetchHistorianData(string historianServer, string instanceName, DateTime startTime, DateTime stopTime, int dataRate, int equipmentRate, string measurementIDs = null, TimeSpan interval = default)
        {
            if (string.IsNullOrWhiteSpace(historianServer))
                throw new ArgumentNullException(nameof(historianServer), "Missing historian server parameter");

            if (startTime > stopTime)
                throw new ArgumentException("Invalid time range specified", nameof(startTime));

            if (string.IsNullOrWhiteSpace(instanceName))
                throw new ArgumentNullException(nameof(instanceName), "Missing historian instance name parameter");

            ParseServer(historianServer, out string host, out int port);
            ulong[] pointIds = ParseMeasurementIds(measurementIDs);

            var settings = new SnapNetworkClientSettings
            {
                ServerNameOrIP = host,
                NetworkPort = port
            };

            using var client = new SnapNetworkClient(settings, null, false);
            using var database = client.GetDatabase<HistorianKey, HistorianValue>(instanceName);
            using var stream = CreateReadStream(database, startTime, stopTime, dataRate, equipmentRate, interval, pointIds);

            var key = new HistorianKey();
            var value = new HistorianValue();

            while (stream.Read(key, value))
            {
                yield return new MeasurementData(
                    key.PointID,
                    new DateTime((long)key.Timestamp, DateTimeKind.Unspecified),
                    value.AsSingle,
                    29);
            }
        }

        public string FetchHistorianDataAsString(string historianServer, string instanceName, DateTime startTime, DateTime stopTime, int dataRate, int equipmentRate, string measurementIDs = null, TimeSpan interval = default)
        {
            var wrapper = new TimeSeriesDataWrapper();

            foreach (MeasurementData point in FetchHistorianData(historianServer, instanceName, startTime, stopTime, dataRate, equipmentRate, measurementIDs, interval))
                wrapper.TimeSeriesDataPoints.Add(point);

            return JsonSerializer.Serialize(wrapper, JsonOptions);
        }

        private static SnapDB.Snap.TreeStream<HistorianKey, HistorianValue> CreateReadStream(
            dynamic database,
            DateTime startTime,
            DateTime stopTime,
            int dataRate,
            int equipmentRate,
            TimeSpan interval,
            IEnumerable<ulong> pointIds)
        {
            var seekFilter = CreateTimestampFilter(startTime, stopTime, dataRate, equipmentRate, interval);
            var matchFilter = pointIds.Any()
                ? PointIDMatchFilter.CreateFromList<HistorianKey, HistorianValue>(pointIds)
                : null;

            return matchFilter is null
                ? database.Read(SortedTreeEngineReaderOptions.Default, seekFilter, null)
                : database.Read(SortedTreeEngineReaderOptions.Default, seekFilter, matchFilter);
        }

        private static SeekFilterBase<HistorianKey> CreateTimestampFilter(DateTime startTime, DateTime stopTime, int dataRate, int equipmentRate, TimeSpan interval)
        {
            bool useIntervalData = interval > TimeSpan.Zero && dataRate > 0 && equipmentRate > 0 && dataRate != equipmentRate;

            return useIntervalData
                ? TimestampSeekFilter.CreateFromIntervalData<HistorianKey>(startTime, stopTime, interval, interval)
                : TimestampSeekFilter.CreateFromRange<HistorianKey>(startTime, stopTime);
        }

        private static ulong[] ParseMeasurementIds(string measurementIDs)
        {
            if (string.IsNullOrWhiteSpace(measurementIDs))
                return Array.Empty<ulong>();

            return measurementIDs
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(id => ulong.Parse(id, CultureInfo.InvariantCulture))
                .Distinct()
                .ToArray();
        }

        private static void ParseServer(string historianServer, out string host, out int port)
        {
            string[] parts = historianServer.Split(':', 2, StringSplitOptions.TrimEntries);
            host = parts[0];
            port = parts.Length == 2 && int.TryParse(parts[1], out int parsedPort)
                ? parsedPort
                : DefaultHistorianPort;
        }
    }
}