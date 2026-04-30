using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using OpenPlot.Ingestor.Gsf.Data;
using OpenPlot.Ingestor.Gsf.Utils;

namespace OpenPlot.Ingestor.Gsf.Repository
{
    public class MeasurementHistorian : Database, IMeasurementDb
    {
        private static readonly HttpClient httpClient = new HttpClient();

        readonly string connectionString;
        readonly string connectionStringHTTPS;

        public MeasurementHistorian(string ip, int port, string user, string pass) : base(ip, user, pass)
        {
            connectionString = "http://" + ip + ":" + port + "/historian/timeseriesdata/read/historic/";

            if (ip.ToLower().Contains("https"))
            {
                connectionString = ip.Replace("https://", "http://") + ":" + port + "/historian/timeseriesdata/read/historic/";

                if (port == 6156)
                    connectionStringHTTPS = ip + "/historian/timeseriesdata/read/historic/";
            }
        }

        public Dictionary<Channel, ITimeSeries> QueryTerminalSeries(string Id, DateTime start, DateTime finish, List<Channel> measurements, int dataRate, int equipmentRate, bool downloadStat = false)
        {
            Console.WriteLine("QueryTerminal");
            Console.Out.Flush();

            var builtMeasurements = new Dictionary<int, Channel>();

            foreach (Channel channel in measurements)
                builtMeasurements[channel.Id] = channel;

            string channels = GetChannels(measurements);
            string path = BuildPath(start, finish, channels);

            if (!string.IsNullOrEmpty(connectionStringHTTPS))
            {
                string pathHTTPS = BuildPathHTTPS(start, finish, channels);

                try
                {
                    string json = httpClient.GetStringAsync(pathHTTPS).GetAwaiter().GetResult();
                    return ParseJson(json, builtMeasurements, dataRate, downloadStat);
                }
                catch (Exception)
                {
                    // HTTPS falhou — tenta HTTP como fallback
                }
            }

            try
            {
                string json = httpClient.GetStringAsync(path).GetAwaiter().GetResult();
                return ParseJson(json, builtMeasurements, dataRate, downloadStat);
            }
            catch (Exception e)
            {
                HandleQueryException(e);
            }

            throw new InvalidQueryException(InvalidQueryException.BAD_HIST_QUERY);
        }

        private void HandleQueryException(Exception e)
        {
            // Unwrap AggregateException caso o chamador ainda use .Wait() em algum ponto
            Exception actual = e is AggregateException ae ? (ae.InnerException ?? e) : e;

            if (actual is TaskCanceledException || actual is OperationCanceledException)
                throw new QueryTimeoutException();

            if (actual is HttpRequestException httpEx)
            {
                if (httpEx.InnerException is SocketException)
                    throw new InvalidConnectionException(Ip);

                throw new InvalidQueryException(InvalidQueryException.BAD_HIST_QUERY);
            }

            throw new InvalidQueryException(actual.Message);
        }

        private string BuildPath(DateTime start, DateTime finish, string channels)
        {
            return connectionString + channels + "/" + start.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "/" + finish.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "/json";
        }

        private string BuildPathHTTPS(DateTime start, DateTime finish, string channels)
        {
            return connectionStringHTTPS + channels + "/" + start.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "/" + finish.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "/json";
        }

        private static string GetChannels(List<Channel> measurements)
        {
            var seen = new HashSet<int>();
            var sb = new StringBuilder();

            foreach (var ch in measurements)
            {
                if (seen.Add(ch.Id))
                {
                    if (sb.Length > 0)
                        sb.Append(',');

                    sb.Append(ch.Id);
                }
            }

            return sb.ToString();
        }

        private static Dictionary<Channel, ITimeSeries> ParseJson(
            string jsonData,
            Dictionary<int, Channel> measurementsById,
            double framesPerSecond,
            bool downloadStat)
        {
            if (jsonData == "{\"TimeSeriesDataPoints\":[]}")
                throw new InvalidQueryException(InvalidQueryException.EMPTY);

            var series = new Dictionary<Channel, ITimeSeries>(measurementsById.Count + (downloadStat ? 1 : 0));
            foreach (var ch in measurementsById.Values)
                series[ch] = new TimeSeries();

            if (downloadStat && !series.ContainsKey(Channel.MISSING))
                series[Channel.MISSING] = new TimeSeries();

            bool oneValid = downloadStat;
            bool hasData = false;
            double frameMs = 1000.0 / framesPerSecond;

            byte[] utf8 = Encoding.UTF8.GetBytes(jsonData);
            var reader = new Utf8JsonReader(utf8, isFinalBlock: true, state: default);

            int historianId = 0;
            DateTime measureTime = default;
            double value = 0;
            int qualityCode = 0;
            bool gotId = false, gotTime = false, gotValue = false, gotQuality = false;

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.PropertyName)
                {
                    if (reader.ValueTextEquals("TimeSeriesDataPoints"))
                    {
                        reader.Read();
                        continue;
                    }

                    if (reader.ValueTextEquals("HistorianID"))
                    {
                        reader.Read();
                        historianId = reader.GetInt32();
                        gotId = true;
                        continue;
                    }

                    if (reader.ValueTextEquals("Time"))
                    {
                        reader.Read();
                        string t = reader.GetString()!;
                        t = t.TrimStart();

                        gotTime = DateTime.TryParseExact(
                            t,
                            "yyyy-MM-dd HH:mm:ss.fff",
                            CultureInfo.InvariantCulture,
                            DateTimeStyles.None,
                            out measureTime);

                        continue;
                    }

                    if (reader.ValueTextEquals("Value"))
                    {
                        reader.Read();
                        value = reader.GetDouble();
                        gotValue = true;
                        continue;
                    }

                    if (reader.ValueTextEquals("Quality"))
                    {
                        reader.Read();
                        qualityCode = reader.GetInt32();
                        gotQuality = true;
                        continue;
                    }
                }

                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    if (gotId && gotTime && gotValue && gotQuality)
                    {
                        double timeModulus = measureTime.Millisecond % frameMs;
                        double timeModulusDiff = Math.Abs(frameMs - timeModulus);

                        if (timeModulus < 2 || timeModulusDiff < 2)
                        {
                            bool qualityOk = qualityCode == 29;

                            if (qualityOk || downloadStat)
                            {
                                oneValid |= qualityOk;
                                hasData = true;

                                if (measurementsById.TryGetValue(historianId, out Channel key))
                                {
                                    double time = TimeUtils.OaDate(measureTime);
                                    series[key].Add(time, value);

                                    if (!qualityOk && downloadStat)
                                        series[Channel.MISSING].Add(time, 2);
                                }
                            }
                        }
                    }

                    historianId = 0;
                    measureTime = default;
                    value = 0;
                    qualityCode = 0;
                    gotId = gotTime = gotValue = gotQuality = false;
                }
            }

            if (!hasData)
                throw new InvalidQueryException(InvalidQueryException.EMPTY);
            if (!oneValid)
                throw new InvalidQueryException(InvalidQueryException.NO_VALID);

            return series;
        }
    }
}