using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.Eventing.Reader;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using OpenPlot.Ingestor.Gsf.Data;
using OpenPlot.Ingestor.Gsf.Utils;
using static System.Net.WebRequestMethods;


namespace OpenPlot.Ingestor.Gsf.Repository
{
    public class MeasurementHistorian : Database, IMeasurementDb
    {
        // Variável estática para controlar o incremento

        readonly string connectionString;
        readonly string connectionStringHTTPS;
        public MeasurementHistorian(string ip, int port, string user, string pass) : base(ip, user, pass)
        {
            connectionString = "http://" + ip + ":" + port + "/historian/timeseriesdata/read/historic/";

            if (ip.ToLower().Contains("https"))
            {
                //if (port == 6152)
                //{
                //    connectionStringHTTPS = "https://" + ip + "/historian/timeseriesdata/read/historic/"; //Talvez precisemos mudar isso aqui no futuro
                //}

                connectionString = ip.Replace("https://", "http://") + ":" + port + "/historian/timeseriesdata/read/historic/";

                if (port == 6156)
                {
                    connectionStringHTTPS = ip + "/historian/timeseriesdata/read/historic/";
                }
            }


        }

        public Dictionary<Channel, ITimeSeries> QueryTerminalSeries(string Id, DateTime start, DateTime finish, List<Channel> measurements, int dataRate, int equipmentRate, bool downloadStat = false)
        {
            Console.WriteLine("QueryTerminal");
            Console.Out.Flush();
            Dictionary<Channel, ITimeSeries> result = new Dictionary<Channel, ITimeSeries>();

            Dictionary<string, Channel> builtMeasurements = new Dictionary<string, Channel>();

            bool sucessoBusca = false;

            foreach (Channel channel in measurements)
                builtMeasurements[channel.Id.ToString()] = channel;

            if (downloadStat)
                builtMeasurements["MISSING"] = Channel.MISSING;

            string path = BuildPath(start, finish, GetChannels(measurements));

            if (!string.IsNullOrEmpty(connectionStringHTTPS))
            {
                string pathHTTPS = BuildPathHTTPS(start, finish, GetChannels(measurements));

                try
                {
                    using (var httpClient = new HttpClient())
                    {
                        var queryTask = httpClient.GetStringAsync(pathHTTPS);

                        result = ParseJson(ref queryTask, builtMeasurements, dataRate, downloadStat);

                    }

                    sucessoBusca = true;

                }
                catch (Exception e)
                {
                    try
                    {
                        using (var httpClient = new HttpClient())
                        {
                            var queryTask = httpClient.GetStringAsync(path);

                            result = ParseJson(ref queryTask, builtMeasurements, dataRate, downloadStat);

                        }


                    }
                    catch (Exception _ex)
                    {
                        if (e.InnerException != null)
                        {
                            if (e.InnerException is HttpRequestException)
                            {
                                if (e.InnerException.HResult == -2147467259)
                                    throw new InvalidConnectionException(Ip);
                                else if (e.InnerException.HResult == -2146233088)
                                    throw new InvalidQueryException(InvalidQueryException.BAD_HIST_QUERY);
                            }
                            if (e.InnerException is TaskCanceledException && e.InnerException.HResult == -2146233029)
                            {
                                throw new QueryTimeoutException();
                            }
                            else
                            {
                                throw new InvalidQueryException(e.InnerException.Message);
                            }
                        }
                        else
                            throw new InvalidQueryException(e.Message);
                    }
                }
            }

            if (string.IsNullOrEmpty(connectionStringHTTPS))
            {


                try
                {
                    using (var httpClient = new HttpClient())
                    {
                        var queryTask = httpClient.GetStringAsync(path);

                        result = ParseJson(ref queryTask, builtMeasurements, dataRate, downloadStat);

                    }


                }
                catch (Exception e)
                {
                    if (e.InnerException != null)
                    {
                        if (e.InnerException is HttpRequestException)
                        {
                            if (e.InnerException.HResult == -2147467259)
                                throw new InvalidConnectionException(Ip);
                            else if (e.InnerException.HResult == -2146233088)
                                throw new InvalidQueryException(InvalidQueryException.BAD_HIST_QUERY);
                        }
                        if (e.InnerException is TaskCanceledException && e.InnerException.HResult == -2146233029)
                        {
                            throw new QueryTimeoutException();
                        }
                        else
                        {
                            throw new InvalidQueryException(e.InnerException.Message);
                        }
                    }
                    else
                        throw new InvalidQueryException(e.Message);
                }
            }



            return result;

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
                if (seen.Add(ch.Id)) // só entra 1x por HistorianID
                {
                    if (sb.Length > 0) sb.Append(',');
                    sb.Append(ch.Id);
                }
            }
            return sb.ToString();
        }


        private static Dictionary<Channel, ITimeSeries> ParseJson(
     ref Task<string> query,
     Dictionary<string, Channel> measurements,
     double framesPerSecond,
     bool downloadStat)
        {
            query.Wait();

            string jsonData = query.Result;

            if (jsonData == "{\"TimeSeriesDataPoints\":[]}")
                throw new InvalidQueryException(InvalidQueryException.EMPTY);

            // ------------------------------------------------------------
            // 1) Converte measurements (string -> int) UMA VEZ
            // ------------------------------------------------------------
            var measurementsById = new Dictionary<int, Channel>(measurements.Count);
            foreach (var kv in measurements)
            {
                if (int.TryParse(kv.Key, NumberStyles.Integer, CultureInfo.InvariantCulture, out int id))
                    measurementsById[id] = kv.Value;
            }

            // ------------------------------------------------------------
            // 2) Inicializa séries
            // ------------------------------------------------------------
            var series = new Dictionary<Channel, ITimeSeries>(measurementsById.Count + (downloadStat ? 1 : 0));
            foreach (var ch in measurementsById.Values)
                series[ch] = new TimeSeries();

            if (downloadStat && !series.ContainsKey(Channel.MISSING))
                series[Channel.MISSING] = new TimeSeries();

            bool oneValid = downloadStat;
            bool hasData = false;

            double frameMs = 1000.0 / framesPerSecond;

            // ------------------------------------------------------------
            // 3) Utf8JsonReader (streaming, rápido)
            // ------------------------------------------------------------
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
                        reader.Read(); // StartArray
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

                // --------------------------------------------------------
                // 4) Fecha um objeto { ... } → processa o ponto
                // --------------------------------------------------------
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
                                // se não tiver no dicionário, ignora
                            }
                        }
                    }

                    // reset do estado
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