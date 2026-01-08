
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
    public class MeasurementHistorian2 : Database, IMeasurementDb
    {
        readonly string _ip;
        public MeasurementHistorian2(string ip, string user, string pass) : base(ip, user, pass)
        {
            _ip = ip;
        }

        public Dictionary<Channel, ITimeSeries> QueryTerminalSeries(string Id, DateTime start, DateTime finish, List<Channel> measurements, int dataRate, int equipmentRate, bool downloadStat = false)
        {
            Console.WriteLine("QueryTerminal");
            Console.Out.Flush();
            Dictionary<Channel, ITimeSeries> result;
            Dictionary<string, Channel> builtMeasurements = new Dictionary<string, Channel>();

            foreach (Channel channel in measurements)
                builtMeasurements[channel.Id.ToString()] = channel;

            if (downloadStat)
                builtMeasurements["MISSING"] = Channel.MISSING;
            var channels = GetChannels(measurements);
            Console.WriteLine($"[ids enviados] {channels}");

            var interval = Resolutions.GetInterval(dataRate);

            try
            {
                var data = new HistorianDataFetcher();

                var queryTask = data.FetchHistorianDataAsString(_ip, "PPA", start, finish, dataRate, equipmentRate, channels, interval);

                result = ParseJson(queryTask, builtMeasurements, dataRate, downloadStat);

            }
            catch (Exception e)
            {
                if (e.InnerException != null)
                {
                    if (e.InnerException is SocketException)
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
                        {
                            Console.WriteLine("=================================");
                            Console.WriteLine("[QueryTerminalSeries] Falhou");
                            Console.WriteLine(e.ToString());
                            if (e.InnerException != null)
                                Console.WriteLine("Inner: " + e.InnerException.ToString());
                            Console.WriteLine("=================================");

                            throw new InvalidQueryException(e.InnerException?.Message ?? e.Message);
                        }

                    }
                }
                else
                {
                    Console.WriteLine("=================================");
                    Console.WriteLine("[QueryTerminalSeries] Falhou");
                    Console.WriteLine(e.ToString());
                    if (e.InnerException != null)
                        Console.WriteLine("Inner: " + e.InnerException.ToString());
                    Console.WriteLine("=================================");

                    throw new InvalidQueryException(e.InnerException?.Message ?? e.Message);
                }

            }

            return result;

        }

        private static string GetChannels(List<Channel> measurements)
        {
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < measurements.Count; i++)
            {
                if (i > 0)
                    result.Append(",");
                result.Append(measurements[i].Id);
            }

            return result.ToString();
        }


        private static Dictionary<Channel, ITimeSeries> ParseJson(
    string jsonData,
    Dictionary<string, Channel> measurements, // <- assinatura original
    double framesPerSecond,
    bool downloadStat)
        {
            if (jsonData == "{\"TimeSeriesDataPoints\":[]}")
                throw new InvalidQueryException(InvalidQueryException.EMPTY);

            // ------------------------------------------------------------
            // 1) Converte measurements (string -> int) UMA VEZ
            // ------------------------------------------------------------
            var measurementsById = new Dictionary<int, Channel>(measurements.Count);
            foreach (var kv in measurements)
            {
                // esperado: "843", "844", ...
                if (int.TryParse(kv.Key, NumberStyles.Integer, CultureInfo.InvariantCulture, out int id))
                    measurementsById[id] = kv.Value;
            }

            // ------------------------------------------------------------
            // 2) Inicializa séries
            // ------------------------------------------------------------
            var series = new Dictionary<Channel, ITimeSeries>(measurementsById.Count + 1);
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
                        // vem com espaço no início
                        string t = reader.GetString()!.TrimStart();

                        // formato: "yyyy-MM-dd HH:mm:ss.fff"
                        if (DateTime.TryParseExact(
                                t,
                                "yyyy-MM-dd HH:mm:ss.fff",
                                CultureInfo.InvariantCulture,
                                DateTimeStyles.None,
                                out measureTime))
                        {
                            gotTime = true;
                        }
                        else
                        {
                            gotTime = false;
                        }
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

            return series;
        }
    }
}
