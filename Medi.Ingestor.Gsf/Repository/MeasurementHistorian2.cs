
using Medi.Ingestor.Gsf.Data;
using Medi.Ingestor.Gsf.Utils;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Medi.Ingestor.Gsf.Repository
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
            Console.WriteLine("QueryTarminal");
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


        private static Dictionary<Channel, ITimeSeries> ParseJson(string jsonData, Dictionary<string, Channel> measurements, double framesPerSecond, bool downloadStat)
        {
            if (jsonData == "{\"TimeSeriesDataPoints\":[]}")
                throw new InvalidQueryException(InvalidQueryException.EMPTY);

            Dictionary<Channel, ITimeSeries> series = new Dictionary<Channel, ITimeSeries>();
            bool oneValid = downloadStat;
            bool hasData = false;

            foreach (KeyValuePair<string, Channel> pair in measurements)
                series.Add(pair.Value, new TimeSeries());

            int rowSize = 0;
            int rowStart = 26;

            while (rowStart < jsonData.Length)
            {
                rowSize = jsonData.IndexOf("}", rowStart);
                rowSize = rowSize == -1 ? jsonData.Length - 3 : rowSize;

                string[] fields = jsonData.Substring(rowStart, rowSize - rowStart).Replace("\"", string.Empty)
                    .Replace("HistorianID:", string.Empty)
                    .Replace("Time:", string.Empty)
                    .Replace("Value:", string.Empty)
                    .Replace("Quality:", string.Empty).Split(',');

                DateTime measureTime = DateTime.Parse(fields[1]);

                double timeModulus = measureTime.Millisecond % (1000 / framesPerSecond);
                double timeModulusDiff = Math.Abs((1000 / framesPerSecond) - timeModulus);
                if (timeModulus < 2 || timeModulusDiff < 2)
                {
                    bool quality = fields[3] == "29";

                    if ((quality || downloadStat) && double.TryParse(fields[2], NumberStyles.Float, CultureInfo.InvariantCulture, out double value))
                    {
                        oneValid |= quality;

                        if (!hasData)
                            hasData = true;
                        double time = TimeUtils.OaDate(measureTime);
                        Channel key = measurements[fields[0]];
                        series[key].Add(time, value);

                        if (!quality && downloadStat)
                            series[Channel.MISSING].Add(time, 2);
                    }
                }

                rowStart = rowSize + 3;
            }

            if (!hasData)
                throw new InvalidQueryException(InvalidQueryException.EMPTY);
            if (!oneValid)
                throw new InvalidQueryException(InvalidQueryException.NO_VALID);

            return series;
        }

    }
}
