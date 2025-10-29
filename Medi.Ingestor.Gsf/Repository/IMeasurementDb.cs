using Medi.Ingestor.Gsf.Data;
using System;
using System.Collections.Generic;


namespace Medi.Ingestor.Gsf.Repository
{
    public interface IMeasurementDb
    {

        Dictionary<Channel, ITimeSeries> QueryTerminalSeries(string Id, DateTime start, DateTime finish, List<Channel> Measurements, int dataRate, int equipmentRate, bool downloadStat);

    }
}
