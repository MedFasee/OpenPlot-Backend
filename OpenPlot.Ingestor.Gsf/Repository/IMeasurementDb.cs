using OpenPlot.Ingestor.Gsf.Data;
using System;
using System.Collections.Generic;


namespace OpenPlot.Ingestor.Gsf.Repository
{
    public interface IMeasurementDb
    {

        Dictionary<Channel, ITimeSeries> QueryTerminalSeries(string Id, DateTime start, DateTime finish, List<Channel> Measurements, int dataRate, int equipmentRate, bool downloadStat);

    }
}
