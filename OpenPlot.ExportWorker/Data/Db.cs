using System.Data;

namespace OpenPlot.ExportWorker.Data;

public sealed class Db
{
    public IDbConnection Conn { get; }
    public Db(IDbConnection conn) => Conn = conn;
}