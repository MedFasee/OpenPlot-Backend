namespace Data.Sql
{
    public static class PdcSql
    {
        public const string ListPdcNames = @"SELECT name FROM OpenPlot.pdc ORDER BY name;";
    }
}
