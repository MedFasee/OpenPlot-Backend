namespace Data.Sql
{
    public static class PdcSql
    {
        public const string ListPdcNames = @"SELECT name, fps FROM OpenPlot.pdc WHERE active = true ORDER BY name;";
    }
}
