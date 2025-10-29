namespace Data.Sql
{
    public static class PdcSql
    {
        public const string ListPdcNames = @"SELECT name FROM medi.pdc ORDER BY name;";
    }
}
