using System.Data;
using Npgsql;

public interface IDbConnectionFactory
{
    IDbConnection Create();
}

public sealed class NpgsqlConnectionFactory : IDbConnectionFactory
{
    private readonly string _cs;
    public NpgsqlConnectionFactory(string cs) => _cs = cs;
    public IDbConnection Create() => new NpgsqlConnection(_cs);
}
