using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Npgsql;

namespace OpenPlot.Ingestor.Gsf
{
    public sealed class Terminal
    {
        public string Id { get; }
        public int IdNumber { get; }
        public string DisplayName { get; }
        public int EquipmentRate { get; }
        public double VoltageLevel { get; } // kV
        public string Area { get; }
        public string State { get; }
        public string Station { get; }

        public Terminal(string id, int idNumber, string displayName, int equipmentRate,
                        double voltageLevel, string area, string state, string station)
        {
            Id = id;
            IdNumber = idNumber;
            DisplayName = displayName;
            EquipmentRate = equipmentRate;
            VoltageLevel = voltageLevel;
            Area = area;
            State = state;
            Station = station;
        }
    }

    public sealed class SystemData
    {
        public int NominalFrequency { get; }
        public string Ip { get; }
        public int Port { get; }
        public string Name { get; }
        public DatabaseType Type { get; }
        public string User { get; }
        public string Password { get; }
        public string Database { get; }
        public List<Terminal> Terminals { get; internal set; }

        public SystemData(string ip, int port, string name, int nominalFrequency, DatabaseType type,
                          string user, string pass, string db)
        {
            Ip = ip;
            Port = port;
            Name = name;
            NominalFrequency = nominalFrequency;
            Type = type;
            User = user;
            Password = pass;
            Database = db;
            Terminals = new List<Terminal>();
        }
    }

    public static class DbSystemDataFactory
    {
        // cache simples por pdc_id com TTL
        private static readonly ConcurrentDictionary<int, (SystemData data, DateTime expires)>
            _cache = new ConcurrentDictionary<int, (SystemData data, DateTime expires)>();

        // --------- API pública: monta SystemData a partir do nome do PDC ---------

        public static SystemData BuildByPdcName(string connString, string pdcName, TimeSpan? ttl = null)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();

                const string sql = @"
                    SELECT pdc_id
                      FROM openplot.pdc
                     WHERE LOWER(name) = LOWER(@name);";

                int? pdcId = null;
                using (var cmd = new NpgsqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("name", pdcName);
                    var obj = cmd.ExecuteScalar();
                    if (obj != null && obj != DBNull.Value)
                        pdcId = Convert.ToInt32(obj);
                }

                if (!pdcId.HasValue)
                    throw new InvalidOperationException($"PDC '{pdcName}' não encontrado.");

                return BuildByPdcId(conn, pdcId.Value, ttl);
            }
        }

        // --------- Implementação interna por pdc_id ---------

        private static SystemData BuildByPdcId(NpgsqlConnection conn, int pdcId, TimeSpan? ttl)
        {
            // cache em memória
            if (_cache.TryGetValue(pdcId, out var entry) && entry.expires > DateTime.UtcNow)
                return entry.data;

            // 1) PDC
            const string sqlPdc = @"
    SELECT pdc_id, name, kind, fps, address, user_name, password, db_name
    FROM openplot.pdc
    WHERE pdc_id = @pdc_id;";

            string name = null;
            string kind = null;
            int fps = 0;
            string addr = null;
            string user = null;
            string pass = null;
            string dbName = "";   // 👈 NOVO

            using (var cmd = new NpgsqlCommand(sqlPdc, conn))
            {
                cmd.Parameters.AddWithValue("pdc_id", pdcId);
                using (var r = cmd.ExecuteReader())
                {
                    if (!r.Read())
                        throw new InvalidOperationException($"PDC id={pdcId} não encontrado.");

                    name = r.GetString(r.GetOrdinal("name"));
                    kind = r.GetString(r.GetOrdinal("kind"));
                    fps = r.GetInt32(r.GetOrdinal("fps"));
                    addr = r.IsDBNull(r.GetOrdinal("address")) ? null : r.GetString(r.GetOrdinal("address"));
                    user = r.IsDBNull(r.GetOrdinal("user_name")) ? "" : r.GetString(r.GetOrdinal("user_name"));
                    pass = r.IsDBNull(r.GetOrdinal("password")) ? "" : r.GetString(r.GetOrdinal("password"));

                    // pega o db_name que o XmlImporter gravou (ex: "db_smf")
                    if (!r.IsDBNull(r.GetOrdinal("db_name")))
                        dbName = r.GetString(r.GetOrdinal("db_name"));
                }
            }


            // 2) parse host:port
            string host;
            int port;
            var parts = addr?.Split(':') ?? new string[0];

            if (parts.Length >= 2 && int.TryParse(parts[parts.Length - 1], out var parsedPort))
            {
                host = string.Join(":", parts.Take(parts.Length - 1));
                port = parsedPort;
            }
            else
            {
                host = addr ?? "127.0.0.1";
                port = string.Equals(kind, "openpdc", StringComparison.OrdinalIgnoreCase) ? 6152 : 3306;
            }

            var type = MapKindToType(kind);

            var sys = new SystemData(
                ip: host,
                port: port,
                name: name,
                nominalFrequency: fps,  // usa fps como base
                type: type,
                user: user,
                pass: pass,
                db: dbName           // AQUI entra o db_name
            );

            // 3) Terminais
            const string sqlTerminals = @"
                SELECT pmu.pmu_id          AS pmu_id,
                       pp.local_numeric_id AS local_numeric_id,
                       pmu.id_name         AS id,
                       COALESCE(pmu.full_name, pmu.id_name) AS display_name,
                       pmu.volt_level      AS volt_kV,
                       pmu.area,
                       pmu.state,
                       pmu.station
                  FROM openplot.pdc_pmu pp
                  JOIN openplot.pmu pmu ON pmu.pmu_id = pp.pmu_id
                 WHERE pp.pdc_id = @pdc_id
              ORDER BY pmu.area   NULLS LAST,
                       pmu.state  NULLS LAST,
                       pmu.station NULLS LAST,
                       pmu.volt_level NULLS LAST,
                       pmu.id_name;";


            using (var cmd = new NpgsqlCommand(sqlTerminals, conn))
            {
                cmd.Parameters.AddWithValue("pdc_id", pdcId);
                using (var r = cmd.ExecuteReader())
                {
                    while (r.Read())
                    {
                        var pmuId = r.GetInt32(r.GetOrdinal("pmu_id"));
                        var id = r.GetString(r.GetOrdinal("id"));
                        var display = r.GetString(r.GetOrdinal("display_name"));
                        var area = r.IsDBNull(r.GetOrdinal("area")) ? "" : r.GetString(r.GetOrdinal("area"));
                        var state = r.IsDBNull(r.GetOrdinal("state")) ? "" : r.GetString(r.GetOrdinal("state"));
                        var station = r.IsDBNull(r.GetOrdinal("station")) ? "" : r.GetString(r.GetOrdinal("station"));

                        double kv = r.IsDBNull(r.GetOrdinal("volt_kV"))
                            ? 0.0
                            : Convert.ToDouble(r.GetValue(r.GetOrdinal("volt_kV")));

                        
                        int idNumber;
                        var localNumOrdinal = r.GetOrdinal("local_numeric_id");
                        if (MapKindToType(kind) == DatabaseType.Medfasee &&
                            !r.IsDBNull(localNumOrdinal))
                        {
                            // Para MedFasee, IdNumber = idNumber do XML (idcodepmu)
                            idNumber = r.GetInt32(localNumOrdinal);
                        }
                        else
                        {
                            // Para outras fontes, IdNumber pode ser o próprio pmu_id
                            idNumber = pmuId;
                        }

                        int equipmentRate = fps;

                        sys.Terminals.Add(new Terminal(
                            id: id,
                            idNumber: idNumber,
                            displayName: display,
                            equipmentRate: equipmentRate,
                            voltageLevel: kv,
                            area: area,
                            state: state,
                            station: station
                        ));
                    }
                }
            }


            // ordena igual ao XML antigo
            sys.Terminals = sys.Terminals
                .OrderBy(p => p.Area)
                .ThenBy(p => p.State)
                .ThenBy(p => p.Station)
                .ThenBy(p => p.VoltageLevel)
                .ThenBy(p => p.Id)
                .ToList();

            // coloca no cache
            var expiry = DateTime.UtcNow + (ttl ?? TimeSpan.FromMinutes(15));
            _cache[pdcId] = (sys, expiry);

            return sys;
        }

        private static DatabaseType MapKindToType(string kind)
        {
            if (string.Equals(kind, "medfasee", StringComparison.OrdinalIgnoreCase))
                return DatabaseType.Medfasee;
            if (string.Equals(kind, "openpdc", StringComparison.OrdinalIgnoreCase))
                return DatabaseType.Historian_OpenPDC;
            if (string.Equals(kind, "openhistorian2", StringComparison.OrdinalIgnoreCase))
                return DatabaseType.Historian2_OpenHistorian2;

            return DatabaseType.Historian_OpenPDC;
        }

        public static void Invalidate(int pdcId)
        {
            (SystemData, DateTime) _;
            _cache.TryRemove(pdcId, out _);
        }
    }
}
