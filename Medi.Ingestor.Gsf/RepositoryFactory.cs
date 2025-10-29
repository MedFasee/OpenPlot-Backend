
using Medi.Ingestor.Gsf.Repository;

namespace Medi.Ingestor.Gsf
{
    internal static class RepositoryFactory
    {
        /// <summary>
        /// Escolhe e instancia o repositório correto (MeasurementMedFasee / MeasurementHistorian / MeasurementHistorian2)
        /// conforme o tipo definido no XML (SystemData.Type).
        /// </summary>
        public static IMeasurementDb Create(SystemData sys)
        {
            switch (sys.Type)
            {
                case DatabaseType.Medfasee:
                    return new MeasurementMedFasee(sys.Ip, sys.Port, sys.User, sys.Password, sys.Database);

                case DatabaseType.Historian_OpenPDC:
                    return new MeasurementHistorian(sys.Ip, sys.Port, sys.User, sys.Password);

                case DatabaseType.Historian2_OpenHistorian2:
                    return new MeasurementHistorian2(sys.Ip, sys.User, sys.Password);

                default:
                    throw new System.NotSupportedException(sys.Type.ToString());
            }
        }
    }
}
