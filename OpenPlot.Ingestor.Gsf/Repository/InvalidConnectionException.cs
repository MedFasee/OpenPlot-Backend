using System;
using System.Collections.Generic;
using System.Text;

namespace OpenPlot.Ingestor.Gsf.Repository
{
    public class InvalidConnectionException : Exception
    {
        public InvalidConnectionException(string ip) : base(string.Format("The supplied connection is invalid: {0}", ip)) { }
        public InvalidConnectionException(string ip, string db) : base(string.Format("The supplied connection is invalid: {0}, database: {1}", ip, db)) { }

        public InvalidConnectionException()
        {
        }

        public InvalidConnectionException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
