using System;
using System.Collections.Generic;
using System.Text;

namespace OpenPlot.Ingestor.Gsf.Repository
{
    public class QueryTimeoutException : Exception
    {
        public QueryTimeoutException() : base("Query timed out") { }

        public QueryTimeoutException(string message) : base(message)
        {
        }

        public QueryTimeoutException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
