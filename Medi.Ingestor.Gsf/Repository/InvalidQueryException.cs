using System;
using System.Collections.Generic;
using System.Text;

namespace Medi.Ingestor.Gsf.Repository
{
    public class InvalidQueryException : Exception
    {
        public const string NO_TABLE = "NO_TABLE";
        public const string BAD_HIST_QUERY = "BAD_HIST_QUERY";
        public const string NO_VALID = "NO_VALID";
        public const string EMPTY = "EMPTY";

        public InvalidQueryException()
        {
        }
        public InvalidQueryException(string message) : base(message)
        {
        }

        public InvalidQueryException(string message, Exception innerException) : base(message, innerException)
        {
        }

    }
}
