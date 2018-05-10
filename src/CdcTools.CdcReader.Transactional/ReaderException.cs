using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.CdcReader.Transactional
{
    public class ReaderException : Exception
    {
        public ReaderException(string message, Exception ex, byte[] currentLsn)
            : base(message, ex)
        {
            CurrentLsn = currentLsn;
        }

        public byte[] CurrentLsn { get; set; }
    }
}
