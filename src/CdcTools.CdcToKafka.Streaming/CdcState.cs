using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.CdcToKafka.Streaming
{
    public class CdcState
    {
        public byte[] ToLsn { get; set; }
        public byte[] FromSeqVal { get; set; }
        public byte[] FromLsn { get; set; }
        public bool UnfinishedLsn { get; set; }
    }
}
