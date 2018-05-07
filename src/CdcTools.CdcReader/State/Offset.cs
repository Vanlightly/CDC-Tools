using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.CdcReader.State
{
    public class Offset
    {
        public byte[] Lsn { get; set; }
        public byte[] SeqVal { get; set; }
        public bool UnfinishedLsn { get; set; }
    }
}
