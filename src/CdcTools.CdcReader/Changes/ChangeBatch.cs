using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.CdcReader.Changes
{
    public class ChangeBatch
    {
        public ChangeBatch()
        {
            Changes = new List<ChangeRecord>();
        }

        public List<ChangeRecord> Changes { get; set; }
        public bool MoreChanges { get; set; }
        public bool MoreOfLastTransaction { get; set; }
        public byte[] FromLsn { get; set; }
        public byte[] FromSeqVal { get; set; }
        public byte[] ToLsn { get; set; }
        public byte[] ToSeqVal { get; set; }
    }
}
