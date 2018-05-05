using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.CdcReader.Changes
{
    public class FullLoadRecord
    {
        public FullLoadRecord()
        {
            Data = new Dictionary<string, object>();
        }

        public string TableName { get; set; }
        public string ChangeKey { get; set; }
        public Dictionary<string, object> Data { get; set; }
        public int BatchSeqNo { get; set; }
    }
}
