using CdcTools.CdcReader.Tables;
using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.CdcReader.Changes
{
    public class FullLoadBatch
    {
        public FullLoadBatch()
        {
            Records = new List<FullLoadRecord>();
        }

        public TableSchema TableSchema { get; set; }
        public PrimaryKeyValue FirstRowKey { get; set; }
        public PrimaryKeyValue LastRowKey { get; set; }
        public List<FullLoadRecord> Records { get; set; }
    }
}
