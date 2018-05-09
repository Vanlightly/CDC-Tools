using CdcTools.CdcReader.Changes;
using System;
using System.Collections.Generic;
using System.Text;

namespace CdcTools.CdcReader.Transactional
{
    public class TransactionBatch
    {
        public TransactionBatch()
        {
            Changes = new List<ChangeRecord>();
        }

        public TransactionId Id { get; set; }
        public List<ChangeRecord> Changes { get; set; }
        public int Part { get; set; }
        public bool IsMultiPart { get; set; }
    }
}
