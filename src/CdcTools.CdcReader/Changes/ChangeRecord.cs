using System;
using System.Collections.Generic;
using System.Numerics;
using System.Text;

namespace CdcTools.CdcReader.Changes
{
    public class ChangeRecord
    {
        public ChangeRecord()
        {
            Data = new Dictionary<string, object>();
        }

        public string TableName { get; set; }
        public byte[] Lsn { get; set; }
        public string LsnStr { get; set; }

        public byte[] SeqVal { get; set; }
        public string SeqValStr { get; set; }
        public string ChangeKey { get; set; }
        public ChangeType ChangeType { get; set; }
        public Dictionary<string, object> Data { get; set; }

        private BigInteger _lsn;
        public BigInteger LsnInt
        {
            get
            {
                if (_lsn == 0)
                    _lsn = BigInteger.Parse(LsnStr);

                return _lsn;
            }
        }

        private BigInteger _seqVal;
        public BigInteger SeqValInt
        {
            get
            {
                if (_seqVal == 0)
                    _seqVal = BigInteger.Parse(SeqValStr);

                return _seqVal;
            }
        }
    }
}
