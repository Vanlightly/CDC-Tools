using System;
using System.Collections.Generic;
using System.Numerics;
using System.Text;

namespace CdcTools.Redshift.Changes
{
    public class RowChange
    {
        public RowChange()
        {
            Data = new Dictionary<string, object>();
        }

        public string Lsn { get; set; }
        public string SeqVal { get; set; }
        public string ChangeKey { get; set; }
        public ChangeType ChangeType { get; set; }
        public Dictionary<string, object> Data { get; set; }

        private BigInteger _lsn;
        public BigInteger LsnInteger
        {
            get
            {
                if (_lsn == 0)
                {
                    if (Lsn != null)
                        _lsn = BigInteger.Parse(Lsn);
                }

                return _lsn;
            }
        }

        private BigInteger _seqVal;
        public BigInteger SeqValInteger
        {
            get
            {
                if (_seqVal == 0)
                {
                    if (SeqVal != null)
                        _seqVal = BigInteger.Parse(SeqVal);
                }

                return _seqVal;
            }
        }
    }
}
